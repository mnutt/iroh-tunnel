#![allow(refining_impl_trait)]

include!("sandstorm_capnp.rs");

mod app;
mod backend;
mod untyped_local;
#[allow(dead_code)]
mod quinn_adapter;
#[allow(dead_code)]
mod raw_udp_capnp;
#[allow(dead_code)]
mod sandstorm_custom_transport;
mod storage;
#[cfg(test)]
mod test_support;

use base64::Engine as _;
use std::collections::{HashMap, HashSet};
use std::os::fd::FromRawFd;
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use capnp::capability::{Promise, Rc};
use capnp::text;
use capnp::traits::HasTypeId;
use capnp_rpc::{RpcSystem, new_client, pry, rpc_twoparty_capnp, twoparty};
use futures::AsyncReadExt;
use futures::TryFutureExt;
use iroh::{Endpoint, RelayMode, SecretKey, TransportAddr, endpoint::presets};
use iroh_base::CustomAddr;
use serde_json::json;
use tokio::runtime::Builder;
use tokio::sync::Notify;
use tokio::task::LocalSet;
use tokio::time::{Duration, timeout};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::backend::SandstormBackend;
use crate::app::App;
use crate::raw_udp_capnp::{get_local_endpoint, new_capnp_raw_udp_custom_transport};
use crate::sandstorm_custom_transport::{
    socket_addr_to_custom_addr, SANDSTORM_RAW_UDP_TRANSPORT_ID,
};
use crate::storage::{
    LocalProxyCapabilityRecord, LocalProxyTargetKind, PersistedReceivedCapabilityRecord,
    ReceivedCapabilityKind, RegisteredRemoteCapabilityDurability,
    RegisteredRemoteCapabilityRecord, SavedCapabilityRecord, SharedCapabilityKind,
    SharedCapabilityRecord, Storage,
};

const CLIENT_ROOT: &str = "/opt/app/client";
const STATE_DIR: &str = "/var/iroh-tunnel";
const WEB_SESSION_TYPE_ID: u64 = web_session_capnp::web_session::Client::TYPE_ID;
const IROH_ALPN: &[u8] = b"dev.iroh-tunnel.capnp/1";
const IROH_RPC_ALPN: &[u8] = b"dev.iroh-tunnel.capnp/rpc/1";
const IROH_PAIR_ALPN: &[u8] = b"dev.iroh-tunnel.capnp/pair/1";
const PAIRING_PROTOCOL_VERSION: u16 = 1;
const IROH_TRANSPORT_ASSESSMENT: &str = "Saved IpNetwork is proven for outbound TCP and UDP. Native iroh 0.97.0 now exposes custom transports behind unstable-custom-transports, and this prototype has both a proxy-based Quinn RawUdp adapter and a native iroh CustomTransport scaffold for Sandstorm RawUdp. The remaining work is application plumbing: restore an IpInterface capability early enough to bind RawUdp, publish custom transport addresses in tickets, and decide how Sandstorm mode is configured and discovered.";
const IROH_SANDSTORM_RAW_UDP_INTERFACE_TOKEN_ENV: &str = "IROH_SANDSTORM_RAW_UDP_INTERFACE_TOKEN";
const IROH_SANDSTORM_RAW_UDP_PORT_ENV: &str = "IROH_SANDSTORM_RAW_UDP_PORT";

fn app_storage() -> Storage {
    Storage::new(STATE_DIR)
}

fn app_core(app_state: &Arc<Mutex<AppState>>) -> App {
    let storage = app_state
        .lock()
        .map(|guard| guard.storage.clone())
        .unwrap_or_else(|_| app_storage());
    App::new(app_state.clone(), storage)
}

fn exported_ip_network_id_path() -> std::path::PathBuf {
    app_storage().exported_ip_network_id_path()
}

fn exported_api_session_id_path() -> std::path::PathBuf {
    app_storage().exported_api_session_id_path()
}

fn raw_udp_interface_token_path() -> std::path::PathBuf {
    app_storage().raw_udp_interface_token_path()
}

fn raw_udp_port_path() -> std::path::PathBuf {
    app_storage().raw_udp_port_path()
}

fn iroh_secret_key_path() -> std::path::PathBuf {
    app_storage().iroh_secret_key_path()
}

fn remote_ticket_path() -> std::path::PathBuf {
    app_storage().remote_ticket_path()
}

fn approved_peer_node_id_path() -> std::path::PathBuf {
    app_storage().approved_peer_node_id_path()
}

fn tunnel_enabled_path() -> std::path::PathBuf {
    app_storage().tunnel_enabled_path()
}

fn main() {
    if let Err(err) = run() {
        eprintln!("fatal error: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let runtime = Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .map_err(|err| format!("failed to create tokio runtime: {err}"))?;
    let local_set = LocalSet::new();

    runtime.block_on(local_set.run_until(async {
        let rpc_fd = 3;

        let stream: std::os::unix::net::UnixStream =
            unsafe { std::os::unix::net::UnixStream::from_raw_fd(rpc_fd) };
        stream
            .set_nonblocking(true)
            .map_err(|err| format!("failed to set fd {rpc_fd} nonblocking: {err}"))?;

        let stream = tokio::net::UnixStream::from_std(stream)
            .map_err(|err| format!("failed to adopt fd {rpc_fd} as tokio unix stream: {err}"))?;
        let (read_half, write_half) = stream.compat().split();

        let network = Box::new(twoparty::VatNetwork::new(
            read_half,
            write_half,
            rpc_twoparty_capnp::Side::Client,
            Default::default(),
        ));

        let (tx, rx) = futures::channel::oneshot::channel();
        let sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned> =
            capnp_rpc::new_future_client(rx.map_err(|_| {
                capnp::Error::failed("sandstorm api bootstrap channel was canceled".to_string())
            }));
        let app_state = Arc::new(Mutex::new(AppState::initialize()?));

        let client: grain_capnp::main_view::Client<text::Owned> =
            new_client(UiViewImpl::new(sandstorm_api.clone(), app_state.clone()));

        let mut rpc_system = RpcSystem::new(network, Some(client.client));
        let remote_api = rpc_system
            .bootstrap::<grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>>(
                rpc_twoparty_capnp::Side::Server,
            );
        let _ = tx.send(remote_api);
        tokio::task::spawn_local({
            let app_state = app_state.clone();
            let sandstorm_api = sandstorm_api.clone();
            async move {
                eprintln!("iroh startup: beginning background endpoint initialization");
                if let Err(err) = initialize_iroh_endpoint(app_state, sandstorm_api).await {
                    eprintln!("iroh startup: endpoint initialization failed: {err}");
                } else {
                    eprintln!("iroh startup: endpoint initialization finished");
                }
            }
        });

        rpc_system
            .await
            .map_err(|err| format!("rpc system failed: {err}"))
    }))
}

struct UiViewImpl {
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    app_state: Arc<Mutex<AppState>>,
}

impl UiViewImpl {
    fn new(
        sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
        app_state: Arc<Mutex<AppState>>,
    ) -> Self {
        Self {
            sandstorm_api,
            app_state,
        }
    }
}

impl grain_capnp::ui_view::Server for UiViewImpl {
    fn get_view_info(
        self: Rc<Self>,
        _: grain_capnp::ui_view::GetViewInfoParams,
        mut results: grain_capnp::ui_view::GetViewInfoResults,
    ) -> Promise<(), capnp::Error> {
        let match_request_descriptors =
            collect_match_request_descriptors(&self.app_state).unwrap_or_else(|err| {
                eprintln!("failed to collect powerbox match requests: {err}");
                Vec::new()
            });
        let mut view_info = results.get();
        init_localized_text(view_info.reborrow().init_app_title(), "Iroh Tunnel");

        let mut match_requests =
            view_info
                .reborrow()
                .init_match_requests(match_request_descriptors.len() as u32);
        for (index, encoded) in match_request_descriptors.iter().enumerate() {
            let Ok(message) = decode_powerbox_descriptor(encoded) else {
                continue;
            };
            let Ok(descriptor) = message.get_root::<powerbox_capnp::powerbox_descriptor::Reader<'_>>()
            else {
                continue;
            };
            if let Err(err) = copy_powerbox_descriptor(
                descriptor,
                match_requests.reborrow().get(index as u32),
            ) {
                eprintln!("failed to copy powerbox match request descriptor: {err}");
            }
        }

        let mut permissions = view_info.reborrow().init_permissions(2);
        {
            let mut permission = permissions.reborrow().get(0);
            permission.set_name("manageTunnel");
            init_localized_text(permission.reborrow().init_title(), "manage tunnel");
            init_localized_text(
                permission.init_description(),
                "Can pair peers and manage shared capabilities.",
            );
        }
        {
            let mut permission = permissions.get(1);
            permission.set_name("useReceivedCaps");
            init_localized_text(
                permission.reborrow().init_title(),
                "use received capabilities",
            );
            init_localized_text(
                permission.init_description(),
                "Can use capabilities received from the remote peer.",
            );
        }

        let mut roles = view_info.init_roles(2);
        {
            let mut role = roles.reborrow().get(0);
            init_localized_text(role.reborrow().init_title(), "manager");
            init_localized_text(role.reborrow().init_verb_phrase(), "can manage the tunnel");
            let mut perms = role.init_permissions(2);
            perms.set(0, true);
            perms.set(1, true);
        }
        {
            let mut role = roles.get(1);
            init_localized_text(role.reborrow().init_title(), "user");
            init_localized_text(
                role.reborrow().init_verb_phrase(),
                "can use received capabilities",
            );
            let mut perms = role.init_permissions(2);
            perms.set(0, false);
            perms.set(1, true);
        }

        Promise::ok(())
    }

    fn new_session(
        self: Rc<Self>,
        params: grain_capnp::ui_view::NewSessionParams,
        mut results: grain_capnp::ui_view::NewSessionResults,
    ) -> Promise<(), capnp::Error> {
        let params = pry!(params.get());
        let session_type = params.get_session_type();

        if session_type != WEB_SESSION_TYPE_ID {
            return Promise::err(capnp::Error::failed(format!(
                "unsupported session type 0x{session_type:016x}"
            )));
        }

        let _session_params = pry!(
            params
                .get_session_params()
                .get_as::<web_session_capnp::web_session::params::Reader<'_>>()
        );
        let user_info = pry!(params.get_user_info());
        let permissions = pry!(user_info.get_permissions());
        let can_manage = permissions.len() > 0 && permissions.get(0);
        let session_client: web_session_capnp::web_session::Client = new_client(WebSessionImpl {
            can_manage,
            sandstorm_api: self.sandstorm_api.clone(),
            session_context: pry!(params.get_context()),
            app_state: self.app_state.clone(),
            request_descriptor_b64: None,
        });
        results.get().set_session(grain_capnp::ui_session::Client {
            client: session_client.client,
        });
        Promise::ok(())
    }

    fn new_request_session(
        self: Rc<Self>,
        params: grain_capnp::ui_view::NewRequestSessionParams,
        mut results: grain_capnp::ui_view::NewRequestSessionResults,
    ) -> Promise<(), capnp::Error> {
        let params = pry!(params.get());
        let session_type = params.get_session_type();

        if session_type != WEB_SESSION_TYPE_ID {
            return Promise::err(capnp::Error::failed(format!(
                "unsupported request session type 0x{session_type:016x}"
            )));
        }

        let _session_params = pry!(
            params
                .get_session_params()
                .get_as::<web_session_capnp::web_session::params::Reader<'_>>()
        );
        let user_info = pry!(params.get_user_info());
        let permissions = pry!(user_info.get_permissions());
        let can_manage = permissions.len() > 0 && permissions.get(0);
        let request_descriptor_b64 = match pry!(params.get_request_info())
            .iter()
            .next()
            .map(encode_powerbox_descriptor)
            .transpose()
        {
            Ok(value) => value,
            Err(err) => return Promise::err(err),
        };

        let session_client: web_session_capnp::web_session::Client = new_client(WebSessionImpl {
            can_manage,
            sandstorm_api: self.sandstorm_api.clone(),
            session_context: pry!(params.get_context()),
            app_state: self.app_state.clone(),
            request_descriptor_b64,
        });
        results.get().set_session(grain_capnp::ui_session::Client {
            client: session_client.client,
        });
        Promise::ok(())
    }
}

impl grain_capnp::main_view::Server<text::Owned> for UiViewImpl {
    fn restore(
        self: Rc<Self>,
        params: grain_capnp::main_view::RestoreParams<text::Owned>,
        mut results: grain_capnp::main_view::RestoreResults<text::Owned>,
    ) -> Promise<(), capnp::Error> {
        let params = pry!(params.get());
        let object_id = pry!(params.get_object_id())
            .to_str()
            .unwrap_or("")
            .to_string();
        let sandstorm_api = self.sandstorm_api.clone();
        let app_state = self.app_state.clone();
        Promise::from_future(async move {
            eprintln!(
                "main_view.restore: object_id={object_id} at_ms={}",
                now_ms()
            );
            let restored_cap =
                restore_app_object_capability(sandstorm_api, &app_state, &object_id)
                    .await
                    .map_err(|err| {
                        eprintln!(
                            "main_view.restore: object_id={object_id} failed at_ms={} err={err}",
                            now_ms()
                        );
                        capnp::Error::failed(err)
                    })?;
            eprintln!(
                "main_view.restore: object_id={object_id} ok at_ms={}",
                now_ms()
            );
            results
                .get()
                .get_cap()
                .set_as_capability(restored_cap.hook);
            Ok(())
        })
    }

    fn drop(
        self: Rc<Self>,
        params: grain_capnp::main_view::DropParams<text::Owned>,
        _: grain_capnp::main_view::DropResults<text::Owned>,
    ) -> Promise<(), capnp::Error> {
        let params = pry!(params.get());
        let object_id = pry!(params.get_object_id())
            .to_str()
            .unwrap_or("")
            .to_string();
        let app_state = self.app_state.clone();
        Promise::from_future(async move {
            eprintln!("main_view.drop: object_id={object_id} at_ms={}", now_ms());
            match drop_received_remote_capability(&app_state, &object_id) {
                Ok(true) => {
                    eprintln!("main_view.drop: object_id={object_id} dropped at_ms={}", now_ms());
                    Ok(())
                }
                Ok(false) => {
                    eprintln!(
                        "main_view.drop: object_id={object_id} no-op at_ms={}",
                        now_ms()
                    );
                    Ok(())
                }
                Err(err) => {
                    eprintln!(
                        "main_view.drop: object_id={object_id} failed at_ms={} err={err}",
                        now_ms()
                    );
                    Err(capnp::Error::failed(err))
                }
            }
        })
    }
}

struct WebSessionImpl {
    can_manage: bool,
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    session_context: grain_capnp::session_context::Client,
    app_state: Arc<Mutex<AppState>>,
    request_descriptor_b64: Option<String>,
}

impl grain_capnp::ui_session::Server for WebSessionImpl {}

impl web_session_capnp::web_session::Server for WebSessionImpl {
    fn get(
        self: Rc<Self>,
        params: web_session_capnp::web_session::GetParams,
        mut results: web_session_capnp::web_session::GetResults,
    ) -> Promise<(), capnp::Error> {
        let params = pry!(params.get());
        let path = pry!(params.get_path()).to_str().unwrap_or("").to_string();

        if let Err(err) = self.require_canonical_path(&path) {
            return Promise::err(err);
        }

        if path == ".can-write" {
            let mut response = results.get().init_content();
            response.set_mime_type("text/plain");
            response
                .init_body()
                .set_bytes(format!("{}", self.can_manage).as_bytes());
            return Promise::ok(());
        }

        if path == "api/state" {
            let app_state = self.app_state.clone();
            let request_descriptor_active = self.request_descriptor_b64.is_some();
            return Promise::from_future(async move {
                if let Err(err) = refresh_peer_rpc_exports(&app_state).await {
                    if err != "peer rpc session is not connected" {
                        eprintln!("state refresh of peer exports failed: {err}");
                    }
                }

                let mut body: serde_json::Value = match render_state_json(&app_state)
                    .and_then(|body| {
                        serde_json::from_str(&body)
                            .map_err(|err| format!("failed to parse rendered state json: {err}"))
                    }) {
                    Ok(body) => body,
                    Err(err) => {
                        let mut error = results.get().init_server_error();
                        error.set_description_html(
                            format!(
                                "<!doctype html><title>State Render Failed</title><pre>{}</pre>",
                                escape_html(&err)
                            )
                            .as_str(),
                        );
                        return Ok(());
                    }
                };
                body["powerboxRequestSession"] = serde_json::json!({
                    "active": request_descriptor_active,
                    "localProvideCaps": if request_descriptor_active {
                        serde_json::json!([
                            {
                                "objectId": crate::app::LOCAL_TEST_API_SESSION_OBJECT_ID,
                                "label": crate::app::LOCAL_TEST_API_SESSION_LABEL,
                                "kind": "ApiSession",
                                "typeTag": "sandstorm/api-session",
                                "source": "local-test"
                            }
                        ])
                    } else {
                        serde_json::json!([])
                    },
                });
                let body = match serde_json::to_string(&body) {
                    Ok(body) => body,
                    Err(err) => {
                        let mut error = results.get().init_server_error();
                        error.set_description_html(
                            format!(
                                "<!doctype html><title>State Serialize Failed</title><pre>{}</pre>",
                                escape_html(&err.to_string())
                            )
                            .as_str(),
                        );
                        return Ok(());
                    }
                };
                let mut response = results.get().init_content();
                response.set_status_code(web_session_capnp::web_session::response::SuccessCode::Ok);
                response.set_mime_type("application/json");
                response.init_body().set_bytes(body.as_bytes());
                Ok(())
            });
        }

        if path.is_empty() || path.ends_with('/') {
            let filename = format!("{CLIENT_ROOT}/{}index.html", path);
            return match self.read_file(&filename, results, "text/html; charset=UTF-8") {
                Ok(()) => Promise::ok(()),
                Err(err) => Promise::err(err),
            };
        }

        let filename = format!("{CLIENT_ROOT}/{path}");
        if let Ok(true) = std::fs::metadata(&filename).map(|metadata| metadata.is_dir()) {
            let mut redirect = results.get().init_redirect();
            let location = format!("{path}/");
            redirect.set_is_permanent(true);
            redirect.set_switch_to_get(true);
            redirect.set_location(location.as_str());
            return Promise::ok(());
        }

        match self.read_file(&filename, results, self.infer_content_type(&path)) {
            Ok(()) => Promise::ok(()),
            Err(err) => Promise::err(err),
        }
    }

    fn post(
        self: Rc<Self>,
        params: web_session_capnp::web_session::PostParams,
        mut results: web_session_capnp::web_session::PostResults,
    ) -> Promise<(), capnp::Error> {
        let params = pry!(params.get());
        let path = pry!(params.get_path()).to_str().unwrap_or("").to_string();
        if let Err(err) = self.require_canonical_path(&path) {
            return Promise::err(err);
        }

        if path == "api/pairing/remote-ticket" {
            if !self.can_manage {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::Forbidden,
                );
                return Promise::ok(());
            }

            let body = pry!(params.get_content())
                .get_content()
                .unwrap_or(&[])
                .to_vec();
            let remote_ticket = match std::str::from_utf8(&body) {
                Ok(value) => value.trim().to_string(),
                Err(err) => {
                    let mut error = results.get().init_client_error();
                    let description =
                        format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                    );
                    error.set_description_html(description.as_str());
                    return Promise::ok(());
                }
            };

            let outcome = update_remote_ticket(&self.app_state, remote_ticket);
            match outcome {
                Ok(()) => {
                    let mut content = results.get().init_content();
                    content
                        .set_status_code(web_session_capnp::web_session::response::SuccessCode::Ok);
                    content.set_mime_type("application/json");
                    content.init_body().set_bytes(br#"{"ok":true}"#);
                }
                Err(err) => {
                    let mut error = results.get().init_server_error();
                    let description = format!(
                        "<!doctype html><title>Pairing Update Failed</title><pre>{}</pre>",
                        escape_html(&err)
                    );
                    error.set_description_html(description.as_str());
                }
            }
            return Promise::ok(());
        }

        if path == "api/pairing/probe-connect" {
            if !self.can_manage {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::Forbidden,
                );
                return Promise::ok(());
            }

            let app_state = self.app_state.clone();
            return Promise::from_future(async move {
                match probe_remote_connection(app_state).await {
                    Ok(summary) => {
                        let body = format!(
                            "{{\"ok\":true,\"remoteNodeId\":\"{}\",\"response\":\"{}\"}}",
                            json_escape(&summary.remote_node_id),
                            json_escape(&summary.response)
                        );
                        let mut content = results.get().init_content();
                        content.set_status_code(
                            web_session_capnp::web_session::response::SuccessCode::Ok,
                        );
                        content.set_mime_type("application/json");
                        content.init_body().set_bytes(body.as_bytes());
                    }
                    Err(err) => {
                        eprintln!("iroh probe failed: {err}");
                        let mut error = results.get().init_server_error();
                        let description = format!(
                            "<!doctype html><title>Probe Failed</title><pre>{}</pre>",
                            escape_html(&err)
                        );
                        error.set_description_html(description.as_str());
                    }
                }
                Ok(())
            });
        }

        if path == "api/tunnel/connect" {
            if !self.can_manage {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::Forbidden,
                );
                return Promise::ok(());
            }

            let app_state = self.app_state.clone();
            let sandstorm_api = self.sandstorm_api.clone();
            return Promise::from_future(async move {
                tokio::task::spawn_local(async move {
                    if let Err(err) = begin_pair_connection(app_state.clone(), sandstorm_api).await {
                        if let Ok(mut guard) = app_state.lock() {
                            guard.pending_outgoing_peer_node_id = None;
                            guard.peer_rpc_error = Some(err.clone());
                            guard.pairing_status = PairingStatus::Error;
                        }
                    }
                });
                let mut content = results.get().init_content();
                content.set_status_code(
                    web_session_capnp::web_session::response::SuccessCode::Accepted,
                );
                content.set_mime_type("application/json");
                content.init_body().set_bytes(br#"{"ok":true}"#);
                Ok(())
            });
        }

        if path == "api/tunnel/accept" {
            if !self.can_manage {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::Forbidden,
                );
                return Promise::ok(());
            }

            let app_state = self.app_state.clone();
            let sandstorm_api = self.sandstorm_api.clone();
            return Promise::from_future(async move {
                tokio::task::spawn_local(async move {
                    if let Err(err) =
                        accept_pending_pair_connection(app_state.clone(), sandstorm_api).await
                    {
                        if let Ok(mut guard) = app_state.lock() {
                            guard.peer_rpc_error = Some(err.clone());
                            guard.pairing_status = PairingStatus::Error;
                        }
                    }
                });
                let mut content = results.get().init_content();
                content.set_status_code(
                    web_session_capnp::web_session::response::SuccessCode::Accepted,
                );
                content.set_mime_type("application/json");
                content.init_body().set_bytes(br#"{"ok":true}"#);
                Ok(())
            });
        }

        if path == "api/tunnel/enable" {
            if !self.can_manage {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::Forbidden,
                );
                return Promise::ok(());
            }

            let app_state = self.app_state.clone();
            let sandstorm_api = self.sandstorm_api.clone();
            return Promise::from_future(async move {
                tokio::task::spawn_local(async move {
                    if let Err(err) = enable_tunnel(app_state.clone(), sandstorm_api).await {
                        if let Ok(mut guard) = app_state.lock() {
                            guard.peer_rpc_error = Some(err.clone());
                            guard.pairing_status = PairingStatus::Error;
                        }
                    }
                });
                let mut content = results.get().init_content();
                content.set_status_code(
                    web_session_capnp::web_session::response::SuccessCode::Accepted,
                );
                content.set_mime_type("application/json");
                content.init_body().set_bytes(br#"{"ok":true}"#);
                Ok(())
            });
        }

        if path == "api/tunnel/disable" {
            if !self.can_manage {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::Forbidden,
                );
                return Promise::ok(());
            }

            match disable_tunnel(&self.app_state) {
                Ok(()) => {
                    let mut content = results.get().init_content();
                    content
                        .set_status_code(web_session_capnp::web_session::response::SuccessCode::Ok);
                    content.set_mime_type("application/json");
                    content.init_body().set_bytes(br#"{"ok":true}"#);
                }
                Err(err) => {
                    let mut error = results.get().init_server_error();
                    let description = format!(
                        "<!doctype html><title>Disable Tunnel Failed</title><pre>{}</pre>",
                        escape_html(&err)
                    );
                    error.set_description_html(description.as_str());
                }
            }
            return Promise::ok(());
        }

        if path == "api/tunnel/reject" {
            if !self.can_manage {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::Forbidden,
                );
                return Promise::ok(());
            }

            let app_state = self.app_state.clone();
            return Promise::from_future(async move {
                match reject_pending_pair_connection(app_state).await {
                    Ok(()) => {
                        let mut content = results.get().init_content();
                        content.set_status_code(
                            web_session_capnp::web_session::response::SuccessCode::Ok,
                        );
                        content.set_mime_type("application/json");
                        content.init_body().set_bytes(br#"{"ok":true}"#);
                    }
                    Err(err) => {
                        let mut error = results.get().init_server_error();
                        let description = format!(
                            "<!doctype html><title>Reject Failed</title><pre>{}</pre>",
                            escape_html(&err)
                        );
                        error.set_description_html(description.as_str());
                    }
                }
                Ok(())
            });
        }

        if path == "api/endpoint/raw-udp-interface" {
            if !self.can_manage {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::Forbidden,
                );
                return Promise::ok(());
            }

            let body = pry!(params.get_content())
                .get_content()
                .unwrap_or(&[])
                .to_vec();
            let saved_token = match std::str::from_utf8(&body) {
                Ok(value) => value.trim().to_string(),
                Err(err) => {
                    let mut error = results.get().init_client_error();
                    let description =
                        format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                    );
                    error.set_description_html(description.as_str());
                    return Promise::ok(());
                }
            };
            if saved_token.is_empty() {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                );
                error.set_description_html("missing saved token");
                return Promise::ok(());
            }

            let saved_cap = match require_saved_capability_by_token(&saved_token) {
                Ok(saved_cap) => saved_cap,
                Err(err) if err == "saved capability token not found" => {
                    let mut error = results.get().init_client_error();
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::NotFound,
                    );
                    error.set_description_html(err.as_str());
                    return Promise::ok(());
                }
                Err(err) => return Promise::err(capnp::Error::failed(err)),
            };

            let app_state = self.app_state.clone();
            let sandstorm_api = self.sandstorm_api.clone();
            return Promise::from_future(async move {
                let validate_api = sandstorm_api.clone();
                let rebind_api = sandstorm_api.clone();
                let outcome = configure_raw_udp_interface_binding(
                    &saved_cap,
                    |saved_token| {
                        let sandstorm_api = validate_api.clone();
                        async move {
                            restore_saved_ip_interface(sandstorm_api, &saved_token)
                                .await
                                .map(|_| ())
                        }
                    },
                    persist_raw_udp_interface_token,
                    || initialize_iroh_endpoint(app_state, rebind_api),
                )
                .await;

                match outcome {
                    Ok(()) => {
                        let mut content = results.get().init_content();
                        content.set_status_code(
                            web_session_capnp::web_session::response::SuccessCode::Ok,
                        );
                        content.set_mime_type("application/json");
                        content.init_body().set_bytes(br#"{"ok":true}"#);
                    }
                    Err(err) => {
                        let mut error = results.get().init_server_error();
                        let description = format!(
                            "<!doctype html><title>Raw UDP Interface Update Failed</title><pre>{}</pre>",
                            escape_html(&err)
                        );
                        error.set_description_html(description.as_str());
                    }
                }
                Ok(())
            });
        }

        if path == "api/tunnel/exported-ip-network" {
            if !self.can_manage {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::Forbidden,
                );
                return Promise::ok(());
            }

            let body = pry!(params.get_content())
                .get_content()
                .unwrap_or(&[])
                .to_vec();
            let saved_cap_id = match std::str::from_utf8(&body) {
                Ok(value) => value.trim().to_string(),
                Err(err) => {
                    let mut error = results.get().init_client_error();
                    let description =
                        format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                    );
                    error.set_description_html(description.as_str());
                    return Promise::ok(());
                }
            };

            let app_state = self.app_state.clone();
            let sandstorm_api = self.sandstorm_api.clone();
            return Promise::from_future(async move {
                match configure_exported_ip_network(&app_state, sandstorm_api, &saved_cap_id).await {
                    Ok(()) => {
                        let mut content = results.get().init_content();
                        content.set_status_code(
                            web_session_capnp::web_session::response::SuccessCode::Ok,
                        );
                        content.set_mime_type("application/json");
                        content.init_body().set_bytes(br#"{"ok":true}"#);
                    }
                    Err(err) => {
                        let mut error = results.get().init_server_error();
                        let description = format!(
                            "<!doctype html><title>Export Configuration Failed</title><pre>{}</pre>",
                            escape_html(&err)
                        );
                        error.set_description_html(description.as_str());
                    }
                }
                Ok(())
            });
        }

        if path == "api/tunnel/exported-api-session" {
            if !self.can_manage {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::Forbidden,
                );
                return Promise::ok(());
            }

            let body = pry!(params.get_content())
                .get_content()
                .unwrap_or(&[])
                .to_vec();
            let saved_cap_id = match std::str::from_utf8(&body) {
                Ok(value) => value.trim().to_string(),
                Err(err) => {
                    let mut error = results.get().init_client_error();
                    let description =
                        format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                    );
                    error.set_description_html(description.as_str());
                    return Promise::ok(());
                }
            };

            let app_state = self.app_state.clone();
            let sandstorm_api = self.sandstorm_api.clone();
            return Promise::from_future(async move {
                match configure_exported_api_session(&app_state, sandstorm_api, &saved_cap_id).await
                {
                    Ok(()) => {
                        let mut content = results.get().init_content();
                        content.set_status_code(
                            web_session_capnp::web_session::response::SuccessCode::Ok,
                        );
                        content.set_mime_type("application/json");
                        content.init_body().set_bytes(br#"{"ok":true}"#);
                    }
                    Err(err) => {
                        let mut error = results.get().init_server_error();
                        let description = format!(
                            "<!doctype html><title>Export Configuration Failed</title><pre>{}</pre>",
                            escape_html(&err)
                        );
                        error.set_description_html(description.as_str());
                    }
                }
                Ok(())
            });
        }

        if path == "api/tunnel/rpc/connect" {
            if !self.can_manage {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::Forbidden,
                );
                return Promise::ok(());
            }

            let app_state = self.app_state.clone();
            let sandstorm_api = self.sandstorm_api.clone();
            return Promise::from_future(async move {
                match connect_peer_rpc_session(app_state, sandstorm_api).await {
                    Ok((ip_network_exports, api_session_exports)) => {
                        let ip_network_exports_json = ip_network_exports
                            .iter()
                            .map(|export| {
                                format!(
                                    "{{\"id\":\"{}\",\"label\":\"{}\"}}",
                                    json_escape(&export.id),
                                    json_escape(&export.label)
                                )
                            })
                            .collect::<Vec<_>>()
                            .join(",");
                        let api_session_exports_json = api_session_exports
                            .iter()
                            .map(|export| {
                                format!(
                                    "{{\"id\":\"{}\",\"label\":\"{}\"}}",
                                    json_escape(&export.id),
                                    json_escape(&export.label)
                                )
                            })
                            .collect::<Vec<_>>()
                            .join(",");
                        let body = format!(
                            "{{\"ok\":true,\"ipNetworkExports\":[{}],\"apiSessionExports\":[{}]}}",
                            ip_network_exports_json, api_session_exports_json
                        );
                        let mut content = results.get().init_content();
                        content.set_status_code(
                            web_session_capnp::web_session::response::SuccessCode::Ok,
                        );
                        content.set_mime_type("application/json");
                        content.init_body().set_bytes(body.as_bytes());
                    }
                    Err(err) => {
                        eprintln!("peer rpc connect failed: {err}");
                        let mut error = results.get().init_server_error();
                        let description = format!(
                            "<!doctype html><title>Peer RPC Connect Failed</title><pre>{}</pre>",
                            escape_html(&err)
                        );
                        error.set_description_html(description.as_str());
                    }
                }
                Ok(())
            });
        }

        if path == "api/tunnel/rpc/disconnect" {
            if !self.can_manage {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::Forbidden,
                );
                return Promise::ok(());
            }

            match disconnect_peer_rpc_session(&self.app_state) {
                Ok(()) => {
                    let mut content = results.get().init_content();
                    content
                        .set_status_code(web_session_capnp::web_session::response::SuccessCode::Ok);
                    content.set_mime_type("application/json");
                    content.init_body().set_bytes(br#"{"ok":true}"#);
                }
                Err(err) => {
                    let mut error = results.get().init_server_error();
                    let description = format!(
                        "<!doctype html><title>Peer RPC Disconnect Failed</title><pre>{}</pre>",
                        escape_html(&err)
                    );
                    error.set_description_html(description.as_str());
                }
            }
            return Promise::ok(());
        }

        if path == "api/tunnel/rpc/refresh-exports" {
            if !self.can_manage {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::Forbidden,
                );
                return Promise::ok(());
            }

            let app_state = self.app_state.clone();
            return Promise::from_future(async move {
                match refresh_peer_rpc_exports(&app_state).await {
                    Ok(refreshed) => {
                        let body = if refreshed {
                            br#"{"ok":true,"refreshed":true}"#.as_slice()
                        } else {
                            br#"{"ok":true,"refreshed":false}"#.as_slice()
                        };
                        let mut content = results.get().init_content();
                        content.set_status_code(
                            web_session_capnp::web_session::response::SuccessCode::Ok,
                        );
                        content.set_mime_type("application/json");
                        content.init_body().set_bytes(body);
                    }
                    Err(err) => {
                        let mut error = results.get().init_server_error();
                        let description = format!(
                            "<!doctype html><title>Peer RPC Refresh Failed</title><pre>{}</pre>",
                            escape_html(&err)
                        );
                        error.set_description_html(description.as_str());
                    }
                }
                Ok(())
            });
        }

        if path == "api/tunnel/rpc/import-ip-network" {
            if !self.can_manage {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::Forbidden,
                );
                return Promise::ok(());
            }

            let body = pry!(params.get_content())
                .get_content()
                .unwrap_or(&[])
                .to_vec();
            let export_id = match std::str::from_utf8(&body) {
                Ok(value) => value.trim().to_string(),
                Err(err) => {
                    let mut error = results.get().init_client_error();
                    let description =
                        format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                    );
                    error.set_description_html(description.as_str());
                    return Promise::ok(());
                }
            };
            if export_id.is_empty() {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                );
                error.set_description_html("missing remote export id");
                return Promise::ok(());
            }

            let app_state = self.app_state.clone();
            return Promise::from_future(async move {
                match import_remote_ip_network_export(&app_state, &export_id).await {
                    Ok((label, object_id)) => {
                        let body = format!(
                            "{{\"ok\":true,\"objectId\":\"{}\",\"exportId\":\"{}\",\"label\":\"{}\"}}",
                            json_escape(&object_id),
                            json_escape(&export_id),
                            json_escape(&label)
                        );
                        let mut content = results.get().init_content();
                        content.set_status_code(
                            web_session_capnp::web_session::response::SuccessCode::Ok,
                        );
                        content.set_mime_type("application/json");
                        content.init_body().set_bytes(body.as_bytes());
                    }
                    Err(err) => {
                        eprintln!("peer rpc import failed: {err}");
                        let mut error = results.get().init_server_error();
                        let description = format!(
                            "<!doctype html><title>Peer RPC Import Failed</title><pre>{}</pre>",
                            escape_html(&err)
                        );
                        error.set_description_html(description.as_str());
                    }
                }
                Ok(())
            });
        }

        if path == "api/tunnel/rpc/import-api-session" {
            if !self.can_manage {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::Forbidden,
                );
                return Promise::ok(());
            }

            let body = pry!(params.get_content())
                .get_content()
                .unwrap_or(&[])
                .to_vec();
            let export_id = match std::str::from_utf8(&body) {
                Ok(value) => value.trim().to_string(),
                Err(err) => {
                    let mut error = results.get().init_client_error();
                    let description =
                        format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                    );
                    error.set_description_html(description.as_str());
                    return Promise::ok(());
                }
            };
            if export_id.is_empty() {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                );
                error.set_description_html("missing remote export id");
                return Promise::ok(());
            }

            let app_state = self.app_state.clone();
            return Promise::from_future(async move {
                match import_remote_api_session_export(&app_state, &export_id).await {
                    Ok((label, object_id)) => {
                        let body = format!(
                            "{{\"ok\":true,\"objectId\":\"{}\",\"exportId\":\"{}\",\"label\":\"{}\"}}",
                            json_escape(&object_id),
                            json_escape(&export_id),
                            json_escape(&label)
                        );
                        let mut content = results.get().init_content();
                        content.set_status_code(
                            web_session_capnp::web_session::response::SuccessCode::Ok,
                        );
                        content.set_mime_type("application/json");
                        content.init_body().set_bytes(body.as_bytes());
                    }
                    Err(err) => {
                        eprintln!("peer rpc api session import failed: {err}");
                        let mut error = results.get().init_server_error();
                        let description = format!(
                            "<!doctype html><title>Peer RPC Import Failed</title><pre>{}</pre>",
                            escape_html(&err)
                        );
                        error.set_description_html(description.as_str());
                    }
                }
                Ok(())
            });
        }

        if path == "api/tunnel/rpc/import-capability" {
            if !self.can_manage {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::Forbidden,
                );
                return Promise::ok(());
            }

            let body = pry!(params.get_content())
                .get_content()
                .unwrap_or(&[])
                .to_vec();
            let export_id = match std::str::from_utf8(&body) {
                Ok(value) => value.trim().to_string(),
                Err(err) => {
                    let mut error = results.get().init_client_error();
                    let description =
                        format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                    );
                    error.set_description_html(description.as_str());
                    return Promise::ok(());
                }
            };
            if export_id.is_empty() {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                );
                error.set_description_html("missing remote export id");
                return Promise::ok(());
            }

            let app_state = self.app_state.clone();
            return Promise::from_future(async move {
                match import_remote_capability_export(&app_state, &export_id).await {
                    Ok((label, object_id, kind)) => {
                        let body = format!(
                            "{{\"ok\":true,\"objectId\":\"{}\",\"exportId\":\"{}\",\"label\":\"{}\",\"kind\":\"{}\"}}",
                            json_escape(&object_id),
                            json_escape(&export_id),
                            json_escape(&label),
                            json_escape(imported_kind_label(kind))
                        );
                        let mut content = results.get().init_content();
                        content.set_status_code(
                            web_session_capnp::web_session::response::SuccessCode::Ok,
                        );
                        content.set_mime_type("application/json");
                        content.init_body().set_bytes(body.as_bytes());
                    }
                    Err(err) => {
                        eprintln!("peer rpc generic import failed: {err}");
                        let mut error = results.get().init_server_error();
                        let description = format!(
                            "<!doctype html><title>Peer RPC Import Failed</title><pre>{}</pre>",
                            escape_html(&err)
                        );
                        error.set_description_html(description.as_str());
                    }
                }
                Ok(())
            });
        }

        if path == "api/tunnel/rpc/invoke-ip-network" {
            if !self.can_manage {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::Forbidden,
                );
                return Promise::ok(());
            }

            let body = pry!(params.get_content())
                .get_content()
                .unwrap_or(&[])
                .to_vec();
            let request = match std::str::from_utf8(&body) {
                Ok(value) => parse_remote_ip_network_invoke_request(value),
                Err(err) => {
                    let mut error = results.get().init_client_error();
                    let description =
                        format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                    );
                    error.set_description_html(description.as_str());
                    return Promise::ok(());
                }
            };
            let request = match request {
                Ok(value) => value,
                Err(err) => {
                    let mut error = results.get().init_client_error();
                    let description = format!(
                        "<!doctype html><title>Bad Request</title><p>{}</p>",
                        escape_html(&err)
                    );
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                    );
                    error.set_description_html(description.as_str());
                    return Promise::ok(());
                }
            };

            let app_state = self.app_state.clone();
            return Promise::from_future(async move {
                match invoke_imported_remote_ip_network(&app_state, &request.host, request.port).await {
                    Ok(summary) => {
                        let preview = String::from_utf8_lossy(&summary.response_bytes)
                            .lines()
                            .take(12)
                            .collect::<Vec<_>>()
                            .join("\n");
                        let body = format!(
                            "{{\"ok\":true,\"host\":\"{}\",\"port\":{},\"responseByteCount\":{},\"responsePreview\":\"{}\",\"trace\":\"{}\"}}",
                            json_escape(&summary.host),
                            summary.port,
                            summary.response_bytes.len(),
                            json_escape(&preview),
                            json_escape(&summary.trace)
                        );
                        let mut content = results.get().init_content();
                        content.set_status_code(
                            web_session_capnp::web_session::response::SuccessCode::Ok,
                        );
                        content.set_mime_type("application/json");
                        content.init_body().set_bytes(body.as_bytes());
                    }
                    Err(err) => {
                        eprintln!("peer rpc invoke failed: {err}");
                        let mut error = results.get().init_server_error();
                        let description = format!(
                            "<!doctype html><title>Peer RPC Invoke Failed</title><pre>{}</pre>",
                            escape_html(&err)
                        );
                        error.set_description_html(description.as_str());
                    }
                }
                Ok(())
            });
        }

        if path == "api/tunnel/rpc/invoke-api-session" {
            if !self.can_manage {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::Forbidden,
                );
                return Promise::ok(());
            }

            let body = pry!(params.get_content())
                .get_content()
                .unwrap_or(&[])
                .to_vec();
            let filename = match params.get_context() {
                Ok(context) => match get_request_header(context, "x-sandstorm-app-filename") {
                    Ok(Some(value)) => value,
                    Ok(None) => String::new(),
                    Err(err) => {
                        let mut error = results.get().init_client_error();
                        let description = format!(
                            "<!doctype html><title>Bad Request</title><p>{}</p>",
                            escape_html(&err)
                        );
                        error.set_status_code(
                            web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                        );
                        error.set_description_html(description.as_str());
                        return Promise::ok(());
                    }
                },
                Err(err) => {
                    let mut error = results.get().init_client_error();
                    let description =
                        format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                    );
                    error.set_description_html(description.as_str());
                    return Promise::ok(());
                }
            };
            let request = match parse_remote_api_session_invoke_request(&filename, &body) {
                Ok(value) => value,
                Err(err) => {
                    let mut error = results.get().init_client_error();
                    let description = format!(
                        "<!doctype html><title>Bad Request</title><p>{}</p>",
                        escape_html(&err)
                    );
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                    );
                    error.set_description_html(description.as_str());
                    return Promise::ok(());
                }
            };

            let app_state = self.app_state.clone();
            return Promise::from_future(async move {
                match invoke_imported_remote_api_session(
                    &app_state,
                    &request.filename,
                    &request.payload,
                )
                .await
                {
                    Ok(summary) => {
                        let preview = String::from_utf8_lossy(&summary.response_bytes)
                            .lines()
                            .take(12)
                            .collect::<Vec<_>>()
                            .join("\n");
                        let body = format!(
                            "{{\"ok\":true,\"status\":{},\"contentType\":\"{}\",\"responseByteCount\":{},\"responsePreview\":\"{}\",\"responsePreviewBase64\":\"{}\",\"trace\":\"{}\"}}",
                            summary.status_code,
                            json_escape(&summary.content_type),
                            summary.response_bytes.len(),
                            json_escape(&preview),
                            json_escape(&base64::engine::general_purpose::STANDARD.encode(
                                summary.response_bytes.iter().take(256).copied().collect::<Vec<_>>()
                            )),
                            json_escape(&summary.trace)
                        );
                        let mut content = results.get().init_content();
                        content.set_status_code(
                            web_session_capnp::web_session::response::SuccessCode::Ok,
                        );
                        content.set_mime_type("application/json");
                        content.init_body().set_bytes(body.as_bytes());
                    }
                    Err(err) => {
                        eprintln!("peer rpc api session invoke failed: {err}");
                        let mut error = results.get().init_server_error();
                        let description = format!(
                            "<!doctype html><title>Peer RPC Invoke Failed</title><pre>{}</pre>",
                            escape_html(&err)
                        );
                        error.set_description_html(description.as_str());
                    }
                }
                Ok(())
            });
        }

        if path == "api/network/http-probe" {
            if !self.can_manage {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::Forbidden,
                );
                return Promise::ok(());
            }

            let body = pry!(params.get_content())
                .get_content()
                .unwrap_or(&[])
                .to_vec();
            let probe_request = match std::str::from_utf8(&body) {
                Ok(value) => parse_http_probe_request(value),
                Err(err) => {
                    let mut error = results.get().init_client_error();
                    let description =
                        format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                    );
                    error.set_description_html(description.as_str());
                    return Promise::ok(());
                }
            };
            let probe_request = match probe_request {
                Ok(request) => request,
                Err(err) => {
                    let mut error = results.get().init_client_error();
                    let description =
                        format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                    );
                    error.set_description_html(description.as_str());
                    return Promise::ok(());
                }
            };

            let sandstorm_api = self.sandstorm_api.clone();
            return Promise::from_future(async move {
                match probe_saved_ip_network_http(sandstorm_api, probe_request).await {
                    Ok(summary) => {
                        let body = format!(
                            "{{\"ok\":true,\"host\":\"{}\",\"port\":{},\"path\":\"{}\",\"responsePreview\":\"{}\",\"trace\":\"{}\"}}",
                            json_escape(&summary.host),
                            summary.port,
                            json_escape(&summary.path),
                            json_escape(&summary.response_preview),
                            json_escape(&summary.trace)
                        );
                        let mut content = results.get().init_content();
                        content.set_status_code(
                            web_session_capnp::web_session::response::SuccessCode::Ok,
                        );
                        content.set_mime_type("application/json");
                        content.init_body().set_bytes(body.as_bytes());
                    }
                    Err(err) => {
                        eprintln!("ip network probe failed: {err}");
                        let mut error = results.get().init_server_error();
                        let description = format!(
                            "<!doctype html><title>Network Probe Failed</title><pre>{}</pre>",
                            escape_html(&err)
                        );
                        error.set_description_html(description.as_str());
                    }
                }
                Ok(())
            });
        }

        if path == "api/network/tcp-probe" {
            if !self.can_manage {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::Forbidden,
                );
                return Promise::ok(());
            }

            let body = pry!(params.get_content())
                .get_content()
                .unwrap_or(&[])
                .to_vec();
            let probe_request = match std::str::from_utf8(&body) {
                Ok(value) => parse_tcp_probe_request(value),
                Err(err) => {
                    let mut error = results.get().init_client_error();
                    let description =
                        format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                    );
                    error.set_description_html(description.as_str());
                    return Promise::ok(());
                }
            };
            let probe_request = match probe_request {
                Ok(request) => request,
                Err(err) => {
                    let mut error = results.get().init_client_error();
                    let description =
                        format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                    );
                    error.set_description_html(description.as_str());
                    return Promise::ok(());
                }
            };

            let sandstorm_api = self.sandstorm_api.clone();
            return Promise::from_future(async move {
                match probe_saved_ip_network_tcp(sandstorm_api, probe_request).await {
                    Ok(summary) => {
                        let response_preview = String::from_utf8_lossy(&summary.response_bytes)
                            .lines()
                            .take(12)
                            .collect::<Vec<_>>()
                            .join("\n");
                        let body = format!(
                            "{{\"ok\":true,\"host\":\"{}\",\"port\":{},\"responsePreview\":\"{}\",\"responseByteCount\":{},\"trace\":\"{}\"}}",
                            json_escape(&summary.host),
                            summary.port,
                            json_escape(&response_preview),
                            summary.response_bytes.len(),
                            json_escape(&summary.trace)
                        );
                        let mut content = results.get().init_content();
                        content.set_status_code(
                            web_session_capnp::web_session::response::SuccessCode::Ok,
                        );
                        content.set_mime_type("application/json");
                        content.init_body().set_bytes(body.as_bytes());
                    }
                    Err(err) => {
                        eprintln!("tcp probe failed: {err}");
                        let mut error = results.get().init_server_error();
                        let description = format!(
                            "<!doctype html><title>TCP Probe Failed</title><pre>{}</pre>",
                            escape_html(&err)
                        );
                        error.set_description_html(description.as_str());
                    }
                }
                Ok(())
            });
        }

        if path == "api/network/udp-probe" {
            if !self.can_manage {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::Forbidden,
                );
                return Promise::ok(());
            }

            let body = pry!(params.get_content())
                .get_content()
                .unwrap_or(&[])
                .to_vec();
            let probe_request = match std::str::from_utf8(&body) {
                Ok(value) => parse_udp_probe_request(value),
                Err(err) => {
                    let mut error = results.get().init_client_error();
                    let description =
                        format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                    );
                    error.set_description_html(description.as_str());
                    return Promise::ok(());
                }
            };
            let probe_request = match probe_request {
                Ok(request) => request,
                Err(err) => {
                    let mut error = results.get().init_client_error();
                    let description =
                        format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                    );
                    error.set_description_html(description.as_str());
                    return Promise::ok(());
                }
            };

            let sandstorm_api = self.sandstorm_api.clone();
            return Promise::from_future(async move {
                eprintln!("udp probe request: begin");
                let request_timeout_ms = probe_request.wait_ms.max(1_000) + 2_000;
                match timeout(
                    Duration::from_millis(request_timeout_ms),
                    probe_saved_ip_network_udp(sandstorm_api, probe_request),
                )
                .await
                {
                    Ok(Ok(summary)) => {
                        eprintln!("udp probe request: success");
                        let response_preview = String::from_utf8_lossy(&summary.response_packet)
                            .lines()
                            .take(12)
                            .collect::<Vec<_>>()
                            .join("\n");
                        let body = json!({
                            "ok": true,
                            "host": summary.host,
                            "port": summary.port,
                            "responsePreview": response_preview,
                            "responseBase64": base64::engine::general_purpose::STANDARD
                                .encode(&summary.response_packet),
                            "responseByteCount": summary.response_byte_count,
                            "responsePacketCount": summary.response_packet_count,
                            "trace": summary.trace,
                        })
                        .to_string();
                        let mut content = results.get().init_content();
                        content.set_status_code(
                            web_session_capnp::web_session::response::SuccessCode::Ok,
                        );
                        content.set_mime_type("application/json");
                        content.init_body().set_bytes(body.as_bytes());
                    }
                    Ok(Err(err)) => {
                        eprintln!("udp probe failed: {err}");
                        let mut error = results.get().init_server_error();
                        let description = format!(
                            "<!doctype html><title>UDP Probe Failed</title><pre>{}</pre>",
                            escape_html(&err)
                        );
                        error.set_description_html(description.as_str());
                    }
                    Err(_) => {
                        eprintln!(
                            "udp probe request: outer timeout after {}ms",
                            request_timeout_ms
                        );
                        let mut error = results.get().init_server_error();
                        let description = format!(
                            "<!doctype html><title>UDP Probe Failed</title><pre>UDP probe request timed out after {}ms before the server produced a response</pre>",
                            request_timeout_ms
                        );
                        error.set_description_html(description.as_str());
                    }
                }
                Ok(())
            });
        }

        if path == "api/network/exchange" {
            if !self.can_manage {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::Forbidden,
                );
                return Promise::ok(());
            }

            let body = pry!(params.get_content())
                .get_content()
                .unwrap_or(&[])
                .to_vec();
            let exchange_request = match std::str::from_utf8(&body) {
                Ok(value) => parse_network_exchange_request(value),
                Err(err) => {
                    let mut error = results.get().init_client_error();
                    let description =
                        format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                    );
                    error.set_description_html(description.as_str());
                    return Promise::ok(());
                }
            };
            let exchange_request = match exchange_request {
                Ok(request) => request,
                Err(err) => {
                    let mut error = results.get().init_client_error();
                    let description =
                        format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                    );
                    error.set_description_html(description.as_str());
                    return Promise::ok(());
                }
            };

            let sandstorm_api = self.sandstorm_api.clone();
            return Promise::from_future(async move {
                let connection = connect_saved_ip_network_tcp(
                    sandstorm_api,
                    &exchange_request.saved_token_hex,
                    &exchange_request.host,
                    exchange_request.port,
                )
                .await;

                match connection {
                    Ok(connection) => {
                        match finish_saved_ip_network_tcp_exchange(
                            connection,
                            &exchange_request.payload,
                        )
                        .await
                        {
                            Ok((response_bytes, trace)) => {
                                let body = format!(
                                    "{{\"ok\":true,\"host\":\"{}\",\"port\":{},\"responseBase64\":\"{}\",\"responseByteCount\":{},\"trace\":\"{}\"}}",
                                    json_escape(&exchange_request.host),
                                    exchange_request.port,
                                    json_escape(
                                        &base64::engine::general_purpose::STANDARD
                                            .encode(&response_bytes)
                                    ),
                                    response_bytes.len(),
                                    json_escape(&trace)
                                );
                                let mut content = results.get().init_content();
                                content.set_status_code(
                                    web_session_capnp::web_session::response::SuccessCode::Ok,
                                );
                                content.set_mime_type("application/json");
                                content.init_body().set_bytes(body.as_bytes());
                            }
                            Err(err) => {
                                eprintln!("network exchange failed: {err}");
                                let mut error = results.get().init_server_error();
                                let description = format!(
                                    "<!doctype html><title>Network Exchange Failed</title><pre>{}</pre>",
                                    escape_html(&err)
                                );
                                error.set_description_html(description.as_str());
                            }
                        }
                    }
                    Err(err) => {
                        eprintln!("network exchange connect failed: {err}");
                        let mut error = results.get().init_server_error();
                        let description = format!(
                            "<!doctype html><title>Network Exchange Failed</title><pre>{}</pre>",
                            escape_html(&err)
                        );
                        error.set_description_html(description.as_str());
                    }
                }
                Ok(())
            });
        }

        if path == "api/network/session/open" {
            if !self.can_manage {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::Forbidden,
                );
                return Promise::ok(());
            }

            let body = pry!(params.get_content())
                .get_content()
                .unwrap_or(&[])
                .to_vec();
            let open_request = match std::str::from_utf8(&body) {
                Ok(value) => parse_tcp_session_open_request(value),
                Err(err) => {
                    let mut error = results.get().init_client_error();
                    let description =
                        format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                    );
                    error.set_description_html(description.as_str());
                    return Promise::ok(());
                }
            };
            let open_request = match open_request {
                Ok(request) => request,
                Err(err) => {
                    let mut error = results.get().init_client_error();
                    let description =
                        format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                    );
                    error.set_description_html(description.as_str());
                    return Promise::ok(());
                }
            };

            let sandstorm_api = self.sandstorm_api.clone();
            let app_state = self.app_state.clone();
            return Promise::from_future(async move {
                match connect_saved_ip_network_tcp(
                    sandstorm_api,
                    &open_request.saved_token_hex,
                    &open_request.host,
                    open_request.port,
                )
                .await
                {
                    Ok(connection) => {
                        let session = connection_into_session(
                            connection,
                            open_request.host.clone(),
                            open_request.port,
                        );
                        let trace = match session.snapshot() {
                            Ok(snapshot) => snapshot.trace,
                            Err(err) => {
                                eprintln!("tcp session snapshot failed: {err}");
                                let mut error = results.get().init_server_error();
                                let description = format!(
                                    "<!doctype html><title>TCP Session Open Failed</title><pre>{}</pre>",
                                    escape_html(&err)
                                );
                                error.set_description_html(description.as_str());
                                return Ok(());
                            }
                        };
                        match insert_tcp_session(&app_state, session) {
                            Ok(session_id) => {
                                let body = format!(
                                    "{{\"ok\":true,\"sessionId\":\"{}\",\"host\":\"{}\",\"port\":{},\"trace\":\"{}\"}}",
                                    json_escape(&session_id),
                                    json_escape(&open_request.host),
                                    open_request.port,
                                    json_escape(&trace)
                                );
                                let mut content = results.get().init_content();
                                content.set_status_code(
                                    web_session_capnp::web_session::response::SuccessCode::Ok,
                                );
                                content.set_mime_type("application/json");
                                content.init_body().set_bytes(body.as_bytes());
                            }
                            Err(err) => {
                                eprintln!("tcp session insert failed: {err}");
                                let mut error = results.get().init_server_error();
                                let description = format!(
                                    "<!doctype html><title>TCP Session Open Failed</title><pre>{}</pre>",
                                    escape_html(&err)
                                );
                                error.set_description_html(description.as_str());
                            }
                        }
                    }
                    Err(err) => {
                        eprintln!("tcp session open failed: {err}");
                        let mut error = results.get().init_server_error();
                        let description = format!(
                            "<!doctype html><title>TCP Session Open Failed</title><pre>{}</pre>",
                            escape_html(&err)
                        );
                        error.set_description_html(description.as_str());
                    }
                }
                Ok(())
            });
        }

        if path == "api/network/session/send" {
            if !self.can_manage {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::Forbidden,
                );
                return Promise::ok(());
            }

            let body = pry!(params.get_content())
                .get_content()
                .unwrap_or(&[])
                .to_vec();
            let send_request = match std::str::from_utf8(&body) {
                Ok(value) => parse_tcp_session_send_request(value),
                Err(err) => {
                    let mut error = results.get().init_client_error();
                    let description =
                        format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                    );
                    error.set_description_html(description.as_str());
                    return Promise::ok(());
                }
            };
            let send_request = match send_request {
                Ok(request) => request,
                Err(err) => {
                    let mut error = results.get().init_client_error();
                    let description =
                        format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                    );
                    error.set_description_html(description.as_str());
                    return Promise::ok(());
                }
            };

            let app_state = self.app_state.clone();
            return Promise::from_future(async move {
                match get_tcp_session(&app_state, &send_request.session_id) {
                    Ok(session) => {
                        match send_tcp_session_bytes(&session, &send_request.payload).await {
                            Ok(()) => {
                                let snapshot = match session.snapshot() {
                                    Ok(snapshot) => snapshot,
                                    Err(err) => {
                                        eprintln!("tcp session snapshot failed: {err}");
                                        let mut error = results.get().init_server_error();
                                        let description = format!(
                                            "<!doctype html><title>TCP Session Send Failed</title><pre>{}</pre>",
                                            escape_html(&err)
                                        );
                                        error.set_description_html(description.as_str());
                                        return Ok(());
                                    }
                                };
                                let body = format!(
                                    "{{\"ok\":true,\"sessionId\":\"{}\",\"bytesSent\":{},\"trace\":\"{}\"}}",
                                    json_escape(&send_request.session_id),
                                    send_request.payload.len(),
                                    json_escape(&snapshot.trace)
                                );
                                let mut content = results.get().init_content();
                                content.set_status_code(
                                    web_session_capnp::web_session::response::SuccessCode::Ok,
                                );
                                content.set_mime_type("application/json");
                                content.init_body().set_bytes(body.as_bytes());
                            }
                            Err(err) => {
                                eprintln!("tcp session send failed: {err}");
                                let mut error = results.get().init_server_error();
                                let description = format!(
                                    "<!doctype html><title>TCP Session Send Failed</title><pre>{}</pre>",
                                    escape_html(&err)
                                );
                                error.set_description_html(description.as_str());
                            }
                        }
                    }
                    Err(err) => {
                        let mut error = results.get().init_client_error();
                        error.set_status_code(
                            web_session_capnp::web_session::response::ClientErrorCode::NotFound,
                        );
                        let description = format!(
                            "<!doctype html><title>TCP Session Send Failed</title><pre>{}</pre>",
                            escape_html(&err)
                        );
                        error.set_description_html(description.as_str());
                    }
                }
                Ok(())
            });
        }

        if path == "api/network/session/receive" {
            if !self.can_manage {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::Forbidden,
                );
                return Promise::ok(());
            }

            let body = pry!(params.get_content())
                .get_content()
                .unwrap_or(&[])
                .to_vec();
            let receive_request = match std::str::from_utf8(&body) {
                Ok(value) => parse_tcp_session_receive_request(value),
                Err(err) => {
                    let mut error = results.get().init_client_error();
                    let description =
                        format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                    );
                    error.set_description_html(description.as_str());
                    return Promise::ok(());
                }
            };
            let receive_request = match receive_request {
                Ok(request) => request,
                Err(err) => {
                    let mut error = results.get().init_client_error();
                    let description =
                        format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                    );
                    error.set_description_html(description.as_str());
                    return Promise::ok(());
                }
            };

            let app_state = self.app_state.clone();
            return Promise::from_future(async move {
                match get_tcp_session(&app_state, &receive_request.session_id) {
                    Ok(session) => match read_tcp_session_bytes(
                        &session,
                        receive_request.max_bytes,
                        receive_request.wait_ms,
                    )
                    .await
                    {
                        Ok(read_result) => {
                            let body = format!(
                                "{{\"ok\":true,\"sessionId\":\"{}\",\"responseBase64\":\"{}\",\"responseByteCount\":{},\"bufferedBytes\":{},\"receivedBytes\":{},\"writeCalls\":{},\"done\":{},\"trace\":\"{}\"}}",
                                json_escape(&receive_request.session_id),
                                json_escape(
                                    &base64::engine::general_purpose::STANDARD
                                        .encode(&read_result.bytes)
                                ),
                                read_result.bytes.len(),
                                read_result.buffered_bytes,
                                read_result.received_bytes,
                                read_result.write_calls,
                                if read_result.done { "true" } else { "false" },
                                json_escape(&read_result.trace)
                            );
                            let mut content = results.get().init_content();
                            content.set_status_code(
                                web_session_capnp::web_session::response::SuccessCode::Ok,
                            );
                            content.set_mime_type("application/json");
                            content.init_body().set_bytes(body.as_bytes());
                        }
                        Err(err) => {
                            eprintln!("tcp session receive failed: {err}");
                            let mut error = results.get().init_server_error();
                            let description = format!(
                                "<!doctype html><title>TCP Session Receive Failed</title><pre>{}</pre>",
                                escape_html(&err)
                            );
                            error.set_description_html(description.as_str());
                        }
                    },
                    Err(err) => {
                        let mut error = results.get().init_client_error();
                        error.set_status_code(
                            web_session_capnp::web_session::response::ClientErrorCode::NotFound,
                        );
                        let description = format!(
                            "<!doctype html><title>TCP Session Receive Failed</title><pre>{}</pre>",
                            escape_html(&err)
                        );
                        error.set_description_html(description.as_str());
                    }
                }
                Ok(())
            });
        }

        if path == "api/network/session/close" {
            if !self.can_manage {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::Forbidden,
                );
                return Promise::ok(());
            }

            let body = pry!(params.get_content())
                .get_content()
                .unwrap_or(&[])
                .to_vec();
            let close_request = match std::str::from_utf8(&body) {
                Ok(value) => parse_tcp_session_close_request(value),
                Err(err) => {
                    let mut error = results.get().init_client_error();
                    let description =
                        format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                    );
                    error.set_description_html(description.as_str());
                    return Promise::ok(());
                }
            };
            let close_request = match close_request {
                Ok(request) => request,
                Err(err) => {
                    let mut error = results.get().init_client_error();
                    let description =
                        format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                    );
                    error.set_description_html(description.as_str());
                    return Promise::ok(());
                }
            };

            let app_state = self.app_state.clone();
            return Promise::from_future(async move {
                match remove_tcp_session(&app_state, &close_request.session_id) {
                    Ok(session) => {
                        let close_result = close_tcp_session_writer(&session).await;
                        let snapshot = match session.snapshot() {
                            Ok(snapshot) => snapshot,
                            Err(err) => {
                                eprintln!("tcp session snapshot failed: {err}");
                                let mut error = results.get().init_server_error();
                                let description = format!(
                                    "<!doctype html><title>TCP Session Close Failed</title><pre>{}</pre>",
                                    escape_html(&err)
                                );
                                error.set_description_html(description.as_str());
                                return Ok(());
                            }
                        };
                        match close_result {
                            Ok(()) => {
                                let body = format!(
                                    "{{\"ok\":true,\"sessionId\":\"{}\",\"trace\":\"{}\",\"done\":{}}}",
                                    json_escape(&close_request.session_id),
                                    json_escape(&snapshot.trace),
                                    if snapshot.done { "true" } else { "false" }
                                );
                                let mut content = results.get().init_content();
                                content.set_status_code(
                                    web_session_capnp::web_session::response::SuccessCode::Ok,
                                );
                                content.set_mime_type("application/json");
                                content.init_body().set_bytes(body.as_bytes());
                            }
                            Err(err) => {
                                eprintln!("tcp session close failed: {err}");
                                let mut error = results.get().init_server_error();
                                let description = format!(
                                    "<!doctype html><title>TCP Session Close Failed</title><pre>{}</pre>",
                                    escape_html(&err)
                                );
                                error.set_description_html(description.as_str());
                            }
                        }
                    }
                    Err(err) => {
                        let mut error = results.get().init_client_error();
                        error.set_status_code(
                            web_session_capnp::web_session::response::ClientErrorCode::NotFound,
                        );
                        let description = format!(
                            "<!doctype html><title>TCP Session Close Failed</title><pre>{}</pre>",
                            escape_html(&err)
                        );
                        error.set_description_html(description.as_str());
                    }
                }
                Ok(())
            });
        }

        if path != "api/powerbox/claim" {
            let mut error = results.get().init_client_error();
            error.set_status_code(
                web_session_capnp::web_session::response::ClientErrorCode::NotFound,
            );
            return Promise::ok(());
        }

        let body = pry!(params.get_content())
            .get_content()
            .unwrap_or(&[])
            .to_vec();
        let (request_token, save_label, descriptor_json) = match std::str::from_utf8(&body) {
            Ok(value) => match parse_claim_payload(value) {
                Ok(payload) => payload,
                Err(err) => {
                    let mut error = results.get().init_client_error();
                    let description = format!(
                        "<!doctype html><title>Bad Request</title><p>{}</p>",
                        escape_html(&err)
                    );
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                    );
                    error.set_description_html(description.as_str());
                    return Promise::ok(());
                }
            },
            Err(err) => {
                let mut error = results.get().init_client_error();
                let description = format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                );
                error.set_description_html(description.as_str());
                return Promise::ok(());
            }
        };

        let sandstorm_api = self.sandstorm_api.clone();
        let session_context = self.session_context.clone();
        Promise::from_future(async move {
            let outcome = claim_and_save_capability(
                sandstorm_api,
                session_context,
                &request_token,
                &save_label,
            )
            .await
            .and_then(|saved_token| {
                let saved_cap = persist_saved_capability(
                    &save_label,
                    &saved_token,
                    descriptor_json.as_deref(),
                )?;
                Ok(saved_cap)
            });

            match outcome {
                Ok(saved_cap) => {
                    let body = format!(
                        "{{\"ok\":true,\"savedToken\":\"{}\",\"id\":\"{}\"}}",
                        json_escape(&saved_cap.saved_token),
                        json_escape(&saved_cap.id)
                    );
                    let mut content = results.get().init_content();
                    content
                        .set_status_code(web_session_capnp::web_session::response::SuccessCode::Ok);
                    content.set_mime_type("application/json");
                    content.init_body().set_bytes(body.as_bytes());
                }
                Err(err) => {
                    let mut error = results.get().init_server_error();
                    let description = format!(
                        "<!doctype html><title>Powerbox Claim Failed</title><pre>{}</pre>",
                        escape_html(&err)
                    );
                    error.set_description_html(description.as_str());
                }
            }

            Ok(())
        })
    }

    fn put(
        self: Rc<Self>,
        params: web_session_capnp::web_session::PutParams,
        mut results: web_session_capnp::web_session::PutResults,
    ) -> Promise<(), capnp::Error> {
        let params = pry!(params.get());
        let path = pry!(params.get_path()).to_str().unwrap_or("").to_string();
        if let Err(err) = self.require_canonical_path(&path) {
            return Promise::err(err);
        }

        if path == "api/pairing/remote-ticket" {
            if !self.can_manage {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::Forbidden,
                );
                return Promise::ok(());
            }

            match update_remote_ticket(&self.app_state, String::new()) {
                Ok(()) => {
                    results.get().init_no_content();
                }
                Err(err) => {
                    let mut error = results.get().init_server_error();
                    let description = format!(
                        "<!doctype html><title>Pairing Delete Failed</title><pre>{}</pre>",
                        escape_html(&err)
                    );
                    error.set_description_html(description.as_str());
                }
            }
            return Promise::ok(());
        }

        if path == "api/pairing/forget" {
            if !self.can_manage {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::Forbidden,
                );
                return Promise::ok(());
            }

            match forget_peer(&self.app_state) {
                Ok(()) => {
                    let mut content = results.get().init_content();
                    content
                        .set_status_code(web_session_capnp::web_session::response::SuccessCode::Ok);
                    content.set_mime_type("application/json");
                    content.init_body().set_bytes(br#"{"ok":true}"#);
                }
                Err(err) => {
                    let mut error = results.get().init_server_error();
                    let description = format!(
                        "<!doctype html><title>Forget Peer Failed</title><pre>{}</pre>",
                        escape_html(&err)
                    );
                    error.set_description_html(description.as_str());
                }
            }
            return Promise::ok(());
        }

        if path == "api/endpoint/raw-udp-interface" {
            if !self.can_manage {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::Forbidden,
                );
                return Promise::ok(());
            }

            let app_state = self.app_state.clone();
            let sandstorm_api = self.sandstorm_api.clone();
            return Promise::from_future(async move {
                let outcome = clear_raw_udp_interface_binding(
                    || {
                        clear_persisted_raw_udp_interface_token()?;
                        clear_persisted_raw_udp_port()
                    },
                    || initialize_iroh_endpoint(app_state, sandstorm_api),
                )
                .await;

                match outcome {
                    Ok(()) => {
                        results.get().init_no_content();
                    }
                    Err(err) => {
                        let mut error = results.get().init_server_error();
                        let description = format!(
                            "<!doctype html><title>Raw UDP Interface Clear Failed</title><pre>{}</pre>",
                            escape_html(&err)
                        );
                        error.set_description_html(description.as_str());
                    }
                }
                Ok(())
            });
        }

        if path != "api/saved-cap/restore" {
            if path == "api/saved-cap/resolve-object" {
                let body = pry!(params.get_content())
                    .get_content()
                    .unwrap_or(&[])
                    .to_vec();
                let object_id = match std::str::from_utf8(&body) {
                    Ok(value) => value.trim().to_string(),
                    Err(err) => {
                        let mut error = results.get().init_client_error();
                        let description =
                            format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                        error.set_status_code(
                            web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                        );
                        error.set_description_html(description.as_str());
                        return Promise::ok(());
                    }
                };

                let sandstorm_api = self.sandstorm_api.clone();
                let app_state = self.app_state.clone();
                return Promise::from_future(async move {
                    let outcome =
                        restore_app_object_capability(sandstorm_api, &app_state, &object_id).await;
                    match outcome {
                        Ok(_) => {
                            results.get().init_no_content();
                        }
                        Err(err) => {
                            let mut error = results.get().init_client_error();
                            error.set_status_code(
                                web_session_capnp::web_session::response::ClientErrorCode::NotFound,
                            );
                            let description = format!(
                                "<!doctype html><title>Resolve Failed</title><pre>{}</pre>",
                                escape_html(&err)
                            );
                            error.set_description_html(description.as_str());
                        }
                    }
                    Ok(())
                });
            } else if path == "api/received-cap/disable" {
                let body = pry!(params.get_content())
                    .get_content()
                    .unwrap_or(&[])
                    .to_vec();
                let object_id = match std::str::from_utf8(&body) {
                    Ok(value) => value.trim().to_string(),
                    Err(err) => {
                        let mut error = results.get().init_client_error();
                        let description =
                            format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                        error.set_status_code(
                            web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                        );
                        error.set_description_html(description.as_str());
                        return Promise::ok(());
                    }
                };

                let app_state = self.app_state.clone();
                return Promise::from_future(async move {
                    match disable_received_remote_capability(&app_state, &object_id) {
                        Ok(true) => {
                            results.get().init_no_content();
                        }
                        Ok(false) => {
                            let mut error = results.get().init_client_error();
                            error.set_status_code(
                                web_session_capnp::web_session::response::ClientErrorCode::NotFound,
                            );
                        }
                        Err(err) => {
                            let mut error = results.get().init_server_error();
                            let description = format!(
                                "<!doctype html><title>Disable Received Capability Failed</title><pre>{}</pre>",
                                escape_html(&err)
                            );
                            error.set_description_html(description.as_str());
                        }
                    }
                    Ok(())
                });
            } else if path == "api/received-cap/enable" {
                let body = pry!(params.get_content())
                    .get_content()
                    .unwrap_or(&[])
                    .to_vec();
                let object_id = match std::str::from_utf8(&body) {
                    Ok(value) => value.trim().to_string(),
                    Err(err) => {
                        let mut error = results.get().init_client_error();
                        let description =
                            format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                        error.set_status_code(
                            web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                        );
                        error.set_description_html(description.as_str());
                        return Promise::ok(());
                    }
                };

                let app_state = self.app_state.clone();
                return Promise::from_future(async move {
                    match enable_received_remote_capability(&app_state, &object_id).await {
                        Ok(true) => {
                            results.get().init_no_content();
                        }
                        Ok(false) => {
                            let mut error = results.get().init_client_error();
                            error.set_status_code(
                                web_session_capnp::web_session::response::ClientErrorCode::NotFound,
                            );
                        }
                        Err(err) => {
                            let mut error = results.get().init_server_error();
                            let description = format!(
                                "<!doctype html><title>Enable Received Capability Failed</title><pre>{}</pre>",
                                escape_html(&err)
                            );
                            error.set_description_html(description.as_str());
                        }
                    }
                    Ok(())
                });
            } else if path == "api/received-cap/save-local" {
                let body = pry!(params.get_content())
                    .get_content()
                    .unwrap_or(&[])
                    .to_vec();
                let object_id = match std::str::from_utf8(&body) {
                    Ok(value) => value.trim().to_string(),
                    Err(err) => {
                        let mut error = results.get().init_client_error();
                        let description =
                            format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                        error.set_status_code(
                            web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                        );
                        error.set_description_html(description.as_str());
                        return Promise::ok(());
                    }
                };

                let app_state = self.app_state.clone();
                let sandstorm_api = self.sandstorm_api.clone();
                return Promise::from_future(async move {
                    match save_received_remote_capability_locally(
                        &app_state,
                        sandstorm_api,
                        &object_id,
                    )
                    .await
                    {
                        Ok(saved_cap) => {
                            let body = format!(
                                "{{\"ok\":true,\"id\":\"{}\",\"label\":\"{}\",\"savedToken\":\"{}\"}}",
                                json_escape(&saved_cap.id),
                                json_escape(&saved_cap.label),
                                json_escape(&saved_cap.saved_token)
                            );
                            let mut content = results.get().init_content();
                            content.set_status_code(
                                web_session_capnp::web_session::response::SuccessCode::Ok,
                            );
                            content.set_mime_type("application/json");
                            content.init_body().set_bytes(body.as_bytes());
                        }
                        Err(err) => {
                            let mut error = results.get().init_server_error();
                            let description = format!(
                                "<!doctype html><title>Save Local Capability Failed</title><pre>{}</pre>",
                                escape_html(&err)
                            );
                            error.set_description_html(description.as_str());
                        }
                    }
                    Ok(())
                });
            } else if path == "api/powerbox/fulfill-object" {
                let body = pry!(params.get_content())
                    .get_content()
                    .unwrap_or(&[])
                    .to_vec();
                let object_id = match std::str::from_utf8(&body) {
                    Ok(value) => value.trim().to_string(),
                    Err(err) => {
                        let mut error = results.get().init_client_error();
                        let description =
                            format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                        error.set_status_code(
                            web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                        );
                        error.set_description_html(description.as_str());
                        return Promise::ok(());
                    }
                };
                let Some(request_descriptor_b64) = self.request_descriptor_b64.clone() else {
                    let mut error = results.get().init_client_error();
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                    );
                    error.set_description_html("not in a powerbox request session");
                    return Promise::ok(());
                };

                let app_state = self.app_state.clone();
                let sandstorm_api = self.sandstorm_api.clone();
                let session_context = self.session_context.clone();
                return Promise::from_future(async move {
                    let effective_object_id =
                        match create_local_proxy_for_received_object(&app_state, &object_id).await {
                            Ok((_label, local_proxy_object_id)) => local_proxy_object_id,
                            Err(_) => object_id.clone(),
                        };
                    let label = match capability_label_for_object_id(&app_state, &effective_object_id) {
                        Ok(label) => label,
                        Err(err) => {
                            eprintln!(
                                "powerbox.fulfill_object: label lookup failed object_id={} effective_object_id={} at_ms={} err={}",
                                object_id,
                                effective_object_id,
                                now_ms(),
                                err
                            );
                            let mut error = results.get().init_server_error();
                            error.set_description_html(
                                format!(
                                    "<!doctype html><title>Fulfill Request Failed</title><pre>{}</pre>",
                                    escape_html(&err)
                                )
                                .as_str(),
                            );
                            return Ok(());
                        }
                    };
                    let cap = match restore_app_object_capability(
                        sandstorm_api.clone(),
                        &app_state,
                        &effective_object_id,
                    )
                        .await
                    {
                        Ok(cap) => cap,
                        Err(err) => {
                            eprintln!(
                                "powerbox.fulfill_object: restore_app_object_capability failed object_id={} effective_object_id={} at_ms={} err={}",
                                object_id,
                                effective_object_id,
                                now_ms(),
                                err
                            );
                            let mut error = results.get().init_server_error();
                            error.set_description_html(
                                format!(
                                    "<!doctype html><title>Fulfill Request Failed</title><pre>{}</pre>",
                                    escape_html(&err)
                                )
                                .as_str(),
                            );
                            return Ok(());
                        }
                    };
                    let descriptor_source = match capability_descriptor_for_object_id(
                        &app_state,
                        &effective_object_id,
                    )
                    {
                        Ok(Some(value)) => value,
                        Ok(None) => request_descriptor_b64,
                        Err(err) => {
                            eprintln!(
                                "powerbox.fulfill_object: descriptor lookup failed object_id={} effective_object_id={} at_ms={} err={}",
                                object_id,
                                effective_object_id,
                                now_ms(),
                                err
                            );
                            let mut error = results.get().init_server_error();
                            error.set_description_html(
                                format!(
                                    "<!doctype html><title>Fulfill Request Failed</title><pre>{}</pre>",
                                    escape_html(&err)
                                )
                                .as_str(),
                            );
                            return Ok(());
                        }
                    };
                    match fulfill_powerbox_request_with_capability(
                        session_context,
                        cap,
                        &label,
                        &descriptor_source,
                    )
                    .await
                    {
                        Ok(_) => {
                            let mut content = results.get().init_content();
                            content.set_status_code(
                                web_session_capnp::web_session::response::SuccessCode::Ok,
                            );
                            content.set_mime_type("application/json");
                            content.init_body().set_bytes(br#"{"ok":true}"#);
                        }
                        Err(err) => {
                            eprintln!(
                                "powerbox.fulfill_object: fulfill failed object_id={} effective_object_id={} label={} at_ms={} err={}",
                                object_id,
                                effective_object_id,
                                label,
                                now_ms(),
                                err
                            );
                            let mut error = results.get().init_server_error();
                            error.set_description_html(
                                format!(
                                    "<!doctype html><title>Fulfill Request Failed</title><pre>{}</pre>",
                                    escape_html(&err)
                                )
                                .as_str(),
                            );
                        }
                    }
                    Ok(())
                });
            } else if path == "api/powerbox/fulfill-export" {
                let body = pry!(params.get_content())
                    .get_content()
                    .unwrap_or(&[])
                    .to_vec();
                let export_id = match std::str::from_utf8(&body) {
                    Ok(value) => value.trim().to_string(),
                    Err(err) => {
                        let mut error = results.get().init_client_error();
                        let description =
                            format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                        error.set_status_code(
                            web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                        );
                        error.set_description_html(description.as_str());
                        return Promise::ok(());
                    }
                };
                let Some(request_descriptor_b64) = self.request_descriptor_b64.clone() else {
                    let mut error = results.get().init_client_error();
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                    );
                    error.set_description_html("not in a powerbox request session");
                    return Promise::ok(());
                };

                let app_state = self.app_state.clone();
                let sandstorm_api = self.sandstorm_api.clone();
                let session_context = self.session_context.clone();
                return Promise::from_future(async move {
                    let (_label, object_id) =
                        match create_local_proxy_for_remote_export(&app_state, &export_id).await {
                            Ok(value) => value,
                            Err(err) => {
                                let mut error = results.get().init_server_error();
                                error.set_description_html(
                                    format!(
                                        "<!doctype html><title>Fulfill Request Failed</title><pre>{}</pre>",
                                        escape_html(&err)
                                    )
                                    .as_str(),
                                );
                                return Ok(());
                            }
                        };
                    let label = match capability_label_for_object_id(&app_state, &object_id) {
                        Ok(value) => value,
                        Err(err) => {
                            let mut error = results.get().init_server_error();
                            error.set_description_html(
                                format!(
                                    "<!doctype html><title>Fulfill Request Failed</title><pre>{}</pre>",
                                    escape_html(&err)
                                )
                                .as_str(),
                            );
                            return Ok(());
                        }
                    };
                    let descriptor_source = match capability_descriptor_for_object_id(&app_state, &object_id) {
                        Ok(Some(value)) => descriptor_json_to_b64(&value).unwrap_or(request_descriptor_b64.clone()),
                        Ok(None) => request_descriptor_b64.clone(),
                        Err(err) => {
                            let mut error = results.get().init_server_error();
                            error.set_description_html(
                                format!(
                                    "<!doctype html><title>Fulfill Request Failed</title><pre>{}</pre>",
                                    escape_html(&err)
                                )
                                .as_str(),
                            );
                            return Ok(());
                        }
                    };
                    let cap = match restore_app_object_capability(
                        sandstorm_api.clone(),
                        &app_state,
                        &object_id,
                    )
                    .await
                    {
                        Ok(cap) => cap,
                        Err(err) => {
                            let mut error = results.get().init_server_error();
                            error.set_description_html(
                                format!(
                                    "<!doctype html><title>Fulfill Request Failed</title><pre>{}</pre>",
                                    escape_html(&err)
                                )
                                .as_str(),
                            );
                            return Ok(());
                        }
                    };
                    match fulfill_powerbox_request_with_capability(
                        session_context,
                        cap,
                        &label,
                        &descriptor_source,
                    )
                    .await
                    {
                        Ok(_) => {
                            let mut content = results.get().init_content();
                            content.set_status_code(
                                web_session_capnp::web_session::response::SuccessCode::Ok,
                            );
                            content.set_mime_type("application/json");
                            content.init_body().set_bytes(br#"{"ok":true}"#);
                        }
                        Err(err) => {
                            let mut error = results.get().init_server_error();
                            error.set_description_html(
                                format!(
                                    "<!doctype html><title>Fulfill Request Failed</title><pre>{}</pre>",
                                    escape_html(&err)
                                )
                                .as_str(),
                            );
                        }
                    }
                    Ok(())
                });
            } else if path == "api/saved-cap/drop-object" {
                let body = pry!(params.get_content())
                    .get_content()
                    .unwrap_or(&[])
                    .to_vec();
                let object_id = match std::str::from_utf8(&body) {
                    Ok(value) => value.trim().to_string(),
                    Err(err) => {
                        let mut error = results.get().init_client_error();
                        let description =
                            format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                        error.set_status_code(
                            web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                        );
                        error.set_description_html(description.as_str());
                        return Promise::ok(());
                    }
                };

                let app_state = self.app_state.clone();
                return Promise::from_future(async move {
                    match drop_received_remote_capability(&app_state, &object_id) {
                        Ok(true) => {
                            results.get().init_no_content();
                        }
                        Ok(false) => {
                            let mut error = results.get().init_client_error();
                            error.set_status_code(
                                web_session_capnp::web_session::response::ClientErrorCode::NotFound,
                            );
                        }
                        Err(err) => {
                            let mut error = results.get().init_server_error();
                            let description = format!(
                                "<!doctype html><title>Drop Failed</title><pre>{}</pre>",
                                escape_html(&err)
                            );
                            error.set_description_html(description.as_str());
                        }
                    }
                    Ok(())
                });
            } else {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::NotFound,
                );
                return Promise::ok(());
            }
        }

        let body = pry!(params.get_content())
            .get_content()
            .unwrap_or(&[])
            .to_vec();
        let saved_token_hex = match std::str::from_utf8(&body) {
            Ok(value) => value.trim().to_string(),
            Err(err) => {
                let mut error = results.get().init_client_error();
                let description = format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                );
                error.set_description_html(description.as_str());
                return Promise::ok(());
            }
        };

        let sandstorm_api = self.sandstorm_api.clone();
        Promise::from_future(async move {
            let outcome = restore_saved_capability(sandstorm_api, &saved_token_hex).await;
            match outcome {
                Ok(()) => {
                    results.get().init_no_content();
                }
                Err(err) => {
                    let mut error = results.get().init_server_error();
                    let description = format!(
                        "<!doctype html><title>Restore Failed</title><pre>{}</pre>",
                        escape_html(&err)
                    );
                    error.set_description_html(description.as_str());
                }
            }
            Ok(())
        })
    }

    fn options(
        self: Rc<Self>,
        _: web_session_capnp::web_session::OptionsParams,
        _: web_session_capnp::web_session::OptionsResults,
    ) -> Promise<(), capnp::Error> {
        Promise::err(capnp::Error::unimplemented(
            "web_session.options not implemented".to_string(),
        ))
    }

    fn open_web_socket(
        self: Rc<Self>,
        _: web_session_capnp::web_session::OpenWebSocketParams,
        _: web_session_capnp::web_session::OpenWebSocketResults,
    ) -> Promise<(), capnp::Error> {
        Promise::err(capnp::Error::unimplemented(
            "web_session.open_web_socket not implemented".to_string(),
        ))
    }
}

impl WebSessionImpl {
    fn require_canonical_path(&self, path: &str) -> Result<(), capnp::Error> {
        for (index, component) in path.split_terminator('/').enumerate() {
            if component == "." || component == ".." || (component.is_empty() && index > 0) {
                return Err(capnp::Error::failed(format!(
                    "non-canonical path requested: {path:?}"
                )));
            }
        }
        Ok(())
    }

    fn infer_content_type(&self, filename: &str) -> &'static str {
        if filename.ends_with(".html") {
            "text/html; charset=UTF-8"
        } else if filename.ends_with(".js") {
            "text/javascript; charset=UTF-8"
        } else if filename.ends_with(".css") {
            "text/css; charset=UTF-8"
        } else if filename.ends_with(".png") {
            "image/png"
        } else if filename.ends_with(".svg") {
            "image/svg+xml; charset=UTF-8"
        } else {
            "application/octet-stream"
        }
    }

    fn read_file(
        &self,
        filename: &str,
        mut results: web_session_capnp::web_session::GetResults,
        content_type: &str,
    ) -> Result<(), capnp::Error> {
        match std::fs::File::open(filename) {
            Ok(mut file) => {
                let size = file.metadata()?.len();
                let mut content = results.get().init_content();
                content.set_status_code(web_session_capnp::web_session::response::SuccessCode::Ok);
                content.set_mime_type(content_type);
                let mut body = content.init_body().init_bytes(size as u32);
                std::io::copy(&mut file, &mut body)?;
                Ok(())
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                let mut error = results.get().init_client_error();
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::NotFound,
                );
                Ok(())
            }
            Err(err) => Err(err.into()),
        }
    }
}

fn init_localized_text(mut builder: util_capnp::localized_text::Builder<'_>, text: &str) {
    builder.set_default_text(text);
    builder.init_localizations(0);
}

async fn claim_and_save_capability(
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    session_context: grain_capnp::session_context::Client,
    request_token: &str,
    save_label: &str,
) -> Result<String, String> {
    let mut claim_req = session_context.claim_request_request();
    claim_req.get().set_request_token(request_token);
    claim_req.get().init_required_permissions(0);
    let claim_resp = claim_req
        .send()
        .promise
        .await
        .map_err(|err| format!("claimRequest() failed: {err}"))?;
    let claimed_cap = claim_resp
        .get()
        .map_err(|err| format!("failed to decode claimRequest() response: {err}"))?
        .get_cap();

    let mut save_req = sandstorm_api.save_request();
    save_req
        .get()
        .get_cap()
        .set_as(claimed_cap)
        .map_err(|err| format!("failed to set save() capability parameter: {err}"))?;
    init_localized_text(save_req.get().init_label(), save_label);

    let save_resp = save_req
        .send()
        .promise
        .await
        .map_err(|err| format!("SandstormApi.save() failed: {err}"))?;
    let token = save_resp
        .get()
        .map_err(|err| format!("failed to decode save() response: {err}"))?
        .get_token()
        .map_err(|err| format!("save() returned no token: {err}"))?;

    Ok(hex_encode(token))
}

async fn save_capability(
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    cap: capnp::capability::Client,
    save_label: &str,
) -> Result<String, String> {
    let mut save_req = sandstorm_api.save_request();
    save_req.get().get_cap().set_as_capability(cap.hook);
    init_localized_text(save_req.get().init_label(), save_label);

    let save_resp = save_req
        .send()
        .promise
        .await
        .map_err(|err| format!("SandstormApi.save() failed: {err}"))?;
    let token = save_resp
        .get()
        .map_err(|err| format!("failed to decode save() response: {err}"))?
        .get_token()
        .map_err(|err| format!("save() returned no token: {err}"))?;

    Ok(hex_encode(token))
}

async fn save_and_restore_capability(
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    cap: capnp::capability::Client,
    save_label: &str,
) -> Result<capnp::capability::Client, String> {
    let saved_token = save_capability(sandstorm_api.clone(), cap, save_label).await?;
    let token = hex_decode(&saved_token)?;
    SandstormBackend::new(sandstorm_api)
        .restore_capability(&token)
        .await
}

async fn restore_saved_capability(
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    saved_token_hex: &str,
) -> Result<(), String> {
    let token = hex_decode(saved_token_hex)?;
    let mut restore_req = sandstorm_api.restore_request();
    restore_req.get().set_token(&token);
    let restore_resp = restore_req
        .send()
        .promise
        .await
        .map_err(|err| format!("SandstormApi.restore() failed: {err}"))?;
    restore_resp
        .get()
        .map_err(|err| format!("failed to decode restore() response: {err}"))?
        .get_cap();
    Ok(())
}

async fn restore_app_object_capability(
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    app_state: &Arc<Mutex<AppState>>,
    object_id: &str,
) -> Result<capnp::capability::Client, String> {
    app_core(app_state)
        .restore_object_capability(sandstorm_api, object_id)
        .await
}

async fn restore_saved_ip_network(
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    saved_token_hex: &str,
) -> Result<ip_capnp::ip_network::Client, String> {
    let token = hex_decode(saved_token_hex)?;
    SandstormBackend::new(sandstorm_api)
        .restore_ip_network(&token)
        .await
}

async fn restore_saved_ip_interface(
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    saved_token_hex: &str,
) -> Result<ip_capnp::ip_interface::Client, String> {
    let token = hex_decode(saved_token_hex)?;
    SandstormBackend::new(sandstorm_api)
        .restore_ip_interface(&token)
        .await
}

async fn restore_saved_api_session(
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    saved_token_hex: &str,
) -> Result<api_session_capnp::api_session::Client, String> {
    let token = hex_decode(saved_token_hex)?;
    SandstormBackend::new(sandstorm_api)
        .restore_api_session(&token)
        .await
}

async fn restore_saved_generic_capability(
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    saved_token_hex: &str,
) -> Result<capnp::capability::Client, String> {
    let token = hex_decode(saved_token_hex)?;
    SandstormBackend::new(sandstorm_api)
        .restore_capability(&token)
        .await
}

fn load_saved_capabilities() -> Result<Vec<SavedCapability>, String> {
    Ok(app_storage()
        .load_saved_capabilities()?
        .into_iter()
        .map(|row: SavedCapabilityRecord| SavedCapability {
            id: row.id,
            label: row.label,
            saved_token: row.saved_token,
            created_at_ms: row.created_at_ms,
            descriptor_json: row.descriptor_json,
        })
        .collect())
}

fn load_saved_capability_by_id(id: &str) -> Result<Option<SavedCapability>, String> {
    for saved_cap in load_saved_capabilities()? {
        if saved_cap.id == id {
            return Ok(Some(saved_cap));
        }
    }
    Ok(None)
}

fn persist_saved_capability(
    label: &str,
    saved_token: &str,
    descriptor_json: Option<&str>,
) -> Result<SavedCapability, String> {
    let saved_cap = SavedCapability {
        id: make_saved_cap_id(),
        label: label.to_string(),
        saved_token: saved_token.to_string(),
        created_at_ms: now_ms(),
        descriptor_json: descriptor_json.map(|value| value.to_string()),
    };
    app_storage().persist_saved_capability(&SavedCapabilityRecord {
        id: saved_cap.id.clone(),
        label: saved_cap.label.clone(),
        saved_token: saved_cap.saved_token.clone(),
        created_at_ms: saved_cap.created_at_ms,
        descriptor_json: saved_cap.descriptor_json.clone(),
    })?;
    Ok(saved_cap)
}

pub(crate) fn make_shared_cap_id() -> String {
    format!("shared-cap-{}", now_ms())
}

fn load_shared_capabilities() -> Result<Vec<SharedCapability>, String> {
    Ok(app_storage()
        .load_shared_capabilities()?
        .into_iter()
        .map(|row: SharedCapabilityRecord| -> Result<SharedCapability, String> {
            let saved_cap =
                load_saved_capability_by_id(&row.saved_cap_id)?.unwrap_or(SavedCapability {
                    id: row.saved_cap_id,
                    label: row.label.clone(),
                    saved_token: String::new(),
                    created_at_ms: 0,
                    descriptor_json: row.descriptor_json.clone(),
                });
            Ok(SharedCapability {
                id: row.id,
                label: row.label,
                kind: row.kind,
                enabled: row.enabled,
                created_at_ms: row.created_at_ms,
                saved_cap,
            })
        })
        .collect::<Result<Vec<_>, _>>()?)
}

fn persist_shared_capabilities(records: &[SharedCapability]) -> Result<(), String> {
    let rows = records
        .iter()
        .map(|record| SharedCapabilityRecord {
            id: record.id.clone(),
            saved_cap_id: record.saved_cap.id.clone(),
            label: record.label.clone(),
            kind: record.kind,
            type_tag: Some(shared_capability_type_tag(record)),
            enabled: record.enabled,
            created_at_ms: record.created_at_ms,
            descriptor_json: record.saved_cap.descriptor_json.clone(),
        })
        .collect::<Vec<_>>();
    app_storage().persist_shared_capabilities(&rows)
}

fn first_enabled_shared_capability(
    shared_caps: &[SharedCapability],
    kind: SharedCapabilityKind,
) -> Option<SavedCapability> {
    shared_caps
        .iter()
        .find(|cap| cap.kind == kind && cap.enabled)
        .map(|cap| cap.saved_cap.clone())
}

fn load_configured_exported_capability(
    path: &Path,
    fallback_label: &str,
) -> Result<Option<SavedCapability>, String> {
    match app_storage().load_text_file(path)? {
        Some(trimmed) => Ok(load_saved_capability_by_id(&trimmed)?.or(Some(SavedCapability {
            id: trimmed,
            label: fallback_label.to_string(),
            saved_token: String::new(),
            created_at_ms: 0,
            descriptor_json: None,
        }))),
        None => Ok(None),
    }
}

fn persist_configured_exported_capability(path: &Path, saved_cap_id: &str) -> Result<(), String> {
    app_storage().persist_text_file(path, saved_cap_id)
}

fn clear_configured_exported_capability(path: &Path) -> Result<(), String> {
    app_storage().clear_file(path)
}

fn load_persisted_received_capabilities() -> Result<Vec<PersistedReceivedCapability>, String> {
    Ok(app_storage()
        .load_persisted_received_capabilities()?
        .into_iter()
        .map(|record: PersistedReceivedCapabilityRecord| PersistedReceivedCapability {
            object_id: record.object_id,
            export_id: record.export_id,
            label: record.label,
            kind: match record.kind {
                ReceivedCapabilityKind::IpNetwork => ImportedRemoteCapabilityKind::IpNetwork,
                ReceivedCapabilityKind::ApiSession => ImportedRemoteCapabilityKind::ApiSession,
                ReceivedCapabilityKind::Other => ImportedRemoteCapabilityKind::Other,
            },
            type_tag: record.type_tag.unwrap_or_else(|| imported_type_tag_for_kind(match record.kind {
                ReceivedCapabilityKind::IpNetwork => ImportedRemoteCapabilityKind::IpNetwork,
                ReceivedCapabilityKind::ApiSession => ImportedRemoteCapabilityKind::ApiSession,
                ReceivedCapabilityKind::Other => ImportedRemoteCapabilityKind::Other,
            })),
            descriptor_json: record.descriptor_json,
            enabled: record.enabled,
        })
        .collect())
}

fn load_local_proxy_capabilities() -> Result<Vec<LocalProxyCapability>, String> {
    Ok(app_storage()
        .load_local_proxy_capabilities()?
        .into_iter()
        .map(|record: LocalProxyCapabilityRecord| LocalProxyCapability {
            object_id: record.object_id,
            peer_node_id: record.peer_node_id,
            target_kind: match record.target_kind {
                LocalProxyTargetKind::ExportId => LocalProxyTargetKindRuntime::ExportId,
                LocalProxyTargetKind::RemoteObjectId => LocalProxyTargetKindRuntime::RemoteObjectId,
            },
            target_id: record.target_id,
            label: record.label,
            kind: record.kind,
            type_tag: record
                .type_tag
                .unwrap_or_else(|| imported_type_tag_for_kind(ImportedRemoteCapabilityKind::Other)),
            descriptor_json: record.descriptor_json,
            created_at_ms: record.created_at_ms,
        })
        .collect())
}

fn load_registered_remote_capabilities() -> Result<Vec<RegisteredRemoteCapability>, String> {
    Ok(app_storage()
        .load_registered_remote_capabilities()?
        .into_iter()
        .map(|record: RegisteredRemoteCapabilityRecord| RegisteredRemoteCapability {
            remote_object_id: record.remote_object_id,
            label: record.label,
            kind: match record.kind {
                ReceivedCapabilityKind::IpNetwork => ImportedRemoteCapabilityKind::IpNetwork,
                ReceivedCapabilityKind::ApiSession => ImportedRemoteCapabilityKind::ApiSession,
                ReceivedCapabilityKind::Other => ImportedRemoteCapabilityKind::Other,
            },
            type_tag: record
                .type_tag
                .unwrap_or_else(|| imported_type_tag_for_kind(ImportedRemoteCapabilityKind::Other)),
            descriptor_json: record.descriptor_json,
            durability: record.durability,
            saved_token: record.saved_token,
            created_at_ms: record.created_at_ms,
            client: None,
        })
        .collect())
}

fn parse_remote_cap_numeric_suffix(object_id: &str) -> Option<u64> {
    object_id
        .strip_prefix("remote-cap-")
        .and_then(|value| value.parse::<u64>().ok())
}

fn max_persisted_received_cap_id(records: &[PersistedReceivedCapability]) -> Option<u64> {
    records
        .iter()
        .filter_map(|record| parse_remote_cap_numeric_suffix(&record.object_id))
        .max()
}

fn parse_local_proxy_cap_numeric_suffix(object_id: &str) -> Option<u64> {
    object_id
        .strip_prefix("local-proxy-cap-")
        .and_then(|value| value.parse::<u64>().ok())
}

fn max_local_proxy_cap_id(records: &[LocalProxyCapability]) -> Option<u64> {
    records
        .iter()
        .filter_map(|record| parse_local_proxy_cap_numeric_suffix(&record.object_id))
        .max()
}

fn parse_registered_remote_cap_numeric_suffix(object_id: &str) -> Option<u64> {
    object_id
        .strip_prefix("remote-registered-cap-")
        .and_then(|value| value.parse::<u64>().ok())
}

fn max_registered_remote_cap_id(records: &[RegisteredRemoteCapability]) -> Option<u64> {
    records
        .iter()
        .filter_map(|record| parse_registered_remote_cap_numeric_suffix(&record.remote_object_id))
        .max()
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn hex_decode(value: &str) -> Result<Vec<u8>, String> {
    if value.len() % 2 != 0 {
        return Err("saved token hex has odd length".to_string());
    }

    let mut out = Vec::with_capacity(value.len() / 2);
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let hi = hex_nibble(bytes[index])?;
        let lo = hex_nibble(bytes[index + 1])?;
        out.push((hi << 4) | lo);
        index += 2;
    }
    Ok(out)
}

fn hex_nibble(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(format!("invalid hex digit: {}", byte as char)),
    }
}

fn json_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            ch if ch <= '\u{1f}' => {
                use std::fmt::Write as _;
                let _ = write!(&mut out, "\\u{:04x}", ch as u32);
            }
            _ => out.push(ch),
        }
    }
    out
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

#[derive(Debug)]
#[derive(Clone)]
struct SavedCapability {
    id: String,
    label: String,
    saved_token: String,
    created_at_ms: u64,
    descriptor_json: Option<String>,
}

#[derive(Clone)]
pub(crate) struct SharedCapability {
    pub(crate) id: String,
    pub(crate) label: String,
    pub(crate) kind: SharedCapabilityKind,
    pub(crate) enabled: bool,
    pub(crate) created_at_ms: u64,
    pub(crate) saved_cap: SavedCapability,
}

#[derive(Clone)]
struct PeerRpcExport {
    id: String,
    label: String,
}

#[derive(Clone)]
struct PeerRpcCapabilityExport {
    id: String,
    label: String,
    kind: SharedCapabilityKind,
    type_tag: String,
    descriptor_json: Option<String>,
}

struct PeerRpcSession {
    session_id: u64,
    remote_node_id: String,
    connection: iroh::endpoint::Connection,
    remote_bootstrap: tunnel_capnp::peer_bootstrap::Client,
    capability_exports: Vec<PeerRpcCapabilityExport>,
    ip_network_exports: Vec<PeerRpcExport>,
    api_session_exports: Vec<PeerRpcExport>,
}

struct ImportedRemoteIpNetwork {
    object_id: String,
    export_id: String,
    label: String,
    client: ip_capnp::ip_network::Client,
}

struct ImportedRemoteApiSession {
    object_id: String,
    export_id: String,
    label: String,
    client: api_session_capnp::api_session::Client,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ImportedRemoteCapabilityKind {
    IpNetwork,
    ApiSession,
    Other,
}

#[derive(Clone)]
struct PersistedReceivedCapability {
    object_id: String,
    export_id: String,
    label: String,
    kind: ImportedRemoteCapabilityKind,
    type_tag: String,
    descriptor_json: Option<String>,
    enabled: bool,
}

#[derive(Clone)]
struct ImportedRemoteCapability {
    object_id: String,
    export_id: String,
    label: String,
    kind: ImportedRemoteCapabilityKind,
    type_tag: String,
    descriptor_json: Option<String>,
    client: capnp::capability::Client,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum LocalProxyTargetKindRuntime {
    ExportId,
    RemoteObjectId,
}

#[derive(Clone)]
pub(crate) struct LocalProxyCapability {
    pub(crate) object_id: String,
    pub(crate) peer_node_id: String,
    pub(crate) target_kind: LocalProxyTargetKindRuntime,
    pub(crate) target_id: String,
    pub(crate) label: String,
    pub(crate) kind: SharedCapabilityKind,
    pub(crate) type_tag: String,
    pub(crate) descriptor_json: Option<String>,
    pub(crate) created_at_ms: u64,
}

#[derive(Clone)]
struct RegisteredRemoteCapability {
    remote_object_id: String,
    label: String,
    kind: ImportedRemoteCapabilityKind,
    type_tag: String,
    descriptor_json: Option<String>,
    durability: RegisteredRemoteCapabilityDurability,
    saved_token: Option<String>,
    created_at_ms: u64,
    client: Option<capnp::capability::Client>,
}

fn imported_kind_to_tunnel_kind(kind: ImportedRemoteCapabilityKind) -> tunnel_capnp::CapabilityKind {
    match kind {
        ImportedRemoteCapabilityKind::IpNetwork => tunnel_capnp::CapabilityKind::IpNetwork,
        ImportedRemoteCapabilityKind::ApiSession => tunnel_capnp::CapabilityKind::ApiSession,
        ImportedRemoteCapabilityKind::Other => tunnel_capnp::CapabilityKind::Other,
    }
}

struct ExportedIpNetworkState {
    client: ip_capnp::ip_network::Client,
}

struct ExportedApiSessionState {
    client: api_session_capnp::api_session::Client,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PairControlDecision {
    Accepted,
    Rejected,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PairControlMessage {
    Request { version: u16 },
    Response {
        version: u16,
        decision: PairControlDecision,
    },
}

struct PendingIncomingConnection {
    remote_node_id: String,
    connection: iroh::endpoint::Connection,
    handshake_send: iroh::endpoint::SendStream,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PairingStatus {
    Disconnected,
    Connecting,
    AwaitingRemoteAccept,
    IncomingRequest,
    Connected,
    Disabled,
    Error,
}

impl PairingStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Disconnected => "disconnected",
            Self::Connecting => "connecting",
            Self::AwaitingRemoteAccept => "awaitingRemoteAccept",
            Self::IncomingRequest => "incomingRequest",
            Self::Connected => "connected",
            Self::Disabled => "disabled",
            Self::Error => "error",
        }
    }
}

fn persist_registered_remote_capabilities(
    storage: &Storage,
    records: impl IntoIterator<Item = RegisteredRemoteCapability>,
) -> Result<(), String> {
    let rows = records
        .into_iter()
        .map(|record| RegisteredRemoteCapabilityRecord {
            remote_object_id: record.remote_object_id,
            label: record.label,
            kind: match record.kind {
                ImportedRemoteCapabilityKind::IpNetwork => ReceivedCapabilityKind::IpNetwork,
                ImportedRemoteCapabilityKind::ApiSession => ReceivedCapabilityKind::ApiSession,
                ImportedRemoteCapabilityKind::Other => ReceivedCapabilityKind::Other,
            },
            type_tag: Some(record.type_tag),
            descriptor_json: record.descriptor_json,
            durability: record.durability,
            saved_token: record.saved_token,
            created_at_ms: record.created_at_ms,
        })
        .collect::<Vec<_>>();
    storage.persist_registered_remote_capability_registry(&rows)
}

struct AppState {
    storage: Storage,
    iroh_identity: IrohIdentity,
    iroh_endpoint: Option<Endpoint>,
    iroh_endpoint_addr: IrohEndpointAddrSummary,
    iroh_endpoint_error: Option<String>,
    raw_udp_interface: Option<SavedCapability>,
    raw_udp_interface_source: Option<String>,
    remote_ticket: Option<String>,
    approved_peer_node_id: Option<String>,
    pending_incoming_peer_node_id: Option<String>,
    pending_outgoing_peer_node_id: Option<String>,
    pending_incoming_connection: Option<PendingIncomingConnection>,
    tunnel_enabled: bool,
    pairing_status: PairingStatus,
    shared_caps: Vec<SharedCapability>,
    exported_ip_network: Option<SavedCapability>,
    exported_api_session: Option<SavedCapability>,
    exported_caps_live: HashMap<String, capnp::capability::Client>,
    exported_ip_network_live: HashMap<String, ExportedIpNetworkState>,
    exported_api_session_live: HashMap<String, ExportedApiSessionState>,
    peer_rpc_session: Option<PeerRpcSession>,
    imported_remote_ip_network: Option<ImportedRemoteIpNetwork>,
    imported_remote_api_session: Option<ImportedRemoteApiSession>,
    imported_remote_caps: HashMap<String, ImportedRemoteCapability>,
    persisted_received_caps: Vec<PersistedReceivedCapability>,
    local_proxy_caps: Vec<LocalProxyCapability>,
    registered_remote_caps: HashMap<String, RegisteredRemoteCapability>,
    registered_remote_hook_object_ids: HashMap<usize, String>,
    local_proxy_hook_object_ids: HashMap<usize, String>,
    next_peer_rpc_session_id: u64,
    next_imported_remote_cap_id: u64,
    next_local_proxy_cap_id: u64,
    next_registered_remote_cap_id: u64,
    peer_rpc_error: Option<String>,
    active_tcp_sessions: HashMap<String, Arc<SavedIpNetworkTcpSession>>,
    next_tcp_session_id: u64,
}

impl AppState {
    fn initialize() -> Result<Self, String> {
        let iroh_identity = load_or_create_iroh_identity()?;
        let remote_ticket = load_remote_ticket()?;
        let approved_peer_node_id = load_approved_peer_node_id()?;
        let tunnel_enabled = load_tunnel_enabled()?;
        let legacy_exported_ip_network = load_configured_exported_capability(
            exported_ip_network_id_path().as_path(),
            "Configured IpNetwork export",
        )?;
        let legacy_exported_api_session = load_configured_exported_capability(
            exported_api_session_id_path().as_path(),
            "Configured ApiSession export",
        )?;
        let mut shared_caps = load_shared_capabilities()?;
        if shared_caps.is_empty() {
            if let Some(saved_cap) = legacy_exported_ip_network.clone() {
                shared_caps.push(SharedCapability {
                    id: make_shared_cap_id(),
                    label: saved_cap.label.clone(),
                    kind: SharedCapabilityKind::IpNetwork,
                    enabled: true,
                    created_at_ms: saved_cap.created_at_ms,
                    saved_cap,
                });
            }
            if let Some(saved_cap) = legacy_exported_api_session.clone() {
                shared_caps.push(SharedCapability {
                    id: make_shared_cap_id(),
                    label: saved_cap.label.clone(),
                    kind: SharedCapabilityKind::ApiSession,
                    enabled: true,
                    created_at_ms: saved_cap.created_at_ms,
                    saved_cap,
                });
            }
            if !shared_caps.is_empty() {
                persist_shared_capabilities(&shared_caps)?;
            }
        }
        let exported_ip_network =
            first_enabled_shared_capability(&shared_caps, SharedCapabilityKind::IpNetwork);
        let exported_api_session =
            first_enabled_shared_capability(&shared_caps, SharedCapabilityKind::ApiSession);
        let persisted_received_caps = load_persisted_received_capabilities()?;
        let mut local_proxy_caps = load_local_proxy_capabilities()?;
        let had_remote_object_local_proxies = local_proxy_caps
            .iter()
            .any(|record| record.target_kind == LocalProxyTargetKindRuntime::RemoteObjectId);
        if had_remote_object_local_proxies {
            local_proxy_caps.retain(|record| record.target_kind == LocalProxyTargetKindRuntime::ExportId);
            let records = local_proxy_caps
                .iter()
                .map(|cap| LocalProxyCapabilityRecord {
                    object_id: cap.object_id.clone(),
                    peer_node_id: cap.peer_node_id.clone(),
                    target_kind: LocalProxyTargetKind::ExportId,
                    target_id: cap.target_id.clone(),
                    label: cap.label.clone(),
                    kind: cap.kind,
                    type_tag: Some(cap.type_tag.clone()),
                    descriptor_json: cap.descriptor_json.clone(),
                    created_at_ms: cap.created_at_ms,
                })
                .collect::<Vec<_>>();
            app_storage().persist_local_proxy_capability_registry(&records)?;
        }
        let registered_remote_caps = load_registered_remote_capabilities()?;
        let next_imported_remote_cap_id =
            max_persisted_received_cap_id(&persisted_received_caps).unwrap_or(0);
        let next_local_proxy_cap_id = max_local_proxy_cap_id(&local_proxy_caps).unwrap_or(0);
        let next_registered_remote_cap_id =
            max_registered_remote_cap_id(&registered_remote_caps).unwrap_or(0);
        Ok(Self {
            storage: app_storage(),
            iroh_identity,
            iroh_endpoint: None,
            iroh_endpoint_addr: IrohEndpointAddrSummary::empty(),
            iroh_endpoint_error: None,
            raw_udp_interface: None,
            raw_udp_interface_source: None,
            remote_ticket,
            approved_peer_node_id,
            pending_incoming_peer_node_id: None,
            pending_outgoing_peer_node_id: None,
            pending_incoming_connection: None,
            tunnel_enabled,
            pairing_status: if tunnel_enabled {
                PairingStatus::Disconnected
            } else {
                PairingStatus::Disabled
            },
            shared_caps,
            exported_ip_network,
            exported_api_session,
            exported_caps_live: HashMap::new(),
            exported_ip_network_live: HashMap::new(),
            exported_api_session_live: HashMap::new(),
            peer_rpc_session: None,
            imported_remote_ip_network: None,
            imported_remote_api_session: None,
            imported_remote_caps: HashMap::new(),
            persisted_received_caps,
            local_proxy_caps,
            registered_remote_caps: registered_remote_caps
                .into_iter()
                .map(|record| (record.remote_object_id.clone(), record))
                .collect(),
            registered_remote_hook_object_ids: HashMap::new(),
            local_proxy_hook_object_ids: HashMap::new(),
            next_peer_rpc_session_id: 0,
            next_imported_remote_cap_id,
            next_local_proxy_cap_id,
            next_registered_remote_cap_id,
            peer_rpc_error: None,
            active_tcp_sessions: HashMap::new(),
            next_tcp_session_id: 0,
        })
    }
}

async fn initialize_iroh_endpoint(
    app_state: Arc<Mutex<AppState>>,
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
) -> Result<(), String> {
    let (secret_key, old_endpoint, pending_incoming) = {
        let mut guard = app_state
            .lock()
            .map_err(|_| "app state lock poisoned".to_string())?;
        if let Some(session) = guard.peer_rpc_session.take() {
            session
                .connection
                .close(0u32.into(), b"local endpoint reinitialized");
        }
        let pending_incoming = guard.pending_incoming_connection.take();
        guard.pending_incoming_peer_node_id = None;
        guard.pending_outgoing_peer_node_id = None;
        guard.imported_remote_ip_network = None;
        guard.imported_remote_api_session = None;
        guard.peer_rpc_error = None;
        guard.pairing_status = if guard.tunnel_enabled {
            PairingStatus::Disconnected
        } else {
            PairingStatus::Disabled
        };
        (
            guard.iroh_identity.secret_key.clone(),
            guard.iroh_endpoint.take(),
            pending_incoming,
        )
    };
    if let Some(pending_incoming) = pending_incoming {
        pending_incoming
            .connection
            .close(0u32.into(), b"local endpoint reinitialized");
    }
    if let Some(old_endpoint) = old_endpoint {
        old_endpoint.close().await;
    }

    let bind_result =
        bind_local_iroh_endpoint(app_state.clone(), sandstorm_api.clone(), &secret_key).await;
    let (raw_udp_interface, raw_udp_interface_source) =
        load_configured_raw_udp_interface_state()?;
    {
        let mut guard = app_state
            .lock()
            .map_err(|_| "app state lock poisoned".to_string())?;
        guard.raw_udp_interface = raw_udp_interface;
        guard.raw_udp_interface_source = raw_udp_interface_source;
        match bind_result {
            Ok((endpoint, endpoint_addr)) => {
                guard.iroh_endpoint = Some(endpoint);
                guard.iroh_endpoint_addr = endpoint_addr;
                guard.iroh_endpoint_error = None;
            }
            Err(err) => {
                guard.iroh_endpoint = None;
                guard.iroh_endpoint_addr = IrohEndpointAddrSummary::empty();
                guard.iroh_endpoint_error = Some(err);
            }
        }
    }

    if should_auto_reconnect_tunnel(&app_state)? {
        let app_state = app_state.clone();
        let sandstorm_api = sandstorm_api.clone();
        tokio::task::spawn_local(async move {
            eprintln!("iroh startup: beginning automatic tunnel reconnect");
            if let Err(err) = begin_pair_connection(app_state, sandstorm_api).await {
                eprintln!("iroh startup: automatic tunnel reconnect failed: {err}");
            } else {
                eprintln!("iroh startup: automatic tunnel reconnect initiated");
            }
        });
    }

    Ok(())
}

fn should_auto_reconnect_tunnel(app_state: &Arc<Mutex<AppState>>) -> Result<bool, String> {
    let guard = app_state
        .lock()
        .map_err(|_| "app state lock poisoned".to_string())?;
    if !guard.tunnel_enabled {
        return Ok(false);
    }
    if guard.peer_rpc_session.is_some()
        || guard.pending_outgoing_peer_node_id.is_some()
        || guard.pending_incoming_peer_node_id.is_some()
    {
        return Ok(false);
    }
    let Some(remote_ticket) = guard.remote_ticket.as_ref() else {
        return Ok(false);
    };
    let Some(approved_peer_node_id) = guard.approved_peer_node_id.as_ref() else {
        return Ok(false);
    };

    let remote_addr = parse_remote_ticket(remote_ticket)?;
    let remote_node_id = remote_addr.id.to_string();
    if &remote_node_id != approved_peer_node_id {
        return Ok(false);
    }

    Ok(guard.iroh_identity.node_id >= remote_node_id)
}

fn load_configured_raw_udp_interface_state() -> Result<(Option<SavedCapability>, Option<String>), String>
{
    if let Some(saved_token) = load_saved_raw_udp_interface_token()? {
        return Ok((
            load_saved_capability_by_token(&saved_token)?.or(Some(SavedCapability {
                id: String::new(),
                label: "Configured IpInterface".to_string(),
                saved_token,
                created_at_ms: 0,
                descriptor_json: None,
            })),
            Some("saved".to_string()),
        ));
    }

    match std::env::var(IROH_SANDSTORM_RAW_UDP_INTERFACE_TOKEN_ENV) {
        Ok(token) if !token.trim().is_empty() => Ok((
            Some(SavedCapability {
                id: String::new(),
                label: "Env-configured IpInterface".to_string(),
                saved_token: token.trim().to_string(),
                created_at_ms: 0,
                descriptor_json: None,
            }),
            Some("env".to_string()),
        )),
        Ok(_) => Ok((None, None)),
        Err(std::env::VarError::NotPresent) => Ok((None, None)),
        Err(err) => Err(format!(
            "failed to read {IROH_SANDSTORM_RAW_UDP_INTERFACE_TOKEN_ENV}: {err}"
        )),
    }
}

fn load_saved_raw_udp_interface_token() -> Result<Option<String>, String> {
    let path = raw_udp_interface_token_path();
    match std::fs::read_to_string(&path) {
        Ok(value) => {
            let trimmed = value.trim().to_string();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed))
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(format!("failed to read raw udp interface token {}: {err}", path.display())),
    }
}

fn load_saved_raw_udp_port() -> Result<Option<u16>, String> {
    let path = raw_udp_port_path();
    match std::fs::read_to_string(&path) {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                trimmed
                    .parse::<u16>()
                    .map(Some)
                    .map_err(|_| format!("failed to parse persisted raw udp port: {trimmed:?}"))
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(format!("failed to read raw udp port {}: {err}", path.display())),
    }
}

fn persist_raw_udp_interface_token(saved_token: &str) -> Result<(), String> {
    std::fs::create_dir_all(STATE_DIR)
        .map_err(|err| format!("failed to create state directory: {err}"))?;
    let path = raw_udp_interface_token_path();
    std::fs::write(&path, format!("{saved_token}\n"))
        .map_err(|err| format!("failed to persist raw udp interface token {}: {err}", path.display()))
}

fn persist_raw_udp_port(port: u16) -> Result<(), String> {
    std::fs::create_dir_all(STATE_DIR)
        .map_err(|err| format!("failed to create state directory: {err}"))?;
    let path = raw_udp_port_path();
    std::fs::write(&path, format!("{port}\n"))
        .map_err(|err| format!("failed to persist raw udp port {}: {err}", path.display()))
}

fn clear_persisted_raw_udp_interface_token() -> Result<(), String> {
    let path = raw_udp_interface_token_path();
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(format!("failed to clear raw udp interface token {}: {err}", path.display())),
    }
}

fn clear_persisted_raw_udp_port() -> Result<(), String> {
    let path = raw_udp_port_path();
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(format!("failed to clear raw udp port {}: {err}", path.display())),
    }
}

fn load_saved_capability_by_token(saved_token: &str) -> Result<Option<SavedCapability>, String> {
    Ok(load_saved_capabilities()?
        .into_iter()
        .find(|saved_cap| saved_cap.saved_token == saved_token))
}

fn require_saved_capability_by_token(saved_token: &str) -> Result<SavedCapability, String> {
    match load_saved_capability_by_token(saved_token)? {
        Some(saved_cap) => Ok(saved_cap),
        None => Err("saved capability token not found".to_string()),
    }
}

async fn configure_raw_udp_interface_binding<
    Validate,
    ValidateFut,
    Rebind,
    RebindFut,
>(
    saved_cap: &SavedCapability,
    validate: Validate,
    persist: impl FnOnce(&str) -> Result<(), String>,
    rebind: Rebind,
) -> Result<(), String>
where
    Validate: FnOnce(String) -> ValidateFut,
    ValidateFut: std::future::Future<Output = Result<(), String>>,
    Rebind: FnOnce() -> RebindFut,
    RebindFut: std::future::Future<Output = Result<(), String>>,
{
    validate(saved_cap.saved_token.clone()).await?;
    persist(&saved_cap.saved_token)?;
    rebind().await
}

async fn clear_raw_udp_interface_binding<Rebind, RebindFut>(
    clear: impl FnOnce() -> Result<(), String>,
    rebind: Rebind,
) -> Result<(), String>
where
    Rebind: FnOnce() -> RebindFut,
    RebindFut: std::future::Future<Output = Result<(), String>>,
{
    clear()?;
    rebind().await
}

fn make_saved_cap_id() -> String {
    format!("cap-{}", now_ms())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn log_iroh_endpoint_summary(context: &str, endpoint_addr: &IrohEndpointAddrSummary) {
    eprintln!(
        "iroh endpoint {context}: node_id={} relay_addrs={} direct_addrs={} custom_addrs={} direct={:?} custom={:?}",
        endpoint_addr.node_id,
        endpoint_addr.relay_urls.len(),
        endpoint_addr.direct_addrs.len(),
        endpoint_addr.custom_addrs.len(),
        endpoint_addr.direct_addrs,
        endpoint_addr.custom_addrs,
    );
}

fn load_or_create_iroh_identity() -> Result<IrohIdentity, String> {
    std::fs::create_dir_all(STATE_DIR)
        .map_err(|err| format!("failed to create state directory: {err}"))?;

    let secret_key_path = iroh_secret_key_path();
    let secret_key = match std::fs::read(&secret_key_path) {
        Ok(bytes) => {
            if bytes.len() != 32 {
                return Err(format!(
                    "invalid iroh secret key length at {}: expected 32 bytes, got {}",
                    secret_key_path.display(),
                    bytes.len()
                ));
            }
            let mut raw = [0u8; 32];
            raw.copy_from_slice(&bytes);
            SecretKey::from_bytes(&raw)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            let mut raw = [0u8; 32];
            fill_random(&mut raw)?;
            let secret_key = SecretKey::from_bytes(&raw);
            std::fs::write(&secret_key_path, raw).map_err(|err| {
                format!(
                    "failed to persist iroh secret key {}: {err}",
                    secret_key_path.display()
                )
            })?;
            secret_key
        }
        Err(err) => {
            return Err(format!(
                "failed to read iroh secret key from {}: {err}",
                secret_key_path.display()
            ));
        }
    };

    Ok(IrohIdentity {
        node_id: secret_key.public().to_string(),
        secret_key,
    })
}

fn fill_random(out: &mut [u8]) -> Result<(), String> {
    use std::io::Read as _;

    let mut file = std::fs::File::open("/dev/urandom")
        .map_err(|err| format!("failed to open /dev/urandom: {err}"))?;
    file.read_exact(out)
        .map_err(|err| format!("failed to read random bytes: {err}"))
}

struct IrohIdentity {
    node_id: String,
    secret_key: SecretKey,
}

struct IrohEndpointAddrSummary {
    node_id: String,
    relay_urls: Vec<String>,
    direct_addrs: Vec<String>,
    custom_addrs: Vec<String>,
}

impl IrohEndpointAddrSummary {
    fn empty() -> Self {
        Self {
            node_id: String::new(),
            relay_urls: Vec::new(),
            direct_addrs: Vec::new(),
            custom_addrs: Vec::new(),
        }
    }
}

#[derive(Debug)]
struct SandstormRawUdpBindConfig {
    interface_token_hex: String,
    port: u16,
}

fn parse_sandstorm_raw_udp_bind_config(
    interface_token_hex: Option<&str>,
    port: Option<&str>,
) -> Result<Option<SandstormRawUdpBindConfig>, String> {
    let Some(interface_token_hex) = interface_token_hex else {
        return Ok(None);
    };
    let interface_token_hex = interface_token_hex.trim().to_string();
    if interface_token_hex.is_empty() {
        return Err(format!(
            "{IROH_SANDSTORM_RAW_UDP_INTERFACE_TOKEN_ENV} must not be empty"
        ));
    }

    let port = match port {
        Some(value) => value
            .trim()
            .parse::<u16>()
            .map_err(|_| format!("invalid {IROH_SANDSTORM_RAW_UDP_PORT_ENV}: {value:?}"))?,
        None => 0,
    };

    Ok(Some(SandstormRawUdpBindConfig {
        interface_token_hex,
        port,
    }))
}

fn resolve_sandstorm_raw_udp_bind_config(
    saved_token_hex: Option<&str>,
    saved_port: Option<u16>,
    env_interface_token_hex: Option<&str>,
    env_port: Option<&str>,
) -> Result<Option<SandstormRawUdpBindConfig>, String> {
    if let Some(saved_token_hex) = saved_token_hex {
        let saved_port_string = saved_port.map(|port| port.to_string());
        return parse_sandstorm_raw_udp_bind_config(
            Some(saved_token_hex),
            saved_port_string.as_deref(),
        );
    }

    parse_sandstorm_raw_udp_bind_config(env_interface_token_hex, env_port)
}

fn load_sandstorm_raw_udp_bind_config() -> Result<Option<SandstormRawUdpBindConfig>, String> {
    let saved_token = load_saved_raw_udp_interface_token()?;
    let saved_port = load_saved_raw_udp_port()?;

    let interface_token_hex = match std::env::var(IROH_SANDSTORM_RAW_UDP_INTERFACE_TOKEN_ENV) {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(err) => {
            return Err(format!(
                "failed to read {IROH_SANDSTORM_RAW_UDP_INTERFACE_TOKEN_ENV}: {err}"
            ));
        }
    };
    let port = match std::env::var(IROH_SANDSTORM_RAW_UDP_PORT_ENV) {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(err) => {
            return Err(format!(
                "failed to read {IROH_SANDSTORM_RAW_UDP_PORT_ENV}: {err}"
            ));
        }
    };
    resolve_sandstorm_raw_udp_bind_config(
        saved_token.as_deref(),
        saved_port,
        interface_token_hex.as_deref(),
        port.as_deref(),
    )
}

async fn bind_local_iroh_endpoint(
    app_state: Arc<Mutex<AppState>>,
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    secret_key: &SecretKey,
) -> Result<(Endpoint, IrohEndpointAddrSummary), String> {
    if let Some(config) = load_sandstorm_raw_udp_bind_config()? {
        eprintln!(
            "iroh bind: using Sandstorm RawUdp interface token={} port={}",
            config.interface_token_hex,
            config.port
        );
        return bind_sandstorm_raw_udp_iroh_endpoint(app_state, sandstorm_api, secret_key, config)
            .await;
    }

    eprintln!("iroh bind: using native ambient UDP transports");
    let endpoint = Endpoint::builder(presets::N0)
        .alpns(vec![
            IROH_ALPN.to_vec(),
            IROH_RPC_ALPN.to_vec(),
            IROH_PAIR_ALPN.to_vec(),
        ])
        .secret_key(secret_key.clone())
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await
        .map_err(|err| format!("failed to bind local iroh endpoint: {err}"))?;
    tokio::task::spawn_local(run_iroh_accept_loop(
        endpoint.clone(),
        app_state,
        sandstorm_api,
    ));
    let endpoint_addr = summarize_endpoint_addr(endpoint.addr());
    log_iroh_endpoint_summary("bound(native)", &endpoint_addr);
    Ok((endpoint, endpoint_addr))
}

async fn bind_sandstorm_raw_udp_iroh_endpoint(
    app_state: Arc<Mutex<AppState>>,
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    secret_key: &SecretKey,
    config: SandstormRawUdpBindConfig,
) -> Result<(Endpoint, IrohEndpointAddrSummary), String> {
    eprintln!(
        "iroh bind: restoring Sandstorm IpInterface token={} for raw UDP port={}",
        config.interface_token_hex,
        config.port
    );
    let ip_interface =
        restore_saved_ip_interface(sandstorm_api.clone(), &config.interface_token_hex).await?;
    let mut bind_req = ip_interface.bind_raw_udp_request();
    bind_req.get().set_port_num(config.port);
    let bind_resp = bind_req
        .send()
        .promise
        .await
        .map_err(|err| format!("IpInterface.bindRawUdp() failed: {err}"))?;
    let raw_udp_socket = bind_resp
        .get()
        .map_err(|err| format!("failed to decode bindRawUdp() response: {err}"))?
        .get_socket()
        .map_err(|err| format!("bindRawUdp() returned no RawUdpSocket: {err}"))?;
    eprintln!("iroh bind: IpInterface.bindRawUdp() returned a RawUdpSocket");
    let raw_udp_local_addr = get_local_endpoint(&raw_udp_socket)
        .await
        .map_err(|err| format!("failed to read RawUdp local endpoint: {err}"))?;
    eprintln!("iroh bind: RawUdp local endpoint is {raw_udp_local_addr}");
    persist_raw_udp_port(raw_udp_local_addr.port())?;
    let transport =
        new_capnp_raw_udp_custom_transport(raw_udp_socket, SANDSTORM_RAW_UDP_TRANSPORT_ID)
            .await
            .map_err(|err| format!("failed to create Sandstorm custom transport: {err}"))?;
    eprintln!("iroh bind: Sandstorm RawUdp custom transport initialized");

    let endpoint = Endpoint::builder(presets::N0)
        .alpns(vec![
            IROH_ALPN.to_vec(),
            IROH_RPC_ALPN.to_vec(),
            IROH_PAIR_ALPN.to_vec(),
        ])
        .secret_key(secret_key.clone())
        .relay_mode(RelayMode::Disabled)
        .clear_ip_transports()
        .add_custom_transport(transport)
        .bind()
        .await
        .map_err(|err| format!("failed to bind Sandstorm RawUdp iroh endpoint: {err}"))?;
    tokio::task::spawn_local(run_iroh_accept_loop(
        endpoint.clone(),
        app_state,
        sandstorm_api,
    ));
    let endpoint_addr =
        summarize_endpoint_addr_with_raw_udp_fallback(endpoint.addr(), raw_udp_local_addr);
    log_iroh_endpoint_summary("bound(sandstorm-raw-udp)", &endpoint_addr);
    Ok((endpoint, endpoint_addr))
}

fn summarize_endpoint_addr(endpoint_addr: iroh::EndpointAddr) -> IrohEndpointAddrSummary {
    let mut relay_urls = Vec::new();
    let mut direct_addrs = Vec::new();
    let mut custom_addrs = Vec::new();
    for addr in endpoint_addr.addrs {
        match addr {
            TransportAddr::Relay(url) => relay_urls.push(url.to_string()),
            TransportAddr::Ip(addr) => direct_addrs.push(addr.to_string()),
            TransportAddr::Custom(addr) => custom_addrs.push(addr.to_string()),
            _ => {}
        }
    }
    IrohEndpointAddrSummary {
        node_id: endpoint_addr.id.to_string(),
        relay_urls,
        direct_addrs,
        custom_addrs,
    }
}

fn summarize_endpoint_addr_with_raw_udp_fallback(
    endpoint_addr: iroh::EndpointAddr,
    raw_udp_local_addr: std::net::SocketAddr,
) -> IrohEndpointAddrSummary {
    let mut summary = summarize_endpoint_addr(endpoint_addr);
    if summary.custom_addrs.is_empty() {
        let advertised_addr = normalize_advertised_raw_udp_addr(raw_udp_local_addr);
        summary.custom_addrs.push(
            socket_addr_to_custom_addr(SANDSTORM_RAW_UDP_TRANSPORT_ID, advertised_addr)
                .to_string(),
        );
    }

    summary
}

fn normalize_advertised_raw_udp_addr(addr: std::net::SocketAddr) -> std::net::SocketAddr {
    match addr {
        std::net::SocketAddr::V4(addr) if addr.ip().is_unspecified() => {
            std::net::SocketAddr::from(([127, 0, 0, 1], addr.port()))
        }
        std::net::SocketAddr::V6(addr) if addr.ip().is_unspecified() => {
            std::net::SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 1], addr.port()))
        }
        _ => addr,
    }
}

fn load_remote_ticket() -> Result<Option<String>, String> {
    let path = remote_ticket_path();
    match std::fs::read_to_string(&path) {
        Ok(value) => {
            let trimmed = value.trim().to_string();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed))
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(format!("failed to read remote ticket {}: {err}", path.display())),
    }
}

fn load_approved_peer_node_id() -> Result<Option<String>, String> {
    app_storage().load_text_file(approved_peer_node_id_path().as_path())
}

fn load_tunnel_enabled() -> Result<bool, String> {
    let Some(value) = app_storage().load_text_file(tunnel_enabled_path().as_path())? else {
        return Ok(true);
    };
    match value.as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(format!("failed to parse persisted tunnel enabled flag: {value:?}")),
    }
}

fn update_remote_ticket(
    app_state: &Arc<Mutex<AppState>>,
    remote_ticket: String,
) -> Result<(), String> {
    std::fs::create_dir_all(STATE_DIR)
        .map_err(|err| format!("failed to create state directory: {err}"))?;
    if remote_ticket.trim().is_empty() {
        clear_persisted_remote_ticket()?;
    } else {
        let path = remote_ticket_path();
        std::fs::write(&path, format!("{remote_ticket}\n"))
            .map_err(|err| format!("failed to persist remote ticket {}: {err}", path.display()))?;
    }

    let mut guard = app_state
        .lock()
        .map_err(|_| "app state lock poisoned".to_string())?;
    guard.remote_ticket = if remote_ticket.trim().is_empty() {
        None
    } else {
        Some(remote_ticket)
    };
    Ok(())
}

fn clear_persisted_remote_ticket() -> Result<(), String> {
    let path = remote_ticket_path();
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(format!("failed to clear remote ticket {}: {err}", path.display())),
    }
}

fn render_state_json(app_state: &Arc<Mutex<AppState>>) -> Result<String, String> {
    app_core(app_state).render_state_json()
}

fn join_json_strings(values: &[String]) -> String {
    values
        .iter()
        .map(|value| format!("\"{}\"", json_escape(value)))
        .collect::<Vec<_>>()
        .join(",")
}

fn imported_kind_label(kind: ImportedRemoteCapabilityKind) -> &'static str {
    match kind {
        ImportedRemoteCapabilityKind::IpNetwork => "IpNetwork",
        ImportedRemoteCapabilityKind::ApiSession => "ApiSession",
        ImportedRemoteCapabilityKind::Other => "Other",
    }
}

fn imported_type_tag_for_kind(kind: ImportedRemoteCapabilityKind) -> String {
    match kind {
        ImportedRemoteCapabilityKind::IpNetwork => "sandstorm/ip-network".to_string(),
        ImportedRemoteCapabilityKind::ApiSession => "sandstorm/api-session".to_string(),
        ImportedRemoteCapabilityKind::Other => "capnp/unknown".to_string(),
    }
}

fn descriptor_type_tag(descriptor_json: Option<&str>) -> Option<String> {
    let descriptor_json = descriptor_json?;
    let value: serde_json::Value = serde_json::from_str(descriptor_json).ok()?;
    let raw = match value {
        serde_json::Value::String(text) => text,
        _ => return None,
    };
    let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, raw).ok()?;
    let ascii = bytes
        .iter()
        .map(|byte| {
            let ch = *byte as char;
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '/' | '-' | '_') {
                ch
            } else {
                ' '
            }
        })
        .collect::<String>();
    ascii
        .split_whitespace()
        .find(|part| part.contains('.') && part.contains('/'))
        .map(|value| value.to_string())
}

fn normalize_descriptor_string(value: &str) -> Option<String> {
    let mut current = value.trim().to_string();
    if current.is_empty() {
        return None;
    }

    loop {
        let parsed = serde_json::from_str::<serde_json::Value>(&current);
        match parsed {
            Ok(serde_json::Value::String(inner)) => {
                let trimmed = inner.trim().to_string();
                if trimmed.is_empty() {
                    return None;
                }
                if trimmed == current {
                    return Some(trimmed);
                }
                current = trimmed;
            }
            _ => break,
        }
    }

    Some(current)
}

fn descriptor_json_to_b64(descriptor_json: &str) -> Option<String> {
    normalize_descriptor_string(descriptor_json)
}

fn make_powerbox_query_from_descriptor_b64(encoded: &str) -> Option<String> {
    let message = decode_powerbox_descriptor(encoded).ok()?;
    let descriptor = message
        .get_root::<powerbox_capnp::powerbox_descriptor::Reader<'_>>()
        .ok()?;
    let tags = descriptor.get_tags().ok()?;

    let mut query_message = capnp::message::Builder::new_default();
    let mut query = query_message.init_root::<powerbox_capnp::powerbox_descriptor::Builder<'_>>();
    let mut query_tags = query.reborrow().init_tags(tags.len() as u32);
    for index in 0..tags.len() {
        let tag = tags.get(index);
        query_tags.reborrow().get(index as u32).set_id(tag.get_id());
    }
    query.set_quality(powerbox_capnp::powerbox_descriptor::MatchQuality::Acceptable);

    let mut bytes = Vec::new();
    capnp::serialize_packed::write_message(&mut bytes, &query_message).ok()?;
    Some(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

pub(crate) fn descriptor_json_to_match_request_b64(descriptor_json: &str) -> Option<String> {
    let encoded = descriptor_json_to_b64(descriptor_json)?;
    make_powerbox_query_from_descriptor_b64(&encoded)
}

fn collect_match_request_descriptors(
    app_state: &Arc<Mutex<AppState>>,
) -> Result<Vec<String>, String> {
    let mut seen = HashSet::new();
    let mut encoded_descriptors = Vec::new();

    for saved_cap in load_saved_capabilities()? {
        let Some(encoded) = saved_cap
            .descriptor_json
            .as_deref()
            .and_then(descriptor_json_to_match_request_b64)
        else {
            continue;
        };
        if decode_powerbox_descriptor(&encoded).is_ok() && seen.insert(encoded.clone()) {
            encoded_descriptors.push(encoded);
        }
    }

    let imported_descriptors = {
        let guard = app_state
            .lock()
            .map_err(|_| "app state lock poisoned".to_string())?;
        let mut values = guard
            .imported_remote_caps
            .values()
            .filter_map(|cap| cap.descriptor_json.clone())
            .collect::<Vec<_>>();
        if let Some(session) = guard.peer_rpc_session.as_ref() {
            values.extend(
                session
                    .capability_exports
                    .iter()
                    .filter_map(|cap| cap.descriptor_json.clone()),
            );
        }
        values
    };

    for descriptor_json in imported_descriptors {
        let Some(encoded) = descriptor_json_to_match_request_b64(&descriptor_json) else {
            continue;
        };
        if decode_powerbox_descriptor(&encoded).is_ok() && seen.insert(encoded.clone()) {
            encoded_descriptors.push(encoded);
        }
    }

    Ok(encoded_descriptors)
}

pub(crate) fn advertised_powerbox_matches_json(
    app_state: &Arc<Mutex<AppState>>,
) -> Result<String, String> {
    let descriptors = collect_match_request_descriptors(app_state)?;
    advertised_powerbox_matches_json_from_b64(&descriptors)
}

pub(crate) fn advertised_powerbox_matches_json_from_b64(
    descriptors: &[String],
) -> Result<String, String> {
    let mut rows = Vec::new();
    for encoded in descriptors {
        let message = match decode_powerbox_descriptor(&encoded) {
            Ok(message) => message,
            Err(_) => continue,
        };
        let descriptor = match message.get_root::<powerbox_capnp::powerbox_descriptor::Reader<'_>>()
        {
            Ok(descriptor) => descriptor,
            Err(_) => continue,
        };
        let tag_ids = match descriptor.get_tags() {
            Ok(tags) => (0..tags.len())
                .map(|index| tags.get(index).get_id().to_string())
                .collect::<Vec<_>>(),
            Err(_) => Vec::new(),
        };
        rows.push(format!(
            "{{\"descriptor\":\"{}\",\"tagIds\":[{}]}}",
            json_escape(&encoded),
            tag_ids
                .iter()
                .map(|value| format!("\"{}\"", json_escape(value)))
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    Ok(format!("[{}]", rows.join(",")))
}

fn copy_powerbox_descriptor(
    source: powerbox_capnp::powerbox_descriptor::Reader<'_>,
    mut target: powerbox_capnp::powerbox_descriptor::Builder<'_>,
) -> Result<(), capnp::Error> {
    if let Ok(tags) = source.get_tags() {
        let mut target_tags = target.reborrow().init_tags(tags.len() as u32);
        for index in 0..tags.len() {
            let tag = tags.get(index);
            let mut target_tag = target_tags.reborrow().get(index as u32);
            target_tag.set_id(tag.get_id());
            if tag.has_value() {
                target_tag
                    .reborrow()
                    .init_value()
                    .set_as::<capnp::any_pointer::Owned>(tag.get_value())?;
            }
        }
    }
    if let Ok(quality) = source.get_quality() {
        target.set_quality(quality);
    }
    Ok(())
}

fn shared_capability_type_tag(shared: &SharedCapability) -> String {
    descriptor_type_tag(shared.saved_cap.descriptor_json.as_deref()).unwrap_or_else(|| {
        match shared.kind {
            SharedCapabilityKind::IpNetwork => "sandstorm/ip-network".to_string(),
            SharedCapabilityKind::ApiSession => "sandstorm/api-session".to_string(),
            SharedCapabilityKind::Other => "capnp/unknown".to_string(),
        }
    })
}

fn drop_received_remote_capability(
    app_state: &Arc<Mutex<AppState>>,
    object_id: &str,
) -> Result<bool, String> {
    app_core(app_state).drop_received_remote_capability(object_id)
}

fn disable_received_remote_capability(
    app_state: &Arc<Mutex<AppState>>,
    object_id: &str,
) -> Result<bool, String> {
    app_core(app_state).disable_received_remote_capability(object_id)
}

async fn enable_received_remote_capability(
    app_state: &Arc<Mutex<AppState>>,
    object_id: &str,
) -> Result<bool, String> {
    app_core(app_state)
        .enable_received_remote_capability(object_id)
        .await
}

async fn save_received_remote_capability_locally(
    app_state: &Arc<Mutex<AppState>>,
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    object_id: &str,
) -> Result<SavedCapability, String> {
    app_core(app_state)
        .save_received_remote_capability_locally(sandstorm_api, object_id)
        .await
}

async fn create_local_proxy_for_remote_export(
    app_state: &Arc<Mutex<AppState>>,
    export_id: &str,
) -> Result<(String, String), String> {
    app_core(app_state)
        .create_local_proxy_for_remote_export(export_id)
        .await
}

async fn create_local_proxy_for_received_object(
    app_state: &Arc<Mutex<AppState>>,
    object_id: &str,
) -> Result<(String, String), String> {
    app_core(app_state)
        .create_local_proxy_for_received_object(object_id)
        .await
}

async fn create_local_proxy_for_registered_remote_object(
    app_state: &Arc<Mutex<AppState>>,
    remote_object_id: &str,
) -> Result<(String, String), String> {
    app_core(app_state)
        .create_local_proxy_for_registered_remote_object(remote_object_id)
        .await
}

fn capability_label_for_object_id(
    app_state: &Arc<Mutex<AppState>>,
    object_id: &str,
) -> Result<String, String> {
    if object_id == crate::app::LOCAL_TEST_API_SESSION_OBJECT_ID {
        return Ok(crate::app::LOCAL_TEST_API_SESSION_LABEL.to_string());
    }
    {
        let guard = app_state
            .lock()
            .map_err(|_| "app state lock poisoned".to_string())?;
        if let Some(cap) = guard.imported_remote_caps.get(object_id) {
            return Ok(cap.label.clone());
        }
        if let Some(record) = guard
            .persisted_received_caps
            .iter()
            .find(|record| record.object_id == object_id)
        {
            return Ok(record.label.clone());
        }
        if let Some(record) = guard
            .local_proxy_caps
            .iter()
            .find(|record| record.object_id == object_id)
        {
            return Ok(record.label.clone());
        }
    }
    let saved_cap = load_saved_capability_by_id(object_id)?
        .ok_or_else(|| format!("unknown capability object id: {object_id}"))?;
    Ok(saved_cap.label)
}

fn capability_descriptor_for_object_id(
    app_state: &Arc<Mutex<AppState>>,
    object_id: &str,
) -> Result<Option<String>, String> {
    if object_id == crate::app::LOCAL_TEST_API_SESSION_OBJECT_ID {
        return Ok(None);
    }
    {
        let guard = app_state
            .lock()
            .map_err(|_| "app state lock poisoned".to_string())?;
        if let Some(cap) = guard.imported_remote_caps.get(object_id) {
            return Ok(cap.descriptor_json.clone());
        }
        if let Some(record) = guard
            .persisted_received_caps
            .iter()
            .find(|record| record.object_id == object_id)
        {
            return Ok(record.descriptor_json.clone());
        }
        if let Some(record) = guard
            .local_proxy_caps
            .iter()
            .find(|record| record.object_id == object_id)
        {
            return Ok(record.descriptor_json.clone());
        }
    }
    let saved_cap = load_saved_capability_by_id(object_id)?
        .ok_or_else(|| format!("unknown capability object id: {object_id}"))?;
    Ok(saved_cap.descriptor_json)
}

fn capability_descriptor_for_remote_export_id(
    app_state: &Arc<Mutex<AppState>>,
    export_id: &str,
) -> Result<Option<String>, String> {
    let guard = app_state
        .lock()
        .map_err(|_| "app state lock poisoned".to_string())?;
    let Some(session) = guard.peer_rpc_session.as_ref() else {
        return Ok(None);
    };
    Ok(session
        .capability_exports
        .iter()
        .find(|export| export.id == export_id)
        .and_then(|export| export.descriptor_json.clone()))
}

async fn fetch_remote_capability_by_export_id(
    app_state: &Arc<Mutex<AppState>>,
    export_id: &str,
) -> Result<(String, Option<String>, capnp::capability::Client), String> {
    let remote_bootstrap = {
        let guard = app_state
            .lock()
            .map_err(|_| "app state lock poisoned".to_string())?;
        guard
            .peer_rpc_session
            .as_ref()
            .ok_or_else(|| "peer rpc session is not connected".to_string())?
            .remote_bootstrap
            .clone()
    };
    let (label, _kind, _type_tag, descriptor_json, client) =
        fetch_remote_capability_export(remote_bootstrap, export_id).await?;
    let descriptor_json =
        descriptor_json.or(capability_descriptor_for_remote_export_id(app_state, export_id)?);
    Ok((label, descriptor_json, client))
}

async fn fulfill_powerbox_request_with_capability(
    session_context: grain_capnp::session_context::Client,
    cap: capnp::capability::Client,
    label: &str,
    descriptor_source: &str,
) -> Result<(), String> {
    let normalized_descriptor = descriptor_json_to_b64(descriptor_source)
        .unwrap_or_else(|| descriptor_source.trim().to_string());
    let descriptor_message = decode_powerbox_descriptor(&normalized_descriptor)?;
    let descriptor = descriptor_message
        .get_root::<powerbox_capnp::powerbox_descriptor::Reader<'_>>()
        .map_err(|err| format!("failed to read request descriptor: {err}"))?;

    let mut request = session_context.fulfill_request_request();
    request.get().get_cap().set_as_capability(cap.hook);
    request.get().init_required_permissions(0);
    request
        .get()
        .set_descriptor(descriptor)
        .map_err(|err| format!("failed to encode fulfillRequest() descriptor: {err}"))?;
    init_localized_text(request.get().init_display_info().init_title(), label);
    init_localized_text(
        request.get().init_display_info().init_verb_phrase(),
        "use tunneled capability",
    );
    init_localized_text(
        request.get().init_display_info().init_description(),
        "Capability provided through Iroh Tunnel.",
    );

    match request.send().promise.await {
        Ok(_) => {}
        Err(err) => return Err(format!("SessionContext.fulfillRequest() failed: {err}")),
    }
    Ok(())
}

async fn configure_exported_ip_network(
    app_state: &Arc<Mutex<AppState>>,
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    saved_cap_id: &str,
) -> Result<(), String> {
    app_core(app_state)
        .configure_exported_ip_network(sandstorm_api, saved_cap_id)
        .await
}

async fn configure_exported_api_session(
    app_state: &Arc<Mutex<AppState>>,
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    saved_cap_id: &str,
) -> Result<(), String> {
    app_core(app_state)
        .configure_exported_api_session(sandstorm_api, saved_cap_id)
        .await
}

async fn validate_saved_ip_network_capability(
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    saved_token_hex: &str,
) -> Result<ip_capnp::ip_network::Client, String> {
    let ip_network = restore_saved_ip_network(sandstorm_api, saved_token_hex).await?;
    let mut host_req = ip_network.get_remote_host_by_name_request();
    host_req.get().set_address("127.0.0.1");
    host_req
        .send()
        .promise
        .await
        .map_err(|err| format!("saved capability is not a usable IpNetwork: {err}"))?;
    Ok(ip_network)
}

async fn validate_saved_api_session_capability(
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    saved_token_hex: &str,
) -> Result<api_session_capnp::api_session::Client, String> {
    restore_saved_api_session(sandstorm_api, saved_token_hex).await
}

async fn connect_ip_network_tcp_client(
    ip_network: ip_capnp::ip_network::Client,
    host: &str,
    port: u16,
) -> Result<SavedIpNetworkTcpConnection, String> {
    let mut trace = vec!["remote-restore:ok".to_string()];

    let mut host_req = ip_network.get_remote_host_by_name_request();
    host_req.get().set_address(host);
    let host_resp = host_req
        .send()
        .promise
        .await
        .map_err(|err| format!("IpNetwork.getRemoteHostByName() failed: {err}"))?;
    trace.push("resolve-host:ok".to_string());
    let remote_host = host_resp
        .get()
        .map_err(|err| format!("failed to decode getRemoteHostByName() response: {err}"))?
        .get_host()
        .map_err(|err| format!("getRemoteHostByName() returned no host: {err}"))?;

    let mut port_req = remote_host.get_tcp_port_request();
    port_req.get().set_port_num(port);
    let port_resp = port_req
        .send()
        .promise
        .await
        .map_err(|err| format!("IpRemoteHost.getTcpPort() failed: {err}"))?;
    trace.push("get-tcp-port:ok".to_string());
    let tcp_port = port_resp
        .get()
        .map_err(|err| format!("failed to decode getTcpPort() response: {err}"))?
        .get_port()
        .map_err(|err| format!("getTcpPort() returned no port: {err}"))?;

    let incoming = Arc::new(Mutex::new(TcpSessionBuffer {
        bytes: Vec::new(),
        read_offset: 0,
        total_received_bytes: 0,
        write_calls: 0,
        saw_done: false,
    }));
    let trace = Arc::new(Mutex::new(trace));
    let notify = Arc::new(Notify::new());
    let downstream: util_capnp::byte_stream::Client = new_client(ByteStreamCollector {
        incoming: incoming.clone(),
        trace: trace.clone(),
        notify: notify.clone(),
    });

    let mut connect_req = tcp_port.connect_request();
    connect_req.get().set_downstream(downstream);
    let connect_resp = connect_req
        .send()
        .promise
        .await
        .map_err(|err| format!("TcpPort.connect() failed: {err}"))?;
    let upstream = connect_resp
        .get()
        .map_err(|err| format!("failed to decode connect() response: {err}"))?
        .get_upstream()
        .map_err(|err| format!("connect() returned no upstream stream: {err}"))?;

    {
        let mut trace_guard = trace
            .lock()
            .map_err(|_| "tcp session trace lock poisoned".to_string())?;
        trace_guard.push("connect:ok".to_string());
    }

    Ok(SavedIpNetworkTcpConnection {
        upstream,
        incoming,
        trace,
        notify,
    })
}

async fn list_remote_ip_network_exports(
    remote_bootstrap: tunnel_capnp::peer_bootstrap::Client,
) -> Result<Vec<PeerRpcExport>, String> {
    let response = remote_bootstrap
        .list_ip_network_exports_request()
        .send()
        .promise
        .await
        .map_err(|err| format!("PeerBootstrap.listIpNetworkExports() failed: {err}"))?;
    let response = response
        .get()
        .map_err(|err| format!("failed to decode listIpNetworkExports() response: {err}"))?;
    let exports = response
        .get_exports()
        .map_err(|err| format!("listIpNetworkExports() returned invalid exports: {err}"))?;
    let mut values = Vec::new();
    for export in exports.iter() {
        values.push(PeerRpcExport {
            id: export
                .get_id()
                .map_err(|err| format!("failed to read export id: {err}"))?
                .to_str()
                .unwrap_or("")
                .to_string(),
            label: export
                .get_label()
                .map_err(|err| format!("failed to read export label: {err}"))?
                .to_str()
                .unwrap_or("")
                .to_string(),
        });
    }
    Ok(values)
}

async fn list_remote_capability_exports(
    remote_bootstrap: tunnel_capnp::peer_bootstrap::Client,
) -> Result<Vec<PeerRpcCapabilityExport>, String> {
    let response = remote_bootstrap
        .list_capability_exports_request()
        .send()
        .promise
        .await
        .map_err(|err| format!("PeerBootstrap.listCapabilityExports() failed: {err}"))?;
    let response = response
        .get()
        .map_err(|err| format!("failed to decode listCapabilityExports() response: {err}"))?;
    let exports = response
        .get_exports()
        .map_err(|err| format!("listCapabilityExports() returned invalid exports: {err}"))?;
    let mut values = Vec::new();
    for export in exports.iter() {
        values.push(PeerRpcCapabilityExport {
            id: export
                .get_id()
                .map_err(|err| format!("failed to read export id: {err}"))?
                .to_str()
                .unwrap_or("")
                .to_string(),
            label: export
                .get_label()
                .map_err(|err| format!("failed to read export label: {err}"))?
                .to_str()
                .unwrap_or("")
                .to_string(),
            kind: match export
                .get_kind()
                .map_err(|err| format!("failed to read export kind: {err}"))?
            {
                tunnel_capnp::CapabilityKind::IpNetwork => SharedCapabilityKind::IpNetwork,
                tunnel_capnp::CapabilityKind::ApiSession => SharedCapabilityKind::ApiSession,
                tunnel_capnp::CapabilityKind::Other => SharedCapabilityKind::Other,
            },
            type_tag: export
                .get_type_tag()
                .map_err(|err| format!("failed to read export type tag: {err}"))?
                .to_str()
                .unwrap_or("")
                .to_string(),
            descriptor_json: export
                .get_descriptor_json()
                .ok()
                .and_then(|value| value.to_str().ok())
                .map(|value| value.to_string())
                .filter(|value| !value.is_empty()),
        });
    }
    Ok(values)
}

async fn list_remote_api_session_exports(
    remote_bootstrap: tunnel_capnp::peer_bootstrap::Client,
) -> Result<Vec<PeerRpcExport>, String> {
    let response = remote_bootstrap
        .list_api_session_exports_request()
        .send()
        .promise
        .await
        .map_err(|err| format!("PeerBootstrap.listApiSessionExports() failed: {err}"))?;
    let response = response
        .get()
        .map_err(|err| format!("failed to decode listApiSessionExports() response: {err}"))?;
    let exports = response
        .get_exports()
        .map_err(|err| format!("listApiSessionExports() returned invalid exports: {err}"))?;
    let mut values = Vec::new();
    for export in exports.iter() {
        values.push(PeerRpcExport {
            id: export
                .get_id()
                .map_err(|err| format!("failed to read export id: {err}"))?
                .to_str()
                .unwrap_or("")
                .to_string(),
            label: export
                .get_label()
                .map_err(|err| format!("failed to read export label: {err}"))?
                .to_str()
                .unwrap_or("")
                .to_string(),
        });
    }
    Ok(values)
}

async fn fetch_remote_ip_network_export(
    remote_bootstrap: tunnel_capnp::peer_bootstrap::Client,
    export_id: &str,
) -> Result<(String, ip_capnp::ip_network::Client), String> {
    let mut request = remote_bootstrap.get_ip_network_export_request();
    request.get().set_id(export_id);
    let response = request
        .send()
        .promise
        .await
        .map_err(|err| format!("PeerBootstrap.getIpNetworkExport() failed: {err}"))?;
    let response = response
        .get()
        .map_err(|err| format!("failed to decode getIpNetworkExport() response: {err}"))?;
    let label = response
        .get_label()
        .map_err(|err| format!("failed to read imported label: {err}"))?
        .to_str()
        .unwrap_or("")
        .to_string();
    let client = response
        .get_cap()
        .map_err(|err| format!("failed to read imported IpNetwork capability: {err}"))?;
    Ok((label, client))
}

async fn fetch_remote_api_session_export(
    remote_bootstrap: tunnel_capnp::peer_bootstrap::Client,
    export_id: &str,
) -> Result<(String, api_session_capnp::api_session::Client), String> {
    let mut request = remote_bootstrap.get_api_session_export_request();
    request.get().set_id(export_id);
    let response = request
        .send()
        .promise
        .await
        .map_err(|err| format!("PeerBootstrap.getApiSessionExport() failed: {err}"))?;
    let response = response
        .get()
        .map_err(|err| format!("failed to decode getApiSessionExport() response: {err}"))?;
    let label = response
        .get_label()
        .map_err(|err| format!("failed to read imported label: {err}"))?
        .to_str()
        .unwrap_or("")
        .to_string();
    let client = response
        .get_cap()
        .map_err(|err| format!("failed to read imported ApiSession capability: {err}"))?;
    Ok((label, client))
}

async fn fetch_remote_capability_export(
    remote_bootstrap: tunnel_capnp::peer_bootstrap::Client,
    export_id: &str,
) -> Result<
    (
        String,
        ImportedRemoteCapabilityKind,
        String,
        Option<String>,
        capnp::capability::Client,
    ),
    String,
> {
    let mut request = remote_bootstrap.get_capability_export_request();
    request.get().set_id(export_id);
    let response = request
        .send()
        .promise
        .await
        .map_err(|err| format!("PeerBootstrap.getCapabilityExport() failed: {err}"))?;
    let response = response
        .get()
        .map_err(|err| format!("failed to decode getCapabilityExport() response: {err}"))?;
    let label = response
        .get_label()
        .map_err(|err| format!("failed to read imported label: {err}"))?
        .to_str()
        .unwrap_or("")
        .to_string();
    let kind = match response
        .get_kind()
        .map_err(|err| format!("failed to read imported capability kind: {err}"))?
    {
        tunnel_capnp::CapabilityKind::IpNetwork => ImportedRemoteCapabilityKind::IpNetwork,
        tunnel_capnp::CapabilityKind::ApiSession => ImportedRemoteCapabilityKind::ApiSession,
        tunnel_capnp::CapabilityKind::Other => ImportedRemoteCapabilityKind::Other,
    };
    let type_tag = response
        .get_type_tag()
        .map_err(|err| format!("failed to read imported capability type tag: {err}"))?
        .to_str()
        .unwrap_or("")
        .to_string();
    let descriptor_json = response
        .get_descriptor_json()
        .ok()
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_string())
        .filter(|value| !value.is_empty());
    let client = response
        .get_cap()
        .get_as_capability::<capnp::capability::Client>()
        .map_err(|err| format!("failed to read imported capability: {err}"))?;
    Ok((label, kind, type_tag, descriptor_json, client))
}

async fn register_remote_capability(
    remote_bootstrap: tunnel_capnp::peer_bootstrap::Client,
    client: capnp::capability::Client,
    label: &str,
    kind: ImportedRemoteCapabilityKind,
    type_tag: &str,
    descriptor_json: Option<&str>,
) -> Result<String, String> {
    let mut request = remote_bootstrap.register_capability_request();
    let mut params = request.get();
    params.reborrow().get_cap().set_as_capability(client.hook);
    params.set_label(label);
    params.set_kind(imported_kind_to_tunnel_kind(kind));
    params.set_type_tag(type_tag);
    params.set_descriptor_json(descriptor_json.unwrap_or(""));
    let response = request
        .send()
        .promise
        .await
        .map_err(|err| format!("PeerBootstrap.registerCapability() failed: {err}"))?;
    let response = response
        .get()
        .map_err(|err| format!("failed to decode registerCapability() response: {err}"))?;
    response
        .get_remote_object_id()
        .map_err(|err| format!("failed to read registered remote object id: {err}"))?
        .to_str()
        .map(|value| value.to_string())
        .map_err(|err| format!("registered remote object id was not valid UTF-8: {err}"))
}

pub(crate) fn register_local_ephemeral_capability(
    app_state: &Arc<Mutex<AppState>>,
    client: capnp::capability::Client,
    label: &str,
    kind: ImportedRemoteCapabilityKind,
    type_tag: &str,
    descriptor_json: Option<&str>,
) -> Result<String, String> {
    let mut guard = app_state
        .lock()
        .map_err(|_| "app state lock poisoned".to_string())?;
    guard.next_registered_remote_cap_id += 1;
    let remote_object_id = format!("remote-registered-cap-{}", guard.next_registered_remote_cap_id);
    guard.registered_remote_caps.insert(
        remote_object_id.clone(),
        RegisteredRemoteCapability {
            remote_object_id: remote_object_id.clone(),
            label: label.to_string(),
            kind,
            type_tag: type_tag.to_string(),
            descriptor_json: descriptor_json.map(|value| value.to_string()),
            durability: RegisteredRemoteCapabilityDurability::Ephemeral,
            saved_token: None,
            created_at_ms: now_ms(),
            client: Some(client),
        },
    );
    eprintln!(
        "register_local_ephemeral_capability: remote_object_id={} label={} kind={} type_tag={} at_ms={}",
        remote_object_id,
        label,
        match kind {
            ImportedRemoteCapabilityKind::IpNetwork => "IpNetwork",
            ImportedRemoteCapabilityKind::ApiSession => "ApiSession",
            ImportedRemoteCapabilityKind::Other => "Other",
        },
        type_tag,
        now_ms()
    );
    Ok(remote_object_id)
}

async fn fetch_remote_registered_capability(
    remote_bootstrap: tunnel_capnp::peer_bootstrap::Client,
    remote_object_id: &str,
) -> Result<
    (
        String,
        ImportedRemoteCapabilityKind,
        String,
        Option<String>,
        capnp::capability::Client,
    ),
    String,
> {
    let mut request = remote_bootstrap.get_registered_capability_request();
    request.get().set_remote_object_id(remote_object_id);
    let response = request
        .send()
        .promise
        .await
        .map_err(|err| format!("PeerBootstrap.getRegisteredCapability() failed: {err}"))?;
    let response = response
        .get()
        .map_err(|err| format!("failed to decode getRegisteredCapability() response: {err}"))?;
    let label = response
        .get_label()
        .map_err(|err| format!("failed to read registered capability label: {err}"))?
        .to_str()
        .unwrap_or("")
        .to_string();
    let kind = match response
        .get_kind()
        .map_err(|err| format!("failed to read registered capability kind: {err}"))?
    {
        tunnel_capnp::CapabilityKind::IpNetwork => ImportedRemoteCapabilityKind::IpNetwork,
        tunnel_capnp::CapabilityKind::ApiSession => ImportedRemoteCapabilityKind::ApiSession,
        tunnel_capnp::CapabilityKind::Other => ImportedRemoteCapabilityKind::Other,
    };
    let type_tag = response
        .get_type_tag()
        .map_err(|err| format!("failed to read registered capability type tag: {err}"))?
        .to_str()
        .unwrap_or("")
        .to_string();
    let descriptor_json = response
        .get_descriptor_json()
        .ok()
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_string())
        .filter(|value| !value.is_empty());
    let client = response
        .get_cap()
        .get_as_capability::<capnp::capability::Client>()
        .map_err(|err| format!("failed to read registered capability: {err}"))?;
    Ok((label, kind, type_tag, descriptor_json, client))
}

pub(crate) async fn fetch_remote_local_proxy_for_peer_registered_capability(
    remote_bootstrap: tunnel_capnp::peer_bootstrap::Client,
    remote_object_id: &str,
) -> Result<
    (
        String,
        ImportedRemoteCapabilityKind,
        String,
        Option<String>,
        capnp::capability::Client,
    ),
    String,
> {
    let mut request = remote_bootstrap.get_local_proxy_for_peer_registered_capability_request();
    request.get().set_remote_object_id(remote_object_id);
    let response = request
        .send()
        .promise
        .await
        .map_err(|err| {
            format!("PeerBootstrap.getLocalProxyForPeerRegisteredCapability() failed: {err}")
        })?;
    let response = response
        .get()
        .map_err(|err| format!("failed to decode local proxy capability response: {err}"))?;
    let label = response
        .get_label()
        .map_err(|err| format!("failed to read local proxy capability label: {err}"))?
        .to_str()
        .unwrap_or("")
        .to_string();
    let kind = match response
        .get_kind()
        .map_err(|err| format!("failed to read local proxy capability kind: {err}"))?
    {
        tunnel_capnp::CapabilityKind::IpNetwork => ImportedRemoteCapabilityKind::IpNetwork,
        tunnel_capnp::CapabilityKind::ApiSession => ImportedRemoteCapabilityKind::ApiSession,
        tunnel_capnp::CapabilityKind::Other => ImportedRemoteCapabilityKind::Other,
    };
    let type_tag = response
        .get_type_tag()
        .map_err(|err| format!("failed to read local proxy capability type tag: {err}"))?
        .to_str()
        .unwrap_or("")
        .to_string();
    let descriptor_json = response
        .get_descriptor_json()
        .ok()
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_string())
        .filter(|value| !value.is_empty());
    let client = response
        .get_cap()
        .get_as_capability::<capnp::capability::Client>()
        .map_err(|err| format!("failed to read local proxy capability: {err}"))?;
    Ok((label, kind, type_tag, descriptor_json, client))
}

async fn connect_peer_rpc_session(
    app_state: Arc<Mutex<AppState>>,
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
) -> Result<(Vec<PeerRpcExport>, Vec<PeerRpcExport>), String> {
    app_core(&app_state)
        .connect_peer_rpc_session(sandstorm_api)
        .await
}

async fn begin_pair_connection(
    app_state: Arc<Mutex<AppState>>,
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
) -> Result<(), String> {
    app_core(&app_state).begin_pair_connection(sandstorm_api).await
}

async fn refresh_peer_rpc_exports(app_state: &Arc<Mutex<AppState>>) -> Result<bool, String> {
    app_core(app_state).refresh_peer_rpc_exports().await
}

async fn accept_pending_pair_connection(
    app_state: Arc<Mutex<AppState>>,
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
) -> Result<(), String> {
    app_core(&app_state)
        .accept_pending_pair_connection(sandstorm_api)
        .await
}

async fn reject_pending_pair_connection(app_state: Arc<Mutex<AppState>>) -> Result<(), String> {
    app_core(&app_state).reject_pending_pair_connection().await
}

async fn enable_tunnel(
    app_state: Arc<Mutex<AppState>>,
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
) -> Result<(), String> {
    app_core(&app_state).enable_tunnel(sandstorm_api).await
}

fn disable_tunnel(app_state: &Arc<Mutex<AppState>>) -> Result<(), String> {
    app_core(app_state).disable_tunnel()
}

fn forget_peer(app_state: &Arc<Mutex<AppState>>) -> Result<(), String> {
    app_core(app_state).forget_peer()
}

fn disconnect_peer_rpc_session(app_state: &Arc<Mutex<AppState>>) -> Result<(), String> {
    app_core(app_state).disconnect_peer_rpc_session()
}

async fn import_remote_ip_network_export(
    app_state: &Arc<Mutex<AppState>>,
    export_id: &str,
) -> Result<(String, String), String> {
    app_core(app_state)
        .import_remote_ip_network_export(export_id)
        .await
}

async fn import_remote_api_session_export(
    app_state: &Arc<Mutex<AppState>>,
    export_id: &str,
) -> Result<(String, String), String> {
    app_core(app_state)
        .import_remote_api_session_export(export_id)
        .await
}

async fn import_remote_capability_export(
    app_state: &Arc<Mutex<AppState>>,
    export_id: &str,
) -> Result<(String, String, ImportedRemoteCapabilityKind), String> {
    app_core(app_state)
        .import_remote_capability_export(export_id)
        .await
}

async fn invoke_imported_remote_ip_network(
    app_state: &Arc<Mutex<AppState>>,
    host: &str,
    port: u16,
) -> Result<TcpProbeSummary, String> {
    app_core(app_state)
        .invoke_imported_remote_ip_network(host, port)
        .await
}

fn api_session_as_web_session(
    api_session: api_session_capnp::api_session::Client,
) -> web_session_capnp::web_session::Client {
    web_session_capnp::web_session::Client {
        client: api_session.client,
    }
}

async fn wait_for_byte_stream_completion(
    incoming: &Arc<Mutex<TcpSessionBuffer>>,
    notify: &Arc<Notify>,
    timeout_ms: u64,
) -> Result<(), String> {
    loop {
        {
            let guard = incoming
                .lock()
                .map_err(|_| "byte stream buffer lock poisoned".to_string())?;
            if guard.saw_done {
                return Ok(());
            }
        }

        timeout(Duration::from_millis(timeout_ms), notify.notified())
            .await
            .map_err(|_| format!("timed out waiting for streamed response after {timeout_ms}ms"))?;
    }
}

fn response_success_code_to_status(
    code: web_session_capnp::web_session::response::SuccessCode,
) -> u16 {
    match code {
        web_session_capnp::web_session::response::SuccessCode::Ok => 200,
        web_session_capnp::web_session::response::SuccessCode::Created => 201,
        web_session_capnp::web_session::response::SuccessCode::Accepted => 202,
        web_session_capnp::web_session::response::SuccessCode::NoContent => 204,
        web_session_capnp::web_session::response::SuccessCode::PartialContent => 206,
        web_session_capnp::web_session::response::SuccessCode::MultiStatus => 207,
        web_session_capnp::web_session::response::SuccessCode::NotModified => 304,
    }
}

fn response_client_error_code_to_status(
    code: web_session_capnp::web_session::response::ClientErrorCode,
) -> u16 {
    match code {
        web_session_capnp::web_session::response::ClientErrorCode::BadRequest => 400,
        web_session_capnp::web_session::response::ClientErrorCode::Forbidden => 403,
        web_session_capnp::web_session::response::ClientErrorCode::NotFound => 404,
        web_session_capnp::web_session::response::ClientErrorCode::MethodNotAllowed => 405,
        web_session_capnp::web_session::response::ClientErrorCode::NotAcceptable => 406,
        web_session_capnp::web_session::response::ClientErrorCode::Conflict => 409,
        web_session_capnp::web_session::response::ClientErrorCode::Gone => 410,
        web_session_capnp::web_session::response::ClientErrorCode::PreconditionFailed => 412,
        web_session_capnp::web_session::response::ClientErrorCode::RequestEntityTooLarge => 413,
        web_session_capnp::web_session::response::ClientErrorCode::RequestUriTooLong => 414,
        web_session_capnp::web_session::response::ClientErrorCode::UnsupportedMediaType => 415,
        web_session_capnp::web_session::response::ClientErrorCode::ImATeapot => 418,
        web_session_capnp::web_session::response::ClientErrorCode::UnprocessableEntity => 422,
    }
}

async fn invoke_imported_remote_api_session(
    app_state: &Arc<Mutex<AppState>>,
    filename: &str,
    payload: &[u8],
) -> Result<ApiSessionInvokeSummary, String> {
    app_core(app_state)
        .invoke_imported_remote_api_session(filename, payload)
        .await
}

fn parse_claim_payload(value: &str) -> Result<(String, String, Option<String>), String> {
    let trimmed = value.trim();
    if trimmed.starts_with('{') {
        let payload: serde_json::Value = serde_json::from_str(trimmed)
            .map_err(|err| format!("invalid claim payload json: {err}"))?;
        let token = payload
            .get("token")
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        if token.is_empty() {
            return Err("missing powerbox request token".to_string());
        }
        let label = payload
            .get("label")
            .and_then(|value| value.as_str())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or("Powerbox capability")
            .to_string();
        let descriptor_json = payload
            .get("descriptor")
            .map(|value| value.to_string());
        return Ok((token, label, descriptor_json));
    }
    if let Some((token, label)) = trimmed.split_once('\n') {
        let token = token.trim().to_string();
        let label = label.trim();
        if !token.is_empty() && !label.is_empty() {
            return Ok((token, label.to_string(), None));
        }
    }
    Ok((trimmed.to_string(), "Powerbox capability".to_string(), None))
}

fn parse_http_probe_request(value: &str) -> Result<HttpProbeRequest, String> {
    let mut lines = value.lines();
    let saved_token_hex = lines.next().unwrap_or("").trim().to_string();
    let host = lines.next().unwrap_or("neverssl.com").trim().to_string();
    let port_text = lines.next().unwrap_or("80").trim();
    let path = lines.next().unwrap_or("/").trim().to_string();

    if saved_token_hex.is_empty() {
        return Err("missing saved token".to_string());
    }

    if host.is_empty() {
        return Err("missing probe host".to_string());
    }

    let port = port_text
        .parse::<u16>()
        .map_err(|_| format!("invalid probe port: {port_text:?}"))?;
    if port == 0 {
        return Err("probe port must be non-zero".to_string());
    }

    let path = if path.is_empty() {
        "/".to_string()
    } else if path.starts_with('/') {
        path
    } else {
        format!("/{path}")
    };

    Ok(HttpProbeRequest {
        saved_token_hex,
        host,
        port,
        path,
    })
}

fn parse_tcp_probe_request(value: &str) -> Result<TcpProbeRequest, String> {
    let (header, payload) = value
        .split_once("\n\n")
        .ok_or_else(|| "tcp probe request must contain a blank line before payload".to_string())?;
    let mut lines = header.lines();
    let saved_token_hex = lines.next().unwrap_or("").trim().to_string();
    let host = lines.next().unwrap_or("").trim().to_string();
    let port_text = lines.next().unwrap_or("").trim();

    if saved_token_hex.is_empty() {
        return Err("missing saved token".to_string());
    }
    if host.is_empty() {
        return Err("missing probe host".to_string());
    }

    let port = port_text
        .parse::<u16>()
        .map_err(|_| format!("invalid probe port: {port_text:?}"))?;
    if port == 0 {
        return Err("probe port must be non-zero".to_string());
    }

    Ok(TcpProbeRequest {
        saved_token_hex,
        host,
        port,
        payload: payload.as_bytes().to_vec(),
    })
}

fn parse_network_exchange_request(value: &str) -> Result<NetworkExchangeRequest, String> {
    let mut lines = value.lines();
    let saved_token_hex = lines.next().unwrap_or("").trim().to_string();
    let host = lines.next().unwrap_or("").trim().to_string();
    let port_text = lines.next().unwrap_or("").trim();
    let payload_b64 = lines.next().unwrap_or("").trim();

    if saved_token_hex.is_empty() {
        return Err("missing saved token".to_string());
    }
    if host.is_empty() {
        return Err("missing exchange host".to_string());
    }

    let port = port_text
        .parse::<u16>()
        .map_err(|_| format!("invalid exchange port: {port_text:?}"))?;
    if port == 0 {
        return Err("exchange port must be non-zero".to_string());
    }

    let payload = base64::engine::general_purpose::STANDARD
        .decode(payload_b64)
        .map_err(|err| format!("invalid payload base64: {err}"))?;

    Ok(NetworkExchangeRequest {
        saved_token_hex,
        host,
        port,
        payload,
    })
}

fn parse_udp_probe_request(value: &str) -> Result<UdpProbeRequest, String> {
    let mut lines = value.lines();
    let saved_token_hex = lines.next().unwrap_or("").trim().to_string();
    let host = lines.next().unwrap_or("").trim().to_string();
    let port_text = lines.next().unwrap_or("").trim();
    let payload_b64 = lines.next().unwrap_or("").trim();
    let wait_ms_text = lines.next().unwrap_or("1000").trim();

    if saved_token_hex.is_empty() {
        return Err("missing saved token".to_string());
    }
    if host.is_empty() {
        return Err("missing udp host".to_string());
    }

    let port = port_text
        .parse::<u16>()
        .map_err(|_| format!("invalid udp port: {port_text:?}"))?;
    if port == 0 {
        return Err("udp port must be non-zero".to_string());
    }

    let payload = base64::engine::general_purpose::STANDARD
        .decode(payload_b64)
        .map_err(|err| format!("invalid udp payload base64: {err}"))?;
    let wait_ms = wait_ms_text
        .parse::<u64>()
        .map_err(|_| format!("invalid udp wait_ms: {wait_ms_text:?}"))?;

    Ok(UdpProbeRequest {
        saved_token_hex,
        host,
        port,
        payload,
        wait_ms,
    })
}

fn parse_remote_ip_network_invoke_request(
    value: &str,
) -> Result<RemoteIpNetworkInvokeRequest, String> {
    let mut lines = value.lines();
    let host = lines.next().unwrap_or("").trim().to_string();
    let port_text = lines.next().unwrap_or("80").trim();
    if host.is_empty() {
        return Err("missing remote invoke host".to_string());
    }
    let port = port_text
        .parse::<u16>()
        .map_err(|_| format!("invalid remote invoke port: {port_text:?}"))?;
    if port == 0 {
        return Err("remote invoke port must be non-zero".to_string());
    }
    Ok(RemoteIpNetworkInvokeRequest { host, port })
}

fn parse_remote_api_session_invoke_request(
    filename: &str,
    payload: &[u8],
) -> Result<RemoteApiSessionInvokeRequest, String> {
    let filename = filename.trim().to_string();
    if filename.is_empty() {
        return Err("missing x-sandstorm-app-filename header".to_string());
    }
    if payload.is_empty() {
        return Err("request body is empty".to_string());
    }
    Ok(RemoteApiSessionInvokeRequest {
        filename,
        payload: payload.to_vec(),
    })
}

fn get_request_header(
    context: web_session_capnp::web_session::context::Reader<'_>,
    name: &str,
) -> Result<Option<String>, String> {
    let headers = context
        .get_additional_headers()
        .map_err(|err| format!("failed to read request headers: {err}"))?;
    for header in headers.iter() {
        let header_name = header
            .get_name()
            .map_err(|err| format!("failed to read request header name: {err}"))?
            .to_str()
            .unwrap_or("");
        if header_name.eq_ignore_ascii_case(name) {
            let value = header
                .get_value()
                .map_err(|err| format!("failed to read request header value: {err}"))?
                .to_str()
                .unwrap_or("")
                .to_string();
            return Ok(Some(value));
        }
    }
    Ok(None)
}

fn parse_tcp_session_open_request(value: &str) -> Result<TcpSessionOpenRequest, String> {
    let mut lines = value.lines();
    let saved_token_hex = lines.next().unwrap_or("").trim().to_string();
    let host = lines.next().unwrap_or("").trim().to_string();
    let port_text = lines.next().unwrap_or("").trim();

    if saved_token_hex.is_empty() {
        return Err("missing saved token".to_string());
    }
    if host.is_empty() {
        return Err("missing session host".to_string());
    }

    let port = port_text
        .parse::<u16>()
        .map_err(|_| format!("invalid session port: {port_text:?}"))?;
    if port == 0 {
        return Err("session port must be non-zero".to_string());
    }

    Ok(TcpSessionOpenRequest {
        saved_token_hex,
        host,
        port,
    })
}

fn parse_tcp_session_send_request(value: &str) -> Result<TcpSessionSendRequest, String> {
    let mut lines = value.lines();
    let session_id = lines.next().unwrap_or("").trim().to_string();
    let payload_b64 = lines.next().unwrap_or("").trim();

    if session_id.is_empty() {
        return Err("missing session id".to_string());
    }

    let payload = base64::engine::general_purpose::STANDARD
        .decode(payload_b64)
        .map_err(|err| format!("invalid payload base64: {err}"))?;

    Ok(TcpSessionSendRequest {
        session_id,
        payload,
    })
}

fn parse_tcp_session_receive_request(value: &str) -> Result<TcpSessionReceiveRequest, String> {
    let mut lines = value.lines();
    let session_id = lines.next().unwrap_or("").trim().to_string();
    let max_bytes_text = lines.next().unwrap_or("4096").trim();
    let wait_ms_text = lines.next().unwrap_or("250").trim();

    if session_id.is_empty() {
        return Err("missing session id".to_string());
    }

    let max_bytes = max_bytes_text
        .parse::<usize>()
        .map_err(|_| format!("invalid max bytes: {max_bytes_text:?}"))?;
    if max_bytes == 0 {
        return Err("max bytes must be non-zero".to_string());
    }

    let wait_ms = wait_ms_text
        .parse::<u64>()
        .map_err(|_| format!("invalid wait_ms: {wait_ms_text:?}"))?;

    Ok(TcpSessionReceiveRequest {
        session_id,
        max_bytes,
        wait_ms,
    })
}

fn parse_tcp_session_close_request(value: &str) -> Result<TcpSessionCloseRequest, String> {
    let session_id = value.trim().to_string();
    if session_id.is_empty() {
        return Err("missing session id".to_string());
    }
    Ok(TcpSessionCloseRequest { session_id })
}

fn powerbox_query_for_interface(type_id: u64) -> Result<String, String> {
    let mut message = capnp::message::Builder::new_default();
    let mut descriptor = message.init_root::<powerbox_capnp::powerbox_descriptor::Builder<'_>>();
    let mut tags = descriptor.reborrow().init_tags(1);
    let mut tag = tags.reborrow().get(0);
    tag.set_id(type_id);
    let mut bytes = Vec::new();
    capnp::serialize_packed::write_message(&mut bytes, &message)
        .map_err(|err| format!("failed to encode powerbox query: {err}"))?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

fn encode_powerbox_descriptor(
    descriptor: powerbox_capnp::powerbox_descriptor::Reader<'_>,
) -> Result<String, capnp::Error> {
    let mut message = capnp::message::Builder::new_default();
    message
        .set_root(descriptor)
        .map_err(|err| capnp::Error::failed(format!("failed to copy powerbox descriptor: {err}")))?;
    let mut bytes = Vec::new();
    capnp::serialize_packed::write_message(&mut bytes, &message)
        .map_err(|err| capnp::Error::failed(format!("failed to encode powerbox descriptor: {err}")))?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

fn decode_powerbox_descriptor(
    encoded: &str,
) -> Result<capnp::message::Reader<capnp::serialize::OwnedSegments>, String> {
    let normalized =
        normalize_descriptor_string(encoded).unwrap_or_else(|| encoded.trim().to_string());
    let trimmed = normalized.trim();
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(trimmed)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(trimmed))
        .or_else(|_| base64::engine::general_purpose::STANDARD.decode(trimmed))
        .or_else(|_| base64::engine::general_purpose::STANDARD_NO_PAD.decode(trimmed))
        .map_err(|err| format!("failed to decode powerbox descriptor base64: {err}"))?;
    let reader = capnp::serialize_packed::read_message(
        &mut bytes.as_slice(),
        capnp::message::ReaderOptions::new(),
    )
    .map_err(|err| format!("failed to decode powerbox descriptor message: {err}"))?;
    Ok(reader)
}

fn encode_pair_control_message(message: PairControlMessage) -> Result<Vec<u8>, String> {
    let mut builder = capnp::message::Builder::new_default();
    let mut control = builder.init_root::<tunnel_capnp::pair_control::Builder<'_>>();
    match message {
        PairControlMessage::Request { version } => {
            control.reborrow().init_request().set_version(version);
        }
        PairControlMessage::Response { version, decision } => {
            let mut response = control.reborrow().init_response();
            response.set_version(version);
            response.set_decision(match decision {
                PairControlDecision::Accepted => tunnel_capnp::PairDecision::Accepted,
                PairControlDecision::Rejected => tunnel_capnp::PairDecision::Rejected,
            });
        }
    }

    let mut bytes = Vec::new();
    capnp::serialize_packed::write_message(&mut bytes, &builder)
        .map_err(|err| format!("failed to encode pair control message: {err}"))?;
    Ok(bytes)
}

fn decode_pair_control_message(bytes: &[u8]) -> Result<PairControlMessage, String> {
    let mut slice = bytes;
    let reader = capnp::serialize_packed::read_message(
        &mut slice,
        capnp::message::ReaderOptions::new(),
    )
    .map_err(|err| format!("failed to decode pair control message: {err}"))?;
    let control = reader
        .get_root::<tunnel_capnp::pair_control::Reader<'_>>()
        .map_err(|err| format!("failed to read pair control root: {err}"))?;
    match control
        .which()
        .map_err(|err| format!("failed to decode pair control union: {err}"))?
    {
        tunnel_capnp::pair_control::Request(request) => Ok(PairControlMessage::Request {
            version: request
                .map_err(|err| format!("failed to read pair request: {err}"))?
                .get_version(),
        }),
        tunnel_capnp::pair_control::Response(response) => {
            let response = response
                .map_err(|err| format!("failed to read pair response: {err}"))?;
            let decision = match response
                .get_decision()
                .map_err(|err| format!("failed to read pair response decision: {err}"))?
            {
                tunnel_capnp::PairDecision::Accepted => PairControlDecision::Accepted,
                tunnel_capnp::PairDecision::Rejected => PairControlDecision::Rejected,
            };
            Ok(PairControlMessage::Response {
                version: response.get_version(),
                decision,
            })
        }
    }
}

fn format_local_ticket(endpoint_addr: &IrohEndpointAddrSummary) -> String {
    let mut lines = vec![endpoint_addr.node_id.clone()];
    lines.extend(
        endpoint_addr
            .relay_urls
            .iter()
            .map(|url| format!("relay:{url}")),
    );
    lines.extend(
        endpoint_addr
            .direct_addrs
            .iter()
            .map(|addr| format!("ip:{addr}")),
    );
    lines.extend(
        endpoint_addr
            .custom_addrs
            .iter()
            .map(|addr| format!("custom:{addr}")),
    );
    lines.join("\n")
}

fn parse_remote_ticket(value: &str) -> Result<iroh::EndpointAddr, String> {
    let mut lines = value.lines().map(str::trim).filter(|line| !line.is_empty());
    let node_id = lines
        .next()
        .ok_or_else(|| "remote ticket is empty".to_string())?;
    let endpoint_id = iroh::EndpointId::from_str(node_id)
        .map_err(|err| format!("invalid remote node id: {err}"))?;
    let mut addrs = Vec::new();
    for line in lines {
        if let Some(rest) = line.strip_prefix("relay:") {
            let relay_url = iroh::RelayUrl::from_str(rest)
                .map_err(|err| format!("invalid remote relay url {line:?}: {err}"))?;
            addrs.push(TransportAddr::Relay(relay_url));
            continue;
        }
        if let Some(rest) = line.strip_prefix("ip:") {
            let socket_addr = std::net::SocketAddr::from_str(rest)
                .map_err(|err| format!("invalid remote socket address {line:?}: {err}"))?;
            addrs.push(TransportAddr::Ip(socket_addr));
            continue;
        }
        if let Some(rest) = line.strip_prefix("custom:") {
            let custom_addr = CustomAddr::from_str(rest)
                .map_err(|err| format!("invalid remote custom address {line:?}: {err}"))?;
            addrs.push(TransportAddr::Custom(custom_addr));
            continue;
        }

        let socket_addr = std::net::SocketAddr::from_str(line)
            .map_err(|err| format!("invalid remote address {line:?}: {err}"))?;
        addrs.push(TransportAddr::Ip(socket_addr));
    }
    let endpoint_addr = iroh::EndpointAddr::from_parts(endpoint_id, addrs);
    if endpoint_addr.is_empty() {
        return Err("remote ticket has no transport addresses".to_string());
    }
    Ok(endpoint_addr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use crate::test_support::*;

    fn test_endpoint_id(seed: u8) -> iroh::EndpointId {
        iroh::SecretKey::from_bytes(&[seed; 32]).public()
    }

    fn run_async_test<F>(future: F)
    where
        F: std::future::Future<Output = ()>,
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(future);
    }

    fn imported_api_session_object_id(app: &App) -> Option<String> {
        let state = app.shared_state_for_test();
        let guard = state.lock().unwrap();
        guard
            .imported_remote_api_session
            .as_ref()
            .map(|value| value.object_id.clone())
    }

    fn imported_ip_network_object_id(app: &App) -> Option<String> {
        let state = app.shared_state_for_test();
        let guard = state.lock().unwrap();
        guard
            .imported_remote_ip_network
            .as_ref()
            .map(|value| value.object_id.clone())
    }

    async fn wait_for_test_condition<F>(label: &str, mut predicate: F)
    where
        F: FnMut() -> bool,
    {
        let started = std::time::Instant::now();
        loop {
            if predicate() {
                return;
            }
            if started.elapsed() > Duration::from_secs(3) {
                panic!("timed out waiting for condition: {label}");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn persisted_api_session_object_id(app: &App) -> Option<String> {
        let state = app.shared_state_for_test();
        let guard = state.lock().unwrap();
        guard
            .persisted_received_caps
            .iter()
            .find(|record| record.kind == ImportedRemoteCapabilityKind::ApiSession)
            .map(|value| value.object_id.clone())
    }

    fn imported_remote_cap_count(app: &App) -> usize {
        let state = app.shared_state_for_test();
        let guard = state.lock().unwrap();
        guard.imported_remote_caps.len()
    }

    #[test]
    fn parse_sandstorm_raw_udp_bind_config_defaults_port() {
        let config = parse_sandstorm_raw_udp_bind_config(Some("deadbeef"), None)
            .unwrap()
            .unwrap();
        assert_eq!(config.interface_token_hex, "deadbeef");
        assert_eq!(config.port, 0);
    }

    #[test]
    fn parse_sandstorm_raw_udp_bind_config_rejects_empty_token() {
        let err = parse_sandstorm_raw_udp_bind_config(Some("   "), None).unwrap_err();
        assert!(err.contains(IROH_SANDSTORM_RAW_UDP_INTERFACE_TOKEN_ENV));
    }

    #[test]
    fn parse_sandstorm_raw_udp_bind_config_rejects_invalid_port() {
        let err = parse_sandstorm_raw_udp_bind_config(Some("deadbeef"), Some("nope")).unwrap_err();
        assert!(err.contains(IROH_SANDSTORM_RAW_UDP_PORT_ENV));
    }

    #[test]
    fn resolve_sandstorm_raw_udp_bind_config_prefers_saved_token_over_env() {
        let config = resolve_sandstorm_raw_udp_bind_config(
            Some("saved-token"),
            Some(31337),
            Some("env-token"),
            Some("4242"),
        )
        .unwrap()
        .unwrap();
        assert_eq!(config.interface_token_hex, "saved-token");
        assert_eq!(config.port, 31337);
    }

    #[test]
    fn resolve_sandstorm_raw_udp_bind_config_uses_env_when_no_saved_token() {
        let config =
            resolve_sandstorm_raw_udp_bind_config(None, None, Some("env-token"), Some("4242"))
                .unwrap()
                .unwrap();
        assert_eq!(config.interface_token_hex, "env-token");
        assert_eq!(config.port, 4242);
    }

    #[test]
    fn resolve_sandstorm_raw_udp_bind_config_returns_none_without_any_source() {
        assert!(
            resolve_sandstorm_raw_udp_bind_config(None, None, None, None)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn summarize_endpoint_addr_collects_all_transport_types() {
        let endpoint_id = test_endpoint_id(1);
        let endpoint_addr = iroh::EndpointAddr::from_parts(
            endpoint_id,
            [
                TransportAddr::Relay(iroh::RelayUrl::from_str("https://relay.example").unwrap()),
                TransportAddr::Ip(std::net::SocketAddr::from(([127, 0, 0, 1], 7777))),
                TransportAddr::Custom(CustomAddr::from_parts(
                    SANDSTORM_RAW_UDP_TRANSPORT_ID,
                    &[0x01, 0x02, 0x03],
                )),
            ],
        );

        let summary = summarize_endpoint_addr(endpoint_addr);
        assert_eq!(summary.node_id, endpoint_id.to_string());
        assert_eq!(summary.relay_urls, vec!["https://relay.example/"]);
        assert_eq!(summary.direct_addrs, vec!["127.0.0.1:7777"]);
        assert_eq!(
            summary.custom_addrs,
            vec![CustomAddr::from_parts(SANDSTORM_RAW_UDP_TRANSPORT_ID, &[0x01, 0x02, 0x03])
                .to_string()]
        );
    }

    #[test]
    fn summarize_endpoint_addr_with_raw_udp_fallback_adds_custom_addr_when_missing() {
        let endpoint_id = test_endpoint_id(7);
        let endpoint_addr = iroh::EndpointAddr::from_parts(endpoint_id, []);
        let raw_udp_local_addr = std::net::SocketAddr::from(([0, 0, 0, 0], 4242));

        let summary =
            summarize_endpoint_addr_with_raw_udp_fallback(endpoint_addr, raw_udp_local_addr);

        assert_eq!(summary.node_id, endpoint_id.to_string());
        assert!(summary.direct_addrs.is_empty());
        assert!(summary.relay_urls.is_empty());
        assert_eq!(
            summary.custom_addrs,
            vec![socket_addr_to_custom_addr(
                SANDSTORM_RAW_UDP_TRANSPORT_ID,
                std::net::SocketAddr::from(([127, 0, 0, 1], 4242))
            )
            .to_string()]
        );
    }

    #[test]
    fn normalize_advertised_raw_udp_addr_rewrites_unspecified_to_loopback() {
        assert_eq!(
            normalize_advertised_raw_udp_addr(std::net::SocketAddr::from(([0, 0, 0, 0], 9999))),
            std::net::SocketAddr::from(([127, 0, 0, 1], 9999))
        );
        assert_eq!(
            normalize_advertised_raw_udp_addr("[::]:7777".parse().unwrap()),
            "[::1]:7777".parse().unwrap()
        );
        assert_eq!(
            normalize_advertised_raw_udp_addr(std::net::SocketAddr::from(([127, 0, 0, 1], 5555))),
            std::net::SocketAddr::from(([127, 0, 0, 1], 5555))
        );
    }

    #[test]
    fn local_ticket_roundtrips_mixed_transports() {
        let custom_addr =
            CustomAddr::from_parts(SANDSTORM_RAW_UDP_TRANSPORT_ID, &[0xaa, 0xbb, 0xcc]);
        let summary = IrohEndpointAddrSummary {
            node_id: test_endpoint_id(2).to_string(),
            relay_urls: vec!["https://relay.example/".to_string()],
            direct_addrs: vec!["127.0.0.1:9000".to_string()],
            custom_addrs: vec![custom_addr.to_string()],
        };

        let ticket = format_local_ticket(&summary);
        let parsed = parse_remote_ticket(&ticket).unwrap();

        assert_eq!(parsed.id.to_string(), summary.node_id);
        assert!(parsed
            .addrs
            .contains(&TransportAddr::Relay(iroh::RelayUrl::from_str(
                "https://relay.example/"
            )
            .unwrap())));
        assert!(parsed
            .addrs
            .contains(&TransportAddr::Ip(std::net::SocketAddr::from((
                [127, 0, 0, 1],
                9000
            )))));
        assert!(parsed.addrs.contains(&TransportAddr::Custom(custom_addr)));
    }

    #[test]
    fn parse_remote_ticket_accepts_legacy_bare_socket_addr() {
        let node_id = test_endpoint_id(3).to_string();
        let parsed = parse_remote_ticket(&format!("{node_id}\n127.0.0.1:4444")).unwrap();

        assert!(parsed
            .addrs
            .contains(&TransportAddr::Ip(std::net::SocketAddr::from((
                [127, 0, 0, 1],
                4444
            )))));
    }

    #[test]
    fn parse_remote_ticket_accepts_custom_only_ticket() {
        let node_id = test_endpoint_id(4).to_string();
        let custom_addr =
            CustomAddr::from_parts(SANDSTORM_RAW_UDP_TRANSPORT_ID, &[0xde, 0xad, 0xbe, 0xef]);
        let parsed = parse_remote_ticket(&format!("{node_id}\ncustom:{custom_addr}")).unwrap();

        assert_eq!(parsed.addrs.len(), 1);
        assert!(parsed.addrs.contains(&TransportAddr::Custom(custom_addr)));
    }

    #[test]
    fn parse_remote_ticket_rejects_invalid_custom_addr() {
        let node_id = test_endpoint_id(5).to_string();
        let err = parse_remote_ticket(&format!("{node_id}\ncustom:not-valid")).unwrap_err();
        assert!(err.contains("invalid remote custom address"));
    }

    #[test]
    fn pair_control_request_round_trips_through_capnp() {
        let bytes = encode_pair_control_message(PairControlMessage::Request {
            version: PAIRING_PROTOCOL_VERSION,
        })
        .unwrap();
        let decoded = decode_pair_control_message(&bytes).unwrap();
        assert_eq!(
            decoded,
            PairControlMessage::Request {
                version: PAIRING_PROTOCOL_VERSION,
            }
        );
    }

    #[test]
    fn pair_control_response_round_trips_through_capnp() {
        let bytes = encode_pair_control_message(PairControlMessage::Response {
            version: PAIRING_PROTOCOL_VERSION,
            decision: PairControlDecision::Accepted,
        })
        .unwrap();
        let decoded = decode_pair_control_message(&bytes).unwrap();
        assert_eq!(
            decoded,
            PairControlMessage::Response {
                version: PAIRING_PROTOCOL_VERSION,
                decision: PairControlDecision::Accepted,
            }
        );
    }

    #[test]
    fn require_saved_capability_by_token_reports_missing_token() {
        let err = require_saved_capability_by_token("definitely-missing").unwrap_err();
        assert_eq!(err, "saved capability token not found");
    }

    #[test]
    fn configure_raw_udp_interface_binding_runs_validate_persist_then_rebind() {
        run_async_test(async {
            let log = Arc::new(Mutex::new(Vec::new()));
            let saved_cap = SavedCapability {
                id: "cap-1".to_string(),
                label: "IpInterface capability".to_string(),
                saved_token: "saved-token".to_string(),
                created_at_ms: 1,
                descriptor_json: None,
            };

            configure_raw_udp_interface_binding(
                &saved_cap,
                {
                    let log = log.clone();
                    move |saved_token| {
                        let log = log.clone();
                        async move {
                            log.lock().unwrap().push(format!("validate:{saved_token}"));
                            Ok(())
                        }
                    }
                },
                {
                    let log = log.clone();
                    move |saved_token| {
                        log.lock().unwrap().push(format!("persist:{saved_token}"));
                        Ok(())
                    }
                },
                {
                    let log = log.clone();
                    move || {
                        let log = log.clone();
                        async move {
                            log.lock().unwrap().push("rebind".to_string());
                            Ok(())
                        }
                    }
                },
            )
            .await
            .unwrap();

            assert_eq!(
                *log.lock().unwrap(),
                vec![
                    "validate:saved-token".to_string(),
                    "persist:saved-token".to_string(),
                    "rebind".to_string()
                ]
            );
        });
    }

    #[test]
    fn clear_raw_udp_interface_binding_runs_clear_then_rebind() {
        run_async_test(async {
            let log = Arc::new(Mutex::new(Vec::new()));

            clear_raw_udp_interface_binding(
                {
                    let log = log.clone();
                    move || {
                        log.lock().unwrap().push("clear".to_string());
                        Ok(())
                    }
                },
                {
                    let log = log.clone();
                    move || {
                        let log = log.clone();
                        async move {
                            log.lock().unwrap().push("rebind".to_string());
                            Ok(())
                        }
                    }
                },
            )
            .await
            .unwrap();

            assert_eq!(
                *log.lock().unwrap(),
                vec!["clear".to_string(), "rebind".to_string()]
            );
        });
    }

    #[test]
    fn render_state_json_includes_raw_udp_interface_and_ip_interface_query() {
        let app_state = Arc::new(Mutex::new(AppState {
            storage: Storage::new(make_test_storage_root("render-state-json")),
            iroh_identity: IrohIdentity {
                node_id: test_endpoint_id(6).to_string(),
                secret_key: SecretKey::from_bytes(&[6; 32]),
            },
            iroh_endpoint: None,
            iroh_endpoint_addr: IrohEndpointAddrSummary {
                node_id: test_endpoint_id(6).to_string(),
                relay_urls: vec![],
                direct_addrs: vec!["127.0.0.1:7000".to_string()],
                custom_addrs: vec!["1234_deadbeef".to_string()],
            },
            iroh_endpoint_error: Some("not bound".to_string()),
            raw_udp_interface: Some(SavedCapability {
                id: "cap-raw".to_string(),
                label: "IpInterface capability".to_string(),
                saved_token: "saved-raw-token".to_string(),
                created_at_ms: 42,
                descriptor_json: None,
            }),
            raw_udp_interface_source: Some("saved".to_string()),
            remote_ticket: None,
            approved_peer_node_id: None,
            pending_incoming_peer_node_id: None,
            pending_outgoing_peer_node_id: None,
            pending_incoming_connection: None,
            tunnel_enabled: true,
            pairing_status: PairingStatus::Disconnected,
            shared_caps: Vec::new(),
            exported_ip_network: None,
            exported_api_session: None,
            exported_caps_live: HashMap::new(),
            exported_ip_network_live: HashMap::new(),
            exported_api_session_live: HashMap::new(),
            peer_rpc_session: None,
            imported_remote_ip_network: None,
            imported_remote_api_session: None,
            imported_remote_caps: HashMap::new(),
            persisted_received_caps: Vec::new(),
            local_proxy_caps: Vec::new(),
            registered_remote_caps: HashMap::new(),
            registered_remote_hook_object_ids: HashMap::new(),
            local_proxy_hook_object_ids: HashMap::new(),
            next_peer_rpc_session_id: 0,
            next_imported_remote_cap_id: 0,
            next_local_proxy_cap_id: 0,
            next_registered_remote_cap_id: 0,
            peer_rpc_error: None,
            active_tcp_sessions: HashMap::new(),
            next_tcp_session_id: 0,
        }));

        let state_json = render_state_json(&app_state).unwrap();
        assert!(state_json.contains("\"ipInterface\":\""));
        assert!(state_json.contains("\"rawUdpInterface\":{"));
        assert!(state_json.contains("saved-raw-token"));
        assert!(state_json.contains("\"source\":\"saved\""));
        assert!(state_json.contains("1234_deadbeef"));
        assert!(state_json.contains("\"pairing\":{"));
        assert!(state_json.contains("\"status\":\"disconnected\""));
    }

    #[test]
    fn render_state_json_tracks_live_persisted_disconnected_and_cleared_states() {
        run_local_async_test(async {
            let (exporter_app, _, exporter_sandstorm_api) =
                build_test_app("state-json-exporter", 121).await;
            let (importer_app, _, importer_sandstorm_api) =
                build_test_app("state-json-importer", 122).await;

            exporter_app
                .seed_exported_api_session_for_test(
                    "preview-api",
                    "Preview API",
                    new_client(crate::test_support::FakePreviewApiSession {
                        response_bytes: b"%PDF-STATE\n".to_vec(),
                    }),
                )
                .unwrap();
            importer_app
                .set_remote_ticket_for_test(exporter_app.local_ticket_for_test().unwrap())
                .unwrap();

            let initial_state = importer_app.render_state_json().unwrap();
            assert!(initial_state.contains("\"peerRpc\":{\"connected\":false"));
            assert!(initial_state.contains("\"importedRemoteApiSession\":null"));
            assert!(initial_state.contains("\"persistedReceivedCaps\":[]"));

            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();
            importer_app
                .import_remote_api_session_export("preview-api")
                .await
                .unwrap();

            let live_state = importer_app.render_state_json().unwrap();
            assert!(live_state.contains("\"connected\":true"));
            assert!(live_state.contains("\"exportedApiSession\":null"));
            assert!(live_state.contains("\"importedRemoteApiSession\":{\"objectId\":\"remote-cap-1\""));
            assert!(live_state.contains("\"persistedReceivedCaps\":[{\"objectId\":\"remote-cap-1\""));
            assert!(live_state.contains("\"peerRpcError\":null"));

            importer_app.disconnect_peer_rpc_session().unwrap();
            let disconnected_state = importer_app.render_state_json().unwrap();
            assert!(disconnected_state.contains("\"connected\":false"));
            assert!(disconnected_state.contains("\"importedRemoteApiSession\":null"));
            assert!(disconnected_state.contains("\"persistedReceivedCaps\":[{\"objectId\":\"remote-cap-1\""));
            assert!(disconnected_state.contains("\"peerRpcError\":null"));

            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();
            exporter_app.clear_exported_api_session_for_test().unwrap();
            importer_app.disconnect_peer_rpc_session().unwrap();
            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api)
                .await
                .unwrap();
            let cleared_state = importer_app.render_state_json().unwrap();
            assert!(cleared_state.contains("\"connected\":true"));
            assert!(cleared_state.contains("\"importedRemoteApiSession\":null"));
            assert!(cleared_state.contains("\"persistedReceivedCaps\":[{\"objectId\":\"remote-cap-1\""));
            assert!(cleared_state.contains("\"apiSessionExports\":[]"));

            importer_app
                .drop_received_remote_capability("remote-cap-1")
                .unwrap();
            let dropped_state = importer_app.render_state_json().unwrap();
            assert!(dropped_state.contains("\"importedRemoteApiSession\":null"));
            assert!(dropped_state.contains("\"persistedReceivedCaps\":[]"));

            let _ = exporter_sandstorm_api;
            importer_app.close_test_endpoint().await;
            exporter_app.close_test_endpoint().await;
        });
    }

    #[test]
    fn begin_pair_connection_creates_pending_outgoing_and_incoming_states() {
        run_local_async_test(async {
            let (app_a, _, app_a_sandstorm_api) = build_test_app("pair-state-a", 201).await;
            let (app_b, _, _) = build_test_app("pair-state-b", 202).await;

            let app_a_node_id = app_a.local_ticket_for_test().unwrap().lines().next().unwrap().to_string();
            let app_b_node_id = app_b.local_ticket_for_test().unwrap().lines().next().unwrap().to_string();

            app_a
                .set_remote_ticket_for_test(app_b.local_ticket_for_test().unwrap())
                .unwrap();

            let app_a_clone = app_a.clone();
            let app_a_api = app_a_sandstorm_api.clone();
            tokio::task::spawn_local(async move {
                let _ = app_a_clone.begin_pair_connection(app_a_api).await;
            });

            wait_for_test_condition("outgoing awaiting accept", || {
                let state_json = app_a.render_state_json().unwrap();
                state_json.contains("\"status\":\"awaitingRemoteAccept\"")
                    && state_json.contains(&format!(
                        "\"pendingOutgoingPeerNodeId\":\"{app_b_node_id}\""
                    ))
            })
            .await;

            wait_for_test_condition("incoming request visible", || {
                let state_json = app_b.render_state_json().unwrap();
                state_json.contains("\"status\":\"incomingRequest\"")
                    && state_json.contains(&format!(
                        "\"pendingIncomingPeerNodeId\":\"{app_a_node_id}\""
                    ))
            })
            .await;

            app_a.close_test_endpoint().await;
            app_b.close_test_endpoint().await;
        });
    }

    #[test]
    fn accepting_pending_pair_connection_transitions_both_sides_to_connected() {
        run_local_async_test(async {
            let (app_a, _, app_a_sandstorm_api) = build_test_app("pair-accept-a", 211).await;
            let (app_b, _, app_b_sandstorm_api) = build_test_app("pair-accept-b", 212).await;

            app_a
                .set_remote_ticket_for_test(app_b.local_ticket_for_test().unwrap())
                .unwrap();

            let app_a_clone = app_a.clone();
            let app_a_api = app_a_sandstorm_api.clone();
            tokio::task::spawn_local(async move {
                let _ = app_a_clone.begin_pair_connection(app_a_api).await;
            });

            wait_for_test_condition("incoming request before accept", || {
                app_b
                    .render_state_json()
                    .unwrap()
                    .contains("\"status\":\"incomingRequest\"")
            })
            .await;

            let app_b_clone = app_b.clone();
            let app_b_api = app_b_sandstorm_api.clone();
            tokio::task::spawn_local(async move {
                let _ = app_b_clone.accept_pending_pair_connection(app_b_api).await;
            });

            wait_for_test_condition("side a connected", || {
                let state_json = app_a.render_state_json().unwrap();
                state_json.contains("\"status\":\"connected\"")
                    && state_json.contains("\"connected\":true")
            })
            .await;

            wait_for_test_condition("side b connected", || {
                let state_json = app_b.render_state_json().unwrap();
                state_json.contains("\"status\":\"connected\"")
                    && state_json.contains("\"connected\":true")
            })
            .await;

            app_a.close_test_endpoint().await;
            app_b.close_test_endpoint().await;
        });
    }

    #[test]
    fn rejecting_pending_pair_connection_returns_initiator_to_error_and_clears_pending_state() {
        run_local_async_test(async {
            let (app_a, _, app_a_sandstorm_api) = build_test_app("pair-reject-a", 221).await;
            let (app_b, _, _) = build_test_app("pair-reject-b", 222).await;

            app_a
                .set_remote_ticket_for_test(app_b.local_ticket_for_test().unwrap())
                .unwrap();

            let app_a_clone = app_a.clone();
            let app_a_api = app_a_sandstorm_api.clone();
            tokio::task::spawn_local(async move {
                let _ = app_a_clone.begin_pair_connection(app_a_api).await;
            });

            wait_for_test_condition("incoming request before reject", || {
                app_b
                    .render_state_json()
                    .unwrap()
                    .contains("\"status\":\"incomingRequest\"")
            })
            .await;

            app_b.reject_pending_pair_connection().await.unwrap();

            wait_for_test_condition("initiator enters error state", || {
                let state_json = app_a.render_state_json().unwrap();
                state_json.contains("\"status\":\"error\"")
                    && state_json.contains("remote grain rejected the connection")
            })
            .await;

            let responder_state = app_b.render_state_json().unwrap();
            assert!(responder_state.contains("\"pendingIncomingPeerNodeId\":null"));
            assert!(responder_state.contains("\"status\":\"disconnected\""));

            app_a.close_test_endpoint().await;
            app_b.close_test_endpoint().await;
        });
    }

    #[test]
    fn approved_peer_reconnects_without_manual_accept() {
        run_local_async_test(async {
            let (app_a, _, app_a_sandstorm_api) = build_test_app("pair-auto-a", 231).await;
            let (app_b, app_b_state, _) = build_test_app("pair-auto-b", 232).await;

            let app_a_node_id = app_a
                .local_ticket_for_test()
                .unwrap()
                .lines()
                .next()
                .unwrap()
                .to_string();

            {
                let mut guard = app_b_state.lock().unwrap();
                guard.approved_peer_node_id = Some(app_a_node_id);
                guard.tunnel_enabled = true;
                guard.pairing_status = PairingStatus::Disconnected;
            }

            app_a
                .set_remote_ticket_for_test(app_b.local_ticket_for_test().unwrap())
                .unwrap();

            let app_a_clone = app_a.clone();
            tokio::task::spawn_local(async move {
                let _ = app_a_clone.begin_pair_connection(app_a_sandstorm_api).await;
            });

            wait_for_test_condition("auto accepted initiator connected", || {
                app_a
                    .render_state_json()
                    .unwrap()
                    .contains("\"status\":\"connected\"")
            })
            .await;

            wait_for_test_condition("auto accepted responder connected", || {
                app_b
                    .render_state_json()
                    .unwrap()
                    .contains("\"status\":\"connected\"")
            })
            .await;

            app_a.close_test_endpoint().await;
            app_b.close_test_endpoint().await;
        });
    }

    #[test]
    fn startup_auto_reconnect_picks_a_single_initiator_after_restart() {
        run_local_async_test(async {
            let app_a_storage_root = make_test_storage_root("startup-auto-reconnect-a");
            let app_b_storage_root = make_test_storage_root("startup-auto-reconnect-b");
            let (app_a, _, app_a_sandstorm_api) =
                build_test_app_with_storage(app_a_storage_root.clone(), 233).await;
            let (app_b, _, app_b_sandstorm_api) =
                build_test_app_with_storage(app_b_storage_root.clone(), 234).await;

            app_a
                .set_remote_ticket_for_test(app_b.local_ticket_for_test().unwrap())
                .unwrap();
            app_b
                .set_remote_ticket_for_test(app_a.local_ticket_for_test().unwrap())
                .unwrap();

            let app_a_clone = app_a.clone();
            let app_a_api = app_a_sandstorm_api.clone();
            tokio::task::spawn_local(async move {
                let _ = app_a_clone.begin_pair_connection(app_a_api).await;
            });

            wait_for_test_condition("startup reconnect initial incoming request", || {
                app_b
                    .render_state_json()
                    .unwrap()
                    .contains("\"status\":\"incomingRequest\"")
            })
            .await;

            let app_b_clone = app_b.clone();
            let app_b_api = app_b_sandstorm_api.clone();
            tokio::task::spawn_local(async move {
                let _ = app_b_clone.accept_pending_pair_connection(app_b_api).await;
            });

            wait_for_test_condition("startup reconnect initial side a connected", || {
                app_a
                    .render_state_json()
                    .unwrap()
                    .contains("\"status\":\"connected\"")
            })
            .await;

            wait_for_test_condition("startup reconnect initial side b connected", || {
                app_b
                    .render_state_json()
                    .unwrap()
                    .contains("\"status\":\"connected\"")
            })
            .await;

            app_a.close_test_endpoint().await;
            app_b.close_test_endpoint().await;

            let (_restarted_app_a, restarted_app_a_state, _restarted_app_a_sandstorm_api) =
                build_test_app_with_storage_loaded(app_a_storage_root, 233).await;
            let (_restarted_app_b, restarted_app_b_state, _restarted_app_b_sandstorm_api) =
                build_test_app_with_storage_loaded(app_b_storage_root, 234).await;

            let app_a_should_reconnect = should_auto_reconnect_tunnel(&restarted_app_a_state).unwrap();
            let app_b_should_reconnect = should_auto_reconnect_tunnel(&restarted_app_b_state).unwrap();
            assert_ne!(app_a_should_reconnect, app_b_should_reconnect);
        });
    }

    #[test]
    fn approved_disabled_peer_waits_for_enable_then_connects() {
        run_local_async_test(async {
            let (app_a, _, app_a_sandstorm_api) = build_test_app("pair-enable-a", 241).await;
            let (app_b, app_b_state, app_b_sandstorm_api) =
                build_test_app("pair-enable-b", 242).await;

            let app_a_node_id = app_a
                .local_ticket_for_test()
                .unwrap()
                .lines()
                .next()
                .unwrap()
                .to_string();

            {
                let mut guard = app_b_state.lock().unwrap();
                guard.approved_peer_node_id = Some(app_a_node_id.clone());
                guard.tunnel_enabled = false;
                guard.pairing_status = PairingStatus::Disabled;
            }

            app_a
                .set_remote_ticket_for_test(app_b.local_ticket_for_test().unwrap())
                .unwrap();

            let app_a_clone = app_a.clone();
            tokio::task::spawn_local(async move {
                let _ = app_a_clone.begin_pair_connection(app_a_sandstorm_api).await;
            });

            wait_for_test_condition("disabled peer shows pending reconnect", || {
                let state_json = app_b.render_state_json().unwrap();
                state_json.contains("\"status\":\"disabled\"")
                    && state_json.contains(&format!(
                        "\"pendingIncomingPeerNodeId\":\"{app_a_node_id}\""
                    ))
            })
            .await;

            app_b.enable_tunnel(app_b_sandstorm_api).await.unwrap();

            wait_for_test_condition("initiator connected after enable", || {
                app_a
                    .render_state_json()
                    .unwrap()
                    .contains("\"status\":\"connected\"")
            })
            .await;

            wait_for_test_condition("responder connected after enable", || {
                app_b
                    .render_state_json()
                    .unwrap()
                    .contains("\"status\":\"connected\"")
            })
            .await;

            app_a.close_test_endpoint().await;
            app_b.close_test_endpoint().await;
        });
    }

    #[test]
    fn load_persisted_received_capabilities_reads_json_records() {
        let root = make_test_storage_root("malformed-received-caps");
        let storage = Storage::new(&root);
        std::fs::write(
            storage.received_caps_path(),
            serde_json::to_string_pretty(&vec![
                PersistedReceivedCapabilityRecord {
                    object_id: "remote-cap-9".to_string(),
                    export_id: "export-a".to_string(),
                    label: "Preview A".to_string(),
                    kind: ReceivedCapabilityKind::ApiSession,
                    type_tag: Some("sandstorm/api-session".to_string()),
                    descriptor_json: None,
                    enabled: true,
                },
                PersistedReceivedCapabilityRecord {
                    object_id: "remote-cap-3".to_string(),
                    export_id: "export-net".to_string(),
                    label: "Network A".to_string(),
                    kind: ReceivedCapabilityKind::IpNetwork,
                    type_tag: Some("sandstorm/ip-network".to_string()),
                    descriptor_json: None,
                    enabled: true,
                },
                PersistedReceivedCapabilityRecord {
                    object_id: "remote-cap-10".to_string(),
                    export_id: "export-b".to_string(),
                    label: "Preview B".to_string(),
                    kind: ReceivedCapabilityKind::ApiSession,
                    type_tag: Some("sandstorm/api-session".to_string()),
                    descriptor_json: None,
                    enabled: true,
                },
            ])
            .unwrap(),
        )
        .unwrap();

        let records = storage.load_persisted_received_capabilities().unwrap();
        let ip_network = records
            .iter()
            .find(|record| record.kind == ReceivedCapabilityKind::IpNetwork)
            .expect("expected ip network record");
        let api_session = records
            .iter()
            .rev()
            .find(|record| record.kind == ReceivedCapabilityKind::ApiSession)
            .expect("expected api session record");

        assert_eq!(ip_network.object_id, "remote-cap-3");
        assert_eq!(ip_network.export_id, "export-net");
        assert_eq!(ip_network.label, "Network A");
        assert_eq!(ip_network.kind, ReceivedCapabilityKind::IpNetwork);

        assert_eq!(api_session.object_id, "remote-cap-10");
        assert_eq!(api_session.export_id, "export-b");
        assert_eq!(api_session.label, "Preview B");
        assert_eq!(api_session.kind, ReceivedCapabilityKind::ApiSession);
    }

    #[test]
    fn drop_disconnect_and_restore_missing_are_idempotent() {
        run_local_async_test(async {
            let (exporter_app, _, exporter_sandstorm_api) =
                build_test_app("idempotence-exporter", 131).await;
            let (importer_app, _, importer_sandstorm_api) =
                build_test_app("idempotence-importer", 132).await;

            exporter_app
                .seed_exported_api_session_for_test(
                    "preview-api",
                    "Preview API",
                    new_client(crate::test_support::FakePreviewApiSession {
                        response_bytes: b"%PDF-IDEMPOTENT\n".to_vec(),
                    }),
                )
                .unwrap();
            importer_app
                .set_remote_ticket_for_test(exporter_app.local_ticket_for_test().unwrap())
                .unwrap();
            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();
            let (_, object_id) = importer_app
                .import_remote_api_session_export("preview-api")
                .await
                .unwrap();

            assert!(importer_app.drop_received_remote_capability(&object_id).unwrap());
            assert!(!importer_app.drop_received_remote_capability(&object_id).unwrap());

            importer_app.disconnect_peer_rpc_session().unwrap();
            importer_app.disconnect_peer_rpc_session().unwrap();

            for missing_id in [&object_id[..], "remote-cap-does-not-exist", "cap-missing-local"] {
                let err = match importer_app
                    .restore_object_capability(importer_sandstorm_api.clone(), missing_id)
                    .await
                {
                    Ok(_) => panic!("restore unexpectedly succeeded for {missing_id}"),
                    Err(err) => err,
                };
                assert!(
                    err.contains("unknown app object id") || err.contains("unknown saved capability"),
                    "unexpected restore error: {err}"
                );
            }

            let _ = exporter_sandstorm_api;
            importer_app.close_test_endpoint().await;
            exporter_app.close_test_endpoint().await;
        });
    }

    #[test]
    fn max_persisted_received_cap_id_uses_highest_remote_cap_suffix() {
        let ip_network = PersistedReceivedCapability {
            object_id: "remote-cap-4".to_string(),
            export_id: "export-net".to_string(),
            label: "Net".to_string(),
            kind: ImportedRemoteCapabilityKind::IpNetwork,
            type_tag: imported_type_tag_for_kind(ImportedRemoteCapabilityKind::IpNetwork),
            descriptor_json: None,
            enabled: true,
        };
        let api_session = PersistedReceivedCapability {
            object_id: "remote-cap-12".to_string(),
            export_id: "export-api".to_string(),
            label: "Api".to_string(),
            kind: ImportedRemoteCapabilityKind::ApiSession,
            type_tag: imported_type_tag_for_kind(ImportedRemoteCapabilityKind::ApiSession),
            descriptor_json: None,
            enabled: true,
        };

        assert_eq!(
            max_persisted_received_cap_id(&[ip_network, api_session]),
            Some(12)
        );
    }

    #[test]
    fn cross_kind_persisted_object_id_collision_preserves_both_records() {
        let root = make_test_storage_root("cross-kind-collision");
        let storage = Storage::new(&root);
        std::fs::write(
            storage.received_caps_path(),
            serde_json::to_string_pretty(&vec![
                PersistedReceivedCapabilityRecord {
                    object_id: "remote-cap-7".to_string(),
                    export_id: "export-net".to_string(),
                    label: "Shared Object".to_string(),
                    kind: ReceivedCapabilityKind::IpNetwork,
                    type_tag: Some("sandstorm/ip-network".to_string()),
                    descriptor_json: None,
                    enabled: true,
                },
                PersistedReceivedCapabilityRecord {
                    object_id: "remote-cap-7".to_string(),
                    export_id: "export-api".to_string(),
                    label: "Shared Object".to_string(),
                    kind: ReceivedCapabilityKind::ApiSession,
                    type_tag: Some("sandstorm/api-session".to_string()),
                    descriptor_json: None,
                    enabled: true,
                },
            ])
            .unwrap(),
        )
        .unwrap();

        let records = storage.load_persisted_received_capabilities().unwrap();
        let ip_network = records
            .iter()
            .find(|record| record.kind == ReceivedCapabilityKind::IpNetwork)
            .expect("expected ip network record");
        let api_session = records
            .iter()
            .find(|record| record.kind == ReceivedCapabilityKind::ApiSession)
            .expect("expected api session record");

        assert_eq!(ip_network.object_id, "remote-cap-7");
        assert_eq!(api_session.object_id, "remote-cap-7");
        assert_eq!(ip_network.kind, ReceivedCapabilityKind::IpNetwork);
        assert_eq!(api_session.kind, ReceivedCapabilityKind::ApiSession);
        assert_ne!(ip_network.export_id, api_session.export_id);
    }

    #[test]
    fn connect_peer_rpc_session_requires_remote_ticket() {
        run_local_async_test(async {
            let (app, _, sandstorm_api) = build_test_app("missing-ticket", 141).await;
            let err = match app.connect_peer_rpc_session(sandstorm_api).await {
                Ok(_) => panic!("connect unexpectedly succeeded without remote ticket"),
                Err(err) => err,
            };
            assert!(err.contains("no remote ticket configured"));
            app.close_test_endpoint().await;
        });
    }

    #[test]
    fn import_requires_connected_peer_rpc_session() {
        run_local_async_test(async {
            let (app, _, _) = build_test_app("import-without-connect", 142).await;
            let err = app
                .import_remote_api_session_export("preview-api")
                .await
                .unwrap_err();
            assert!(err.contains("peer rpc session is not connected"));
            let err = app
                .import_remote_ip_network_export("ip-network-export")
                .await
                .unwrap_err();
            assert!(err.contains("peer rpc session is not connected"));
            app.close_test_endpoint().await;
        });
    }

    #[test]
    fn reimporting_same_export_keeps_same_object_id_and_single_live_cap() {
        run_local_async_test(async {
            let (exporter_app, _, exporter_sandstorm_api) =
                build_test_app("same-export-exporter", 151).await;
            let (importer_app, _, importer_sandstorm_api) =
                build_test_app("same-export-importer", 152).await;

            exporter_app
                .seed_exported_api_session_for_test(
                    "preview-api",
                    "Preview API",
                    new_client(crate::test_support::FakePreviewApiSession {
                        response_bytes: b"%PDF-SAME\n".to_vec(),
                    }),
                )
                .unwrap();
            importer_app
                .set_remote_ticket_for_test(exporter_app.local_ticket_for_test().unwrap())
                .unwrap();
            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();

            let (_, object_id_a) = importer_app
                .import_remote_api_session_export("preview-api")
                .await
                .unwrap();
            let (_, object_id_b) = importer_app
                .import_remote_api_session_export("preview-api")
                .await
                .unwrap();

            assert_eq!(object_id_a, "remote-cap-1");
            assert_eq!(object_id_b, object_id_a);
            assert_eq!(imported_remote_cap_count(&importer_app), 1);
            let summary = importer_app
                .invoke_imported_remote_api_session("same.docx", b"same")
                .await
                .unwrap();
            assert_eq!(summary.response_bytes, b"%PDF-SAME\n".to_vec());

            let _ = exporter_sandstorm_api;
            importer_app.close_test_endpoint().await;
            exporter_app.close_test_endpoint().await;
        });
    }

    #[test]
    fn restore_collision_between_persisted_kinds_currently_prefers_first_record() {
        run_local_async_test(async {
            let app = App::new_for_test(
                Storage::new(make_test_storage_root("restore-collision")),
                SecretKey::from_bytes(&[0x99; 32]),
            );
            {
                let state = app.shared_state_for_test();
                let mut guard = state.lock().unwrap();
                guard.persisted_received_caps.push(PersistedReceivedCapability {
                    object_id: "remote-cap-7".to_string(),
                    export_id: "export-net".to_string(),
                    label: "Net".to_string(),
                    kind: ImportedRemoteCapabilityKind::IpNetwork,
                    type_tag: imported_type_tag_for_kind(ImportedRemoteCapabilityKind::IpNetwork),
                    descriptor_json: None,
                    enabled: true,
                });
                guard.persisted_received_caps.push(PersistedReceivedCapability {
                    object_id: "remote-cap-7".to_string(),
                    export_id: "export-api".to_string(),
                    label: "Api".to_string(),
                    kind: ImportedRemoteCapabilityKind::ApiSession,
                    type_tag: imported_type_tag_for_kind(ImportedRemoteCapabilityKind::ApiSession),
                    descriptor_json: None,
                    enabled: true,
                });
            }

            let err = app
                .restore_object_capability(dummy_sandstorm_api_client(), "remote-cap-7")
                .await;
            let err = match err {
                Ok(_) => panic!("restore unexpectedly succeeded for collided object id"),
                Err(err) => err,
            };
            assert!(err.contains("received IpNetwork object remote-cap-7 is known"));
        });
    }

    #[test]
    fn acceptance_peer_rpc_api_session_flow_works_without_sandstorm() {
        run_local_async_test(async {
            let (exporter_app, _exporter_state, exporter_sandstorm_api) =
                build_test_app("acceptance-exporter", 31).await;
            let (importer_app, _importer_state, importer_sandstorm_api) =
                build_test_app("acceptance-importer", 32).await;

            let expected_pdf = b"%PDF-TEST remote preview\n".to_vec();
            exporter_app
                .seed_exported_api_session_for_test(
                "preview-api",
                "Preview API",
                new_client(crate::test_support::FakePreviewApiSession {
                    response_bytes: expected_pdf.clone(),
                }),
            )
                .unwrap();
            importer_app
                .set_remote_ticket_for_test(exporter_app.local_ticket_for_test().unwrap())
                .unwrap();

            let (_ip_exports, api_exports) = importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();
            assert_eq!(api_exports.len(), 1);
            assert_eq!(api_exports[0].id, "preview-api");

            let (_label, object_id) = importer_app
                .import_remote_api_session_export("preview-api")
                .await
                .unwrap();
            assert_eq!(object_id, "remote-cap-1");

            let imported_summary = importer_app
                .invoke_imported_remote_api_session("document.docx", b"fake office bytes")
                .await
                .unwrap();
            assert_eq!(imported_summary.status_code, 200);
            assert_eq!(imported_summary.content_type, "application/pdf");
            assert_eq!(imported_summary.response_bytes, expected_pdf);

            let restored_summary = importer_app
                .invoke_restored_api_session_for_test(
                    importer_sandstorm_api.clone(),
                    &object_id,
                    "restored.docx",
                    b"restored bytes",
                )
            .await
            .unwrap();
            assert_eq!(restored_summary.status_code, 200);
            assert_eq!(restored_summary.content_type, "application/pdf");
            assert_eq!(restored_summary.response_bytes, expected_pdf);

            importer_app.disconnect_peer_rpc_session().unwrap();
            let disconnected_err = match importer_app
                .restore_object_capability(importer_sandstorm_api.clone(), &object_id)
                .await
            {
                Ok(_) => panic!("restore unexpectedly succeeded while disconnected"),
                Err(err) => err,
            };
            assert!(disconnected_err.contains("not currently connected"));

            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();
            let reimported_summary = importer_app
                .invoke_imported_remote_api_session("reconnected.docx", b"reconnected bytes")
                .await
                .unwrap();
            assert_eq!(reimported_summary.response_bytes, expected_pdf);

            assert!(importer_app
                .drop_received_remote_capability(&object_id)
                .unwrap());
            let dropped_err = match importer_app
                .restore_object_capability(importer_sandstorm_api, &object_id)
                .await
            {
                Ok(_) => panic!("restore unexpectedly succeeded after drop"),
                Err(err) => err,
            };
            assert!(dropped_err.contains("unknown app object id"));

            let _ = exporter_app;
            let _ = exporter_sandstorm_api;
            importer_app.close_test_endpoint().await;
            exporter_app.close_test_endpoint().await;
        });
    }

    #[test]
    fn acceptance_peer_rpc_ip_network_flow_works_without_sandstorm() {
        run_local_async_test(async {
            let (exporter_app, _exporter_state, exporter_sandstorm_api) =
                build_test_app("acceptance-ip-exporter", 41).await;
            let (importer_app, _importer_state, importer_sandstorm_api) =
                build_test_app("acceptance-ip-importer", 42).await;

            let expected_response =
                b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\nhello from fake ipnetwork"
                    .to_vec();
            exporter_app
                .seed_exported_ip_network_for_test(
                "ip-network-export",
                "Fake IpNetwork",
                new_client(crate::test_support::FakeIpNetwork {
                    response_bytes: expected_response.clone(),
                }),
            )
                .unwrap();
            importer_app
                .set_remote_ticket_for_test(exporter_app.local_ticket_for_test().unwrap())
                .unwrap();

            let (ip_exports, _api_exports) = importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();
            assert_eq!(ip_exports.len(), 1);
            assert_eq!(ip_exports[0].id, "ip-network-export");

            let (_label, object_id) = importer_app
                .import_remote_ip_network_export("ip-network-export")
                .await
                .unwrap();
            assert_eq!(object_id, "remote-cap-1");

            let imported_summary = importer_app
                .invoke_imported_remote_ip_network("example.test", 8080)
                .await
                .unwrap();
            assert_eq!(imported_summary.response_bytes, expected_response);
            assert!(imported_summary.trace.contains("connect:ok"));

            let restored_summary = importer_app
                .invoke_restored_ip_network_for_test(
                    importer_sandstorm_api.clone(),
                    &object_id,
                    "restored.example",
                    8080,
                )
            .await
            .unwrap();
            assert_eq!(restored_summary.response_bytes, expected_response);

            importer_app.disconnect_peer_rpc_session().unwrap();
            let disconnected_err = match importer_app
                .restore_object_capability(importer_sandstorm_api.clone(), &object_id)
                .await
            {
                Ok(_) => panic!("restore unexpectedly succeeded while disconnected"),
                Err(err) => err,
            };
            assert!(disconnected_err.contains("not currently connected"));

            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();
            let reimported_summary = importer_app
                .invoke_imported_remote_ip_network("reconnected.example", 8080)
                .await
                .unwrap();
            assert_eq!(reimported_summary.response_bytes, expected_response);

            assert!(importer_app
                .drop_received_remote_capability(&object_id)
                .unwrap());
            let dropped_err = match importer_app
                .restore_object_capability(importer_sandstorm_api, &object_id)
                .await
            {
                Ok(_) => panic!("restore unexpectedly succeeded after drop"),
                Err(err) => err,
            };
            assert!(dropped_err.contains("unknown app object id"));

            let _ = exporter_app;
            let _ = exporter_sandstorm_api;
            importer_app.close_test_endpoint().await;
            exporter_app.close_test_endpoint().await;
        });
    }

    #[test]
    fn reconnect_auto_reimports_same_api_session_object_id() {
        run_local_async_test(async {
            let (exporter_app, _, exporter_sandstorm_api) =
                build_test_app("reimport-exporter", 51).await;
            let (importer_app, _, importer_sandstorm_api) =
                build_test_app("reimport-importer", 52).await;

            exporter_app
                .seed_exported_api_session_for_test(
                    "preview-api",
                    "Preview API",
                    new_client(crate::test_support::FakePreviewApiSession {
                        response_bytes: b"%PDF-REIMPORT\n".to_vec(),
                    }),
                )
                .unwrap();
            importer_app
                .set_remote_ticket_for_test(exporter_app.local_ticket_for_test().unwrap())
                .unwrap();

            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();
            let (_, object_id) = importer_app
                .import_remote_api_session_export("preview-api")
                .await
                .unwrap();
            assert_eq!(imported_api_session_object_id(&importer_app).as_deref(), Some("remote-cap-1"));

            importer_app.disconnect_peer_rpc_session().unwrap();
            assert_eq!(imported_api_session_object_id(&importer_app), None);

            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();
            assert_eq!(
                imported_api_session_object_id(&importer_app).as_deref(),
                Some(object_id.as_str())
            );
            let restored = importer_app
                .invoke_restored_api_session_for_test(
                    importer_sandstorm_api,
                    &object_id,
                    "same-id.docx",
                    b"bytes",
                )
                .await
                .unwrap();
            assert_eq!(restored.response_bytes, b"%PDF-REIMPORT\n".to_vec());

            let _ = exporter_sandstorm_api;
            importer_app.close_test_endpoint().await;
            exporter_app.close_test_endpoint().await;
        });
    }

    #[test]
    fn peer_session_lists_and_fetches_multiple_api_session_exports() {
        run_local_async_test(async {
            let (exporter_app, _, exporter_sandstorm_api) =
                build_test_app("multi-api-exporter", 53).await;
            let (importer_app, _, importer_sandstorm_api) =
                build_test_app("multi-api-importer", 54).await;

            exporter_app
                .seed_exported_api_session_for_test(
                    "preview-api-a",
                    "Preview API A",
                    new_client(crate::test_support::FakePreviewApiSession {
                        response_bytes: b"%PDF-A\n".to_vec(),
                    }),
                )
                .unwrap();
            exporter_app
                .append_exported_api_session_for_test(
                    "preview-api-b",
                    "Preview API B",
                    new_client(crate::test_support::FakePreviewApiSession {
                        response_bytes: b"%PDF-B\n".to_vec(),
                    }),
                )
                .unwrap();
            importer_app
                .set_remote_ticket_for_test(exporter_app.local_ticket_for_test().unwrap())
                .unwrap();

            let (_, api_exports) = importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();
            assert_eq!(api_exports.len(), 2);
            assert!(api_exports.iter().any(|export| export.id == "preview-api-a"));
            assert!(api_exports.iter().any(|export| export.id == "preview-api-b"));

            let _ = importer_app
                .import_remote_api_session_export("preview-api-a")
                .await
                .unwrap();
            let summary_a = importer_app
                .invoke_imported_remote_api_session("a.docx", b"a")
                .await
                .unwrap();
            assert_eq!(summary_a.response_bytes, b"%PDF-A\n".to_vec());

            let _ = importer_app.drop_received_remote_capability("remote-cap-1").unwrap();

            let _ = importer_app
                .import_remote_api_session_export("preview-api-b")
                .await
                .unwrap();
            let summary_b = importer_app
                .invoke_imported_remote_api_session("b.docx", b"b")
                .await
                .unwrap();
            assert_eq!(summary_b.response_bytes, b"%PDF-B\n".to_vec());

            let _ = exporter_sandstorm_api;
            importer_app.close_test_endpoint().await;
            exporter_app.close_test_endpoint().await;
        });
    }

    #[test]
    fn dropped_api_session_does_not_reappear_after_reconnect() {
        run_local_async_test(async {
            let (exporter_app, _, exporter_sandstorm_api) =
                build_test_app("drop-exporter", 61).await;
            let (importer_app, _, importer_sandstorm_api) =
                build_test_app("drop-importer", 62).await;

            exporter_app
                .seed_exported_api_session_for_test(
                    "preview-api",
                    "Preview API",
                    new_client(crate::test_support::FakePreviewApiSession {
                        response_bytes: b"%PDF-DROP\n".to_vec(),
                    }),
                )
                .unwrap();
            importer_app
                .set_remote_ticket_for_test(exporter_app.local_ticket_for_test().unwrap())
                .unwrap();

            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();
            let (_, object_id) = importer_app
                .import_remote_api_session_export("preview-api")
                .await
                .unwrap();
            assert_eq!(persisted_api_session_object_id(&importer_app).as_deref(), Some(object_id.as_str()));

            assert!(importer_app.drop_received_remote_capability(&object_id).unwrap());
            assert_eq!(persisted_api_session_object_id(&importer_app), None);

            importer_app.disconnect_peer_rpc_session().unwrap();
            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();

            assert_eq!(imported_api_session_object_id(&importer_app), None);
            assert_eq!(persisted_api_session_object_id(&importer_app), None);
            let err = match importer_app
                .restore_object_capability(importer_sandstorm_api, &object_id)
                .await
            {
                Ok(_) => panic!("restore unexpectedly succeeded after drop+reconnect"),
                Err(err) => err,
            };
            assert!(err.contains("unknown app object id"));

            let _ = exporter_sandstorm_api;
            importer_app.close_test_endpoint().await;
            exporter_app.close_test_endpoint().await;
        });
    }

    #[test]
    fn missing_export_on_reconnect_keeps_object_persisted_but_not_live() {
        run_local_async_test(async {
            let (exporter_app, _, exporter_sandstorm_api) =
                build_test_app("missing-export-exporter", 71).await;
            let (importer_app, _, importer_sandstorm_api) =
                build_test_app("missing-export-importer", 72).await;

            exporter_app
                .seed_exported_api_session_for_test(
                    "preview-api",
                    "Preview API",
                    new_client(crate::test_support::FakePreviewApiSession {
                        response_bytes: b"%PDF-MISSING\n".to_vec(),
                    }),
                )
                .unwrap();
            importer_app
                .set_remote_ticket_for_test(exporter_app.local_ticket_for_test().unwrap())
                .unwrap();

            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();
            let (_, object_id) = importer_app
                .import_remote_api_session_export("preview-api")
                .await
                .unwrap();

            importer_app.disconnect_peer_rpc_session().unwrap();
            exporter_app
                .seed_exported_api_session_for_test(
                    "preview-api-v2",
                    "Preview API v2",
                    new_client(crate::test_support::FakePreviewApiSession {
                        response_bytes: b"%PDF-NEW\n".to_vec(),
                    }),
                )
                .unwrap();

            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();

            assert_eq!(imported_api_session_object_id(&importer_app), None);
            assert_eq!(
                persisted_api_session_object_id(&importer_app).as_deref(),
                Some(object_id.as_str())
            );
            let err = match importer_app
                .restore_object_capability(importer_sandstorm_api, &object_id)
                .await
            {
                Ok(_) => panic!("restore unexpectedly succeeded with missing export"),
                Err(err) => err,
            };
            assert!(err.contains("not currently connected"));

            let _ = exporter_sandstorm_api;
            importer_app.close_test_endpoint().await;
            exporter_app.close_test_endpoint().await;
        });
    }

    #[test]
    fn local_proxy_record_restores_remote_export_while_connected() {
        run_local_async_test(async {
            let (exporter_app, _, exporter_sandstorm_api) =
                build_test_app("local-proxy-exporter", 81).await;
            let (importer_app, _, importer_sandstorm_api) =
                build_test_app("local-proxy-importer", 82).await;

            exporter_app
                .seed_exported_api_session_for_test(
                    "preview-api",
                    "Preview API",
                    new_client(crate::test_support::FakePreviewApiSession {
                        response_bytes: b"%PDF-PROXY\n".to_vec(),
                    }),
                )
                .unwrap();
            importer_app
                .set_remote_ticket_for_test(exporter_app.local_ticket_for_test().unwrap())
                .unwrap();
            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();

            let (label, object_id) = importer_app
                .create_local_proxy_for_remote_export("preview-api")
                .await
                .unwrap();
            let (_, duplicate_id) = importer_app
                .create_local_proxy_for_remote_export("preview-api")
                .await
                .unwrap();

            assert_eq!(label, "Preview API");
            assert_eq!(object_id, duplicate_id);
            let state_json = importer_app.render_state_json().unwrap();
            assert!(state_json.contains("\"localProxyCaps\":[{\"objectId\":\"local-proxy-cap-1\""));
            assert!(state_json.contains("\"targetKind\":\"exportId\""));
            assert!(state_json.contains("\"targetId\":\"preview-api\""));

            let restored = importer_app
                .invoke_restored_api_session_for_test(
                    importer_sandstorm_api,
                    &object_id,
                    "proxy.docx",
                    b"proxy",
                )
                .await
                .unwrap();
            assert_eq!(restored.response_bytes, b"%PDF-PROXY\n".to_vec());

            let _ = exporter_sandstorm_api;
            importer_app.close_test_endpoint().await;
            exporter_app.close_test_endpoint().await;
        });
    }

    #[test]
    fn local_proxy_record_requires_connected_peer_session_to_restore() {
        run_local_async_test(async {
            let (exporter_app, _, exporter_sandstorm_api) =
                build_test_app("local-proxy-disconnect-exporter", 83).await;
            let (importer_app, _, importer_sandstorm_api) =
                build_test_app("local-proxy-disconnect-importer", 84).await;

            exporter_app
                .seed_exported_api_session_for_test(
                    "preview-api",
                    "Preview API",
                    new_client(crate::test_support::FakePreviewApiSession {
                        response_bytes: b"%PDF-PROXY-DISCONNECT\n".to_vec(),
                    }),
                )
                .unwrap();
            importer_app
                .set_remote_ticket_for_test(exporter_app.local_ticket_for_test().unwrap())
                .unwrap();
            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();

            let (_, object_id) = importer_app
                .create_local_proxy_for_remote_export("preview-api")
                .await
                .unwrap();
            importer_app.disconnect_peer_rpc_session().unwrap();

            let err = match importer_app
                .invoke_restored_api_session_for_test(
                    importer_sandstorm_api.clone(),
                    &object_id,
                    "proxy-disconnected.docx",
                    b"proxy",
                )
                .await
            {
                Ok(_) => panic!("invoke unexpectedly succeeded while disconnected"),
                Err(err) => err,
            };
            assert!(err.contains("not currently connected"));

            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();
            let restored = importer_app
                .invoke_restored_api_session_for_test(
                    importer_sandstorm_api,
                    &object_id,
                    "proxy-reconnect.docx",
                    b"proxy",
                )
                .await
                .unwrap();
            assert_eq!(restored.response_bytes, b"%PDF-PROXY-DISCONNECT\n".to_vec());

            let _ = exporter_sandstorm_api;
            importer_app.close_test_endpoint().await;
            exporter_app.close_test_endpoint().await;
        });
    }

    #[test]
    fn local_proxy_app_persistent_save_round_trip_restores_same_object() {
        run_local_async_test(async {
            let (exporter_app, _, exporter_sandstorm_api) =
                build_test_app("local-proxy-save-exporter", 183).await;
            let (importer_app, _, importer_sandstorm_api) =
                build_test_app("local-proxy-save-importer", 184).await;

            exporter_app
                .seed_exported_api_session_for_test(
                    "preview-api",
                    "Preview API",
                    new_client(crate::test_support::FakePreviewApiSession {
                        response_bytes: b"%PDF-PROXY-SAVE\n".to_vec(),
                    }),
                )
                .unwrap();
            importer_app
                .set_remote_ticket_for_test(exporter_app.local_ticket_for_test().unwrap())
                .unwrap();
            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();

            let (_label, object_id) = importer_app
                .create_local_proxy_for_remote_export("preview-api")
                .await
                .unwrap();
            let local_proxy = importer_app
                .restore_object_capability(importer_sandstorm_api.clone(), &object_id)
                .await
                .unwrap();
            let (saved_object_id, saved_label) =
                save_app_persistent_capability_for_test(local_proxy).await.unwrap();

            assert_eq!(saved_object_id, object_id);
            assert_eq!(saved_label, "Preview API");

            let restored = importer_app
                .invoke_restored_api_session_for_test(
                    importer_sandstorm_api,
                    &saved_object_id,
                    "proxy-save.docx",
                    b"proxy",
                )
                .await
                .unwrap();
            assert_eq!(restored.response_bytes, b"%PDF-PROXY-SAVE\n".to_vec());

            let _ = exporter_sandstorm_api;
            importer_app.close_test_endpoint().await;
            exporter_app.close_test_endpoint().await;
        });
    }

    #[test]
    fn local_proxy_for_generic_export_forwards_nested_returned_capability_live() {
        run_local_async_test(async {
            let (exporter_app, _, exporter_sandstorm_api) =
                build_test_app("generic-local-proxy-exporter", 85).await;
            let (importer_app, _, importer_sandstorm_api) =
                build_test_app("generic-local-proxy-importer", 86).await;
            let echo_factory: generic_proxy_test_capnp::echo_factory::Client =
                new_client(crate::test_support::FakeEchoFactory);

            exporter_app
                .seed_exported_generic_capability_for_test(
                    "echo-factory",
                    "Echo Factory",
                    "dev.iroh-tunnel.test/echo-factory",
                    echo_factory.client,
                )
                .unwrap();
            importer_app
                .set_remote_ticket_for_test(exporter_app.local_ticket_for_test().unwrap())
                .unwrap();
            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();

            let (_label, object_id) = importer_app
                .create_local_proxy_for_remote_export("echo-factory")
                .await
                .unwrap();
            let restored = importer_app
                .restore_object_capability(importer_sandstorm_api.clone(), &object_id)
                .await
                .unwrap();
            let echoed = crate::test_support::invoke_echo_factory_for_test(
                generic_proxy_test_capnp::echo_factory::Client { client: restored },
                "proxy:",
                "hello",
            )
            .await
            .unwrap();

            assert_eq!(echoed, "proxy:hello");

            let _ = exporter_sandstorm_api;
            importer_app.close_test_endpoint().await;
            exporter_app.close_test_endpoint().await;
        });
    }

    #[test]
    fn registered_remote_object_can_be_rebound_through_local_proxy() {
        run_local_async_test(async {
            let (exporter_app, _, exporter_sandstorm_api) =
                build_test_app("registered-remote-exporter", 87).await;
            let (importer_app, importer_state, importer_sandstorm_api) =
                build_test_app("registered-remote-importer", 88).await;
            let echo_factory: generic_proxy_test_capnp::echo_factory::Client =
                new_client(crate::test_support::FakeEchoFactory);

            exporter_app
                .seed_exported_generic_capability_for_test(
                    "echo-factory",
                    "Echo Factory",
                    "dev.iroh-tunnel.test/echo-factory",
                    echo_factory.client,
                )
                .unwrap();
            importer_app
                .set_remote_ticket_for_test(exporter_app.local_ticket_for_test().unwrap())
                .unwrap();
            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();

            let remote_bootstrap = importer_state
                .lock()
                .unwrap()
                .peer_rpc_session
                .as_ref()
                .unwrap()
                .remote_bootstrap
                .clone();
            let (_, _, _, _, factory_client_any) =
                fetch_remote_capability_export(remote_bootstrap.clone(), "echo-factory")
                    .await
                    .unwrap();
            let factory = generic_proxy_test_capnp::echo_factory::Client {
                client: factory_client_any,
            };
            let mut get_echo = factory.get_echo_request();
            get_echo.get().set_prefix("registered:");
            let get_echo_resp = get_echo.send().promise.await.unwrap();
            let returned_echo = get_echo_resp.get().unwrap().get_echo().unwrap();

            let remote_object_id = register_remote_capability(
                remote_bootstrap,
                returned_echo.client.clone(),
                "Registered Echo",
                ImportedRemoteCapabilityKind::Other,
                "dev.iroh-tunnel.test/echo",
                None,
            )
            .await
            .unwrap();

            let (_label, object_id) = create_local_proxy_for_registered_remote_object(
                &importer_state,
                &remote_object_id,
            )
            .await
            .unwrap();
            let state_json = importer_app.render_state_json().unwrap();
            assert!(state_json.contains("\"targetKind\":\"remoteObjectId\""));
            assert!(state_json.contains(&format!("\"targetId\":\"{remote_object_id}\"")));

            let restored = importer_app
                .restore_object_capability(importer_sandstorm_api, &object_id)
                .await
                .unwrap();
            let echo = generic_proxy_test_capnp::echo::Client { client: restored };
            let mut ping = echo.ping_request();
            ping.get().set_text("hello");
            let ping_resp = ping.send().promise.await.unwrap();
            let text = ping_resp
                .get()
                .unwrap()
                .get_text()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();
            assert_eq!(text, "registered:hello");

            let _ = exporter_sandstorm_api;
            importer_app.close_test_endpoint().await;
            exporter_app.close_test_endpoint().await;
        });
    }

    #[test]
    fn localized_registered_remote_proxy_app_persistent_save_round_trip_restores_same_object() {
        run_local_async_test(async {
            let (exporter_app, _, exporter_sandstorm_api) =
                build_test_app("registered-remote-save-exporter", 185).await;
            let (importer_app, importer_state, importer_sandstorm_api) =
                build_test_app("registered-remote-save-importer", 186).await;
            let echo_factory: generic_proxy_test_capnp::echo_factory::Client =
                new_client(crate::test_support::FakeEchoFactory);

            exporter_app
                .seed_exported_generic_capability_for_test(
                    "echo-factory",
                    "Echo Factory",
                    "dev.iroh-tunnel.test/echo-factory",
                    echo_factory.client,
                )
                .unwrap();
            importer_app
                .set_remote_ticket_for_test(exporter_app.local_ticket_for_test().unwrap())
                .unwrap();
            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();

            let remote_bootstrap = importer_state
                .lock()
                .unwrap()
                .peer_rpc_session
                .as_ref()
                .unwrap()
                .remote_bootstrap
                .clone();
            let (_, _, _, _, factory_client_any) =
                fetch_remote_capability_export(remote_bootstrap.clone(), "echo-factory")
                    .await
                    .unwrap();
            let factory = generic_proxy_test_capnp::echo_factory::Client {
                client: factory_client_any,
            };
            let mut get_echo = factory.get_echo_request();
            get_echo.get().set_prefix("save-registered:");
            let get_echo_resp = get_echo.send().promise.await.unwrap();
            let returned_echo = get_echo_resp.get().unwrap().get_echo().unwrap();

            let remote_object_id = register_remote_capability(
                remote_bootstrap,
                returned_echo.client,
                "Registered Echo",
                ImportedRemoteCapabilityKind::Other,
                "dev.iroh-tunnel.test/echo",
                None,
            )
            .await
            .unwrap();

            let (_label, object_id) = create_local_proxy_for_registered_remote_object(
                &importer_state,
                &remote_object_id,
            )
            .await
            .unwrap();

            let local_proxy = importer_app
                .restore_object_capability(importer_sandstorm_api.clone(), &object_id)
                .await
                .unwrap();
            let (saved_object_id, saved_label) =
                save_app_persistent_capability_for_test(local_proxy).await.unwrap();
            assert_eq!(saved_object_id, object_id);
            assert_eq!(saved_label, "Registered Echo");

            let restored = importer_app
                .restore_object_capability(importer_sandstorm_api, &saved_object_id)
                .await
                .unwrap();
            let echo = generic_proxy_test_capnp::echo::Client { client: restored };
            let mut ping = echo.ping_request();
            ping.get().set_text("hello");
            let ping_resp = ping.send().promise.await.unwrap();
            let text = ping_resp
                .get()
                .unwrap()
                .get_text()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();
            assert_eq!(text, "save-registered:hello");

            let _ = exporter_sandstorm_api;
            importer_app.close_test_endpoint().await;
            exporter_app.close_test_endpoint().await;
        });
    }

    #[test]
    fn fulfill_request_with_local_proxy_succeeds_in_fake_session_context() {
        run_local_async_test(async {
            let (exporter_app, _, exporter_sandstorm_api) =
                build_test_app("fulfill-local-proxy-exporter", 187).await;
            let (importer_app, _, importer_sandstorm_api) =
                build_test_app("fulfill-local-proxy-importer", 188).await;

            exporter_app
                .seed_exported_api_session_for_test(
                    "preview-api",
                    "Preview API",
                    new_client(crate::test_support::FakePreviewApiSession {
                        response_bytes: b"%PDF-FULFILL\n".to_vec(),
                    }),
                )
                .unwrap();
            importer_app
                .set_remote_ticket_for_test(exporter_app.local_ticket_for_test().unwrap())
                .unwrap();
            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();

            let (_label, object_id) = importer_app
                .create_local_proxy_for_remote_export("preview-api")
                .await
                .unwrap();
            let cap = importer_app
                .restore_object_capability(importer_sandstorm_api.clone(), &object_id)
                .await
                .unwrap();

            let capture = Arc::new(Mutex::new(crate::test_support::FakeFulfillCapture::default()));
            let session_context: grain_capnp::session_context::Client = new_client(
                crate::test_support::FakeSessionContextForFulfill {
                    capture: capture.clone(),
                },
            );
            let descriptor_b64 =
                powerbox_query_for_interface(api_session_capnp::api_session::Client::TYPE_ID)
                    .unwrap();

            fulfill_powerbox_request_with_capability(
                session_context,
                cap,
                "Preview API",
                &descriptor_b64,
            )
            .await
            .unwrap();

            let captured = capture.lock().unwrap().clone();
            assert_eq!(captured.object_id.as_deref(), Some(object_id.as_str()));
            assert_eq!(captured.label.as_deref(), Some("Preview API"));
            assert!(captured.descriptor_b64.is_some());

            let restored = importer_app
                .invoke_restored_api_session_for_test(
                    importer_sandstorm_api,
                    &object_id,
                    "fulfill.docx",
                    b"proxy",
                )
                .await
                .unwrap();
            assert_eq!(restored.response_bytes, b"%PDF-FULFILL\n".to_vec());

            let _ = exporter_sandstorm_api;
            importer_app.close_test_endpoint().await;
            exporter_app.close_test_endpoint().await;
        });
    }

    #[test]
    fn fulfill_request_brokers_received_capability_into_local_proxy_before_providing() {
        run_local_async_test(async {
            let (exporter_app, _, exporter_sandstorm_api) =
                build_test_app("fulfill-received-exporter", 197).await;
            let (importer_app, importer_state, importer_sandstorm_api) =
                build_test_app("fulfill-received-importer", 198).await;

            exporter_app
                .seed_exported_api_session_for_test(
                    "preview-api",
                    "Preview API",
                    new_client(crate::test_support::FakePreviewApiSession {
                        response_bytes: b"%PDF-RECEIVED-FULFILL\n".to_vec(),
                    }),
                )
                .unwrap();
            importer_app
                .set_remote_ticket_for_test(exporter_app.local_ticket_for_test().unwrap())
                .unwrap();
            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();

            let (_label, received_object_id) = importer_app
                .import_remote_capability_export("preview-api")
                .await
                .map(|(label, object_id, _kind)| (label, object_id))
                .unwrap();
            assert_eq!(received_object_id, "remote-cap-1");

            let (_label, local_proxy_object_id) =
                create_local_proxy_for_received_object(&importer_state, &received_object_id)
                    .await
                    .unwrap();
            assert_ne!(local_proxy_object_id, received_object_id);
            assert!(local_proxy_object_id.starts_with("local-proxy-cap-"));

            let capture = Arc::new(Mutex::new(crate::test_support::FakeFulfillCapture::default()));
            let session_context: grain_capnp::session_context::Client = new_client(
                crate::test_support::FakeSessionContextForFulfill {
                    capture: capture.clone(),
                },
            );
            let descriptor_b64 =
                powerbox_query_for_interface(api_session_capnp::api_session::Client::TYPE_ID)
                    .unwrap();
            let cap = importer_app
                .restore_object_capability(importer_sandstorm_api.clone(), &local_proxy_object_id)
                .await
                .unwrap();

            fulfill_powerbox_request_with_capability(
                session_context,
                cap,
                "Preview API",
                &descriptor_b64,
            )
            .await
            .unwrap();

            let captured = capture.lock().unwrap().clone();
            assert_eq!(
                captured.object_id.as_deref(),
                Some(local_proxy_object_id.as_str())
            );

            let restored = importer_app
                .invoke_restored_api_session_for_test(
                    importer_sandstorm_api,
                    &local_proxy_object_id,
                    "fulfill-received.docx",
                    b"proxy",
                )
                .await
                .unwrap();
            assert_eq!(restored.response_bytes, b"%PDF-RECEIVED-FULFILL\n".to_vec());

            let _ = exporter_sandstorm_api;
            importer_app.close_test_endpoint().await;
            exporter_app.close_test_endpoint().await;
        });
    }

    #[test]
    fn brokered_received_api_session_can_be_provided_again_after_both_sides_restart() {
        run_local_async_test(async {
            let exporter_storage_root = make_test_storage_root("restart-provide-exporter");
            let importer_storage_root = make_test_storage_root("restart-provide-importer");
            let (exporter_app, _, exporter_sandstorm_api) =
                build_test_app_with_storage(exporter_storage_root.clone(), 201).await;
            let (importer_app, importer_state, importer_sandstorm_api) =
                build_test_app_with_storage(importer_storage_root.clone(), 202).await;

            exporter_app
                .seed_exported_api_session_for_test(
                    "preview-api",
                    "Preview API",
                    new_client(crate::test_support::FakePreviewApiSession {
                        response_bytes: b"%PDF-RESTART-PROVIDE\n".to_vec(),
                    }),
                )
                .unwrap();
            importer_app
                .set_remote_ticket_for_test(exporter_app.local_ticket_for_test().unwrap())
                .unwrap();
            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();

            let (_label, received_object_id) = importer_app
                .import_remote_capability_export("preview-api")
                .await
                .map(|(label, object_id, _kind)| (label, object_id))
                .unwrap();
            let (_label, local_proxy_object_id) =
                create_local_proxy_for_received_object(&importer_state, &received_object_id)
                    .await
                    .unwrap();

            let initial_capture =
                Arc::new(Mutex::new(crate::test_support::FakeFulfillCapture::default()));
            let initial_session_context: grain_capnp::session_context::Client = new_client(
                crate::test_support::FakeSessionContextForFulfill {
                    capture: initial_capture.clone(),
                },
            );
            let descriptor_b64 =
                powerbox_query_for_interface(api_session_capnp::api_session::Client::TYPE_ID)
                    .unwrap();
            let initial_cap = importer_app
                .restore_object_capability(importer_sandstorm_api.clone(), &local_proxy_object_id)
                .await
                .unwrap();
            fulfill_powerbox_request_with_capability(
                initial_session_context,
                initial_cap,
                "Preview API",
                &descriptor_b64,
            )
            .await
            .unwrap();
            let initial_restored = importer_app
                .invoke_restored_api_session_for_test(
                    importer_sandstorm_api.clone(),
                    &local_proxy_object_id,
                    "restart-provide-initial.docx",
                    b"proxy",
                )
                .await
                .unwrap();
            assert_eq!(
                initial_restored.response_bytes,
                b"%PDF-RESTART-PROVIDE\n".to_vec()
            );

            importer_app.close_test_endpoint().await;
            exporter_app.close_test_endpoint().await;

            let (restarted_exporter_app, _, restarted_exporter_sandstorm_api) =
                build_test_app_with_storage_loaded(exporter_storage_root, 201).await;
            let (restarted_importer_app, restarted_importer_state, restarted_importer_sandstorm_api) =
                build_test_app_with_storage_loaded(importer_storage_root, 202).await;

            restarted_exporter_app
                .seed_exported_api_session_for_test(
                    "preview-api",
                    "Preview API",
                    new_client(crate::test_support::FakePreviewApiSession {
                        response_bytes: b"%PDF-RESTART-PROVIDE\n".to_vec(),
                    }),
                )
                .unwrap();
            restarted_importer_app
                .set_remote_ticket_for_test(restarted_exporter_app.local_ticket_for_test().unwrap())
                .unwrap();
            restarted_importer_app
                .connect_peer_rpc_session(restarted_importer_sandstorm_api.clone())
                .await
                .unwrap();

            let (_label, restarted_local_proxy_object_id) =
                create_local_proxy_for_received_object(&restarted_importer_state, &received_object_id)
                    .await
                    .unwrap();
            assert_eq!(restarted_local_proxy_object_id, local_proxy_object_id);

            let restarted_capture =
                Arc::new(Mutex::new(crate::test_support::FakeFulfillCapture::default()));
            let restarted_session_context: grain_capnp::session_context::Client = new_client(
                crate::test_support::FakeSessionContextForFulfill {
                    capture: restarted_capture.clone(),
                },
            );
            let restarted_cap = restarted_importer_app
                .restore_object_capability(
                    restarted_importer_sandstorm_api.clone(),
                    &restarted_local_proxy_object_id,
                )
                .await
                .unwrap();
            fulfill_powerbox_request_with_capability(
                restarted_session_context,
                restarted_cap,
                "Preview API",
                &descriptor_b64,
            )
            .await
            .unwrap();

            let captured = restarted_capture.lock().unwrap().clone();
            assert_eq!(
                captured.object_id.as_deref(),
                Some(restarted_local_proxy_object_id.as_str())
            );

            let restarted_restored = restarted_importer_app
                .invoke_restored_api_session_for_test(
                    restarted_importer_sandstorm_api,
                    &restarted_local_proxy_object_id,
                    "restart-provide-after-restart.docx",
                    b"proxy",
                )
                .await
                .unwrap();
            assert_eq!(
                restarted_restored.response_bytes,
                b"%PDF-RESTART-PROVIDE\n".to_vec()
            );

            let _ = exporter_sandstorm_api;
            let _ = restarted_exporter_sandstorm_api;
            restarted_importer_app.close_test_endpoint().await;
            restarted_exporter_app.close_test_endpoint().await;
        });
    }

    #[test]
    fn restart_keeps_export_backed_local_proxy_but_discards_remote_object_helper_proxies() {
        run_local_async_test(async {
            let exporter_storage_root = make_test_storage_root("restart-export-backed-exporter");
            let importer_storage_root = make_test_storage_root("restart-export-backed-importer");
            let (exporter_app, _, exporter_sandstorm_api) =
                build_test_app_with_storage(exporter_storage_root.clone(), 199).await;
            let (importer_app, importer_state, importer_sandstorm_api) =
                build_test_app_with_storage(importer_storage_root.clone(), 200).await;
            let echo_factory: generic_proxy_test_capnp::echo_factory::Client =
                new_client(crate::test_support::FakeEchoFactory);

            exporter_app
                .seed_exported_api_session_for_test(
                    "preview-api",
                    "Preview API",
                    new_client(crate::test_support::FakePreviewApiSession {
                        response_bytes: b"%PDF-RESTART-PROXY\n".to_vec(),
                    }),
                )
                .unwrap();
            exporter_app
                .seed_exported_generic_capability_for_test(
                    "echo-factory",
                    "Echo Factory",
                    "dev.iroh-tunnel.test/echo-factory",
                    echo_factory.client,
                )
                .unwrap();

            importer_app
                .set_remote_ticket_for_test(exporter_app.local_ticket_for_test().unwrap())
                .unwrap();
            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();

            let (_label, top_level_object_id) = importer_app
                .create_local_proxy_for_remote_export("preview-api")
                .await
                .unwrap();

            let remote_bootstrap = importer_state
                .lock()
                .unwrap()
                .peer_rpc_session
                .as_ref()
                .unwrap()
                .remote_bootstrap
                .clone();
            let (_, _, _, _, factory_client_any) =
                fetch_remote_capability_export(remote_bootstrap.clone(), "echo-factory")
                    .await
                    .unwrap();
            let factory = generic_proxy_test_capnp::echo_factory::Client {
                client: factory_client_any,
            };
            let mut get_echo = factory.get_echo_request();
            get_echo.get().set_prefix("restart-helper:");
            let get_echo_resp = get_echo.send().promise.await.unwrap();
            let returned_echo = get_echo_resp.get().unwrap().get_echo().unwrap();
            let remote_object_id = register_remote_capability(
                remote_bootstrap,
                returned_echo.client,
                "Registered Echo",
                ImportedRemoteCapabilityKind::Other,
                "dev.iroh-tunnel.test/echo",
                None,
            )
            .await
            .unwrap();
            let (_label, helper_object_id) =
                create_local_proxy_for_registered_remote_object(&importer_state, &remote_object_id)
                    .await
                    .unwrap();
            assert_ne!(top_level_object_id, helper_object_id);

            importer_app.close_test_endpoint().await;
            exporter_app.close_test_endpoint().await;

            let (restarted_exporter_app, _, restarted_exporter_sandstorm_api) =
                build_test_app_with_storage_loaded(exporter_storage_root, 199).await;
            let (restarted_importer_app, restarted_importer_state, restarted_importer_sandstorm_api) =
                build_test_app_with_storage_loaded(importer_storage_root, 200).await;

            restarted_exporter_app
                .seed_exported_api_session_for_test(
                    "preview-api",
                    "Preview API",
                    new_client(crate::test_support::FakePreviewApiSession {
                        response_bytes: b"%PDF-RESTART-PROXY\n".to_vec(),
                    }),
                )
                .unwrap();

            let restarted_local_proxy_caps = restarted_importer_state
                .lock()
                .unwrap()
                .local_proxy_caps
                .clone();
            assert!(restarted_local_proxy_caps
                .iter()
                .any(|record| record.object_id == top_level_object_id
                    && record.target_kind == LocalProxyTargetKindRuntime::ExportId));
            assert!(!restarted_local_proxy_caps
                .iter()
                .any(|record| record.object_id == helper_object_id));
            assert!(!restarted_local_proxy_caps
                .iter()
                .any(|record| record.target_kind == LocalProxyTargetKindRuntime::RemoteObjectId));

            restarted_importer_app
                .set_remote_ticket_for_test(restarted_exporter_app.local_ticket_for_test().unwrap())
                .unwrap();
            restarted_importer_app
                .connect_peer_rpc_session(restarted_importer_sandstorm_api.clone())
                .await
                .unwrap();

            let restored = restarted_importer_app
                .invoke_restored_api_session_for_test(
                    restarted_importer_sandstorm_api,
                    &top_level_object_id,
                    "restart-proxy.docx",
                    b"proxy",
                )
                .await
                .unwrap();
            assert_eq!(restored.response_bytes, b"%PDF-RESTART-PROXY\n".to_vec());

            let _ = exporter_sandstorm_api;
            let _ = restarted_exporter_sandstorm_api;
            restarted_importer_app.close_test_endpoint().await;
            restarted_exporter_app.close_test_endpoint().await;
        });
    }

    #[test]
    fn registering_same_remote_capability_twice_reuses_remote_object_id() {
        run_local_async_test(async {
            let (exporter_app, _, exporter_sandstorm_api) =
                build_test_app("registered-dedupe-exporter", 93).await;
            let (importer_app, importer_state, importer_sandstorm_api) =
                build_test_app("registered-dedupe-importer", 94).await;
            let echo_factory: generic_proxy_test_capnp::echo_factory::Client =
                new_client(crate::test_support::FakeEchoFactory);

            exporter_app
                .seed_exported_generic_capability_for_test(
                    "echo-factory",
                    "Echo Factory",
                    "dev.iroh-tunnel.test/echo-factory",
                    echo_factory.client,
                )
                .unwrap();
            importer_app
                .set_remote_ticket_for_test(exporter_app.local_ticket_for_test().unwrap())
                .unwrap();
            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api)
                .await
                .unwrap();

            let remote_bootstrap = importer_state
                .lock()
                .unwrap()
                .peer_rpc_session
                .as_ref()
                .unwrap()
                .remote_bootstrap
                .clone();
            let (_, _, _, _, factory_client_any) =
                fetch_remote_capability_export(remote_bootstrap.clone(), "echo-factory")
                    .await
                    .unwrap();
            let factory = generic_proxy_test_capnp::echo_factory::Client {
                client: factory_client_any,
            };
            let mut get_echo = factory.get_echo_request();
            get_echo.get().set_prefix("reuse:");
            let get_echo_resp = get_echo.send().promise.await.unwrap();
            let returned_echo = get_echo_resp.get().unwrap().get_echo().unwrap();

            let first = register_remote_capability(
                remote_bootstrap.clone(),
                returned_echo.client.clone(),
                "Registered Echo",
                ImportedRemoteCapabilityKind::Other,
                "dev.iroh-tunnel.test/echo",
                None,
            )
            .await
            .unwrap();
            let second = register_remote_capability(
                remote_bootstrap,
                returned_echo.client,
                "Registered Echo",
                ImportedRemoteCapabilityKind::Other,
                "dev.iroh-tunnel.test/echo",
                None,
            )
            .await
            .unwrap();

            assert_eq!(first, second);

            let _ = exporter_sandstorm_api;
            importer_app.close_test_endpoint().await;
            exporter_app.close_test_endpoint().await;
        });
    }

    #[test]
    fn registered_remote_capability_records_ephemeral_durability_when_save_fails() {
        run_local_async_test(async {
            let (exporter_app, exporter_state, exporter_sandstorm_api) =
                build_test_app("registered-ephemeral-exporter", 95).await;
            let (importer_app, importer_state, importer_sandstorm_api) =
                build_test_app("registered-ephemeral-importer", 96).await;
            let echo_factory: generic_proxy_test_capnp::echo_factory::Client =
                new_client(crate::test_support::FakeEchoFactory);

            exporter_app
                .seed_exported_generic_capability_for_test(
                    "echo-factory",
                    "Echo Factory",
                    "dev.iroh-tunnel.test/echo-factory",
                    echo_factory.client,
                )
                .unwrap();
            importer_app
                .set_remote_ticket_for_test(exporter_app.local_ticket_for_test().unwrap())
                .unwrap();
            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api)
                .await
                .unwrap();

            let remote_bootstrap = importer_state
                .lock()
                .unwrap()
                .peer_rpc_session
                .as_ref()
                .unwrap()
                .remote_bootstrap
                .clone();
            let (_, _, _, _, factory_client_any) =
                fetch_remote_capability_export(remote_bootstrap.clone(), "echo-factory")
                    .await
                    .unwrap();
            let factory = generic_proxy_test_capnp::echo_factory::Client {
                client: factory_client_any,
            };
            let mut get_echo = factory.get_echo_request();
            get_echo.get().set_prefix("ephemeral:");
            let get_echo_resp = get_echo.send().promise.await.unwrap();
            let returned_echo = get_echo_resp.get().unwrap().get_echo().unwrap();

            let remote_object_id = register_remote_capability(
                remote_bootstrap,
                returned_echo.client,
                "Registered Echo",
                ImportedRemoteCapabilityKind::Other,
                "dev.iroh-tunnel.test/echo",
                None,
            )
            .await
            .unwrap();

            let registered = exporter_state
                .lock()
                .unwrap()
                .registered_remote_caps
                .get(&remote_object_id)
                .cloned()
                .unwrap();
            assert!(matches!(
                registered.durability,
                RegisteredRemoteCapabilityDurability::Ephemeral
            ));
            assert!(registered.saved_token.is_none());

            let _ = exporter_sandstorm_api;
            importer_app.close_test_endpoint().await;
            exporter_app.close_test_endpoint().await;
        });
    }

    #[test]
    fn localized_proxy_for_ephemeral_registered_remote_capability_fails_after_remote_restart() {
        run_local_async_test(async {
            let exporter_storage_root = make_test_storage_root("registered-ephemeral-restart-exporter");
            let (exporter_app, exporter_state, exporter_sandstorm_api) =
                build_test_app_with_storage(exporter_storage_root.clone(), 97).await;
            let (importer_app, importer_state, importer_sandstorm_api) =
                build_test_app("registered-ephemeral-restart-importer", 98).await;
            let echo_factory: generic_proxy_test_capnp::echo_factory::Client =
                new_client(crate::test_support::FakeEchoFactory);

            exporter_app
                .seed_exported_generic_capability_for_test(
                    "echo-factory",
                    "Echo Factory",
                    "dev.iroh-tunnel.test/echo-factory",
                    echo_factory.client,
                )
                .unwrap();
            importer_app
                .set_remote_ticket_for_test(exporter_app.local_ticket_for_test().unwrap())
                .unwrap();
            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();

            let remote_bootstrap = importer_state
                .lock()
                .unwrap()
                .peer_rpc_session
                .as_ref()
                .unwrap()
                .remote_bootstrap
                .clone();
            let (_, _, _, _, factory_client_any) =
                fetch_remote_capability_export(remote_bootstrap.clone(), "echo-factory")
                    .await
                    .unwrap();
            let factory = generic_proxy_test_capnp::echo_factory::Client {
                client: factory_client_any,
            };
            let mut get_echo = factory.get_echo_request();
            get_echo.get().set_prefix("restart:");
            let get_echo_resp = get_echo.send().promise.await.unwrap();
            let returned_echo = get_echo_resp.get().unwrap().get_echo().unwrap();

            let remote_object_id = register_remote_capability(
                remote_bootstrap,
                returned_echo.client,
                "Registered Echo",
                ImportedRemoteCapabilityKind::Other,
                "dev.iroh-tunnel.test/echo",
                None,
            )
            .await
            .unwrap();

            let registered = exporter_state
                .lock()
                .unwrap()
                .registered_remote_caps
                .get(&remote_object_id)
                .cloned()
                .unwrap();
            assert!(matches!(
                registered.durability,
                RegisteredRemoteCapabilityDurability::Ephemeral
            ));

            let (_label, object_id) = create_local_proxy_for_registered_remote_object(
                &importer_state,
                &remote_object_id,
            )
            .await
            .unwrap();
            let restored = importer_app
                .restore_object_capability(importer_sandstorm_api.clone(), &object_id)
                .await
                .unwrap();
            let echo = generic_proxy_test_capnp::echo::Client { client: restored };
            let mut ping = echo.ping_request();
            ping.get().set_text("before");
            let ping_resp = ping.send().promise.await.unwrap();
            assert_eq!(
                ping_resp.get().unwrap().get_text().unwrap().to_str().unwrap(),
                "restart:before"
            );

            importer_app.disconnect_peer_rpc_session().unwrap();
            exporter_app.close_test_endpoint().await;

            let (restarted_exporter_app, restarted_exporter_state, restarted_exporter_sandstorm_api) =
                build_test_app_with_storage_loaded(exporter_storage_root, 97).await;
            let restarted_registered = restarted_exporter_state
                .lock()
                .unwrap()
                .registered_remote_caps
                .get(&remote_object_id)
                .cloned()
                .unwrap();
            assert!(restarted_registered.client.is_none());
            assert!(matches!(
                restarted_registered.durability,
                RegisteredRemoteCapabilityDurability::Ephemeral
            ));

            importer_app
                .set_remote_ticket_for_test(restarted_exporter_app.local_ticket_for_test().unwrap())
                .unwrap();
            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();

            let restored = importer_app
                .restore_object_capability(importer_sandstorm_api, &object_id)
                .await
                .unwrap();
            let echo = generic_proxy_test_capnp::echo::Client { client: restored };
            let mut ping = echo.ping_request();
            ping.get().set_text("after");
            let err = match ping.send().promise.await {
                Ok(_) => panic!("ping unexpectedly succeeded after remote restart"),
                Err(err) => err.to_string(),
            };
            assert!(err.contains("is ephemeral and cannot be restored after restart"));

            let _ = exporter_sandstorm_api;
            let _ = restarted_exporter_sandstorm_api;
            importer_app.close_test_endpoint().await;
            restarted_exporter_app.close_test_endpoint().await;
        });
    }

    #[test]
    fn returned_capability_from_local_proxy_survives_reconnect_via_auto_localization() {
        run_local_async_test(async {
            let (exporter_app, _, exporter_sandstorm_api) =
                build_test_app("auto-localized-return-exporter", 89).await;
            let (importer_app, _, importer_sandstorm_api) =
                build_test_app("auto-localized-return-importer", 90).await;
            let echo_factory: generic_proxy_test_capnp::echo_factory::Client =
                new_client(crate::test_support::FakeEchoFactory);

            exporter_app
                .seed_exported_generic_capability_for_test(
                    "echo-factory",
                    "Echo Factory",
                    "dev.iroh-tunnel.test/echo-factory",
                    echo_factory.client,
                )
                .unwrap();
            importer_app
                .set_remote_ticket_for_test(exporter_app.local_ticket_for_test().unwrap())
                .unwrap();
            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();

            let (_, object_id) = importer_app
                .create_local_proxy_for_remote_export("echo-factory")
                .await
                .unwrap();
            let restored = importer_app
                .restore_object_capability(importer_sandstorm_api.clone(), &object_id)
                .await
                .unwrap();
            let factory = generic_proxy_test_capnp::echo_factory::Client { client: restored };
            let mut get_echo = factory.get_echo_request();
            get_echo.get().set_prefix("auto:");
            let get_echo_resp = get_echo.send().promise.await.unwrap();
            let echo = get_echo_resp.get().unwrap().get_echo().unwrap();

            importer_app.disconnect_peer_rpc_session().unwrap();
            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api)
                .await
                .unwrap();

            let mut ping = echo.ping_request();
            ping.get().set_text("hello");
            let ping_resp = ping.send().promise.await.unwrap();
            let text = ping_resp
                .get()
                .unwrap()
                .get_text()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();
            assert_eq!(text, "auto:hello");

            let _ = exporter_sandstorm_api;
            importer_app.close_test_endpoint().await;
            exporter_app.close_test_endpoint().await;
        });
    }

    #[test]
    fn local_proxy_unwraps_localized_capability_parameters() {
        run_local_async_test(async {
            let (exporter_app, _, exporter_sandstorm_api) =
                build_test_app("param-unwrap-exporter", 91).await;
            let (importer_app, _, importer_sandstorm_api) =
                build_test_app("param-unwrap-importer", 92).await;
            let echo_factory: generic_proxy_test_capnp::echo_factory::Client =
                new_client(crate::test_support::FakeEchoFactory);
            let echo_relay: generic_proxy_test_capnp::echo_relay::Client =
                new_client(crate::test_support::FakeEchoRelay);

            exporter_app
                .seed_exported_generic_capability_for_test(
                    "echo-factory",
                    "Echo Factory",
                    "dev.iroh-tunnel.test/echo-factory",
                    echo_factory.client,
                )
                .unwrap();
            exporter_app
                .seed_exported_generic_capability_for_test(
                    "echo-relay",
                    "Echo Relay",
                    "dev.iroh-tunnel.test/echo-relay",
                    echo_relay.client,
                )
                .unwrap();
            importer_app
                .set_remote_ticket_for_test(exporter_app.local_ticket_for_test().unwrap())
                .unwrap();
            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();

            let (_, factory_object_id) = importer_app
                .create_local_proxy_for_remote_export("echo-factory")
                .await
                .unwrap();
            let (_, relay_object_id) = importer_app
                .create_local_proxy_for_remote_export("echo-relay")
                .await
                .unwrap();
            let factory = generic_proxy_test_capnp::echo_factory::Client {
                client: importer_app
                    .restore_object_capability(importer_sandstorm_api.clone(), &factory_object_id)
                    .await
                    .unwrap(),
            };
            let relay = generic_proxy_test_capnp::echo_relay::Client {
                client: importer_app
                    .restore_object_capability(importer_sandstorm_api, &relay_object_id)
                    .await
                    .unwrap(),
            };

            let mut get_echo = factory.get_echo_request();
            get_echo.get().set_prefix("param:");
            let get_echo_resp = get_echo.send().promise.await.unwrap();
            let echo = get_echo_resp.get().unwrap().get_echo().unwrap();
            let echoed =
                crate::test_support::invoke_echo_relay_for_test(relay, echo, "hello").await.unwrap();

            assert_eq!(echoed, "param:hello");

            let _ = exporter_sandstorm_api;
            importer_app.close_test_endpoint().await;
            exporter_app.close_test_endpoint().await;
        });
    }

    #[test]
    fn repeated_reconnect_does_not_duplicate_imported_caps() {
        run_local_async_test(async {
            let (exporter_app, _, exporter_sandstorm_api) =
                build_test_app("churn-exporter", 81).await;
            let (importer_app, _, importer_sandstorm_api) =
                build_test_app("churn-importer", 82).await;

            exporter_app
                .seed_exported_ip_network_for_test(
                    "ip-network-export",
                    "Fake IpNetwork",
                    new_client(crate::test_support::FakeIpNetwork {
                        response_bytes: b"HTTP/1.1 200 OK\r\n\r\nchurn".to_vec(),
                    }),
                )
                .unwrap();
            importer_app
                .set_remote_ticket_for_test(exporter_app.local_ticket_for_test().unwrap())
                .unwrap();

            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();
            let (_, object_id) = importer_app
                .import_remote_ip_network_export("ip-network-export")
                .await
                .unwrap();

            for _ in 0..3 {
                importer_app.disconnect_peer_rpc_session().unwrap();
                importer_app
                    .connect_peer_rpc_session(importer_sandstorm_api.clone())
                    .await
                    .unwrap();
                assert_eq!(
                    imported_ip_network_object_id(&importer_app).as_deref(),
                    Some(object_id.as_str())
                );
                assert_eq!(imported_remote_cap_count(&importer_app), 1);
            }

            let summary = importer_app
                .invoke_imported_remote_ip_network("churn.example", 8080)
                .await
                .unwrap();
            assert_eq!(summary.response_bytes, b"HTTP/1.1 200 OK\r\n\r\nchurn".to_vec());

            let _ = exporter_sandstorm_api;
            importer_app.close_test_endpoint().await;
            exporter_app.close_test_endpoint().await;
        });
    }

    #[test]
    fn swapped_export_gets_a_new_object_id() {
        run_local_async_test(async {
            let (exporter_app, _, exporter_sandstorm_api) =
                build_test_app("swap-exporter", 91).await;
            let (importer_app, _, importer_sandstorm_api) =
                build_test_app("swap-importer", 92).await;

            exporter_app
                .seed_exported_api_session_for_test(
                    "preview-api-v1",
                    "Preview API v1",
                    new_client(crate::test_support::FakePreviewApiSession {
                        response_bytes: b"%PDF-V1\n".to_vec(),
                    }),
                )
                .unwrap();
            importer_app
                .set_remote_ticket_for_test(exporter_app.local_ticket_for_test().unwrap())
                .unwrap();

            let (_, exports) = importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();
            assert_eq!(exports[0].id, "preview-api-v1");
            let (_, object_id) = importer_app
                .import_remote_api_session_export("preview-api-v1")
                .await
                .unwrap();
            assert_eq!(object_id, "remote-cap-1");

            importer_app.disconnect_peer_rpc_session().unwrap();
            exporter_app
                .seed_exported_api_session_for_test(
                    "preview-api-v2",
                    "Preview API v2",
                    new_client(crate::test_support::FakePreviewApiSession {
                        response_bytes: b"%PDF-V2\n".to_vec(),
                    }),
                )
                .unwrap();

            let (_, exports) = importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();
            assert_eq!(exports[0].id, "preview-api-v2");
            let (_, new_object_id) = importer_app
                .import_remote_api_session_export("preview-api-v2")
                .await
                .unwrap();
            assert_ne!(new_object_id, object_id);

            let summary = importer_app
                .invoke_imported_remote_api_session("swap.docx", b"swap")
                .await
                .unwrap();
            assert_eq!(summary.response_bytes, b"%PDF-V2\n".to_vec());

            let _ = exporter_sandstorm_api;
            importer_app.close_test_endpoint().await;
            exporter_app.close_test_endpoint().await;
        });
    }

    #[test]
    fn cleared_export_stays_persisted_but_not_live_after_reconnect() {
        run_local_async_test(async {
            let (exporter_app, _, exporter_sandstorm_api) =
                build_test_app("clear-exporter", 101).await;
            let (importer_app, _, importer_sandstorm_api) =
                build_test_app("clear-importer", 102).await;

            exporter_app
                .seed_exported_api_session_for_test(
                    "preview-api",
                    "Preview API",
                    new_client(crate::test_support::FakePreviewApiSession {
                        response_bytes: b"%PDF-CLEAR\n".to_vec(),
                    }),
                )
                .unwrap();
            importer_app
                .set_remote_ticket_for_test(exporter_app.local_ticket_for_test().unwrap())
                .unwrap();

            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();
            let (_, object_id) = importer_app
                .import_remote_api_session_export("preview-api")
                .await
                .unwrap();

            importer_app.disconnect_peer_rpc_session().unwrap();
            exporter_app.clear_exported_api_session_for_test().unwrap();

            let (_, exports) = importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();
            assert!(exports.is_empty());
            assert_eq!(imported_api_session_object_id(&importer_app), None);
            assert_eq!(
                persisted_api_session_object_id(&importer_app).as_deref(),
                Some(object_id.as_str())
            );

            let err = match importer_app
                .restore_object_capability(importer_sandstorm_api, &object_id)
                .await
            {
                Ok(_) => panic!("restore unexpectedly succeeded after export clear"),
                Err(err) => err,
            };
            assert!(err.contains("not currently connected"));

            let _ = exporter_sandstorm_api;
            importer_app.close_test_endpoint().await;
            exporter_app.close_test_endpoint().await;
        });
    }

    #[test]
    fn reconnecting_to_different_peer_reuses_object_id_when_export_id_matches() {
        run_local_async_test(async {
            let (exporter_a_app, _, exporter_a_sandstorm_api) =
                build_test_app("peer-a-exporter", 111).await;
            let (exporter_b_app, _, exporter_b_sandstorm_api) =
                build_test_app("peer-b-exporter", 112).await;
            let (importer_app, _, importer_sandstorm_api) =
                build_test_app("peer-switch-importer", 113).await;

            exporter_a_app
                .seed_exported_api_session_for_test(
                    "shared-export",
                    "Shared Export A",
                    new_client(crate::test_support::FakePreviewApiSession {
                        response_bytes: b"%PDF-PEER-A\n".to_vec(),
                    }),
                )
                .unwrap();
            exporter_b_app
                .seed_exported_api_session_for_test(
                    "shared-export",
                    "Shared Export B",
                    new_client(crate::test_support::FakePreviewApiSession {
                        response_bytes: b"%PDF-PEER-B\n".to_vec(),
                    }),
                )
                .unwrap();

            importer_app
                .set_remote_ticket_for_test(exporter_a_app.local_ticket_for_test().unwrap())
                .unwrap();
            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();
            let (_, object_id) = importer_app
                .import_remote_api_session_export("shared-export")
                .await
                .unwrap();
            let summary_a = importer_app
                .invoke_imported_remote_api_session("peer-a.docx", b"a")
                .await
                .unwrap();
            assert_eq!(summary_a.response_bytes, b"%PDF-PEER-A\n".to_vec());

            importer_app.disconnect_peer_rpc_session().unwrap();
            importer_app
                .set_remote_ticket_for_test(exporter_b_app.local_ticket_for_test().unwrap())
                .unwrap();
            importer_app
                .connect_peer_rpc_session(importer_sandstorm_api.clone())
                .await
                .unwrap();

            assert_eq!(
                imported_api_session_object_id(&importer_app).as_deref(),
                Some(object_id.as_str())
            );
            let summary_b = importer_app
                .invoke_imported_remote_api_session("peer-b.docx", b"b")
                .await
                .unwrap();
            assert_eq!(summary_b.response_bytes, b"%PDF-PEER-B\n".to_vec());

            let _ = exporter_a_sandstorm_api;
            let _ = exporter_b_sandstorm_api;
            importer_app.close_test_endpoint().await;
            exporter_a_app.close_test_endpoint().await;
            exporter_b_app.close_test_endpoint().await;
        });
    }
}

#[derive(Clone)]
struct PeerBootstrapImpl {
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    app_state: Arc<Mutex<AppState>>,
}

impl tunnel_capnp::peer_bootstrap::Server for PeerBootstrapImpl {
    fn list_capability_exports(
        self: Rc<Self>,
        _: tunnel_capnp::peer_bootstrap::ListCapabilityExportsParams,
        mut results: tunnel_capnp::peer_bootstrap::ListCapabilityExportsResults,
    ) -> Promise<(), capnp::Error> {
        let exports = match self.app_state.lock() {
            Ok(guard) => guard
                .shared_caps
                .iter()
                .filter(|cap| cap.enabled)
                .map(|cap| PeerRpcCapabilityExport {
                    id: cap.saved_cap.id.clone(),
                    label: cap.label.clone(),
                    kind: cap.kind,
                    type_tag: shared_capability_type_tag(cap),
                    descriptor_json: cap.saved_cap.descriptor_json.clone(),
                })
                .collect::<Vec<_>>(),
            Err(_) => {
                return Promise::err(capnp::Error::failed(
                    "app state lock poisoned".to_string(),
                ));
            }
        };

        let mut result = results.get();
        let mut entries = result.reborrow().init_exports(exports.len() as u32);
        for (index, export) in exports.iter().enumerate() {
            let mut entry = entries.reborrow().get(index as u32);
            entry.set_id(&export.id);
            entry.set_label(&export.label);
            entry.set_kind(match export.kind {
                SharedCapabilityKind::IpNetwork => tunnel_capnp::CapabilityKind::IpNetwork,
                SharedCapabilityKind::ApiSession => tunnel_capnp::CapabilityKind::ApiSession,
                SharedCapabilityKind::Other => tunnel_capnp::CapabilityKind::Other,
            });
            entry.set_type_tag(&export.type_tag);
            entry.set_descriptor_json(export.descriptor_json.as_deref().unwrap_or(""));
        }
        Promise::ok(())
    }

    fn list_ip_network_exports(
        self: Rc<Self>,
        _: tunnel_capnp::peer_bootstrap::ListIpNetworkExportsParams,
        mut results: tunnel_capnp::peer_bootstrap::ListIpNetworkExportsResults,
    ) -> Promise<(), capnp::Error> {
        let exports = match self.app_state.lock() {
            Ok(guard) => guard
                .shared_caps
                .iter()
                .filter(|cap| cap.kind == SharedCapabilityKind::IpNetwork && cap.enabled)
                .map(|cap| cap.saved_cap.clone())
                .collect::<Vec<_>>(),
            Err(_) => {
                return Promise::err(capnp::Error::failed(
                    "app state lock poisoned".to_string(),
                ));
            }
        };

        let mut result = results.get();
        let mut entries = result.reborrow().init_exports(exports.len() as u32);
        for (index, export) in exports.iter().enumerate() {
            let mut entry = entries.reborrow().get(index as u32);
            entry.set_id(&export.id);
            entry.set_label(&export.label);
        }
        Promise::ok(())
    }

    fn get_capability_export(
        self: Rc<Self>,
        params: tunnel_capnp::peer_bootstrap::GetCapabilityExportParams,
        mut results: tunnel_capnp::peer_bootstrap::GetCapabilityExportResults,
    ) -> Promise<(), capnp::Error> {
        let requested_id = match params.get() {
            Ok(params) => match params.get_id() {
                Ok(value) => value.to_str().unwrap_or("").to_string(),
                Err(err) => return Promise::err(err),
            },
            Err(err) => return Promise::err(err),
        };

        let export = match self.app_state.lock() {
            Ok(guard) => match guard
                .shared_caps
                .iter()
                .find(|cap| cap.enabled && cap.saved_cap.id == requested_id)
            {
                Some(cap) => cap.clone(),
                None => {
                    return Promise::err(capnp::Error::failed(format!(
                        "unknown capability export id: {requested_id}"
                    )))
                }
            },
            Err(_) => {
                return Promise::err(capnp::Error::failed(
                    "app state lock poisoned".to_string(),
                ));
            }
        };

        let sandstorm_api = self.sandstorm_api.clone();
        let app_state = self.app_state.clone();
        Promise::from_future(async move {
            let cap = {
                let guard = app_state
                    .lock()
                    .map_err(|_| capnp::Error::failed("app state lock poisoned".to_string()))?;
                guard.exported_caps_live.get(&export.saved_cap.id).cloned()
            };
            let cap = match cap {
                Some(cap) => cap,
                None => {
                    let cap =
                        restore_saved_generic_capability(sandstorm_api, &export.saved_cap.saved_token)
                            .await
                            .map_err(capnp::Error::failed)?;
                    if let Ok(mut guard) = app_state.lock() {
                        guard
                            .exported_caps_live
                            .insert(export.saved_cap.id.clone(), cap.clone());
                    }
                    cap
                }
            };
            let mut result = results.get();
            result.reborrow().get_cap().set_as_capability(cap.hook);
            result.set_label(&export.label);
            result.set_kind(match export.kind {
                SharedCapabilityKind::IpNetwork => tunnel_capnp::CapabilityKind::IpNetwork,
                SharedCapabilityKind::ApiSession => tunnel_capnp::CapabilityKind::ApiSession,
                SharedCapabilityKind::Other => tunnel_capnp::CapabilityKind::Other,
            });
            result.set_type_tag(&shared_capability_type_tag(&export));
            result.set_descriptor_json(export.saved_cap.descriptor_json.as_deref().unwrap_or(""));
            Ok(())
        })
    }

    fn register_capability(
        self: Rc<Self>,
        params: tunnel_capnp::peer_bootstrap::RegisterCapabilityParams,
        mut results: tunnel_capnp::peer_bootstrap::RegisterCapabilityResults,
    ) -> Promise<(), capnp::Error> {
        let params = match params.get() {
            Ok(value) => value,
            Err(err) => return Promise::err(err),
        };
        let client = match params.get_cap().get_as_capability::<capnp::capability::Client>() {
            Ok(client) => client,
            Err(err) => return Promise::err(err),
        };
        let label = params
            .get_label()
            .ok()
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_string();
        let kind = match params.get_kind() {
            Ok(tunnel_capnp::CapabilityKind::IpNetwork) => ImportedRemoteCapabilityKind::IpNetwork,
            Ok(tunnel_capnp::CapabilityKind::ApiSession) => ImportedRemoteCapabilityKind::ApiSession,
            Ok(tunnel_capnp::CapabilityKind::Other) | Err(_) => ImportedRemoteCapabilityKind::Other,
        };
        let type_tag = params
            .get_type_tag()
            .ok()
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_string();
        let descriptor_json = params
            .get_descriptor_json()
            .ok()
            .and_then(|value| value.to_str().ok())
            .map(|value| value.to_string())
            .filter(|value| !value.is_empty());
        let hook_ptr = client.hook.get_ptr();

        let app_state = self.app_state.clone();
        let sandstorm_api = self.sandstorm_api.clone();
        Promise::from_future(async move {
            let existing = match app_state.lock() {
                Ok(guard) => guard
                    .registered_remote_hook_object_ids
                    .get(&hook_ptr)
                    .cloned(),
                Err(_) => {
                    return Err(capnp::Error::failed(
                        "app state lock poisoned".to_string(),
                    ))
                }
            };
            if let Some(existing) = existing {
                results.get().set_remote_object_id(&existing);
                return Ok(());
            }

            let saved_token = save_capability(sandstorm_api, client.clone(), &label).await.ok();
            let durability = if saved_token.is_some() {
                RegisteredRemoteCapabilityDurability::Saveable
            } else {
                RegisteredRemoteCapabilityDurability::Ephemeral
            };

            let remote_object_id = {
                let mut guard = app_state
                    .lock()
                    .map_err(|_| capnp::Error::failed("app state lock poisoned".to_string()))?;
                let storage = guard.storage.clone();
                guard.next_registered_remote_cap_id += 1;
                let remote_object_id =
                    format!("remote-registered-cap-{}", guard.next_registered_remote_cap_id);
                guard.registered_remote_caps.insert(
                    remote_object_id.clone(),
                    RegisteredRemoteCapability {
                        remote_object_id: remote_object_id.clone(),
                        label,
                        kind,
                        type_tag,
                        descriptor_json,
                        durability,
                        saved_token,
                        created_at_ms: now_ms(),
                        client: Some(client),
                    },
                );
                guard
                    .registered_remote_hook_object_ids
                    .insert(hook_ptr, remote_object_id.clone());
                persist_registered_remote_capabilities(
                    &storage,
                    guard.registered_remote_caps.values().cloned(),
                )
                .map_err(capnp::Error::failed)?;
                remote_object_id
            };

            results.get().set_remote_object_id(&remote_object_id);
            Ok(())
        })
    }

    fn get_registered_capability(
        self: Rc<Self>,
        params: tunnel_capnp::peer_bootstrap::GetRegisteredCapabilityParams,
        mut results: tunnel_capnp::peer_bootstrap::GetRegisteredCapabilityResults,
    ) -> Promise<(), capnp::Error> {
        let requested_id = match params.get() {
            Ok(params) => match params.get_remote_object_id() {
                Ok(value) => value.to_str().unwrap_or("").to_string(),
                Err(err) => return Promise::err(err),
            },
            Err(err) => return Promise::err(err),
        };
        let app_state = self.app_state.clone();
        let sandstorm_api = self.sandstorm_api.clone();
        Promise::from_future(async move {
            let mut registered = {
                let guard = app_state
                    .lock()
                    .map_err(|_| capnp::Error::failed("app state lock poisoned".to_string()))?;
                guard
                    .registered_remote_caps
                    .get(&requested_id)
                    .cloned()
                    .ok_or_else(|| {
                        capnp::Error::failed(format!(
                            "unknown registered remote capability id: {requested_id}"
                        ))
                    })?
            };
            eprintln!(
                "get_registered_capability: remote_object_id={} has_live_client={} durability={:?} has_saved_token={} at_ms={}",
                requested_id,
                registered.client.is_some(),
                registered.durability,
                registered.saved_token.is_some(),
                now_ms()
            );

            if registered.client.is_none() {
                let saved_token = registered.saved_token.clone().ok_or_else(|| {
                    let reason = match registered.durability {
                        RegisteredRemoteCapabilityDurability::Saveable => {
                            "is marked saveable but is missing its saved token"
                        }
                        RegisteredRemoteCapabilityDurability::Ephemeral => {
                            "is ephemeral and cannot be restored after restart"
                        }
                    };
                    capnp::Error::failed(format!(
                        "registered remote capability {requested_id} {reason}"
                    ))
                })?;
                eprintln!(
                    "get_registered_capability: remote_object_id={} restoring from saved token at_ms={}",
                    requested_id,
                    now_ms()
                );
                let restored = restore_saved_generic_capability(sandstorm_api, &saved_token)
                    .await
                    .map_err(capnp::Error::failed)?;
                let hook_ptr = restored.hook.get_ptr();
                registered.client = Some(restored.clone());
                let mut guard = app_state
                    .lock()
                    .map_err(|_| capnp::Error::failed("app state lock poisoned".to_string()))?;
                if let Some(entry) = guard.registered_remote_caps.get_mut(&requested_id) {
                    entry.client = Some(restored);
                }
                guard
                    .registered_remote_hook_object_ids
                    .insert(hook_ptr, requested_id.clone());
            }

            let client = registered
                .client
                .clone()
                .ok_or_else(|| capnp::Error::failed("registered capability missing live client".to_string()))?;
            let mut result = results.get();
            result.reborrow().get_cap().set_as_capability(client.hook);
            result.set_label(&registered.label);
            result.set_kind(imported_kind_to_tunnel_kind(registered.kind));
            result.set_type_tag(&registered.type_tag);
            result.set_descriptor_json(registered.descriptor_json.as_deref().unwrap_or(""));
            Ok(())
        })
    }

    fn get_local_proxy_for_peer_registered_capability(
        self: Rc<Self>,
        params: tunnel_capnp::peer_bootstrap::GetLocalProxyForPeerRegisteredCapabilityParams,
        mut results: tunnel_capnp::peer_bootstrap::GetLocalProxyForPeerRegisteredCapabilityResults,
    ) -> Promise<(), capnp::Error> {
        let requested_id = match params.get() {
            Ok(params) => match params.get_remote_object_id() {
                Ok(value) => value.to_str().unwrap_or("").to_string(),
                Err(err) => return Promise::err(err),
            },
            Err(err) => return Promise::err(err),
        };
        let app_state = self.app_state.clone();
        let sandstorm_api = self.sandstorm_api.clone();
        Promise::from_future(async move {
            let (label, object_id) =
                create_local_proxy_for_registered_remote_object(&app_state, &requested_id)
                    .await
                    .map_err(capnp::Error::failed)?;
            let cap = restore_app_object_capability(sandstorm_api, &app_state, &object_id)
                .await
                .map_err(capnp::Error::failed)?;
            let type_tag = {
                let guard = app_state
                    .lock()
                    .map_err(|_| capnp::Error::failed("app state lock poisoned".to_string()))?;
                let record = guard
                    .local_proxy_caps
                    .iter()
                    .find(|record| record.object_id == object_id)
                    .ok_or_else(|| {
                        capnp::Error::failed(format!(
                            "missing local proxy record after creation: {object_id}"
                        ))
                    })?;
                record.type_tag.clone()
            };
            let descriptor_json =
                capability_descriptor_for_object_id(&app_state, &object_id).map_err(capnp::Error::failed)?;
            let mut result = results.get();
            result.reborrow().get_cap().set_as_capability(cap.hook);
            result.set_label(&label);
            result.set_kind(tunnel_capnp::CapabilityKind::Other);
            result.set_type_tag(&type_tag);
            result.set_descriptor_json(descriptor_json.as_deref().unwrap_or(""));
            Ok(())
        })
    }

    fn get_ip_network_export(
        self: Rc<Self>,
        params: tunnel_capnp::peer_bootstrap::GetIpNetworkExportParams,
        mut results: tunnel_capnp::peer_bootstrap::GetIpNetworkExportResults,
    ) -> Promise<(), capnp::Error> {
        let export = match self.app_state.lock() {
            Ok(guard) => guard
                .shared_caps
                .iter()
                .find(|cap| cap.kind == SharedCapabilityKind::IpNetwork && cap.enabled)
                .map(|cap| cap.saved_cap.clone()),
            Err(_) => {
                return Promise::err(capnp::Error::failed(
                    "app state lock poisoned".to_string(),
                ));
            }
        };
        let requested_id = match params.get() {
            Ok(params) => match params.get_id() {
                Ok(value) => value.to_str().unwrap_or("").to_string(),
                Err(err) => return Promise::err(err),
            },
            Err(err) => return Promise::err(err),
        };

        let Some(mut export) = export else {
            return Promise::err(capnp::Error::failed(
                "no IpNetwork export is configured".to_string(),
            ));
        };
        if export.id != requested_id {
            export = match self.app_state.lock() {
                Ok(guard) => match guard
                    .shared_caps
                    .iter()
                    .find(|cap| {
                        cap.kind == SharedCapabilityKind::IpNetwork
                            && cap.enabled
                            && cap.saved_cap.id == requested_id
                    }) {
                    Some(cap) => cap.saved_cap.clone(),
                    None => {
                        return Promise::err(capnp::Error::failed(format!(
                            "unknown IpNetwork export id: {requested_id}"
                        )))
                    }
                },
                Err(_) => {
                    return Promise::err(capnp::Error::failed(
                        "app state lock poisoned".to_string(),
                    ));
                }
            };
        }

        let sandstorm_api = self.sandstorm_api.clone();
        let app_state = self.app_state.clone();
        Promise::from_future(async move {
            let cap = {
                let guard = app_state
                    .lock()
                    .map_err(|_| capnp::Error::failed("app state lock poisoned".to_string()))?;
                guard
                    .exported_ip_network_live
                    .get(&export.id)
                    .map(|value| value.client.clone())
            };
            let cap = match cap {
                Some(cap) => cap,
                None => {
                    let cap = restore_saved_ip_network(sandstorm_api, &export.saved_token)
                        .await
                        .map_err(capnp::Error::failed)?;
                    if let Ok(mut guard) = app_state.lock() {
                        guard.exported_ip_network_live.insert(
                            export.id.clone(),
                            ExportedIpNetworkState {
                                client: cap.clone(),
                            },
                        );
                    }
                    cap
                }
            };
            let mut result = results.get();
            result.set_label(&export.label);
            result.set_cap(cap);
            Ok(())
        })
    }

    fn list_api_session_exports(
        self: Rc<Self>,
        _: tunnel_capnp::peer_bootstrap::ListApiSessionExportsParams,
        mut results: tunnel_capnp::peer_bootstrap::ListApiSessionExportsResults,
    ) -> Promise<(), capnp::Error> {
        let exports = match self.app_state.lock() {
            Ok(guard) => guard
                .shared_caps
                .iter()
                .filter(|cap| cap.kind == SharedCapabilityKind::ApiSession && cap.enabled)
                .map(|cap| cap.saved_cap.clone())
                .collect::<Vec<_>>(),
            Err(_) => {
                return Promise::err(capnp::Error::failed(
                    "app state lock poisoned".to_string(),
                ));
            }
        };

        let mut result = results.get();
        let mut entries = result.reborrow().init_exports(exports.len() as u32);
        for (index, export) in exports.iter().enumerate() {
            let mut entry = entries.reborrow().get(index as u32);
            entry.set_id(&export.id);
            entry.set_label(&export.label);
        }
        Promise::ok(())
    }

    fn get_api_session_export(
        self: Rc<Self>,
        params: tunnel_capnp::peer_bootstrap::GetApiSessionExportParams,
        mut results: tunnel_capnp::peer_bootstrap::GetApiSessionExportResults,
    ) -> Promise<(), capnp::Error> {
        let export = match self.app_state.lock() {
            Ok(guard) => guard
                .shared_caps
                .iter()
                .find(|cap| cap.kind == SharedCapabilityKind::ApiSession && cap.enabled)
                .map(|cap| cap.saved_cap.clone()),
            Err(_) => {
                return Promise::err(capnp::Error::failed(
                    "app state lock poisoned".to_string(),
                ));
            }
        };
        let requested_id = match params.get() {
            Ok(params) => match params.get_id() {
                Ok(value) => value.to_str().unwrap_or("").to_string(),
                Err(err) => return Promise::err(err),
            },
            Err(err) => return Promise::err(err),
        };

        let Some(mut export) = export else {
            return Promise::err(capnp::Error::failed(
                "no ApiSession export is configured".to_string(),
            ));
        };
        if export.id != requested_id {
            export = match self.app_state.lock() {
                Ok(guard) => match guard
                    .shared_caps
                    .iter()
                    .find(|cap| {
                        cap.kind == SharedCapabilityKind::ApiSession
                            && cap.enabled
                            && cap.saved_cap.id == requested_id
                    }) {
                    Some(cap) => cap.saved_cap.clone(),
                    None => {
                        return Promise::err(capnp::Error::failed(format!(
                            "unknown ApiSession export id: {requested_id}"
                        )))
                    }
                },
                Err(_) => {
                    return Promise::err(capnp::Error::failed(
                        "app state lock poisoned".to_string(),
                    ));
                }
            };
        }

        let sandstorm_api = self.sandstorm_api.clone();
        let app_state = self.app_state.clone();
        Promise::from_future(async move {
            let cap = {
                let guard = app_state
                    .lock()
                    .map_err(|_| capnp::Error::failed("app state lock poisoned".to_string()))?;
                guard
                    .exported_api_session_live
                    .get(&export.id)
                    .map(|value| value.client.clone())
            };
            let cap = match cap {
                Some(cap) => cap,
                None => {
                    let cap = restore_saved_api_session(sandstorm_api, &export.saved_token)
                        .await
                        .map_err(capnp::Error::failed)?;
                    if let Ok(mut guard) = app_state.lock() {
                        guard.exported_api_session_live.insert(
                            export.id.clone(),
                            ExportedApiSessionState {
                                client: cap.clone(),
                            },
                        );
                    }
                    cap
                }
            };
            let mut result = results.get();
            result.set_label(&export.label);
            result.set_cap(cap);
            Ok(())
        })
    }
}

async fn run_iroh_accept_loop(
    endpoint: Endpoint,
    app_state: Arc<Mutex<AppState>>,
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
) {
    while let Some(incoming) = endpoint.accept().await {
        let app_state = app_state.clone();
        let sandstorm_api = sandstorm_api.clone();
        tokio::task::spawn_local(async move {
            let result = async {
                let mut accepting = incoming
                    .accept()
                    .map_err(|err| format!("failed to begin accepting incoming iroh connection: {err}"))?;
                let alpn = accepting
                    .alpn()
                    .await
                    .map_err(|err| format!("failed to inspect incoming ALPN: {err}"))?;
                if alpn == IROH_RPC_ALPN {
                    return accept_peer_rpc_connection(accepting, app_state, sandstorm_api).await;
                }
                if alpn == IROH_PAIR_ALPN {
                    return accept_pair_connection(accepting, app_state, sandstorm_api).await;
                }
                accept_probe_connection(accepting).await
            }
            .await;

            if let Err(err) = result {
                eprintln!("iroh accept loop error: {err}");
            }
        });
    }
}

async fn accept_probe_connection(
    accepting: iroh::endpoint::Accepting,
) -> Result<(), String> {
    eprintln!("iroh accept: incoming probe connection detected");
    let connection = accepting
        .await
        .map_err(|err| format!("incoming iroh connection failed: {err}"))?;
    eprintln!("iroh accept: probe connection accepted");
    let (mut send, mut recv) = connection
        .accept_bi()
        .await
        .map_err(|err| format!("failed to accept probe bi stream: {err}"))?;
    eprintln!("iroh accept: probe bi stream accepted");
    let data = recv
        .read_to_end(1024)
        .await
        .map_err(|err| format!("failed to read probe payload: {err}"))?;
    eprintln!("iroh accept: received {} probe bytes", data.len());
    send.write_all(&data)
        .await
        .map_err(|err| format!("failed to write probe response: {err}"))?;
    send.finish()
        .map_err(|err| format!("failed to finish probe response: {err}"))?;
    eprintln!("iroh accept: echoed {} probe bytes", data.len());
    let close_reason = connection.closed().await;
    eprintln!("iroh accept: probe connection closed: {close_reason}");
    Ok(())
}

async fn accept_peer_rpc_connection(
    accepting: iroh::endpoint::Accepting,
    app_state: Arc<Mutex<AppState>>,
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
) -> Result<(), String> {
    eprintln!("iroh rpc: incoming rpc connection detected");
    let connection = accepting
        .await
        .map_err(|err| format!("incoming rpc connection failed: {err}"))?;
    let peer_bootstrap: tunnel_capnp::peer_bootstrap::Client = new_client(PeerBootstrapImpl {
        sandstorm_api,
        app_state,
    });
    let (send, recv) = connection
        .accept_bi()
        .await
        .map_err(|err| format!("failed to accept rpc bi stream: {err}"))?;
    let network = Box::new(twoparty::VatNetwork::new(
        recv.compat(),
        send.compat_write(),
        rpc_twoparty_capnp::Side::Server,
        Default::default(),
    ));
    let rpc_system = RpcSystem::new(network, Some(peer_bootstrap.client));
    rpc_system
        .await
        .map_err(|err| format!("peer rpc system failed: {err}"))?;
    Ok(())
}

async fn accept_pair_connection(
    accepting: iroh::endpoint::Accepting,
    app_state: Arc<Mutex<AppState>>,
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
) -> Result<(), String> {
    eprintln!("iroh pair: incoming pairing connection detected");
    let connection = accepting
        .await
        .map_err(|err| format!("incoming pairing connection failed: {err}"))?;
    let remote_node_id = connection.remote_id().to_string();
    let (send, mut recv) = connection
        .accept_bi()
        .await
        .map_err(|err| format!("failed to accept pairing control stream: {err}"))?;
    let request_bytes = recv
        .read_to_end(4096)
        .await
        .map_err(|err| format!("failed to read pairing control request: {err}"))?;
    let request = decode_pair_control_message(&request_bytes)?;
    match request {
        PairControlMessage::Request { version } => {
            if version != PAIRING_PROTOCOL_VERSION {
                connection.close(0u32.into(), b"unsupported pairing protocol");
                return Err(format!("unsupported pairing protocol version: {version}"));
            }
        }
        _ => {
            connection.close(0u32.into(), b"invalid pairing request");
            return Err("unexpected pairing control message".to_string());
        }
    }

    enum IncomingPairAction {
        Store(PairingStatus),
        AutoAccept,
        Reject(&'static [u8], &'static str),
    }

    let action = {
        let guard = app_state
            .lock()
            .map_err(|_| "app state lock poisoned".to_string())?;
        if guard.peer_rpc_session.is_some()
            || guard.pending_incoming_connection.is_some()
            || guard.pending_outgoing_peer_node_id.is_some()
        {
            IncomingPairAction::Reject(
                b"pairing busy",
                "cannot accept pairing while another connection is active or pending",
            )
        } else if let Some(approved_peer_node_id) = guard.approved_peer_node_id.as_ref() {
            if approved_peer_node_id == &remote_node_id {
                if guard.tunnel_enabled {
                    IncomingPairAction::AutoAccept
                } else {
                    IncomingPairAction::Store(PairingStatus::Disabled)
                }
            } else {
                IncomingPairAction::Reject(
                    b"peer not approved",
                    "incoming pairing request does not match the approved peer",
                )
            }
        } else {
            IncomingPairAction::Store(PairingStatus::IncomingRequest)
        }
    };

    match action {
        IncomingPairAction::Reject(close_reason, err) => {
            let response = encode_pair_control_message(PairControlMessage::Response {
                version: PAIRING_PROTOCOL_VERSION,
                decision: PairControlDecision::Rejected,
            })?;
            let mut send = send;
            let _ = send.write_all(&response).await;
            let _ = send.finish();
            connection.close(0u32.into(), close_reason);
            Err(err.to_string())
        }
        IncomingPairAction::Store(status) => {
            let mut guard = app_state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            guard.pending_incoming_peer_node_id = Some(remote_node_id.clone());
            guard.pending_incoming_connection = Some(PendingIncomingConnection {
                remote_node_id,
                connection,
                handshake_send: send,
            });
            guard.pairing_status = status;
            Ok(())
        }
        IncomingPairAction::AutoAccept => {
            {
                let mut guard = app_state
                    .lock()
                    .map_err(|_| "app state lock poisoned".to_string())?;
                guard.pending_incoming_peer_node_id = Some(remote_node_id.clone());
                guard.pending_incoming_connection = Some(PendingIncomingConnection {
                    remote_node_id,
                    connection,
                    handshake_send: send,
                });
                guard.pairing_status = PairingStatus::IncomingRequest;
            }
            accept_pending_pair_connection(app_state, sandstorm_api).await
        }
    }
}

struct ProbeConnectionSummary {
    remote_node_id: String,
    response: String,
}

struct NetworkHttpProbeSummary {
    host: String,
    port: u16,
    path: String,
    response_preview: String,
    trace: String,
}

struct HttpProbeRequest {
    saved_token_hex: String,
    host: String,
    port: u16,
    path: String,
}

struct TcpProbeRequest {
    saved_token_hex: String,
    host: String,
    port: u16,
    payload: Vec<u8>,
}

struct TcpProbeSummary {
    host: String,
    port: u16,
    response_bytes: Vec<u8>,
    trace: String,
}

struct NetworkExchangeRequest {
    saved_token_hex: String,
    host: String,
    port: u16,
    payload: Vec<u8>,
}

struct UdpProbeRequest {
    saved_token_hex: String,
    host: String,
    port: u16,
    payload: Vec<u8>,
    wait_ms: u64,
}

struct TcpSessionOpenRequest {
    saved_token_hex: String,
    host: String,
    port: u16,
}

struct TcpSessionSendRequest {
    session_id: String,
    payload: Vec<u8>,
}

struct TcpSessionReceiveRequest {
    session_id: String,
    max_bytes: usize,
    wait_ms: u64,
}

struct TcpSessionCloseRequest {
    session_id: String,
}

struct RemoteIpNetworkInvokeRequest {
    host: String,
    port: u16,
}

struct RemoteApiSessionInvokeRequest {
    filename: String,
    payload: Vec<u8>,
}

struct TcpSessionBuffer {
    bytes: Vec<u8>,
    read_offset: usize,
    total_received_bytes: usize,
    write_calls: u32,
    saw_done: bool,
}

struct TcpSessionSnapshot {
    host: String,
    port: u16,
    buffered_bytes: usize,
    received_bytes: usize,
    write_calls: u32,
    done: bool,
    trace: String,
}

struct TcpSessionReadResult {
    bytes: Vec<u8>,
    buffered_bytes: usize,
    received_bytes: usize,
    write_calls: u32,
    done: bool,
    trace: String,
}

struct UdpPacketBuffer {
    packets: Vec<Vec<u8>>,
    packet_count: u32,
    total_received_bytes: usize,
}

struct UdpProbeSummary {
    host: String,
    port: u16,
    response_packet: Vec<u8>,
    response_packet_count: u32,
    response_byte_count: usize,
    trace: String,
}

struct SavedIpNetworkTcpConnection {
    upstream: util_capnp::byte_stream::Client,
    incoming: Arc<Mutex<TcpSessionBuffer>>,
    trace: Arc<Mutex<Vec<String>>>,
    notify: Arc<Notify>,
}

struct ApiSessionInvokeSummary {
    status_code: u16,
    content_type: String,
    response_bytes: Vec<u8>,
    trace: String,
}

struct SavedIpNetworkTcpSession {
    host: String,
    port: u16,
    upstream: util_capnp::byte_stream::Client,
    incoming: Arc<Mutex<TcpSessionBuffer>>,
    trace: Arc<Mutex<Vec<String>>>,
    notify: Arc<Notify>,
}

impl SavedIpNetworkTcpSession {
    fn snapshot(&self) -> Result<TcpSessionSnapshot, String> {
        let incoming = self
            .incoming
            .lock()
            .map_err(|_| "tcp session buffer lock poisoned".to_string())?;
        let trace = self
            .trace
            .lock()
            .map_err(|_| "tcp session trace lock poisoned".to_string())?
            .join(" -> ");
        Ok(TcpSessionSnapshot {
            host: self.host.clone(),
            port: self.port,
            buffered_bytes: incoming.bytes.len().saturating_sub(incoming.read_offset),
            received_bytes: incoming.total_received_bytes,
            write_calls: incoming.write_calls,
            done: incoming.saw_done,
            trace,
        })
    }
}

async fn probe_remote_connection(
    app_state: Arc<Mutex<AppState>>,
) -> Result<ProbeConnectionSummary, String> {
    let (endpoint, remote_ticket) = {
        let guard = app_state
            .lock()
            .map_err(|_| "app state lock poisoned".to_string())?;
        let endpoint = guard
            .iroh_endpoint
            .clone()
            .ok_or_else(|| "local iroh endpoint is not bound".to_string())?;
        let remote_ticket = guard
            .remote_ticket
            .clone()
            .ok_or_else(|| "no remote ticket configured".to_string())?;
        (endpoint, remote_ticket)
    };

    let remote_addr = parse_remote_ticket(&remote_ticket)?;
    let remote_node_id = remote_addr.id.to_string();
    eprintln!(
        "iroh probe: attempting connect to node_id={} transport_addrs={:?}",
        remote_node_id,
        remote_addr.addrs
    );
    let connection = endpoint
        .connect(remote_addr, IROH_ALPN)
        .await
        .map_err(|err| format!("failed to connect to remote peer: {err}"))?;
    eprintln!("iroh probe: connection established to node_id={remote_node_id}");
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|err| format!("failed to open bi stream: {err}"))?;
    eprintln!("iroh probe: bi stream opened");
    let payload = format!("iroh-tunnel probe {}", now_ms());
    send.write_all(payload.as_bytes())
        .await
        .map_err(|err| format!("failed to send probe payload: {err}"))?;
    eprintln!("iroh probe: sent {} probe bytes", payload.len());
    send.finish()
        .map_err(|err| format!("failed to finish probe send: {err}"))?;
    eprintln!("iroh probe: finished probe send");
    let response = match recv.read_to_end(1024).await {
        Ok(response) => response,
        Err(err) => {
            let close_reason = connection.closed().await;
            return Err(format!(
                "failed to read probe response: {err}; connection closed: {close_reason}"
            ));
        }
    };
    eprintln!("iroh probe: received {} response bytes", response.len());
    connection.close(0u32.into(), b"probe complete");
    Ok(ProbeConnectionSummary {
        remote_node_id,
        response: String::from_utf8_lossy(&response).to_string(),
    })
}

struct ByteStreamCollector {
    incoming: Arc<Mutex<TcpSessionBuffer>>,
    trace: Arc<Mutex<Vec<String>>>,
    notify: Arc<Notify>,
}

impl util_capnp::byte_stream::Server for ByteStreamCollector {
    fn write(
        self: Rc<Self>,
        params: util_capnp::byte_stream::WriteParams,
    ) -> Promise<(), capnp::Error> {
        let params = pry!(params.get());
        let data = pry!(params.get_data());
        let mut guard =
            pry!(self.incoming.lock().map_err(|_| {
                capnp::Error::failed("byte stream buffer lock poisoned".to_string())
            }));
        guard.write_calls += 1;
        guard.bytes.extend_from_slice(data);
        guard.total_received_bytes += data.len();
        drop(guard);
        if let Ok(mut trace) = self.trace.lock() {
            trace.push(format!("downstream-write:{}-bytes", data.len()));
        }
        self.notify.notify_waiters();
        Promise::ok(())
    }

    fn done(
        self: Rc<Self>,
        _: util_capnp::byte_stream::DoneParams,
        _: util_capnp::byte_stream::DoneResults,
    ) -> Promise<(), capnp::Error> {
        if let Ok(mut incoming) = self.incoming.lock() {
            incoming.saw_done = true;
        }
        if let Ok(mut trace) = self.trace.lock() {
            trace.push("downstream-done:ok".to_string());
        }
        self.notify.notify_waiters();
        Promise::ok(())
    }

    fn expect_size(
        self: Rc<Self>,
        _: util_capnp::byte_stream::ExpectSizeParams,
        _: util_capnp::byte_stream::ExpectSizeResults,
    ) -> Promise<(), capnp::Error> {
        Promise::ok(())
    }
}

struct UdpPacketCollector {
    incoming: Arc<Mutex<UdpPacketBuffer>>,
    trace: Arc<Mutex<Vec<String>>>,
    notify: Arc<Notify>,
}

impl ip_capnp::udp_port::Server for UdpPacketCollector {
    fn send(
        self: Rc<Self>,
        params: ip_capnp::udp_port::SendParams,
        _: ip_capnp::udp_port::SendResults,
    ) -> Promise<(), capnp::Error> {
        let params = pry!(params.get());
        let message = pry!(params.get_message());
        let mut incoming =
            pry!(self.incoming.lock().map_err(|_| {
                capnp::Error::failed("udp packet buffer lock poisoned".to_string())
            }));
        incoming.packets.push(message.to_vec());
        incoming.packet_count += 1;
        incoming.total_received_bytes += message.len();
        drop(incoming);
        if let Ok(mut trace) = self.trace.lock() {
            trace.push(format!("udp-recv:{}-bytes", message.len()));
        }
        self.notify.notify_waiters();
        Promise::ok(())
    }
}

async fn write_to_byte_stream(
    stream: util_capnp::byte_stream::Client,
    data: &[u8],
) -> Result<(), String> {
    let mut write_req = stream.write_request();
    write_req.get().set_data(data);
    write_req
        .send()
        .await
        .map_err(|err| format!("ByteStream.write() failed: {err}"))?;
    Ok(())
}

async fn close_byte_stream(stream: util_capnp::byte_stream::Client) -> Result<(), String> {
    let done_req = stream.done_request();
    done_req
        .send()
        .promise
        .await
        .map_err(|err| format!("ByteStream.done() failed: {err}"))?;
    Ok(())
}

async fn connect_saved_ip_network_tcp(
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    saved_token_hex: &str,
    host: &str,
    port: u16,
) -> Result<SavedIpNetworkTcpConnection, String> {
    let mut trace = vec!["restore:begin".to_string()];

    let ip_network = restore_saved_ip_network(sandstorm_api, saved_token_hex).await?;
    trace.push("restore:ok".to_string());

    let mut host_req = ip_network.get_remote_host_by_name_request();
    host_req.get().set_address(host);
    let host_resp = host_req
        .send()
        .promise
        .await
        .map_err(|err| format!("IpNetwork.getRemoteHostByName() failed: {err}"))?;
    trace.push("resolve-host:ok".to_string());
    let remote_host = host_resp
        .get()
        .map_err(|err| format!("failed to decode getRemoteHostByName() response: {err}"))?
        .get_host()
        .map_err(|err| format!("getRemoteHostByName() returned no host: {err}"))?;

    let mut port_req = remote_host.get_tcp_port_request();
    port_req.get().set_port_num(port);
    let port_resp = port_req
        .send()
        .promise
        .await
        .map_err(|err| format!("IpRemoteHost.getTcpPort() failed: {err}"))?;
    trace.push("get-tcp-port:ok".to_string());
    let tcp_port = port_resp
        .get()
        .map_err(|err| format!("failed to decode getTcpPort() response: {err}"))?
        .get_port()
        .map_err(|err| format!("getTcpPort() returned no port: {err}"))?;

    let incoming = Arc::new(Mutex::new(TcpSessionBuffer {
        bytes: Vec::new(),
        read_offset: 0,
        total_received_bytes: 0,
        write_calls: 0,
        saw_done: false,
    }));
    let trace = Arc::new(Mutex::new(trace));
    let notify = Arc::new(Notify::new());
    let downstream: util_capnp::byte_stream::Client = new_client(ByteStreamCollector {
        incoming: incoming.clone(),
        trace: trace.clone(),
        notify: notify.clone(),
    });

    let mut connect_req = tcp_port.connect_request();
    connect_req.get().set_downstream(downstream);
    let connect_resp = connect_req
        .send()
        .promise
        .await
        .map_err(|err| format!("TcpPort.connect() failed: {err}"))?;
    let upstream = connect_resp
        .get()
        .map_err(|err| format!("failed to decode connect() response: {err}"))?
        .get_upstream()
        .map_err(|err| format!("connect() returned no upstream stream: {err}"))?;

    {
        let mut trace_guard = trace
            .lock()
            .map_err(|_| "tcp session trace lock poisoned".to_string())?;
        trace_guard.push("connect:ok".to_string());
    }

    Ok(SavedIpNetworkTcpConnection {
        upstream,
        incoming,
        trace,
        notify,
    })
}

async fn restore_saved_ip_network_remote_host(
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    saved_token_hex: &str,
    host: &str,
) -> Result<(ip_capnp::ip_remote_host::Client, Vec<String>), String> {
    let mut trace = vec!["restore:begin".to_string()];
    let ip_network = restore_saved_ip_network(sandstorm_api, saved_token_hex).await?;
    trace.push("restore:ok".to_string());

    let mut host_req = ip_network.get_remote_host_by_name_request();
    host_req.get().set_address(host);
    let host_resp = host_req
        .send()
        .promise
        .await
        .map_err(|err| format!("IpNetwork.getRemoteHostByName() failed: {err}"))?;
    trace.push("resolve-host:ok".to_string());
    let remote_host = host_resp
        .get()
        .map_err(|err| format!("failed to decode getRemoteHostByName() response: {err}"))?
        .get_host()
        .map_err(|err| format!("getRemoteHostByName() returned no host: {err}"))?;

    Ok((remote_host, trace))
}

fn connection_into_session(
    connection: SavedIpNetworkTcpConnection,
    host: String,
    port: u16,
) -> Arc<SavedIpNetworkTcpSession> {
    Arc::new(SavedIpNetworkTcpSession {
        host,
        port,
        upstream: connection.upstream,
        incoming: connection.incoming,
        trace: connection.trace,
        notify: connection.notify,
    })
}

async fn send_tcp_session_bytes(
    session: &SavedIpNetworkTcpSession,
    payload: &[u8],
) -> Result<(), String> {
    let saw_done = session
        .incoming
        .lock()
        .map_err(|_| "tcp session buffer lock poisoned".to_string())?
        .saw_done;
    if saw_done {
        return Err("tcp session is already closed by the remote side".to_string());
    }
    write_to_byte_stream(session.upstream.clone(), payload).await?;
    let mut trace = session
        .trace
        .lock()
        .map_err(|_| "tcp session trace lock poisoned".to_string())?;
    trace.push(format!("payload-sent:{}-bytes", payload.len()));
    Ok(())
}

async fn close_tcp_session_writer(session: &SavedIpNetworkTcpSession) -> Result<(), String> {
    close_byte_stream(session.upstream.clone()).await?;
    let mut trace = session
        .trace
        .lock()
        .map_err(|_| "tcp session trace lock poisoned".to_string())?;
    trace.push("upstream-done:ok".to_string());
    Ok(())
}

async fn read_tcp_session_bytes(
    session: &SavedIpNetworkTcpSession,
    max_bytes: usize,
    wait_ms: u64,
) -> Result<TcpSessionReadResult, String> {
    let mut should_wait = true;
    loop {
        let notified = {
            let mut incoming = session
                .incoming
                .lock()
                .map_err(|_| "tcp session buffer lock poisoned".to_string())?;
            let available = incoming.bytes.len().saturating_sub(incoming.read_offset);
            if available > 0 || incoming.saw_done || !should_wait || wait_ms == 0 {
                let take = available.min(max_bytes);
                let start = incoming.read_offset;
                let end = start + take;
                let bytes = incoming.bytes[start..end].to_vec();
                incoming.read_offset = end;
                if incoming.read_offset == incoming.bytes.len() {
                    incoming.bytes.clear();
                    incoming.read_offset = 0;
                } else if incoming.read_offset > 8192
                    && incoming.read_offset * 2 >= incoming.bytes.len()
                {
                    let consumed = incoming.read_offset;
                    incoming.bytes.drain(..consumed);
                    incoming.read_offset = 0;
                }

                let trace = session
                    .trace
                    .lock()
                    .map_err(|_| "tcp session trace lock poisoned".to_string())?
                    .join(" -> ");
                return Ok(TcpSessionReadResult {
                    bytes,
                    buffered_bytes: incoming.bytes.len().saturating_sub(incoming.read_offset),
                    received_bytes: incoming.total_received_bytes,
                    write_calls: incoming.write_calls,
                    done: incoming.saw_done,
                    trace,
                });
            }
            session.notify.notified()
        };

        let _ = timeout(Duration::from_millis(wait_ms), notified).await;
        should_wait = false;
    }
}

fn insert_tcp_session(
    app_state: &Arc<Mutex<AppState>>,
    session: Arc<SavedIpNetworkTcpSession>,
) -> Result<String, String> {
    let mut guard = app_state
        .lock()
        .map_err(|_| "app state lock poisoned".to_string())?;
    guard.next_tcp_session_id += 1;
    let session_id = format!("tcp-session-{}", guard.next_tcp_session_id);
    guard
        .active_tcp_sessions
        .insert(session_id.clone(), session);
    Ok(session_id)
}

fn get_tcp_session(
    app_state: &Arc<Mutex<AppState>>,
    session_id: &str,
) -> Result<Arc<SavedIpNetworkTcpSession>, String> {
    let guard = app_state
        .lock()
        .map_err(|_| "app state lock poisoned".to_string())?;
    guard
        .active_tcp_sessions
        .get(session_id)
        .cloned()
        .ok_or_else(|| format!("unknown tcp session: {session_id}"))
}

fn remove_tcp_session(
    app_state: &Arc<Mutex<AppState>>,
    session_id: &str,
) -> Result<Arc<SavedIpNetworkTcpSession>, String> {
    let mut guard = app_state
        .lock()
        .map_err(|_| "app state lock poisoned".to_string())?;
    guard
        .active_tcp_sessions
        .remove(session_id)
        .ok_or_else(|| format!("unknown tcp session: {session_id}"))
}

async fn probe_saved_ip_network_udp(
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    probe_request: UdpProbeRequest,
) -> Result<UdpProbeSummary, String> {
    eprintln!(
        "udp probe: start host={} port={} wait_ms={} payload_bytes={}",
        probe_request.host,
        probe_request.port,
        probe_request.wait_ms,
        probe_request.payload.len()
    );
    let host = probe_request.host;
    let port = probe_request.port;
    let (remote_host, trace) =
        restore_saved_ip_network_remote_host(sandstorm_api, &probe_request.saved_token_hex, &host)
            .await?;
    eprintln!("udp probe: restore+resolve ok");
    let trace = Arc::new(Mutex::new(trace));

    let mut port_req = remote_host.get_udp_port_request();
    port_req.get().set_port_num(port);
    eprintln!("udp probe: requesting udp port");
    let port_resp = port_req
        .send()
        .promise
        .await
        .map_err(|err| format!("IpRemoteHost.getUdpPort() failed: {err}"))?;
    eprintln!("udp probe: getUdpPort ok");
    {
        let mut trace_guard = trace
            .lock()
            .map_err(|_| "udp trace lock poisoned".to_string())?;
        trace_guard.push("get-udp-port:ok".to_string());
    }
    let udp_port = port_resp
        .get()
        .map_err(|err| format!("failed to decode getUdpPort() response: {err}"))?
        .get_port()
        .map_err(|err| format!("getUdpPort() returned no port: {err}"))?;

    let incoming = Arc::new(Mutex::new(UdpPacketBuffer {
        packets: Vec::new(),
        packet_count: 0,
        total_received_bytes: 0,
    }));
    let notify = Arc::new(Notify::new());
    let return_port: ip_capnp::udp_port::Client = new_client(UdpPacketCollector {
        incoming: incoming.clone(),
        trace: trace.clone(),
        notify: notify.clone(),
    });

    let mut send_req = udp_port.send_request();
    {
        let mut params = send_req.get();
        params.set_message(&probe_request.payload);
        params.set_return_port(return_port);
    }
    let send_timeout_ms = probe_request.wait_ms.max(1_000);
    eprintln!(
        "udp probe: sending packet with timeout {}ms",
        send_timeout_ms
    );
    timeout(
        Duration::from_millis(send_timeout_ms),
        send_req.send().promise,
    )
    .await
    .map_err(|_| format!("UdpPort.send() timed out after {send_timeout_ms}ms"))?
    .map_err(|err| format!("UdpPort.send() failed: {err}"))?;
    eprintln!("udp probe: send ok");
    {
        let mut trace_guard = trace
            .lock()
            .map_err(|_| "udp trace lock poisoned".to_string())?;
        trace_guard.push(format!("udp-send:{}-bytes", probe_request.payload.len()));
    }

    let response_packet = loop {
        let notified = {
            let mut incoming_guard = incoming
                .lock()
                .map_err(|_| "udp packet buffer lock poisoned".to_string())?;
            if let Some(packet) = incoming_guard.packets.first().cloned() {
                incoming_guard.packets.remove(0);
                break packet;
            }
            notify.notified()
        };

        if timeout(Duration::from_millis(probe_request.wait_ms), notified)
            .await
            .is_err()
        {
            eprintln!("udp probe: response wait timed out");
            let trace_text = trace
                .lock()
                .map_err(|_| "udp trace lock poisoned".to_string())?
                .join(" -> ");
            return Err(format!(
                "udp probe timed out waiting for a response packet ({trace_text})"
            ));
        }
    };
    eprintln!("udp probe: received response packet");

    let incoming_guard = incoming
        .lock()
        .map_err(|_| "udp packet buffer lock poisoned".to_string())?;
    let trace_text = trace
        .lock()
        .map_err(|_| "udp trace lock poisoned".to_string())?
        .join(" -> ");

    Ok(UdpProbeSummary {
        host,
        port,
        response_byte_count: response_packet.len(),
        response_packet,
        response_packet_count: incoming_guard.packet_count,
        trace: trace_text,
    })
}

async fn finish_saved_ip_network_tcp_exchange(
    connection: SavedIpNetworkTcpConnection,
    payload: &[u8],
) -> Result<(Vec<u8>, String), String> {
    let session = connection_into_session(connection, String::new(), 0);
    send_tcp_session_bytes(&session, payload).await?;
    close_tcp_session_writer(&session).await?;
    let mut combined_bytes = Vec::new();
    let first_read = read_tcp_session_bytes(&session, usize::MAX, 5_000).await?;
    if !first_read.bytes.is_empty() {
        combined_bytes.extend_from_slice(&first_read.bytes);
    }
    let mut final_trace = first_read.trace;
    let mut saw_done = first_read.done;

    loop {
        if saw_done {
            break;
        }
        let read_result = read_tcp_session_bytes(&session, usize::MAX, 100).await?;
        if !read_result.bytes.is_empty() {
            combined_bytes.extend_from_slice(&read_result.bytes);
        }
        final_trace = read_result.trace;
        saw_done = read_result.done;

        if read_result.bytes.is_empty() {
            break;
        }
    }

    if combined_bytes.is_empty() && !saw_done {
        return Err(format!(
            "network exchange timed out before response bytes or stream completion ({final_trace})"
        ));
    }

    if !saw_done {
        let mut trace = session
            .trace
            .lock()
            .map_err(|_| "tcp session trace lock poisoned".to_string())?;
        trace.push("exchange-finished-without-downstream-done".to_string());
        final_trace = trace.join(" -> ");
    }

    Ok((combined_bytes, final_trace))
}

async fn probe_saved_ip_network_tcp(
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    probe_request: TcpProbeRequest,
) -> Result<TcpProbeSummary, String> {
    let host = probe_request.host;
    let port = probe_request.port;
    let connection =
        connect_saved_ip_network_tcp(sandstorm_api, &probe_request.saved_token_hex, &host, port)
            .await?;
    let (bytes, trace) =
        finish_saved_ip_network_tcp_exchange(connection, &probe_request.payload).await?;

    Ok(TcpProbeSummary {
        host,
        port,
        response_bytes: bytes,
        trace,
    })
}

async fn probe_saved_ip_network_http(
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    probe_request: HttpProbeRequest,
) -> Result<NetworkHttpProbeSummary, String> {
    let host = probe_request.host;
    let port = probe_request.port;
    let path = probe_request.path;
    let request = format!("GET {path} HTTP/1.0\r\nHost: {host}\r\n\r\n").into_bytes();
    let summary = probe_saved_ip_network_tcp(
        sandstorm_api,
        TcpProbeRequest {
            saved_token_hex: probe_request.saved_token_hex,
            host: host.clone(),
            port,
            payload: request.clone(),
        },
    )
    .await?;
    let bytes = summary.response_bytes;
    let response_text = String::from_utf8_lossy(&bytes);
    let response_preview = response_text
        .lines()
        .take(12)
        .collect::<Vec<_>>()
        .join("\n");

    if response_preview.is_empty() {
        return Err(format!(
            "network probe returned no bytes ({})",
            summary.trace
        ));
    }

    Ok(NetworkHttpProbeSummary {
        host,
        port,
        path,
        response_preview,
        trace: summary.trace,
    })
}
