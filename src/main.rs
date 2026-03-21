#![allow(refining_impl_trait)]

include!("sandstorm_capnp.rs");

#[allow(dead_code)]
mod quinn_adapter;
#[allow(dead_code)]
mod raw_udp_capnp;
#[allow(dead_code)]
mod sandstorm_custom_transport;

use base64::Engine as _;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write as _;
use std::os::fd::FromRawFd;
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
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::raw_udp_capnp::{get_local_endpoint, new_capnp_raw_udp_custom_transport};
use crate::sandstorm_custom_transport::{
    socket_addr_to_custom_addr, SANDSTORM_RAW_UDP_TRANSPORT_ID,
};

const CLIENT_ROOT: &str = "/opt/app/client";
const STATE_DIR: &str = "/var/iroh-tunnel";
const SAVED_CAPS_PATH: &str = "/var/iroh-tunnel/saved-caps.tsv";
const RAW_UDP_INTERFACE_TOKEN_PATH: &str = "/var/iroh-tunnel/raw-udp-interface-token";
const RAW_UDP_PORT_PATH: &str = "/var/iroh-tunnel/raw-udp-port";
const IROH_SECRET_KEY_PATH: &str = "/var/iroh-tunnel/iroh-secret-key";
const REMOTE_TICKET_PATH: &str = "/var/iroh-tunnel/remote-ticket.txt";
const WEB_SESSION_TYPE_ID: u64 = web_session_capnp::web_session::Client::TYPE_ID;
const IROH_ALPN: &[u8] = b"dev.iroh-tunnel.capnp/1";
const IROH_TRANSPORT_ASSESSMENT: &str = "Saved IpNetwork is proven for outbound TCP and UDP. Native iroh 0.97.0 now exposes custom transports behind unstable-custom-transports, and this prototype has both a proxy-based Quinn RawUdp adapter and a native iroh CustomTransport scaffold for Sandstorm RawUdp. The remaining work is application plumbing: restore an IpInterface capability early enough to bind RawUdp, publish custom transport addresses in tickets, and decide how Sandstorm mode is configured and discovered.";
const IROH_SANDSTORM_RAW_UDP_INTERFACE_TOKEN_ENV: &str = "IROH_SANDSTORM_RAW_UDP_INTERFACE_TOKEN";
const IROH_SANDSTORM_RAW_UDP_PORT_ENV: &str = "IROH_SANDSTORM_RAW_UDP_PORT";

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
        let mut view_info = results.get();
        init_localized_text(view_info.reborrow().init_app_title(), "Iroh Tunnel");

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
        let saved_cap = match load_saved_capability_by_id(&object_id) {
            Ok(Some(saved_cap)) => saved_cap,
            Ok(None) => {
                return Promise::err(capnp::Error::failed(format!(
                    "unknown app object id: {object_id}"
                )));
            }
            Err(err) => return Promise::err(capnp::Error::failed(err)),
        };

        let sandstorm_api = self.sandstorm_api.clone();
        Promise::from_future(async move {
            let token = hex_decode(&saved_cap.saved_token).map_err(capnp::Error::failed)?;
            let mut restore_req = sandstorm_api.restore_request();
            restore_req.get().set_token(&token);
            let restore_resp = restore_req.send().promise.await.map_err(|err| {
                capnp::Error::failed(format!("SandstormApi.restore() failed: {err}"))
            })?;
            let restored_cap = restore_resp
                .get()
                .map_err(|err| {
                    capnp::Error::failed(format!("failed to decode restore() response: {err}"))
                })?
                .get_cap();
            results
                .get()
                .get_cap()
                .set_as(restored_cap)
                .map_err(|err| {
                    capnp::Error::failed(format!("failed to set restore result capability: {err}"))
                })?;
            Ok(())
        })
    }

    fn drop(
        self: Rc<Self>,
        _: grain_capnp::main_view::DropParams<text::Owned>,
        _: grain_capnp::main_view::DropResults<text::Owned>,
    ) -> Promise<(), capnp::Error> {
        Promise::ok(())
    }
}

struct WebSessionImpl {
    can_manage: bool,
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    session_context: grain_capnp::session_context::Client,
    app_state: Arc<Mutex<AppState>>,
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
            let body = match render_state_json(&self.app_state) {
                Ok(body) => body,
                Err(err) => return Promise::err(capnp::Error::failed(err)),
            };
            let mut response = results.get().init_content();
            response.set_status_code(web_session_capnp::web_session::response::SuccessCode::Ok);
            response.set_mime_type("application/json");
            response.init_body().set_bytes(body.as_bytes());
            return Promise::ok(());
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
        let (request_token, save_label) = match std::str::from_utf8(&body) {
            Ok(value) => parse_claim_payload(value),
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
                let saved_cap = persist_saved_capability(&save_label, &saved_token)?;
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

                let saved_cap = match load_saved_capability_by_id(&object_id) {
                    Ok(Some(saved_cap)) => saved_cap,
                    Ok(None) => {
                        let mut error = results.get().init_client_error();
                        error.set_status_code(
                            web_session_capnp::web_session::response::ClientErrorCode::NotFound,
                        );
                        return Promise::ok(());
                    }
                    Err(err) => return Promise::err(capnp::Error::failed(err)),
                };

                let sandstorm_api = self.sandstorm_api.clone();
                return Promise::from_future(async move {
                    let outcome =
                        restore_saved_capability(sandstorm_api, &saved_cap.saved_token).await;
                    match outcome {
                        Ok(()) => {
                            results.get().init_no_content();
                        }
                        Err(err) => {
                            let mut error = results.get().init_server_error();
                            let description = format!(
                                "<!doctype html><title>Resolve Failed</title><pre>{}</pre>",
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

async fn restore_saved_ip_network(
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    saved_token_hex: &str,
) -> Result<ip_capnp::ip_network::Client, String> {
    let token = hex_decode(saved_token_hex)?;
    let mut restore_req = sandstorm_api.restore_request();
    restore_req.get().set_token(&token);
    let restore_resp = restore_req
        .send()
        .promise
        .await
        .map_err(|err| format!("SandstormApi.restore() failed: {err}"))?;
    let restore_resp = restore_resp
        .get()
        .map_err(|err| format!("failed to decode restore() response: {err}"))?;
    restore_resp
        .get_cap()
        .get_as::<ip_capnp::ip_network::Client>()
        .map_err(|err| format!("restored capability is not an IpNetwork: {err}"))
}

async fn restore_saved_ip_interface(
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    saved_token_hex: &str,
) -> Result<ip_capnp::ip_interface::Client, String> {
    let token = hex_decode(saved_token_hex)?;
    let mut restore_req = sandstorm_api.restore_request();
    restore_req.get().set_token(&token);
    let restore_resp = restore_req
        .send()
        .promise
        .await
        .map_err(|err| format!("SandstormApi.restore() failed: {err}"))?;
    let restore_resp = restore_resp
        .get()
        .map_err(|err| format!("failed to decode restore() response: {err}"))?;
    restore_resp
        .get_cap()
        .get_as::<ip_capnp::ip_interface::Client>()
        .map_err(|err| format!("restored capability is not an IpInterface: {err}"))
}

fn load_saved_capabilities() -> Result<Vec<SavedCapability>, String> {
    let contents = match std::fs::read_to_string(SAVED_CAPS_PATH) {
        Ok(contents) => contents,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => return Err(format!("failed to read saved capability registry: {err}")),
    };

    let mut rows = Vec::new();
    for line in contents.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let parts: Vec<_> = line.split('\t').collect();
        if parts.len() >= 4 {
            rows.push(SavedCapability {
                id: parts[0].to_string(),
                label: parts[1].to_string(),
                saved_token: parts[2].to_string(),
                created_at_ms: parts[3].parse().unwrap_or(0),
            });
            continue;
        }

        if parts.len() >= 2 {
            rows.push(SavedCapability {
                id: make_saved_cap_id(),
                label: parts[0].to_string(),
                saved_token: parts[1].to_string(),
                created_at_ms: now_ms(),
            });
        }
    }
    Ok(rows)
}

fn load_saved_capability_by_id(id: &str) -> Result<Option<SavedCapability>, String> {
    for saved_cap in load_saved_capabilities()? {
        if saved_cap.id == id {
            return Ok(Some(saved_cap));
        }
    }
    Ok(None)
}

fn persist_saved_capability(label: &str, saved_token: &str) -> Result<SavedCapability, String> {
    std::fs::create_dir_all(STATE_DIR)
        .map_err(|err| format!("failed to create state directory: {err}"))?;
    let saved_cap = SavedCapability {
        id: make_saved_cap_id(),
        label: label.to_string(),
        saved_token: saved_token.to_string(),
        created_at_ms: now_ms(),
    };
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(SAVED_CAPS_PATH)
        .map_err(|err| format!("failed to open saved capability registry: {err}"))?;
    writeln!(
        file,
        "{}\t{}\t{}\t{}",
        saved_cap.id, saved_cap.label, saved_cap.saved_token, saved_cap.created_at_ms
    )
    .map_err(|err| format!("failed to persist saved capability: {err}"))?;
    Ok(saved_cap)
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
struct SavedCapability {
    id: String,
    label: String,
    saved_token: String,
    created_at_ms: u64,
}

struct AppState {
    iroh_identity: IrohIdentity,
    iroh_endpoint: Option<Endpoint>,
    iroh_endpoint_addr: IrohEndpointAddrSummary,
    iroh_endpoint_error: Option<String>,
    raw_udp_interface: Option<SavedCapability>,
    raw_udp_interface_source: Option<String>,
    remote_ticket: Option<String>,
    active_tcp_sessions: HashMap<String, Arc<SavedIpNetworkTcpSession>>,
    next_tcp_session_id: u64,
}

impl AppState {
    fn initialize() -> Result<Self, String> {
        let iroh_identity = load_or_create_iroh_identity()?;
        let remote_ticket = load_remote_ticket()?;
        Ok(Self {
            iroh_identity,
            iroh_endpoint: None,
            iroh_endpoint_addr: IrohEndpointAddrSummary::empty(),
            iroh_endpoint_error: None,
            raw_udp_interface: None,
            raw_udp_interface_source: None,
            remote_ticket,
            active_tcp_sessions: HashMap::new(),
            next_tcp_session_id: 0,
        })
    }
}

async fn initialize_iroh_endpoint(
    app_state: Arc<Mutex<AppState>>,
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
) -> Result<(), String> {
    let (secret_key, old_endpoint) = {
        let mut guard = app_state
            .lock()
            .map_err(|_| "app state lock poisoned".to_string())?;
        (
            guard.iroh_identity.secret_key.clone(),
            guard.iroh_endpoint.take(),
        )
    };
    if let Some(old_endpoint) = old_endpoint {
        old_endpoint.close().await;
    }

    let bind_result = bind_local_iroh_endpoint(sandstorm_api, &secret_key).await;
    let (raw_udp_interface, raw_udp_interface_source) =
        load_configured_raw_udp_interface_state()?;
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
    Ok(())
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
    match std::fs::read_to_string(RAW_UDP_INTERFACE_TOKEN_PATH) {
        Ok(value) => {
            let trimmed = value.trim().to_string();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed))
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(format!(
            "failed to read raw udp interface token: {err}"
        )),
    }
}

fn load_saved_raw_udp_port() -> Result<Option<u16>, String> {
    match std::fs::read_to_string(RAW_UDP_PORT_PATH) {
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
        Err(err) => Err(format!("failed to read raw udp port: {err}")),
    }
}

fn persist_raw_udp_interface_token(saved_token: &str) -> Result<(), String> {
    std::fs::create_dir_all(STATE_DIR)
        .map_err(|err| format!("failed to create state directory: {err}"))?;
    std::fs::write(RAW_UDP_INTERFACE_TOKEN_PATH, format!("{saved_token}\n"))
        .map_err(|err| format!("failed to persist raw udp interface token: {err}"))
}

fn persist_raw_udp_port(port: u16) -> Result<(), String> {
    std::fs::create_dir_all(STATE_DIR)
        .map_err(|err| format!("failed to create state directory: {err}"))?;
    std::fs::write(RAW_UDP_PORT_PATH, format!("{port}\n"))
        .map_err(|err| format!("failed to persist raw udp port: {err}"))
}

fn clear_persisted_raw_udp_interface_token() -> Result<(), String> {
    match std::fs::remove_file(RAW_UDP_INTERFACE_TOKEN_PATH) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(format!("failed to clear raw udp interface token: {err}")),
    }
}

fn clear_persisted_raw_udp_port() -> Result<(), String> {
    match std::fs::remove_file(RAW_UDP_PORT_PATH) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(format!("failed to clear raw udp port: {err}")),
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

    let secret_key = match std::fs::read(IROH_SECRET_KEY_PATH) {
        Ok(bytes) => {
            if bytes.len() != 32 {
                return Err(format!(
                    "invalid iroh secret key length at {}: expected 32 bytes, got {}",
                    IROH_SECRET_KEY_PATH,
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
            std::fs::write(IROH_SECRET_KEY_PATH, raw)
                .map_err(|err| format!("failed to persist iroh secret key: {err}"))?;
            secret_key
        }
        Err(err) => {
            return Err(format!(
                "failed to read iroh secret key from {}: {err}",
                IROH_SECRET_KEY_PATH
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
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    secret_key: &SecretKey,
) -> Result<(Endpoint, IrohEndpointAddrSummary), String> {
    if let Some(config) = load_sandstorm_raw_udp_bind_config()? {
        eprintln!(
            "iroh bind: using Sandstorm RawUdp interface token={} port={}",
            config.interface_token_hex,
            config.port
        );
        return bind_sandstorm_raw_udp_iroh_endpoint(sandstorm_api, secret_key, config).await;
    }

    eprintln!("iroh bind: using native ambient UDP transports");
    let endpoint = Endpoint::builder(presets::N0)
        .alpns(vec![IROH_ALPN.to_vec()])
        .secret_key(secret_key.clone())
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await
        .map_err(|err| format!("failed to bind local iroh endpoint: {err}"))?;
    tokio::spawn(run_echo_accept_loop(endpoint.clone()));
    let endpoint_addr = summarize_endpoint_addr(endpoint.addr());
    log_iroh_endpoint_summary("bound(native)", &endpoint_addr);
    Ok((endpoint, endpoint_addr))
}

async fn bind_sandstorm_raw_udp_iroh_endpoint(
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
        restore_saved_ip_interface(sandstorm_api, &config.interface_token_hex).await?;
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
        .alpns(vec![IROH_ALPN.to_vec()])
        .secret_key(secret_key.clone())
        .relay_mode(RelayMode::Disabled)
        .clear_ip_transports()
        .add_custom_transport(transport)
        .bind()
        .await
        .map_err(|err| format!("failed to bind Sandstorm RawUdp iroh endpoint: {err}"))?;
    tokio::spawn(run_echo_accept_loop(endpoint.clone()));
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
    match std::fs::read_to_string(REMOTE_TICKET_PATH) {
        Ok(value) => {
            let trimmed = value.trim().to_string();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed))
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(format!("failed to read remote ticket: {err}")),
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
        std::fs::write(REMOTE_TICKET_PATH, format!("{remote_ticket}\n"))
            .map_err(|err| format!("failed to persist remote ticket: {err}"))?;
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
    match std::fs::remove_file(REMOTE_TICKET_PATH) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(format!("failed to clear remote ticket: {err}")),
    }
}

fn render_state_json(app_state: &Arc<Mutex<AppState>>) -> Result<String, String> {
    let guard = app_state
        .lock()
        .map_err(|_| "app state lock poisoned".to_string())?;
    let active_tcp_sessions = guard
        .active_tcp_sessions
        .iter()
        .map(|(session_id, session)| {
            let summary = session.snapshot()?;
            Ok(format!(
                "{{\"sessionId\":\"{}\",\"host\":\"{}\",\"port\":{},\"bufferedBytes\":{},\"receivedBytes\":{},\"writeCalls\":{},\"done\":{},\"trace\":\"{}\"}}",
                json_escape(session_id),
                json_escape(&summary.host),
                summary.port,
                summary.buffered_bytes,
                summary.received_bytes,
                summary.write_calls,
                if summary.done { "true" } else { "false" },
                json_escape(&summary.trace)
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let mut rows = Vec::new();
    for row in load_saved_capabilities()? {
        rows.push(format!(
            "{{\"id\":\"{}\",\"objectId\":\"{}\",\"label\":\"{}\",\"savedToken\":\"{}\",\"createdAtMs\":{}}}",
            json_escape(&row.id),
            json_escape(&row.id),
            json_escape(&row.label),
            json_escape(&row.saved_token),
            row.created_at_ms
        ));
    }

    let relay_urls = join_json_strings(&guard.iroh_endpoint_addr.relay_urls);
    let direct_addrs = join_json_strings(&guard.iroh_endpoint_addr.direct_addrs);
    let custom_addrs = join_json_strings(&guard.iroh_endpoint_addr.custom_addrs);
    let raw_udp_interface = match &guard.raw_udp_interface {
        Some(value) => format!(
            "{{\"label\":\"{}\",\"savedToken\":\"{}\",\"source\":\"{}\"}}",
            json_escape(&value.label),
            json_escape(&value.saved_token),
            json_escape(guard.raw_udp_interface_source.as_deref().unwrap_or("unknown"))
        ),
        None => "null".to_string(),
    };
    let remote_ticket = match &guard.remote_ticket {
        Some(value) => format!("\"{}\"", json_escape(value)),
        None => "null".to_string(),
    };
    let endpoint_error = match &guard.iroh_endpoint_error {
        Some(value) => format!("\"{}\"", json_escape(value)),
        None => "null".to_string(),
    };
    let local_ticket = format!(
        "\"{}\"",
        json_escape(&format_local_ticket(&guard.iroh_endpoint_addr))
    );
    let endpoint_bound = if guard.iroh_endpoint.is_some() {
        "true"
    } else {
        "false"
    };

    Ok(format!(
        "{{\"powerboxQueries\":{{\"apiSession\":\"{}\",\"ipNetwork\":\"{}\",\"ipInterface\":\"{}\"}},\"irohNodeId\":\"{}\",\"irohEndpoint\":{{\"bound\":{},\"nodeId\":\"{}\",\"relayUrls\":[{}],\"directAddrs\":[{}],\"customAddrs\":[{}],\"error\":{},\"localTicket\":{},\"rawUdpInterface\":{}}},\"transportAssessment\":\"{}\",\"remoteTicket\":{},\"savedCaps\":[{}],\"activeTcpSessions\":[{}]}}",
        json_escape(&powerbox_query_for_interface(
            api_session_capnp::api_session::Client::TYPE_ID
        )?),
        json_escape(&powerbox_query_for_interface(
            ip_capnp::ip_network::Client::TYPE_ID
        )?),
        json_escape(&powerbox_query_for_interface(
            ip_capnp::ip_interface::Client::TYPE_ID
        )?),
        json_escape(&guard.iroh_identity.node_id),
        endpoint_bound,
        json_escape(&guard.iroh_endpoint_addr.node_id),
        relay_urls,
        direct_addrs,
        custom_addrs,
        endpoint_error,
        local_ticket,
        raw_udp_interface,
        json_escape(IROH_TRANSPORT_ASSESSMENT),
        remote_ticket,
        rows.join(","),
        active_tcp_sessions.join(",")
    ))
}

fn join_json_strings(values: &[String]) -> String {
    values
        .iter()
        .map(|value| format!("\"{}\"", json_escape(value)))
        .collect::<Vec<_>>()
        .join(",")
}

fn parse_claim_payload(value: &str) -> (String, String) {
    let trimmed = value.trim();
    if let Some((token, label)) = trimmed.split_once('\n') {
        let token = token.trim().to_string();
        let label = label.trim();
        if !token.is_empty() && !label.is_empty() {
            return (token, label.to_string());
        }
    }
    (trimmed.to_string(), "Powerbox capability".to_string())
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
            }),
            raw_udp_interface_source: Some("saved".to_string()),
            remote_ticket: None,
            active_tcp_sessions: HashMap::new(),
            next_tcp_session_id: 0,
        }));

        let state_json = render_state_json(&app_state).unwrap();
        assert!(state_json.contains("\"ipInterface\":\""));
        assert!(state_json.contains("\"rawUdpInterface\":{"));
        assert!(state_json.contains("saved-raw-token"));
        assert!(state_json.contains("\"source\":\"saved\""));
        assert!(state_json.contains("1234_deadbeef"));
    }
}

async fn run_echo_accept_loop(endpoint: Endpoint) {
    while let Some(incoming) = endpoint.accept().await {
        tokio::spawn(async move {
            let result = async {
                eprintln!("iroh accept: incoming connection detected");
                let connection = incoming
                    .accept()
                    .map_err(|err| format!("failed to accept incoming iroh connection: {err}"))?
                    .await
                    .map_err(|err| format!("incoming iroh connection failed: {err}"))?;
                eprintln!("iroh accept: connection accepted");
                let (mut send, mut recv) = connection
                    .accept_bi()
                    .await
                    .map_err(|err| format!("failed to accept bi stream: {err}"))?;
                eprintln!("iroh accept: bi stream accepted");
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
                eprintln!("iroh accept: connection closed: {close_reason}");
                Ok::<(), String>(())
            }
            .await;

            if let Err(err) = result {
                eprintln!("iroh accept loop error: {err}");
            }
        });
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
