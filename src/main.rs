include!("sandstorm_capnp.rs");

use std::fs::OpenOptions;
use std::io::Write as _;
use std::os::fd::FromRawFd;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use capnp::text;
use capnp::capability::Promise;
use capnp::traits::HasTypeId;
use iroh::{Endpoint, RelayMode, SecretKey, TransportAddr};
use capnp_rpc::{new_client, pry, rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::AsyncReadExt;
use futures::TryFutureExt;
use tokio::runtime::Builder;
use tokio_util::compat::TokioAsyncReadCompatExt;

const CLIENT_ROOT: &str = "/opt/app/client";
const STATE_DIR: &str = "/var/iroh-tunnel";
const SAVED_CAPS_PATH: &str = "/var/iroh-tunnel/saved-caps.tsv";
const IROH_SECRET_KEY_PATH: &str = "/var/iroh-tunnel/iroh-secret-key";
const REMOTE_TICKET_PATH: &str = "/var/iroh-tunnel/remote-ticket.txt";
const WEB_SESSION_TYPE_ID: u64 = web_session_capnp::web_session::Client::TYPE_ID;
const IROH_ALPN: &[u8] = b"dev.iroh-tunnel.capnp/1";

fn main() {
    if let Err(err) = run() {
        eprintln!("fatal error: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let runtime = Builder::new_current_thread()
        .enable_io()
        .build()
        .map_err(|err| format!("failed to create tokio runtime: {err}"))?;

    runtime.block_on(async {
        let app_state = Arc::new(Mutex::new(AppState::initialize().await?));
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
            capnp_rpc::new_promise_client(rx.map_err(|_| {
                capnp::Error::failed("sandstorm api bootstrap channel was canceled".to_string())
            }));

        let client: grain_capnp::main_view::Client<text::Owned> =
            new_client(UiViewImpl::new(sandstorm_api, app_state));

        let mut rpc_system = RpcSystem::new(network, Some(client.client));
        let remote_api = rpc_system
            .bootstrap::<grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>>(
                rpc_twoparty_capnp::Side::Server,
            );
        let _ = tx.send(remote_api.client);

        rpc_system
            .await
            .map_err(|err| format!("rpc system failed: {err}"))
    })
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
        &mut self,
        _: grain_capnp::ui_view::GetViewInfoParams,
        mut results: grain_capnp::ui_view::GetViewInfoResults,
    ) -> Promise<(), capnp::Error> {
        let mut view_info = results.get();
        init_localized_text(view_info.reborrow().init_app_title(), "Iroh Tunnel");

        let mut permissions = view_info.reborrow().init_permissions(2);
        {
            let mut permission = permissions.reborrow().get(0);
            permission.set_name("manageTunnel".into());
            init_localized_text(permission.reborrow().init_title(), "manage tunnel");
            init_localized_text(
                permission.init_description(),
                "Can pair peers and manage shared capabilities.",
            );
        }
        {
            let mut permission = permissions.get(1);
            permission.set_name("useReceivedCaps".into());
            init_localized_text(permission.reborrow().init_title(), "use received capabilities");
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
        &mut self,
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

        let _session_params = pry!(params
            .get_session_params()
            .get_as::<web_session_capnp::web_session::params::Reader<'_>>());
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
        &mut self,
        params: grain_capnp::main_view::RestoreParams<text::Owned>,
        mut results: grain_capnp::main_view::RestoreResults<text::Owned>,
    ) -> Promise<(), capnp::Error> {
        let params = pry!(params.get());
        let object_id = pry!(params.get_object_id()).to_str().unwrap_or("").to_string();
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
            let token = hex_decode(&saved_cap.saved_token)
                .map_err(capnp::Error::failed)?;
            let mut restore_req = sandstorm_api.restore_request();
            restore_req.get().set_token(&token);
            let restore_resp = restore_req
                .send()
                .promise
                .await
                .map_err(|err| capnp::Error::failed(format!("SandstormApi.restore() failed: {err}")))?;
            let restored_cap = restore_resp
                .get()
                .map_err(|err| capnp::Error::failed(format!("failed to decode restore() response: {err}")))?
                .get_cap();
            results
                .get()
                .get_cap()
                .set_as(restored_cap)
                .map_err(|err| capnp::Error::failed(format!("failed to set restore result capability: {err}")))?;
            Ok(())
        })
    }

    fn drop(
        &mut self,
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
        &mut self,
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
            response.set_mime_type("text/plain".into());
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
            response.set_mime_type("application/json".into());
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
            redirect.set_location(location.as_str().into());
            return Promise::ok(());
        }

        match self.read_file(&filename, results, self.infer_content_type(&path)) {
            Ok(()) => Promise::ok(()),
            Err(err) => Promise::err(err),
        }
    }

    fn post(
        &mut self,
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

            let body = pry!(params.get_content()).get_content().unwrap_or(&[]).to_vec();
            let remote_ticket = match std::str::from_utf8(&body) {
                Ok(value) => value.trim().to_string(),
                Err(err) => {
                    let mut error = results.get().init_client_error();
                    let description =
                        format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                    error.set_status_code(
                        web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                    );
                    error.set_description_html(description.as_str().into());
                    return Promise::ok(());
                }
            };

            let outcome = update_remote_ticket(&self.app_state, remote_ticket);
            match outcome {
                Ok(()) => {
                    let mut content = results.get().init_content();
                    content.set_status_code(
                        web_session_capnp::web_session::response::SuccessCode::Ok,
                    );
                    content.set_mime_type("application/json".into());
                    content
                        .init_body()
                        .set_bytes(br#"{"ok":true}"#);
                }
                Err(err) => {
                    let mut error = results.get().init_server_error();
                    let description = format!(
                        "<!doctype html><title>Pairing Update Failed</title><pre>{}</pre>",
                        escape_html(&err)
                    );
                    error.set_description_html(description.as_str().into());
                }
            }
            return Promise::ok(());
        }

        if path != "api/powerbox/claim" {
            let mut error = results.get().init_client_error();
            error.set_status_code(
                web_session_capnp::web_session::response::ClientErrorCode::NotFound,
            );
            return Promise::ok(());
        }

        let body = pry!(params.get_content()).get_content().unwrap_or(&[]).to_vec();
        let request_token = match std::str::from_utf8(&body) {
            Ok(value) => value.trim().to_string(),
            Err(err) => {
                let mut error = results.get().init_client_error();
                let description = format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                );
                error.set_description_html(description.as_str().into());
                return Promise::ok(());
            }
        };

        let sandstorm_api = self.sandstorm_api.clone();
        let session_context = self.session_context.clone();
        Promise::from_future(async move {
            let outcome = claim_and_save_capability(sandstorm_api, session_context, &request_token)
                .await
                .and_then(|saved_token| {
                    let saved_cap = persist_saved_capability("Powerbox capability", &saved_token)?;
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
                    content.set_status_code(
                        web_session_capnp::web_session::response::SuccessCode::Ok,
                    );
                    content.set_mime_type("application/json".into());
                    content.init_body().set_bytes(body.as_bytes());
                }
                Err(err) => {
                    let mut error = results.get().init_server_error();
                    let description = format!(
                        "<!doctype html><title>Powerbox Claim Failed</title><pre>{}</pre>",
                        escape_html(&err)
                    );
                    error.set_description_html(description.as_str().into());
                }
            }

            Ok(())
        })
    }

    fn put(
        &mut self,
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
                    error.set_description_html(description.as_str().into());
                }
            }
            return Promise::ok(());
        }

        if path != "api/saved-cap/restore" {
            if path == "api/saved-cap/resolve-object" {
                let body = pry!(params.get_content()).get_content().unwrap_or(&[]).to_vec();
                let object_id = match std::str::from_utf8(&body) {
                    Ok(value) => value.trim().to_string(),
                    Err(err) => {
                        let mut error = results.get().init_client_error();
                        let description =
                            format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                        error.set_status_code(
                            web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                        );
                        error.set_description_html(description.as_str().into());
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
                    let outcome = restore_saved_capability(sandstorm_api, &saved_cap.saved_token).await;
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
                            error.set_description_html(description.as_str().into());
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

        let body = pry!(params.get_content()).get_content().unwrap_or(&[]).to_vec();
        let saved_token_hex = match std::str::from_utf8(&body) {
            Ok(value) => value.trim().to_string(),
            Err(err) => {
                let mut error = results.get().init_client_error();
                let description = format!("<!doctype html><title>Bad Request</title><p>{err}</p>");
                error.set_status_code(
                    web_session_capnp::web_session::response::ClientErrorCode::BadRequest,
                );
                error.set_description_html(description.as_str().into());
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
                    error.set_description_html(description.as_str().into());
                }
            }
            Ok(())
        })
    }

    fn options(
        &mut self,
        _: web_session_capnp::web_session::OptionsParams,
        _: web_session_capnp::web_session::OptionsResults,
    ) -> Promise<(), capnp::Error> {
        Promise::err(capnp::Error::unimplemented(
            "web_session.options not implemented".to_string(),
        ))
    }

    fn open_web_socket(
        &mut self,
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
                content.set_mime_type(content_type.into());
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
    builder.set_default_text(text.into());
    builder.init_localizations(0);
}

async fn claim_and_save_capability(
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    session_context: grain_capnp::session_context::Client,
    request_token: &str,
) -> Result<String, String> {
    let mut claim_req = session_context.claim_request_request();
    claim_req.get().set_request_token(request_token.into());
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
    init_localized_text(save_req.get().init_label(), "Powerbox capability");

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
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

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
    remote_ticket: Option<String>,
}

impl AppState {
    async fn initialize() -> Result<Self, String> {
        let iroh_identity = load_or_create_iroh_identity()?;
        let remote_ticket = load_remote_ticket()?;
        match bind_local_iroh_endpoint(&iroh_identity.secret_key).await {
            Ok((endpoint, endpoint_addr)) => Ok(Self {
                iroh_identity,
                iroh_endpoint: Some(endpoint),
                iroh_endpoint_addr: endpoint_addr,
                iroh_endpoint_error: None,
                remote_ticket,
            }),
            Err(err) => Ok(Self {
                iroh_identity,
                iroh_endpoint: None,
                iroh_endpoint_addr: IrohEndpointAddrSummary::empty(),
                iroh_endpoint_error: Some(err),
                remote_ticket,
            }),
        }
    }
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
            ))
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
}

impl IrohEndpointAddrSummary {
    fn empty() -> Self {
        Self {
            node_id: String::new(),
            relay_urls: Vec::new(),
            direct_addrs: Vec::new(),
        }
    }
}

async fn bind_local_iroh_endpoint(
    secret_key: &SecretKey,
) -> Result<(Endpoint, IrohEndpointAddrSummary), String> {
    let endpoint = Endpoint::builder()
        .alpns(vec![IROH_ALPN.to_vec()])
        .secret_key(secret_key.clone())
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await
        .map_err(|err| format!("failed to bind local iroh endpoint: {err}"))?;
    let endpoint_addr = summarize_endpoint_addr(endpoint.addr());
    Ok((endpoint, endpoint_addr))
}

fn summarize_endpoint_addr(endpoint_addr: iroh::EndpointAddr) -> IrohEndpointAddrSummary {
    let mut relay_urls = Vec::new();
    let mut direct_addrs = Vec::new();
    for addr in endpoint_addr.addrs {
        match addr {
            TransportAddr::Relay(url) => relay_urls.push(url.to_string()),
            TransportAddr::Ip(addr) => direct_addrs.push(addr.to_string()),
            _ => {}
        }
    }
    IrohEndpointAddrSummary {
        node_id: endpoint_addr.id.to_string(),
        relay_urls,
        direct_addrs,
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

fn update_remote_ticket(app_state: &Arc<Mutex<AppState>>, remote_ticket: String) -> Result<(), String> {
    std::fs::create_dir_all(STATE_DIR)
        .map_err(|err| format!("failed to create state directory: {err}"))?;
    if remote_ticket.trim().is_empty() {
        match std::fs::remove_file(REMOTE_TICKET_PATH) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(format!("failed to clear remote ticket: {err}")),
        }
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

fn render_state_json(app_state: &Arc<Mutex<AppState>>) -> Result<String, String> {
    let guard = app_state
        .lock()
        .map_err(|_| "app state lock poisoned".to_string())?;
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
    let remote_ticket = match &guard.remote_ticket {
        Some(value) => format!("\"{}\"", json_escape(value)),
        None => "null".to_string(),
    };
    let endpoint_error = match &guard.iroh_endpoint_error {
        Some(value) => format!("\"{}\"", json_escape(value)),
        None => "null".to_string(),
    };
    let endpoint_bound = if guard.iroh_endpoint.is_some() {
        "true"
    } else {
        "false"
    };

    Ok(format!(
        "{{\"irohNodeId\":\"{}\",\"irohEndpoint\":{{\"bound\":{},\"nodeId\":\"{}\",\"relayUrls\":[{}],\"directAddrs\":[{}],\"error\":{}}},\"remoteTicket\":{},\"savedCaps\":[{}]}}",
        json_escape(&guard.iroh_identity.node_id),
        endpoint_bound,
        json_escape(&guard.iroh_endpoint_addr.node_id),
        relay_urls,
        direct_addrs,
        endpoint_error,
        remote_ticket,
        rows.join(",")
    ))
}

fn join_json_strings(values: &[String]) -> String {
    values
        .iter()
        .map(|value| format!("\"{}\"", json_escape(value)))
        .collect::<Vec<_>>()
        .join(",")
}
