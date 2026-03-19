include!("sandstorm_capnp.rs");

use std::os::fd::FromRawFd;

use capnp::capability::Promise;
use capnp::traits::HasTypeId;
use capnp_rpc::{new_client, pry, rpc_twoparty_capnp, twoparty, RpcSystem};
use futures::AsyncReadExt;
use futures::TryFutureExt;
use tokio::runtime::Builder;
use tokio_util::compat::TokioAsyncReadCompatExt;

const CLIENT_ROOT: &str = "/opt/app/client";
const WEB_SESSION_TYPE_ID: u64 = web_session_capnp::web_session::Client::TYPE_ID;

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

        let client: grain_capnp::ui_view::Client = new_client(UiViewImpl::new(sandstorm_api));

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
    _sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
}

impl UiViewImpl {
    fn new(sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>) -> Self {
        Self {
            _sandstorm_api: sandstorm_api,
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

        let session_params = pry!(params
            .get_session_params()
            .get_as::<web_session_capnp::web_session::params::Reader<'_>>());
        let user_info = pry!(params.get_user_info());
        let permissions = pry!(user_info.get_permissions());
        let can_manage = permissions.len() > 0 && permissions.get(0);
        let base_path = session_params
            .get_base_path()
            .ok()
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_string();

        let session_client: web_session_capnp::web_session::Client = new_client(WebSessionImpl {
            can_manage,
            base_path,
        });
        results.get().set_session(grain_capnp::ui_session::Client {
            client: session_client.client,
        });
        Promise::ok(())
    }
}

struct WebSessionImpl {
    can_manage: bool,
    base_path: String,
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
        _: web_session_capnp::web_session::PostParams,
        _: web_session_capnp::web_session::PostResults,
    ) -> Promise<(), capnp::Error> {
        Promise::err(capnp::Error::unimplemented(
            "web_session.post not implemented".to_string(),
        ))
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
