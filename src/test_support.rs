use std::sync::{Arc, Mutex};

use capnp::capability::{Promise, Rc};
use capnp_rpc::{new_client, pry};
use futures::TryFutureExt;
use iroh::SecretKey;

use crate::*;

pub(crate) fn run_local_async_test<F>(future: F)
where
    F: std::future::Future<Output = ()>,
{
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let local_set = tokio::task::LocalSet::new();
    runtime.block_on(local_set.run_until(future));
}

pub(crate) fn dummy_sandstorm_api_client(
) -> grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned> {
    let (_tx, rx) = futures::channel::oneshot::channel();
    capnp_rpc::new_future_client(rx.map_err(|_| {
        capnp::Error::failed("test sandstorm api bootstrap channel canceled".to_string())
    }))
}

pub(crate) fn make_test_storage_root(name: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("iroh-tunnel-{name}-{}", now_ms()));
    std::fs::create_dir_all(&path).unwrap();
    path
}

pub(crate) async fn build_test_app(
    name: &str,
    seed: u8,
) -> (
    App,
    Arc<Mutex<AppState>>,
    grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
) {
    let state_root = make_test_storage_root(name);
    let secret_key = SecretKey::from_bytes(&[seed; 32]);
    let app = App::new_for_test(Storage::new(state_root), secret_key);
    let sandstorm_api = dummy_sandstorm_api_client();
    app.bind_test_endpoint(sandstorm_api.clone()).await.unwrap();
    let state = app.shared_state_for_test();
    (app, state, sandstorm_api)
}

pub(crate) async fn invoke_api_session_client_for_test(
    client: api_session_capnp::api_session::Client,
    filename: &str,
    payload: &[u8],
) -> Result<ApiSessionInvokeSummary, String> {
    let web_session = api_session_as_web_session(client);
    let mut request = web_session.post_request();
    {
        let mut req = request.get();
        req.set_path("preview");
        {
            let mut content = req.reborrow().init_content();
            content.set_mime_type("application/octet-stream");
            content.set_content(payload);
        }
        {
            let mut context = req.reborrow().init_context();
            context.reborrow().init_cookies(0);
            context.reborrow().init_accept(0);
            context.reborrow().init_accept_encoding(0);
            context.reborrow().get_e_tag_precondition().set_none(());
            let mut headers = context.reborrow().init_additional_headers(1);
            let mut header = headers.reborrow().get(0);
            header.set_name("x-sandstorm-app-filename");
            header.set_value(filename);
        }
    }

    let response = request
        .send()
        .promise
        .await
        .map_err(|err| format!("test ApiSession.post(preview) failed: {err}"))?;
    let response = response
        .get()
        .map_err(|err| format!("failed to decode test ApiSession response: {err}"))?;

    match response
        .which()
        .map_err(|err| format!("failed to decode test ApiSession response union: {err}"))?
    {
        web_session_capnp::web_session::response::Content(content) => {
            let status_code = response_success_code_to_status(
                content
                    .get_status_code()
                    .map_err(|err| format!("failed to decode response status: {err}"))?,
            );
            let content_type = content
                .get_mime_type()
                .map_err(|err| format!("failed to read response mime type: {err}"))?
                .to_str()
                .unwrap_or("")
                .to_string();
            let response_bytes = match content
                .get_body()
                .which()
                .map_err(|err| format!("failed to decode response body: {err}"))?
            {
                web_session_capnp::web_session::response::content::body::Bytes(bytes) => bytes
                    .map_err(|err| format!("failed to read response bytes: {err}"))?
                    .to_vec(),
                web_session_capnp::web_session::response::content::body::Stream(_) => {
                    return Err("test fake ApiSession unexpectedly returned a stream".to_string())
                }
            };
            Ok(ApiSessionInvokeSummary {
                status_code,
                content_type,
                response_bytes,
                trace: "test-invoke:ok".to_string(),
            })
        }
        _ => Err("unexpected test ApiSession response variant".to_string()),
    }
}

pub(crate) async fn invoke_ip_network_client_for_test(
    client: ip_capnp::ip_network::Client,
    host: &str,
    port: u16,
) -> Result<TcpProbeSummary, String> {
    let connection = connect_ip_network_tcp_client(client, host, port).await?;
    let payload = format!("GET / HTTP/1.0\r\nHost: {host}\r\n\r\n");
    let (response_bytes, trace) =
        finish_saved_ip_network_tcp_exchange(connection, payload.as_bytes()).await?;
    Ok(TcpProbeSummary {
        host: host.to_string(),
        port,
        response_bytes,
        trace,
    })
}

pub(crate) struct FakePreviewApiSession {
    pub(crate) response_bytes: Vec<u8>,
}

impl grain_capnp::ui_session::Server for FakePreviewApiSession {}
impl api_session_capnp::api_session::Server for FakePreviewApiSession {}

impl web_session_capnp::web_session::Server for FakePreviewApiSession {
    fn post(
        self: Rc<Self>,
        params: web_session_capnp::web_session::PostParams,
        mut results: web_session_capnp::web_session::PostResults,
    ) -> Promise<(), capnp::Error> {
        let params = pry!(params.get());
        let path = pry!(params.get_path()).to_str().unwrap_or("").to_string();
        if path != "preview" {
            return Promise::err(capnp::Error::failed(format!(
                "unexpected preview path: {path}"
            )));
        }

        let mut content = results.get().init_content();
        content.set_status_code(web_session_capnp::web_session::response::SuccessCode::Ok);
        content.set_mime_type("application/pdf");
        content.init_body().set_bytes(&self.response_bytes);
        Promise::ok(())
    }
}

pub(crate) struct FakeIpNetwork {
    pub(crate) response_bytes: Vec<u8>,
}

impl ip_capnp::ip_network::Server for FakeIpNetwork {
    fn get_remote_host_by_name(
        self: Rc<Self>,
        params: ip_capnp::ip_network::GetRemoteHostByNameParams,
        mut results: ip_capnp::ip_network::GetRemoteHostByNameResults,
    ) -> impl std::future::Future<Output = Result<(), capnp::Error>> + 'static {
        let host = params
            .get()
            .and_then(|params| params.get_address())
            .ok()
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_string();
        let remote_host: ip_capnp::ip_remote_host::Client = new_client(FakeIpRemoteHost {
            host,
            response_bytes: self.response_bytes.clone(),
        });
        async move {
            results.get().set_host(remote_host);
            Ok(())
        }
    }
}

struct FakeIpRemoteHost {
    host: String,
    response_bytes: Vec<u8>,
}

impl ip_capnp::ip_remote_host::Server for FakeIpRemoteHost {
    fn get_tcp_port(
        self: Rc<Self>,
        params: ip_capnp::ip_remote_host::GetTcpPortParams,
        mut results: ip_capnp::ip_remote_host::GetTcpPortResults,
    ) -> impl std::future::Future<Output = Result<(), capnp::Error>> + 'static {
        let port = params.get().map(|params| params.get_port_num()).unwrap_or(0);
        let tcp_port: ip_capnp::tcp_port::Client = new_client(FakeTcpPort {
            host: self.host.clone(),
            port,
            response_bytes: self.response_bytes.clone(),
        });
        async move {
            results.get().set_port(tcp_port);
            Ok(())
        }
    }
}

struct FakeTcpPort {
    host: String,
    port: u16,
    response_bytes: Vec<u8>,
}

impl ip_capnp::tcp_port::Server for FakeTcpPort {
    fn connect(
        self: Rc<Self>,
        params: ip_capnp::tcp_port::ConnectParams,
        mut results: ip_capnp::tcp_port::ConnectResults,
    ) -> impl std::future::Future<Output = Result<(), capnp::Error>> + 'static {
        let downstream = params
            .get()
            .and_then(|params| params.get_downstream())
            .unwrap();
        let upstream: util_capnp::byte_stream::Client = new_client(FakeTcpUpstream {
            _host: self.host.clone(),
            _port: self.port,
            downstream,
            response_bytes: self.response_bytes.clone(),
            writes: Arc::new(Mutex::new(Vec::new())),
        });
        async move {
            results.get().set_upstream(upstream);
            Ok(())
        }
    }
}

struct FakeTcpUpstream {
    _host: String,
    _port: u16,
    downstream: util_capnp::byte_stream::Client,
    response_bytes: Vec<u8>,
    writes: Arc<Mutex<Vec<u8>>>,
}

impl util_capnp::byte_stream::Server for FakeTcpUpstream {
    fn write(
        self: Rc<Self>,
        params: util_capnp::byte_stream::WriteParams,
    ) -> Promise<(), capnp::Error> {
        let params = pry!(params.get());
        let data = pry!(params.get_data());
        if let Ok(mut writes) = self.writes.lock() {
            writes.extend_from_slice(data);
        }
        Promise::ok(())
    }

    fn done(
        self: Rc<Self>,
        _: util_capnp::byte_stream::DoneParams,
        _: util_capnp::byte_stream::DoneResults,
    ) -> Promise<(), capnp::Error> {
        let downstream = self.downstream.clone();
        let response_bytes = self.response_bytes.clone();
        Promise::from_future(async move {
            let mut write_req = downstream.write_request();
            write_req.get().set_data(&response_bytes);
            write_req
                .send()
                .await
                .map_err(|err| capnp::Error::failed(format!("fake downstream write failed: {err}")))?;
            let done_req = downstream.done_request();
            done_req
                .send()
                .promise
                .await
                .map_err(|err| capnp::Error::failed(format!("fake downstream done failed: {err}")))?;
            Ok(())
        })
    }

    fn expect_size(
        self: Rc<Self>,
        _: util_capnp::byte_stream::ExpectSizeParams,
        _: util_capnp::byte_stream::ExpectSizeResults,
    ) -> Promise<(), capnp::Error> {
        Promise::ok(())
    }
}
