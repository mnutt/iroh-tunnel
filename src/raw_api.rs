use std::fmt::Write as _;

use capnp_rpc::{rpc_twoparty_capnp, twoparty, RpcSystem};
use futures_util::future::try_join;
use futures_util::io::AsyncReadExt;
use tokio::net::UnixStream;
use tokio::runtime::Builder;
use tokio::task::LocalSet;
use tokio_util::compat::TokioAsyncReadCompatExt;

use crate::{sandstorm_http_bridge_capnp, util_capnp};

pub fn redeem_request_token(
    session_id: &str,
    request_token: &str,
    label: &str,
) -> Result<String, String> {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| format!("failed to create tokio runtime: {err}"))?;

    let local = LocalSet::new();
    runtime.block_on(local.run_until(async move {
        redeem_request_token_async(session_id, request_token, label).await
    }))
}

async fn redeem_request_token_async(
    session_id: &str,
    request_token: &str,
    label: &str,
) -> Result<String, String> {
    let stream = UnixStream::connect("/tmp/sandstorm-api")
        .await
        .map_err(|err| format!("failed to connect to /tmp/sandstorm-api: {err}"))?;
    let (reader, writer) = stream.compat().split();
    let network = twoparty::VatNetwork::new(
        reader,
        writer,
        rpc_twoparty_capnp::Side::Client,
        Default::default(),
    );

    let mut rpc_system = RpcSystem::new(Box::new(network), None);
    let bridge: sandstorm_http_bridge_capnp::sandstorm_http_bridge::Client =
        rpc_system.bootstrap(rpc_twoparty_capnp::Side::Server);
    tokio::task::spawn_local(async move {
        let _ = rpc_system.await;
    });

    let api_req = bridge.get_sandstorm_api_request();
    let mut ctx_req = bridge.get_session_context_request();
    ctx_req.get().set_id(session_id.into());

    let (api_resp, ctx_resp) = try_join(api_req.send().promise, ctx_req.send().promise)
        .await
        .map_err(|err| format!("failed to fetch Sandstorm API/session context: {err}"))?;

    let api = api_resp
        .get()
        .map_err(|err| format!("failed to decode Sandstorm API response: {err}"))?
        .get_api()
        .map_err(|err| format!("missing Sandstorm API capability: {err}"))?;
    let context = ctx_resp
        .get()
        .map_err(|err| format!("failed to decode session context response: {err}"))?
        .get_context()
        .map_err(|err| format!("missing session context capability: {err}"))?;

    let mut claim_req = context.claim_request_request();
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

    let mut save_req = api.save_request();
    save_req
        .get()
        .get_cap()
        .set_as(claimed_cap)
        .map_err(|err| format!("failed to set save() capability parameter: {err}"))?;
    init_localized_text(save_req.get().init_label(), label);
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

fn init_localized_text(mut builder: util_capnp::localized_text::Builder<'_>, text: &str) {
    builder.set_default_text(text.into());
    builder.init_localizations(0);
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}
