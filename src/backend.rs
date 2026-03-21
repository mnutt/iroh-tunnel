use crate::{api_session_capnp, grain_capnp, ip_capnp};

#[derive(Clone)]
pub struct SandstormBackend {
    sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
}

impl SandstormBackend {
    pub fn new(
        sandstorm_api: grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    ) -> Self {
        Self { sandstorm_api }
    }

    pub async fn restore_capability(
        &self,
        token: &[u8],
    ) -> Result<capnp::capability::Client, String> {
        let mut restore_req = self.sandstorm_api.restore_request();
        restore_req.get().set_token(token);
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
            .get_as_capability::<capnp::capability::Client>()
            .map_err(|err| format!("failed to extract restored capability: {err}"))
    }

    pub async fn restore_ip_network(
        &self,
        token: &[u8],
    ) -> Result<ip_capnp::ip_network::Client, String> {
        let cap = self.restore_capability(token).await?;
        Ok(ip_capnp::ip_network::Client { client: cap })
    }

    pub async fn restore_ip_interface(
        &self,
        token: &[u8],
    ) -> Result<ip_capnp::ip_interface::Client, String> {
        let cap = self.restore_capability(token).await?;
        Ok(ip_capnp::ip_interface::Client { client: cap })
    }

    pub async fn restore_api_session(
        &self,
        token: &[u8],
    ) -> Result<api_session_capnp::api_session::Client, String> {
        let cap = self.restore_capability(token).await?;
        Ok(api_session_capnp::api_session::Client { client: cap })
    }
}
