use std::collections::HashSet;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use capnp::capability::{
    DispatchCallResult, FromServer, Promise as CapPromise, Server as CapServer,
};
use capnp::traits::HasTypeId;
use capnp_rpc::pry;
use capnp_rpc::{RpcSystem, new_client, rpc_twoparty_capnp, twoparty};
use serde::Serialize;
use tokio::time::{Duration, sleep};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::backend::SandstormBackend;
use crate::storage::{
    LocalProxyCapabilityRecord, LocalProxyTargetKind, SharedCapabilityKind, SharedCapabilityRecord,
    Storage,
};

#[derive(Clone)]
pub(crate) struct App {
    state: Arc<Mutex<crate::AppState>>,
    storage: Storage,
}

pub(crate) const LOCAL_TEST_API_SESSION_OBJECT_ID: &str = "local-test-api-session";
pub(crate) const LOCAL_TEST_API_SESSION_LABEL: &str = "Local Test ApiSession";

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PowerboxQueriesJson {
    api_session: String,
    ip_network: String,
    ip_interface: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RawUdpInterfaceJson {
    label: String,
    saved_token: String,
    source: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct IrohEndpointJson {
    bound: bool,
    node_id: String,
    relay_urls: Vec<String>,
    direct_addrs: Vec<String>,
    custom_addrs: Vec<String>,
    error: Option<String>,
    local_ticket: String,
    raw_udp_interface: Option<RawUdpInterfaceJson>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PairingJson {
    status: String,
    approved_peer_node_id: Option<String>,
    pending_incoming_peer_node_id: Option<String>,
    pending_outgoing_peer_node_id: Option<String>,
    tunnel_enabled: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PeerRpcCapabilityExportJson {
    id: String,
    label: String,
    kind: String,
    type_tag: String,
    descriptor_json: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PeerRpcJson {
    connected: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    session_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    remote_node_id: Option<String>,
    capability_exports: Vec<PeerRpcCapabilityExportJson>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LocalProxyCapabilityJson {
    object_id: String,
    peer_node_id: String,
    target_kind: String,
    target_id: String,
    label: String,
    kind: String,
    type_tag: String,
    descriptor_json: Option<String>,
    enabled: bool,
    created_at_ms: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SavedCapabilityJson {
    id: String,
    object_id: String,
    label: String,
    saved_token: String,
    created_at_ms: u64,
    descriptor_json: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SharedCapabilityJson {
    id: String,
    saved_cap_id: String,
    label: String,
    kind: String,
    type_tag: String,
    enabled: bool,
    created_at_ms: u64,
    saved_token: String,
    descriptor_json: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StateJson {
    powerbox_queries: PowerboxQueriesJson,
    powerbox_advertised_matches: serde_json::Value,
    iroh_node_id: String,
    iroh_endpoint: IrohEndpointJson,
    pairing: PairingJson,
    peer_rpc: PeerRpcJson,
    peer_rpc_error: Option<String>,
    local_proxy_caps: Vec<LocalProxyCapabilityJson>,
    remote_ticket: Option<String>,
    saved_caps: Vec<SavedCapabilityJson>,
    shared_caps: Vec<SharedCapabilityJson>,
}

struct TypelessHostedClient(capnp::capability::Client);

impl capnp::capability::FromClientHook for TypelessHostedClient {
    fn new(hook: Box<capnp::capability::DynClientHook>) -> Self {
        Self(capnp::capability::Client::new(hook))
    }

    fn into_client_hook(self) -> Box<capnp::capability::DynClientHook> {
        self.0.hook
    }

    fn as_client_hook(&self) -> &capnp::capability::DynClientHook {
        &*self.0.hook
    }
}

#[derive(Clone)]
struct TypelessServerDispatch<S> {
    server: capnp::capability::Rc<S>,
}

impl<S> std::ops::Deref for TypelessServerDispatch<S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.server
    }
}

impl<S> CapServer for TypelessServerDispatch<S>
where
    S: CapServer + Clone + 'static,
{
    fn dispatch_call(
        self,
        interface_id: u64,
        method_id: u16,
        params: capnp::capability::Params<capnp::any_pointer::Owned>,
        results: capnp::capability::Results<capnp::any_pointer::Owned>,
    ) -> DispatchCallResult {
        (*self.server)
            .clone()
            .dispatch_call(interface_id, method_id, params, results)
    }

    fn as_ptr(&self) -> usize {
        self.server.as_ptr()
    }
}

impl<S> FromServer<S> for TypelessHostedClient
where
    S: CapServer + Clone + 'static,
{
    type Dispatch = TypelessServerDispatch<S>;

    fn from_server(s: capnp::capability::Rc<S>) -> Self::Dispatch {
        TypelessServerDispatch { server: s }
    }
}

impl App {
    pub(crate) fn new(state: Arc<Mutex<crate::AppState>>, storage: Storage) -> Self {
        Self { state, storage }
    }

    fn local_proxy_response_transform(
        &self,
        local_proxy_object_id: String,
    ) -> crate::untyped_local::ResponseCapTableTransform {
        let app = self.clone();
        std::rc::Rc::new(move |cap_table| {
            let app = app.clone();
            let local_proxy_object_id = local_proxy_object_id.clone();
            Box::pin(async move {
                let mut localized = Vec::with_capacity(cap_table.len());
                for entry in cap_table {
                    let Some(hook) = entry else {
                        localized.push(None);
                        continue;
                    };
                    let client = capnp::capability::Client::new(hook);
                    let localized_client = app
                        .localize_remote_capability_for_local_proxy_target(
                            &local_proxy_object_id,
                            client,
                        )
                        .await
                        .map_err(capnp::Error::failed)?;
                    localized.push(Some(localized_client.hook));
                }
                Ok(localized)
            })
        })
    }

    fn local_proxy_request_transform(&self) -> crate::untyped_local::RequestCapTableTransform {
        let app = self.clone();
        std::rc::Rc::new(move |cap_table| {
            let app = app.clone();
            Box::pin(async move {
                let mut transformed = Vec::with_capacity(cap_table.len());
                for entry in cap_table {
                    let Some(hook) = entry else {
                        transformed.push(None);
                        continue;
                    };
                    let client = capnp::capability::Client::new(hook);
                    let client = app
                        .unwrap_local_proxy_parameter(client)
                        .await
                        .map_err(capnp::Error::failed)?;
                    transformed.push(Some(client.hook));
                }
                Ok(transformed)
            })
        })
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(storage: Storage, secret_key: crate::SecretKey) -> Self {
        let state = Arc::new(Mutex::new(crate::AppState {
            storage: storage.clone(),
            iroh_identity: crate::IrohIdentity {
                node_id: secret_key.public().to_string(),
                secret_key,
            },
            iroh_endpoint: None,
            iroh_endpoint_addr: crate::IrohEndpointAddrSummary::empty(),
            iroh_endpoint_error: None,
            raw_udp_interface: None,
            raw_udp_interface_source: None,
            remote_ticket: None,
            approved_peer_node_id: None,
            pending_incoming_peer_node_id: None,
            pending_outgoing_peer_node_id: None,
            pending_incoming_connection: None,
            tunnel_enabled: true,
            pairing_status: crate::PairingStatus::Disconnected,
            shared_caps: Vec::new(),
            exported_caps_live: std::collections::HashMap::new(),
            peer_rpc_session: None,
            local_proxy_caps: Vec::new(),
            registered_remote_caps: std::collections::HashMap::new(),
            registered_remote_hook_object_ids: std::collections::HashMap::new(),
            local_proxy_hook_object_ids: std::collections::HashMap::new(),
            next_peer_rpc_session_id: 0,
            next_local_proxy_cap_id: 0,
            next_registered_remote_cap_id: 0,
            peer_rpc_error: None,
        }));
        Self::new(state, storage)
    }

    #[cfg(test)]
    pub(crate) fn new_for_test_loaded(
        storage: Storage,
        secret_key: crate::SecretKey,
    ) -> Result<Self, String> {
        let remote_ticket = storage.load_text_file(storage.remote_ticket_path().as_path())?;
        let approved_peer_node_id =
            storage.load_text_file(storage.approved_peer_node_id_path().as_path())?;
        let tunnel_enabled =
            match storage.load_text_file(storage.tunnel_enabled_path().as_path())? {
                Some(value) => value.trim() != "false",
                None => true,
            };
        let shared_caps = storage
            .load_shared_capabilities()?
            .into_iter()
            .map(|record| crate::SharedCapability {
                id: record.id,
                label: record.label.clone(),
                kind: record.kind,
                enabled: record.enabled,
                created_at_ms: record.created_at_ms,
                saved_cap: crate::SavedCapability {
                    id: record.saved_cap_id,
                    label: record.label,
                    saved_token: String::new(),
                    created_at_ms: record.created_at_ms,
                    descriptor_json: record.descriptor_json,
                },
            })
            .collect::<Vec<_>>();
        let mut local_proxy_records = storage.load_local_proxy_capabilities()?;
        if local_proxy_records.iter().any(|record| {
            matches!(
                record.target_kind,
                crate::LocalProxyTargetKind::RemoteObjectId
            )
        }) {
            local_proxy_records.retain(|record| {
                matches!(record.target_kind, crate::LocalProxyTargetKind::ExportId)
            });
            storage.persist_local_proxy_capability_registry(&local_proxy_records)?;
        }
        let local_proxy_caps = local_proxy_records
            .into_iter()
            .map(|record| crate::LocalProxyCapability {
                object_id: record.object_id,
                peer_node_id: record.peer_node_id,
                target_kind: match record.target_kind {
                    crate::LocalProxyTargetKind::ExportId => {
                        crate::LocalProxyTargetKindRuntime::ExportId
                    }
                    crate::LocalProxyTargetKind::RemoteObjectId => {
                        crate::LocalProxyTargetKindRuntime::RemoteObjectId
                    }
                },
                target_id: record.target_id,
                label: record.label,
                kind: record.kind,
                type_tag: record
                    .type_tag
                    .unwrap_or_else(|| "capnp/unknown".to_string()),
                descriptor_json: record.descriptor_json,
                enabled: record.enabled,
                created_at_ms: record.created_at_ms,
            })
            .collect::<Vec<_>>();
        let registered_remote_caps = storage
            .load_registered_remote_capabilities()?
            .into_iter()
            .map(|record| {
                let kind = match record.kind {
                    crate::ReceivedCapabilityKind::IpNetwork => {
                        crate::ImportedRemoteCapabilityKind::IpNetwork
                    }
                    crate::ReceivedCapabilityKind::ApiSession => {
                        crate::ImportedRemoteCapabilityKind::ApiSession
                    }
                    crate::ReceivedCapabilityKind::Other => {
                        crate::ImportedRemoteCapabilityKind::Other
                    }
                };
                (
                    record.remote_object_id.clone(),
                    crate::RegisteredRemoteCapability {
                        remote_object_id: record.remote_object_id,
                        label: record.label,
                        kind,
                        type_tag: record
                            .type_tag
                            .unwrap_or_else(|| "capnp/unknown".to_string()),
                        descriptor_json: record.descriptor_json,
                        durability: record.durability,
                        saved_token: record.saved_token,
                        created_at_ms: record.created_at_ms,
                        client: None,
                    },
                )
            })
            .collect::<std::collections::HashMap<_, _>>();
        let next_local_proxy_cap_id = local_proxy_caps
            .iter()
            .filter_map(|record| {
                record
                    .object_id
                    .strip_prefix("local-proxy-cap-")
                    .and_then(|value| value.parse::<u64>().ok())
            })
            .max()
            .unwrap_or(0);
        let next_registered_remote_cap_id = registered_remote_caps
            .keys()
            .filter_map(|record| {
                record
                    .strip_prefix("remote-registered-cap-")
                    .and_then(|value| value.parse::<u64>().ok())
            })
            .max()
            .unwrap_or(0);

        let state = Arc::new(Mutex::new(crate::AppState {
            storage: storage.clone(),
            iroh_identity: crate::IrohIdentity {
                node_id: secret_key.public().to_string(),
                secret_key,
            },
            iroh_endpoint: None,
            iroh_endpoint_addr: crate::IrohEndpointAddrSummary::empty(),
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
                crate::PairingStatus::Disconnected
            } else {
                crate::PairingStatus::Disabled
            },
            shared_caps,
            exported_caps_live: std::collections::HashMap::new(),
            peer_rpc_session: None,
            local_proxy_caps,
            registered_remote_caps,
            registered_remote_hook_object_ids: std::collections::HashMap::new(),
            local_proxy_hook_object_ids: std::collections::HashMap::new(),
            next_peer_rpc_session_id: 0,
            next_local_proxy_cap_id,
            next_registered_remote_cap_id,
            peer_rpc_error: None,
        }));
        Ok(Self::new(state, storage))
    }

    #[cfg(test)]
    pub(crate) fn shared_state_for_test(&self) -> Arc<Mutex<crate::AppState>> {
        self.state.clone()
    }

    #[cfg(test)]
    pub(crate) async fn bind_test_endpoint(
        &self,
        sandstorm_api: crate::grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    ) -> Result<(), String> {
        let secret_key = {
            let guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            guard.iroh_identity.secret_key.clone()
        };
        let endpoint = crate::Endpoint::builder(crate::presets::N0)
            .alpns(vec![
                crate::IROH_ALPN.to_vec(),
                crate::IROH_RPC_ALPN.to_vec(),
                crate::IROH_PAIR_ALPN.to_vec(),
            ])
            .secret_key(secret_key)
            .relay_mode(crate::RelayMode::Disabled)
            .bind()
            .await
            .map_err(|err| format!("failed to bind local test iroh endpoint: {err}"))?;
        let endpoint_addr = crate::summarize_endpoint_addr(endpoint.addr());
        tokio::task::spawn_local(crate::run_iroh_accept_loop(
            endpoint.clone(),
            self.state.clone(),
            sandstorm_api,
        ));
        let mut guard = self
            .state
            .lock()
            .map_err(|_| "app state lock poisoned".to_string())?;
        guard.iroh_endpoint = Some(endpoint);
        guard.iroh_endpoint_addr = endpoint_addr;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn close_test_endpoint(&self) {
        let endpoint = match self.state.lock() {
            Ok(mut guard) => guard.iroh_endpoint.take(),
            Err(_) => None,
        };
        if let Some(endpoint) = endpoint {
            endpoint.close().await;
        }
    }

    #[cfg(test)]
    pub(crate) fn local_ticket_for_test(&self) -> Result<String, String> {
        let guard = self
            .state
            .lock()
            .map_err(|_| "app state lock poisoned".to_string())?;
        Ok(crate::format_local_ticket(&guard.iroh_endpoint_addr))
    }

    #[cfg(test)]
    pub(crate) fn set_remote_ticket_for_test(&self, remote_ticket: String) -> Result<(), String> {
        {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            guard.remote_ticket = Some(remote_ticket.clone());
        }
        self.storage
            .persist_text_file(self.storage.remote_ticket_path().as_path(), &remote_ticket)
    }

    fn persist_approved_peer_node_id(&self, value: Option<&str>) -> Result<(), String> {
        let path = self.storage.approved_peer_node_id_path();
        match value {
            Some(value) => self.storage.persist_text_file(path.as_path(), value),
            None => self.storage.clear_file(path.as_path()),
        }
    }

    fn persist_tunnel_enabled(&self, enabled: bool) -> Result<(), String> {
        let path = self.storage.tunnel_enabled_path();
        self.storage
            .persist_text_file(path.as_path(), if enabled { "true" } else { "false" })
    }

    #[cfg(test)]
    pub(crate) fn seed_exported_api_session_for_test(
        &self,
        export_id: &str,
        label: &str,
        client: crate::api_session_capnp::api_session::Client,
    ) -> Result<(), String> {
        self.seed_api_session_export_for_test(export_id, label, client, false)
    }

    #[cfg(test)]
    pub(crate) fn append_exported_api_session_for_test(
        &self,
        export_id: &str,
        label: &str,
        client: crate::api_session_capnp::api_session::Client,
    ) -> Result<(), String> {
        self.seed_api_session_export_for_test(export_id, label, client, true)
    }

    #[cfg(test)]
    fn seed_api_session_export_for_test(
        &self,
        export_id: &str,
        label: &str,
        client: crate::api_session_capnp::api_session::Client,
        append: bool,
    ) -> Result<(), String> {
        let saved_cap = crate::SavedCapability {
            id: export_id.to_string(),
            label: label.to_string(),
            saved_token: String::new(),
            created_at_ms: crate::now_ms(),
            descriptor_json: None,
        };
        let mut guard = self
            .state
            .lock()
            .map_err(|_| "app state lock poisoned".to_string())?;
        if !append {
            let removed_ids = guard
                .shared_caps
                .iter()
                .filter(|cap| cap.kind == SharedCapabilityKind::ApiSession)
                .map(|cap| cap.saved_cap.id.clone())
                .collect::<Vec<_>>();
            guard
                .shared_caps
                .retain(|cap| cap.kind != SharedCapabilityKind::ApiSession);
            for removed_id in removed_ids {
                guard.exported_caps_live.remove(&removed_id);
            }
        }
        guard.shared_caps.push(crate::SharedCapability {
            id: crate::make_shared_cap_id(),
            label: saved_cap.label.clone(),
            kind: SharedCapabilityKind::ApiSession,
            enabled: true,
            created_at_ms: saved_cap.created_at_ms,
            saved_cap: saved_cap.clone(),
        });
        guard
            .exported_caps_live
            .insert(saved_cap.id.clone(), client.client.clone());
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn seed_exported_generic_capability_for_test(
        &self,
        export_id: &str,
        label: &str,
        _type_tag: &str,
        client: capnp::capability::Client,
    ) -> Result<(), String> {
        let saved_cap = crate::SavedCapability {
            id: export_id.to_string(),
            label: label.to_string(),
            saved_token: String::new(),
            created_at_ms: crate::now_ms(),
            descriptor_json: None,
        };
        let mut guard = self
            .state
            .lock()
            .map_err(|_| "app state lock poisoned".to_string())?;
        guard.shared_caps.push(crate::SharedCapability {
            id: crate::make_shared_cap_id(),
            label: saved_cap.label.clone(),
            kind: SharedCapabilityKind::Other,
            enabled: true,
            created_at_ms: saved_cap.created_at_ms,
            saved_cap: saved_cap.clone(),
        });
        guard
            .exported_caps_live
            .insert(saved_cap.id.clone(), client);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn seed_exported_ip_network_for_test(
        &self,
        export_id: &str,
        label: &str,
        client: crate::ip_capnp::ip_network::Client,
    ) -> Result<(), String> {
        let saved_cap = crate::SavedCapability {
            id: export_id.to_string(),
            label: label.to_string(),
            saved_token: String::new(),
            created_at_ms: crate::now_ms(),
            descriptor_json: None,
        };
        let mut guard = self
            .state
            .lock()
            .map_err(|_| "app state lock poisoned".to_string())?;
        let removed_ids = guard
            .shared_caps
            .iter()
            .filter(|cap| cap.kind == SharedCapabilityKind::IpNetwork)
            .map(|cap| cap.saved_cap.id.clone())
            .collect::<Vec<_>>();
        guard
            .shared_caps
            .retain(|cap| cap.kind != SharedCapabilityKind::IpNetwork);
        for removed_id in removed_ids {
            guard.exported_caps_live.remove(&removed_id);
        }
        guard.shared_caps.push(crate::SharedCapability {
            id: crate::make_shared_cap_id(),
            label: saved_cap.label.clone(),
            kind: SharedCapabilityKind::IpNetwork,
            enabled: true,
            created_at_ms: saved_cap.created_at_ms,
            saved_cap: saved_cap.clone(),
        });
        guard
            .exported_caps_live
            .insert(saved_cap.id.clone(), client.client.clone());
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn clear_exported_api_session_for_test(&self) -> Result<(), String> {
        let mut guard = self
            .state
            .lock()
            .map_err(|_| "app state lock poisoned".to_string())?;
        for shared_cap in &mut guard.shared_caps {
            if shared_cap.kind == SharedCapabilityKind::ApiSession {
                shared_cap.enabled = false;
            }
        }
        let disabled_ids = guard
            .shared_caps
            .iter()
            .filter(|cap| cap.kind == SharedCapabilityKind::ApiSession)
            .map(|cap| cap.saved_cap.id.clone())
            .collect::<Vec<_>>();
        for disabled_id in disabled_ids {
            guard.exported_caps_live.remove(&disabled_id);
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn invoke_restored_api_session_for_test(
        &self,
        sandstorm_api: crate::grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
        object_id: &str,
        filename: &str,
        payload: &[u8],
    ) -> Result<crate::ApiSessionInvokeSummary, String> {
        let restored_cap = self
            .restore_object_capability(sandstorm_api, object_id)
            .await?;
        crate::test_support::invoke_api_session_client_for_test(
            crate::api_session_capnp::api_session::Client {
                client: restored_cap,
            },
            filename,
            payload,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn invoke_restored_ip_network_for_test(
        &self,
        sandstorm_api: crate::grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
        object_id: &str,
        host: &str,
        port: u16,
    ) -> Result<crate::TcpProbeSummary, String> {
        let restored_cap = self
            .restore_object_capability(sandstorm_api, object_id)
            .await?;
        crate::test_support::invoke_ip_network_client_for_test(
            crate::ip_capnp::ip_network::Client {
                client: restored_cap,
            },
            host,
            port,
        )
        .await
    }

    pub(crate) async fn restore_object_capability(
        &self,
        sandstorm_api: crate::grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
        object_id: &str,
    ) -> Result<capnp::capability::Client, String> {
        if object_id == LOCAL_TEST_API_SESSION_OBJECT_ID {
            return self.build_local_test_api_session_client();
        }
        let local_proxy_cap = {
            let guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            guard
                .local_proxy_caps
                .iter()
                .find(|record| record.object_id == object_id)
                .cloned()
        };
        if let Some(record) = local_proxy_cap {
            if !record.enabled {
                return Err(format!(
                    "local proxy object {} is disabled",
                    record.object_id
                ));
            }
            return self.build_local_proxy_client(record.object_id);
        }
        let saved_cap = crate::load_saved_capability_by_id(object_id)?
            .ok_or_else(|| format!("unknown app object id: {object_id}"))?;
        let token = crate::hex_decode(&saved_cap.saved_token)?;
        SandstormBackend::new(sandstorm_api)
            .restore_capability(&token)
            .await
    }

    fn build_local_proxy_client(
        &self,
        object_id: String,
    ) -> Result<capnp::capability::Client, String> {
        let backend_client = crate::untyped_local::new_client_with_transforms(
            LocalProxyCapabilityServer::new(self.clone(), object_id.clone()),
            Some(self.local_proxy_request_transform()),
            Some(self.local_proxy_response_transform(object_id.clone())),
        );
        let client = new_client::<TypelessHostedClient, _>(HostedLocalProxyCapabilityServer::new(
            self.clone(),
            object_id.clone(),
            backend_client,
        ))
        .0;
        let hook_ptr = client.hook.get_ptr();
        let mut guard = self
            .state
            .lock()
            .map_err(|_| "app state lock poisoned".to_string())?;
        guard
            .local_proxy_hook_object_ids
            .insert(hook_ptr, object_id);
        Ok(client)
    }

    fn build_local_test_api_session_client(&self) -> Result<capnp::capability::Client, String> {
        let backend_client = new_client::<crate::api_session_capnp::api_session::Client, _>(
            LocalTestApiSessionBackend,
        )
        .client;
        Ok(
            new_client::<TypelessHostedClient, _>(HostedStaticCapabilityServer::new(
                LOCAL_TEST_API_SESSION_OBJECT_ID.to_string(),
                LOCAL_TEST_API_SESSION_LABEL.to_string(),
                backend_client,
            ))
            .0,
        )
    }

    pub(crate) async fn resolve_local_proxy_target_remote_capability(
        &self,
        object_id: &str,
    ) -> Result<capnp::capability::Client, String> {
        let record = {
            let guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            guard
                .local_proxy_caps
                .iter()
                .find(|record| record.object_id == object_id)
                .cloned()
                .ok_or_else(|| format!("unknown local proxy object id: {object_id}"))?
        };
        let remote_bootstrap = {
            let guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            let session = guard
                .peer_rpc_session
                .as_ref()
                .ok_or_else(|| {
                    format!(
                        "local proxy object {} is known but the tunnel is not currently connected; reconnect the peer session to restore it",
                        record.object_id
                    )
                })?;
            if session.remote_node_id != record.peer_node_id {
                return Err(format!(
                    "local proxy object {} targets peer {} but the current peer session is {}",
                    record.object_id, record.peer_node_id, session.remote_node_id
                ));
            }
            session.remote_bootstrap.clone()
        };
        let (_label, _kind, _type_tag, _descriptor_json, client) = match record.target_kind {
            crate::LocalProxyTargetKindRuntime::ExportId => {
                crate::fetch_remote_capability_export(remote_bootstrap, &record.target_id).await?
            }
            crate::LocalProxyTargetKindRuntime::RemoteObjectId => {
                crate::fetch_remote_registered_capability(remote_bootstrap, &record.target_id)
                    .await?
            }
        };
        Ok(client)
    }

    pub(crate) async fn create_local_proxy_for_remote_export(
        &self,
        export_id: &str,
    ) -> Result<(String, String), String> {
        let (remote_bootstrap, remote_node_id, export_metadata) = {
            let guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            let session = guard
                .peer_rpc_session
                .as_ref()
                .ok_or_else(|| "peer rpc session is not connected".to_string())?;
            let export = session
                .capability_exports
                .iter()
                .find(|row| row.id == export_id)
                .cloned()
                .ok_or_else(|| format!("unknown remote export id: {export_id}"))?;
            (
                session.remote_bootstrap.clone(),
                session.remote_node_id.clone(),
                export,
            )
        };

        let (_label, fetched_kind, fetched_type_tag, fetched_descriptor_json, _client) =
            crate::fetch_remote_capability_export(remote_bootstrap, export_id).await?;

        self.create_local_proxy_record(
            remote_node_id,
            crate::LocalProxyTargetKindRuntime::ExportId,
            export_id.to_string(),
            export_metadata.label.clone(),
            fetched_kind,
            if fetched_type_tag.is_empty() {
                export_metadata.type_tag.clone()
            } else {
                fetched_type_tag
            },
            fetched_descriptor_json.or(export_metadata.descriptor_json.clone()),
        )
    }

    pub(crate) async fn create_local_proxy_for_registered_remote_object(
        &self,
        remote_object_id: &str,
    ) -> Result<(String, String), String> {
        let (remote_bootstrap, remote_node_id) = {
            let guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            let session = guard
                .peer_rpc_session
                .as_ref()
                .ok_or_else(|| "peer rpc session is not connected".to_string())?;
            (
                session.remote_bootstrap.clone(),
                session.remote_node_id.clone(),
            )
        };
        let (label, fetched_kind, fetched_type_tag, fetched_descriptor_json, _client) =
            crate::fetch_remote_registered_capability(remote_bootstrap, remote_object_id).await?;
        self.create_local_proxy_record(
            remote_node_id,
            crate::LocalProxyTargetKindRuntime::RemoteObjectId,
            remote_object_id.to_string(),
            label,
            fetched_kind,
            fetched_type_tag,
            fetched_descriptor_json,
        )
    }

    pub(crate) async fn localize_remote_capability_for_local_proxy_target(
        &self,
        local_proxy_object_id: &str,
        client: capnp::capability::Client,
    ) -> Result<capnp::capability::Client, String> {
        let (remote_bootstrap, _remote_node_id) = {
            let guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            let record = guard
                .local_proxy_caps
                .iter()
                .find(|record| record.object_id == local_proxy_object_id)
                .ok_or_else(|| format!("unknown local proxy object id: {local_proxy_object_id}"))?;
            let session = guard.peer_rpc_session.as_ref().ok_or_else(|| {
                format!(
                    "local proxy object {} is known but the tunnel is not currently connected; reconnect the peer session to restore it",
                    record.object_id
                )
            })?;
            if session.remote_node_id != record.peer_node_id {
                return Err(format!(
                    "local proxy object {} targets peer {} but the current peer session is {}",
                    record.object_id, record.peer_node_id, session.remote_node_id
                ));
            }
            (
                session.remote_bootstrap.clone(),
                session.remote_node_id.clone(),
            )
        };

        let remote_object_id = crate::register_remote_capability(
            remote_bootstrap,
            client,
            "Remote capability",
            crate::ImportedRemoteCapabilityKind::Other,
            "capnp/unknown",
            None,
        )
        .await?;
        let (_, object_id) = self
            .create_local_proxy_for_registered_remote_object(&remote_object_id)
            .await?;
        self.build_local_proxy_client(object_id)
    }

    pub(crate) async fn unwrap_local_proxy_parameter(
        &self,
        client: capnp::capability::Client,
    ) -> Result<capnp::capability::Client, String> {
        let maybe_object_id = {
            let guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            guard
                .local_proxy_hook_object_ids
                .get(&client.hook.get_ptr())
                .cloned()
        };
        match maybe_object_id {
            Some(object_id) => {
                self.resolve_local_proxy_target_remote_capability(&object_id)
                    .await
            }
            None => Ok(client),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn create_local_proxy_record(
        &self,
        remote_node_id: String,
        target_kind: crate::LocalProxyTargetKindRuntime,
        target_id: String,
        label: String,
        fetched_kind: crate::ImportedRemoteCapabilityKind,
        type_tag: String,
        descriptor_json: Option<String>,
    ) -> Result<(String, String), String> {
        let (record, persisted_records) = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            if let Some(existing) = guard
                .local_proxy_caps
                .iter()
                .find(|record| {
                    record.peer_node_id == remote_node_id
                        && record.target_kind == target_kind
                        && record.target_id == target_id
                })
                .cloned()
            {
                if existing.enabled {
                    return Ok((existing.label, existing.object_id));
                }
                if let Some(record) = guard
                    .local_proxy_caps
                    .iter_mut()
                    .find(|record| record.object_id == existing.object_id)
                {
                    record.enabled = true;
                }
                let persisted_records = guard.local_proxy_caps.clone();
                drop(guard);
                self.persist_local_proxy_capability_registry(&persisted_records)?;
                return Ok((existing.label, existing.object_id));
            }

            guard.next_local_proxy_cap_id += 1;
            let object_id = format!("local-proxy-cap-{}", guard.next_local_proxy_cap_id);
            let now = crate::now_ms();
            let kind = match fetched_kind {
                crate::ImportedRemoteCapabilityKind::IpNetwork => {
                    crate::SharedCapabilityKind::IpNetwork
                }
                crate::ImportedRemoteCapabilityKind::ApiSession => {
                    crate::SharedCapabilityKind::ApiSession
                }
                crate::ImportedRemoteCapabilityKind::Other => crate::SharedCapabilityKind::Other,
            };
            let record = crate::LocalProxyCapability {
                object_id,
                peer_node_id: remote_node_id.clone(),
                target_kind,
                target_id,
                label,
                kind,
                type_tag,
                descriptor_json,
                enabled: true,
                created_at_ms: now,
            };
            guard.local_proxy_caps.push(record.clone());
            (record, guard.local_proxy_caps.clone())
        };

        self.persist_local_proxy_capability_registry(&persisted_records)?;
        Ok((record.label, record.object_id))
    }

    pub(crate) fn drop_received_remote_capability(&self, object_id: &str) -> Result<bool, String> {
        let (removed_local_proxy, local_proxy_caps) = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            let before = guard.local_proxy_caps.len();
            guard
                .local_proxy_caps
                .retain(|record| record.object_id != object_id);
            (
                guard.local_proxy_caps.len() != before,
                guard.local_proxy_caps.clone(),
            )
        };
        if removed_local_proxy {
            self.persist_local_proxy_capability_registry(&local_proxy_caps)?;
            return Ok(true);
        }

        Ok(false)
    }

    pub(crate) async fn save_received_remote_capability_locally(
        &self,
        sandstorm_api: crate::grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
        object_id: &str,
    ) -> Result<crate::SavedCapability, String> {
        let (label, descriptor_json) = {
            let guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            if let Some(record) = guard
                .local_proxy_caps
                .iter()
                .find(|record| record.object_id == object_id)
            {
                (record.label.clone(), record.descriptor_json.clone())
            } else {
                return Err(format!(
                    "unknown received capability object id: {object_id}"
                ));
            }
        };

        let cap = self
            .restore_object_capability(sandstorm_api.clone(), object_id)
            .await?;
        let saved_token = crate::save_capability(sandstorm_api, cap, &label).await?;
        crate::persist_saved_capability(&label, &saved_token, descriptor_json.as_deref())
    }

    pub(crate) fn disable_received_remote_capability(
        &self,
        object_id: &str,
    ) -> Result<bool, String> {
        let (changed_local_proxy, local_proxy_caps) = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            let mut changed = false;
            for record in &mut guard.local_proxy_caps {
                if record.object_id == object_id {
                    record.enabled = false;
                    changed = true;
                }
            }
            (changed, guard.local_proxy_caps.clone())
        };
        if changed_local_proxy {
            self.persist_local_proxy_capability_registry(&local_proxy_caps)?;
            return Ok(true);
        }

        Ok(false)
    }

    pub(crate) async fn enable_received_remote_capability(
        &self,
        object_id: &str,
    ) -> Result<bool, String> {
        let (enabled_local_proxy, local_proxy_caps) = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            let mut changed = false;
            for record in &mut guard.local_proxy_caps {
                if record.object_id == object_id {
                    record.enabled = true;
                    changed = true;
                }
            }
            (changed, guard.local_proxy_caps.clone())
        };
        if enabled_local_proxy {
            self.persist_local_proxy_capability_registry(&local_proxy_caps)?;
            return Ok(true);
        }

        Ok(false)
    }

    pub(crate) async fn configure_exported_ip_network(
        &self,
        sandstorm_api: crate::grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
        saved_cap_id: &str,
    ) -> Result<(), String> {
        if let Some(saved_cap_id) = saved_cap_id.strip_prefix('!') {
            let shared_caps = {
                let mut guard = self
                    .state
                    .lock()
                    .map_err(|_| "app state lock poisoned".to_string())?;
                for shared_cap in &mut guard.shared_caps {
                    if shared_cap.kind == SharedCapabilityKind::IpNetwork
                        && shared_cap.saved_cap.id == saved_cap_id
                    {
                        shared_cap.enabled = false;
                    }
                }
                guard.exported_caps_live.remove(saved_cap_id);
                guard.shared_caps.clone()
            };
            self.persist_shared_capability_registry(&shared_caps)?;
            return Ok(());
        }
        if saved_cap_id.trim().is_empty() {
            let shared_caps = {
                let mut guard = self
                    .state
                    .lock()
                    .map_err(|_| "app state lock poisoned".to_string())?;
                for shared_cap in &mut guard.shared_caps {
                    if shared_cap.kind == SharedCapabilityKind::IpNetwork {
                        shared_cap.enabled = false;
                    }
                }
                let disabled_ids = guard
                    .shared_caps
                    .iter()
                    .filter(|cap| cap.kind == SharedCapabilityKind::IpNetwork)
                    .map(|cap| cap.saved_cap.id.clone())
                    .collect::<Vec<_>>();
                for disabled_id in disabled_ids {
                    guard.exported_caps_live.remove(&disabled_id);
                }
                guard.shared_caps.clone()
            };
            self.persist_shared_capability_registry(&shared_caps)?;
            return Ok(());
        }

        let saved_cap = crate::load_saved_capability_by_id(saved_cap_id)?
            .ok_or_else(|| format!("unknown saved capability id: {saved_cap_id}"))?;
        let client =
            crate::validate_saved_ip_network_capability(sandstorm_api, &saved_cap.saved_token)
                .await?;

        let shared_caps = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            let mut found = false;
            for shared_cap in &mut guard.shared_caps {
                if shared_cap.kind == SharedCapabilityKind::IpNetwork {
                    if shared_cap.saved_cap.id == saved_cap.id {
                        shared_cap.enabled = true;
                    }
                    if shared_cap.saved_cap.id == saved_cap.id {
                        shared_cap.label = saved_cap.label.clone();
                        shared_cap.saved_cap = saved_cap.clone();
                        shared_cap.created_at_ms = saved_cap.created_at_ms;
                        found = true;
                    }
                }
            }
            if !found {
                guard.shared_caps.push(crate::SharedCapability {
                    id: crate::make_shared_cap_id(),
                    label: saved_cap.label.clone(),
                    kind: SharedCapabilityKind::IpNetwork,
                    enabled: true,
                    created_at_ms: saved_cap.created_at_ms,
                    saved_cap: saved_cap.clone(),
                });
            }
            guard
                .exported_caps_live
                .insert(saved_cap.id.clone(), client.client.clone());
            guard.shared_caps.clone()
        };
        self.persist_shared_capability_registry(&shared_caps)?;
        Ok(())
    }

    pub(crate) async fn configure_exported_api_session(
        &self,
        sandstorm_api: crate::grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
        saved_cap_id: &str,
    ) -> Result<(), String> {
        if let Some(saved_cap_id) = saved_cap_id.strip_prefix('!') {
            let shared_caps = {
                let mut guard = self
                    .state
                    .lock()
                    .map_err(|_| "app state lock poisoned".to_string())?;
                for shared_cap in &mut guard.shared_caps {
                    if shared_cap.kind == SharedCapabilityKind::ApiSession
                        && shared_cap.saved_cap.id == saved_cap_id
                    {
                        shared_cap.enabled = false;
                    }
                }
                guard.exported_caps_live.remove(saved_cap_id);
                guard.shared_caps.clone()
            };
            self.persist_shared_capability_registry(&shared_caps)?;
            return Ok(());
        }
        if saved_cap_id.trim().is_empty() {
            let shared_caps = {
                let mut guard = self
                    .state
                    .lock()
                    .map_err(|_| "app state lock poisoned".to_string())?;
                for shared_cap in &mut guard.shared_caps {
                    if shared_cap.kind == SharedCapabilityKind::ApiSession {
                        shared_cap.enabled = false;
                    }
                }
                let disabled_ids = guard
                    .shared_caps
                    .iter()
                    .filter(|cap| cap.kind == SharedCapabilityKind::ApiSession)
                    .map(|cap| cap.saved_cap.id.clone())
                    .collect::<Vec<_>>();
                for disabled_id in disabled_ids {
                    guard.exported_caps_live.remove(&disabled_id);
                }
                guard.shared_caps.clone()
            };
            self.persist_shared_capability_registry(&shared_caps)?;
            return Ok(());
        }

        let saved_cap = crate::load_saved_capability_by_id(saved_cap_id)?
            .ok_or_else(|| format!("unknown saved capability id: {saved_cap_id}"))?;
        let client =
            crate::validate_saved_api_session_capability(sandstorm_api, &saved_cap.saved_token)
                .await?;

        let shared_caps = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            let mut found = false;
            for shared_cap in &mut guard.shared_caps {
                if shared_cap.kind == SharedCapabilityKind::ApiSession {
                    if shared_cap.saved_cap.id == saved_cap.id {
                        shared_cap.enabled = true;
                    }
                    if shared_cap.saved_cap.id == saved_cap.id {
                        shared_cap.label = saved_cap.label.clone();
                        shared_cap.saved_cap = saved_cap.clone();
                        shared_cap.created_at_ms = saved_cap.created_at_ms;
                        found = true;
                    }
                }
            }
            if !found {
                guard.shared_caps.push(crate::SharedCapability {
                    id: crate::make_shared_cap_id(),
                    label: saved_cap.label.clone(),
                    kind: SharedCapabilityKind::ApiSession,
                    enabled: true,
                    created_at_ms: saved_cap.created_at_ms,
                    saved_cap: saved_cap.clone(),
                });
            }
            guard
                .exported_caps_live
                .insert(saved_cap.id.clone(), client.client.clone());
            guard.shared_caps.clone()
        };
        self.persist_shared_capability_registry(&shared_caps)?;
        Ok(())
    }

    pub(crate) async fn connect_peer_rpc_session(
        &self,
        sandstorm_api: crate::grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    ) -> Result<(), String> {
        let (endpoint, remote_ticket, old_connection) = {
            let mut guard = self
                .state
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
            let old_connection = guard
                .peer_rpc_session
                .take()
                .map(|session| session.connection);
            guard.peer_rpc_error = None;
            (endpoint, remote_ticket, old_connection)
        };
        if let Some(old_connection) = old_connection {
            old_connection.close(0u32.into(), b"peer rpc session replaced");
        }

        let remote_addr = crate::parse_remote_ticket(&remote_ticket)?;
        let remote_node_id = remote_addr.id.to_string();
        let connection = endpoint
            .connect(remote_addr, crate::IROH_RPC_ALPN)
            .await
            .map_err(|err| format!("failed to connect peer rpc session: {err}"))?;
        self.attach_client_peer_rpc_session_eager(connection, remote_node_id, sandstorm_api)
            .await
    }

    pub(crate) async fn begin_pair_connection(
        &self,
        sandstorm_api: crate::grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    ) -> Result<(), String> {
        let (endpoint, remote_ticket, old_connection) = {
            let mut guard = self
                .state
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
            let old_connection = guard
                .peer_rpc_session
                .take()
                .map(|session| session.connection);
            guard.peer_rpc_error = None;
            guard.pending_outgoing_peer_node_id = None;
            guard.pairing_status = crate::PairingStatus::Connecting;
            (endpoint, remote_ticket, old_connection)
        };
        if let Some(old_connection) = old_connection {
            old_connection.close(0u32.into(), b"peer rpc session replaced");
        }

        let remote_addr = crate::parse_remote_ticket(&remote_ticket)?;
        let remote_node_id = remote_addr.id.to_string();
        {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            guard.pending_outgoing_peer_node_id = Some(remote_node_id.clone());
            guard.pairing_status = crate::PairingStatus::AwaitingRemoteAccept;
        }

        let result: Result<(), String> = async {
            let connection = endpoint
                .connect(remote_addr, crate::IROH_PAIR_ALPN)
                .await
                .map_err(|err| format!("failed to connect pairing session: {err}"))?;
            let (mut send, mut recv) = connection
                .open_bi()
                .await
                .map_err(|err| format!("failed to open pairing control stream: {err}"))?;
            let request = crate::encode_pair_control_message(crate::PairControlMessage::Request {
                version: crate::PAIRING_PROTOCOL_VERSION,
            })?;
            send.write_all(&request)
                .await
                .map_err(|err| format!("failed to write pairing request: {err}"))?;
            send.finish()
                .map_err(|err| format!("failed to finish pairing request: {err}"))?;

            let response_bytes = recv
                .read_to_end(4096)
                .await
                .map_err(|err| format!("failed to read pairing response: {err}"))?;
            let response = crate::decode_pair_control_message(&response_bytes)?;
            match response {
                crate::PairControlMessage::Response {
                    version,
                    decision: crate::PairControlDecision::Accepted,
                } => {
                    if version != crate::PAIRING_PROTOCOL_VERSION {
                        return Err(format!("unsupported pairing protocol version: {version}"));
                    }
                    {
                        let mut guard = self
                            .state
                            .lock()
                            .map_err(|_| "app state lock poisoned".to_string())?;
                        guard.pending_outgoing_peer_node_id = None;
                        guard.approved_peer_node_id = Some(remote_node_id.clone());
                        guard.tunnel_enabled = true;
                    }
                    self.persist_approved_peer_node_id(Some(&remote_node_id))?;
                    self.persist_tunnel_enabled(true)?;
                    self.attach_client_peer_rpc_session(connection, remote_node_id, sandstorm_api)
                        .await?;
                    Ok(())
                }
                crate::PairControlMessage::Response {
                    decision: crate::PairControlDecision::Rejected,
                    ..
                } => Err("remote grain rejected the connection".to_string()),
                _ => Err("unexpected pairing control message".to_string()),
            }
        }
        .await;

        if let Err(err) = &result {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            guard.pending_outgoing_peer_node_id = None;
            guard.peer_rpc_error = Some(err.clone());
            guard.pairing_status = crate::PairingStatus::Error;
        }

        result
    }

    pub(crate) async fn accept_pending_pair_connection(
        &self,
        sandstorm_api: crate::grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    ) -> Result<(), String> {
        let (remote_node_id, connection, mut handshake_send) = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            let pending = guard
                .pending_incoming_connection
                .take()
                .ok_or_else(|| "no pending incoming connection".to_string())?;
            guard.pending_incoming_peer_node_id = None;
            guard.approved_peer_node_id = Some(pending.remote_node_id.clone());
            guard.tunnel_enabled = true;
            (
                pending.remote_node_id,
                pending.connection,
                pending.handshake_send,
            )
        };
        self.persist_approved_peer_node_id(Some(&remote_node_id))?;
        self.persist_tunnel_enabled(true)?;

        let response = crate::encode_pair_control_message(crate::PairControlMessage::Response {
            version: crate::PAIRING_PROTOCOL_VERSION,
            decision: crate::PairControlDecision::Accepted,
        })?;
        handshake_send
            .write_all(&response)
            .await
            .map_err(|err| format!("failed to write pairing acceptance: {err}"))?;
        handshake_send
            .finish()
            .map_err(|err| format!("failed to finish pairing acceptance: {err}"))?;
        self.attach_server_peer_rpc_session(connection, remote_node_id, sandstorm_api)
            .await
    }

    pub(crate) async fn reject_pending_pair_connection(&self) -> Result<(), String> {
        let pending = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            guard.pending_incoming_peer_node_id = None;
            guard.pairing_status = crate::PairingStatus::Disconnected;
            guard.pending_incoming_connection.take()
        };
        let Some(mut pending) = pending else {
            return Err("no pending incoming connection".to_string());
        };
        let response = crate::encode_pair_control_message(crate::PairControlMessage::Response {
            version: crate::PAIRING_PROTOCOL_VERSION,
            decision: crate::PairControlDecision::Rejected,
        })?;
        pending
            .handshake_send
            .write_all(&response)
            .await
            .map_err(|err| format!("failed to write pairing rejection: {err}"))?;
        pending
            .handshake_send
            .finish()
            .map_err(|err| format!("failed to finish pairing rejection: {err}"))?;
        // Give the rejecting side a short grace period so the control response is
        // delivered before the connection handle is dropped.
        sleep(Duration::from_millis(50)).await;
        Ok(())
    }

    pub(crate) async fn enable_tunnel(
        &self,
        sandstorm_api: crate::grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    ) -> Result<(), String> {
        let should_accept_pending = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            guard.tunnel_enabled = true;
            guard.peer_rpc_error = None;
            guard.pairing_status = crate::PairingStatus::Disconnected;
            matches!(
                (
                    guard.approved_peer_node_id.as_ref(),
                    guard.pending_incoming_connection.as_ref(),
                ),
                (Some(approved), Some(pending)) if approved == &pending.remote_node_id
            )
        };
        self.persist_tunnel_enabled(true)?;
        if should_accept_pending {
            self.accept_pending_pair_connection(sandstorm_api).await?;
        }
        Ok(())
    }

    pub(crate) fn disable_tunnel(&self) -> Result<(), String> {
        let mut guard = self
            .state
            .lock()
            .map_err(|_| "app state lock poisoned".to_string())?;
        self.close_peer_rpc_session_locked(&mut guard, b"tunnel disabled");
        if let Some(pending) = guard.pending_incoming_connection.take() {
            pending.connection.close(0u32.into(), b"tunnel disabled");
        }
        guard.pending_incoming_peer_node_id = None;
        guard.pending_outgoing_peer_node_id = None;
        guard.peer_rpc_error = None;
        guard.tunnel_enabled = false;
        guard.pairing_status = crate::PairingStatus::Disabled;
        drop(guard);
        self.persist_tunnel_enabled(false)
    }

    pub(crate) fn forget_peer(&self) -> Result<(), String> {
        let mut guard = self
            .state
            .lock()
            .map_err(|_| "app state lock poisoned".to_string())?;
        self.close_peer_rpc_session_locked(&mut guard, b"peer forgotten");
        if let Some(pending) = guard.pending_incoming_connection.take() {
            pending.connection.close(0u32.into(), b"peer forgotten");
        }
        guard.pending_incoming_peer_node_id = None;
        guard.pending_outgoing_peer_node_id = None;
        guard.peer_rpc_error = None;
        guard.remote_ticket = None;
        guard.approved_peer_node_id = None;
        guard.tunnel_enabled = true;
        guard.pairing_status = crate::PairingStatus::Disconnected;
        drop(guard);
        let remote_ticket_path = self.storage.remote_ticket_path();
        self.storage.clear_file(remote_ticket_path.as_path())?;
        self.persist_approved_peer_node_id(None)?;
        self.persist_tunnel_enabled(true)
    }

    async fn attach_client_peer_rpc_session(
        &self,
        connection: iroh::endpoint::Connection,
        remote_node_id: String,
        sandstorm_api: crate::grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    ) -> Result<(), String> {
        let (send, recv) = connection
            .open_bi()
            .await
            .map_err(|err| format!("failed to open peer rpc stream: {err}"))?;

        let local_bootstrap: crate::tunnel_capnp::peer_bootstrap::Client =
            new_client(crate::PeerBootstrapImpl {
                sandstorm_api,
                app_state: self.state.clone(),
            });
        let network = Box::new(twoparty::VatNetwork::new(
            recv.compat(),
            send.compat_write(),
            rpc_twoparty_capnp::Side::Client,
            Default::default(),
        ));
        let mut rpc_system = RpcSystem::new(network, Some(local_bootstrap.client));
        let remote_bootstrap = rpc_system.bootstrap::<crate::tunnel_capnp::peer_bootstrap::Client>(
            rpc_twoparty_capnp::Side::Server,
        );
        tokio::task::spawn_local(async move {
            if let Err(err) = rpc_system.await {
                eprintln!("peer rpc system exited with error: {err}");
            }
        });

        let session_id = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            guard.next_peer_rpc_session_id += 1;
            let session_id = guard.next_peer_rpc_session_id;
            guard.peer_rpc_session = Some(crate::PeerRpcSession {
                session_id,
                remote_node_id,
                connection: connection.clone(),
                remote_bootstrap: remote_bootstrap.clone(),
                capability_exports: Vec::new(),
            });
            guard.pairing_status = crate::PairingStatus::Connected;
            session_id
        };

        tokio::task::spawn_local({
            let app = self.clone();
            let remote_bootstrap = remote_bootstrap.clone();
            async move {
                match crate::list_remote_capability_exports(remote_bootstrap.clone()).await {
                    Ok(capability_exports) => {
                        if let Ok(mut guard) = app.state.lock() {
                            let is_current = guard
                                .peer_rpc_session
                                .as_ref()
                                .map(|session| session.session_id == session_id)
                                .unwrap_or(false);
                            if is_current && let Some(session) = guard.peer_rpc_session.as_mut() {
                                session.capability_exports = capability_exports;
                            }
                        }
                    }
                    Err(err) => {
                        if let Ok(mut guard) = app.state.lock() {
                            guard.peer_rpc_error = Some(err);
                        }
                    }
                }
            }
        });

        tokio::task::spawn_local({
            let app = self.clone();
            async move {
                let close_reason = connection.closed().await;
                if let Ok(mut guard) = app.state.lock() {
                    let is_current = guard
                        .peer_rpc_session
                        .as_ref()
                        .map(|session| session.session_id == session_id)
                        .unwrap_or(false);
                    if is_current {
                        guard.peer_rpc_session = None;
                        guard.peer_rpc_error =
                            Some(format!("peer rpc session closed: {close_reason}"));
                        guard.pairing_status = if guard.tunnel_enabled {
                            crate::PairingStatus::Disconnected
                        } else {
                            crate::PairingStatus::Disabled
                        };
                    }
                }
            }
        });

        Ok(())
    }

    async fn attach_client_peer_rpc_session_eager(
        &self,
        connection: iroh::endpoint::Connection,
        remote_node_id: String,
        sandstorm_api: crate::grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    ) -> Result<(), String> {
        let (send, recv) = connection
            .open_bi()
            .await
            .map_err(|err| format!("failed to open peer rpc stream: {err}"))?;

        let local_bootstrap: crate::tunnel_capnp::peer_bootstrap::Client =
            new_client(crate::PeerBootstrapImpl {
                sandstorm_api,
                app_state: self.state.clone(),
            });
        let network = Box::new(twoparty::VatNetwork::new(
            recv.compat(),
            send.compat_write(),
            rpc_twoparty_capnp::Side::Client,
            Default::default(),
        ));
        let mut rpc_system = RpcSystem::new(network, Some(local_bootstrap.client));
        let remote_bootstrap = rpc_system.bootstrap::<crate::tunnel_capnp::peer_bootstrap::Client>(
            rpc_twoparty_capnp::Side::Server,
        );
        tokio::task::spawn_local(async move {
            if let Err(err) = rpc_system.await {
                eprintln!("peer rpc system exited with error: {err}");
            }
        });

        let capability_exports =
            crate::list_remote_capability_exports(remote_bootstrap.clone()).await?;
        let session_id = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            guard.next_peer_rpc_session_id += 1;
            let session_id = guard.next_peer_rpc_session_id;
            guard.peer_rpc_session = Some(crate::PeerRpcSession {
                session_id,
                remote_node_id,
                connection: connection.clone(),
                remote_bootstrap,
                capability_exports,
            });
            guard.pairing_status = crate::PairingStatus::Connected;
            session_id
        };

        tokio::task::spawn_local({
            let app = self.clone();
            async move {
                let close_reason = connection.closed().await;
                if let Ok(mut guard) = app.state.lock() {
                    let is_current = guard
                        .peer_rpc_session
                        .as_ref()
                        .map(|session| session.session_id == session_id)
                        .unwrap_or(false);
                    if is_current {
                        guard.peer_rpc_session = None;
                        guard.peer_rpc_error =
                            Some(format!("peer rpc session closed: {close_reason}"));
                        guard.pairing_status = if guard.tunnel_enabled {
                            crate::PairingStatus::Disconnected
                        } else {
                            crate::PairingStatus::Disabled
                        };
                    }
                }
            }
        });

        Ok(())
    }

    async fn attach_server_peer_rpc_session(
        &self,
        connection: iroh::endpoint::Connection,
        remote_node_id: String,
        sandstorm_api: crate::grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    ) -> Result<(), String> {
        let (send, recv) = connection
            .accept_bi()
            .await
            .map_err(|err| format!("failed to accept peer rpc stream: {err}"))?;

        let local_bootstrap: crate::tunnel_capnp::peer_bootstrap::Client =
            new_client(crate::PeerBootstrapImpl {
                sandstorm_api,
                app_state: self.state.clone(),
            });
        let network = Box::new(twoparty::VatNetwork::new(
            recv.compat(),
            send.compat_write(),
            rpc_twoparty_capnp::Side::Server,
            Default::default(),
        ));
        let mut rpc_system = RpcSystem::new(network, Some(local_bootstrap.client));
        let remote_bootstrap = rpc_system.bootstrap::<crate::tunnel_capnp::peer_bootstrap::Client>(
            rpc_twoparty_capnp::Side::Client,
        );
        let session_id = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            guard.next_peer_rpc_session_id += 1;
            let session_id = guard.next_peer_rpc_session_id;
            guard.peer_rpc_session = Some(crate::PeerRpcSession {
                session_id,
                remote_node_id,
                connection: connection.clone(),
                remote_bootstrap: remote_bootstrap.clone(),
                capability_exports: Vec::new(),
            });
            guard.pairing_status = crate::PairingStatus::Connected;
            session_id
        };

        tokio::task::spawn_local({
            let app = self.clone();
            let remote_bootstrap = remote_bootstrap.clone();
            async move {
                match crate::list_remote_capability_exports(remote_bootstrap.clone()).await {
                    Ok(capability_exports) => {
                        if let Ok(mut guard) = app.state.lock() {
                            let is_current = guard
                                .peer_rpc_session
                                .as_ref()
                                .map(|session| session.session_id == session_id)
                                .unwrap_or(false);
                            if is_current && let Some(session) = guard.peer_rpc_session.as_mut() {
                                session.capability_exports = capability_exports;
                            }
                        }
                    }
                    Err(err) => {
                        if let Ok(mut guard) = app.state.lock() {
                            guard.peer_rpc_error = Some(err);
                        }
                    }
                }
            }
        });

        tokio::task::spawn_local({
            let app = self.clone();
            async move {
                if let Err(err) = rpc_system.await {
                    eprintln!("peer rpc system exited with error: {err}");
                }
                let close_reason = connection.closed().await;
                if let Ok(mut guard) = app.state.lock() {
                    let is_current = guard
                        .peer_rpc_session
                        .as_ref()
                        .map(|session| session.session_id == session_id)
                        .unwrap_or(false);
                    if is_current {
                        guard.peer_rpc_session = None;
                        guard.peer_rpc_error =
                            Some(format!("peer rpc session closed: {close_reason}"));
                        guard.pairing_status = if guard.tunnel_enabled {
                            crate::PairingStatus::Disconnected
                        } else {
                            crate::PairingStatus::Disabled
                        };
                    }
                }
            }
        });

        Ok(())
    }

    pub(crate) async fn refresh_peer_rpc_exports(&self) -> Result<bool, String> {
        let (session_id, remote_bootstrap) = {
            let guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            let Some(session) = guard.peer_rpc_session.as_ref() else {
                return Ok(false);
            };
            (session.session_id, session.remote_bootstrap.clone())
        };

        let capability_exports =
            crate::list_remote_capability_exports(remote_bootstrap.clone()).await?;

        {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            let is_current = guard
                .peer_rpc_session
                .as_ref()
                .map(|session| session.session_id == session_id)
                .unwrap_or(false);
            if !is_current {
                return Ok(false);
            }
            if let Some(session) = guard.peer_rpc_session.as_mut() {
                session.capability_exports = capability_exports;
            }
        }

        Ok(true)
    }

    pub(crate) fn disconnect_peer_rpc_session(&self) -> Result<(), String> {
        let mut guard = self
            .state
            .lock()
            .map_err(|_| "app state lock poisoned".to_string())?;
        self.close_peer_rpc_session_locked(&mut guard, b"peer rpc disconnected");
        guard.pending_outgoing_peer_node_id = None;
        if let Some(pending) = guard.pending_incoming_connection.take() {
            pending
                .connection
                .close(0u32.into(), b"peer rpc disconnected");
        }
        guard.pending_incoming_peer_node_id = None;
        guard.peer_rpc_error = None;
        guard.pairing_status = if guard.tunnel_enabled {
            crate::PairingStatus::Disconnected
        } else {
            crate::PairingStatus::Disabled
        };
        Ok(())
    }

    pub(crate) async fn import_remote_capability_export(
        &self,
        export_id: &str,
    ) -> Result<(String, String, crate::ImportedRemoteCapabilityKind), String> {
        let (remote_bootstrap, remote_node_id) = {
            let guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            let session = guard
                .peer_rpc_session
                .as_ref()
                .ok_or_else(|| "peer rpc session is not connected".to_string())?;
            (
                session.remote_bootstrap.clone(),
                session.remote_node_id.clone(),
            )
        };
        let (fetched_label, fetched_kind, fetched_type_tag, fetched_descriptor_json, _client) =
            crate::fetch_remote_capability_export(remote_bootstrap, export_id).await?;
        let (label, object_id) = self.create_local_proxy_record(
            remote_node_id,
            crate::LocalProxyTargetKindRuntime::ExportId,
            export_id.to_string(),
            fetched_label,
            fetched_kind,
            if fetched_type_tag.is_empty() {
                crate::imported_type_tag_for_kind(fetched_kind)
            } else {
                fetched_type_tag
            },
            fetched_descriptor_json,
        )?;
        Ok((label, object_id, fetched_kind))
    }

    pub(crate) fn render_state_value(&self) -> Result<serde_json::Value, String> {
        let guard = self
            .state
            .lock()
            .map_err(|_| "app state lock poisoned".to_string())?;
        let pairing_status = if guard.peer_rpc_session.is_some() {
            crate::PairingStatus::Connected
        } else if !guard.tunnel_enabled {
            crate::PairingStatus::Disabled
        } else if guard.pending_incoming_peer_node_id.is_some() {
            crate::PairingStatus::IncomingRequest
        } else if guard.pending_outgoing_peer_node_id.is_some() {
            crate::PairingStatus::AwaitingRemoteAccept
        } else if guard.peer_rpc_error.is_some() {
            crate::PairingStatus::Error
        } else {
            guard.pairing_status
        };
        let saved_caps = crate::load_saved_capabilities()?;
        let saved_cap_rows = saved_caps
            .iter()
            .map(|row| SavedCapabilityJson {
                id: row.id.clone(),
                object_id: row.id.clone(),
                label: row.label.clone(),
                saved_token: row.saved_token.clone(),
                created_at_ms: row.created_at_ms,
                descriptor_json: row.descriptor_json.clone(),
            })
            .collect::<Vec<_>>();
        let mut advertised_match_descriptors = saved_caps
            .into_iter()
            .filter_map(|row| {
                row.descriptor_json
                    .as_deref()
                    .and_then(crate::descriptor_json_to_match_request_b64)
            })
            .collect::<Vec<_>>();
        if let Some(session) = guard.peer_rpc_session.as_ref() {
            advertised_match_descriptors.extend(session.capability_exports.iter().filter_map(
                |cap| {
                    cap.descriptor_json
                        .as_deref()
                        .and_then(crate::descriptor_json_to_match_request_b64)
                },
            ));
        }
        let mut seen_advertised_match_descriptors = HashSet::new();
        advertised_match_descriptors
            .retain(|descriptor| seen_advertised_match_descriptors.insert(descriptor.clone()));
        let advertised_powerbox_matches: serde_json::Value = serde_json::from_str(
            &crate::advertised_powerbox_matches_json_from_b64(&advertised_match_descriptors)?,
        )
        .map_err(|err| format!("failed to encode advertised powerbox matches: {err}"))?;

        let raw_udp_interface = guard
            .raw_udp_interface
            .as_ref()
            .map(|value| RawUdpInterfaceJson {
                label: value.label.clone(),
                saved_token: value.saved_token.clone(),
                source: guard
                    .raw_udp_interface_source
                    .as_deref()
                    .unwrap_or("unknown")
                    .to_string(),
            });
        let peer_rpc = match &guard.peer_rpc_session {
            Some(session) => PeerRpcJson {
                connected: true,
                session_id: Some(session.session_id),
                remote_node_id: Some(session.remote_node_id.clone()),
                capability_exports: session
                    .capability_exports
                    .iter()
                    .map(|export| PeerRpcCapabilityExportJson {
                        id: export.id.clone(),
                        label: export.label.clone(),
                        kind: export.kind.as_str().to_string(),
                        type_tag: export.type_tag.clone(),
                        descriptor_json: export.descriptor_json.clone(),
                    })
                    .collect(),
            },
            None => PeerRpcJson {
                connected: false,
                session_id: None,
                remote_node_id: None,
                capability_exports: Vec::new(),
            },
        };
        let local_proxy_caps = guard
            .local_proxy_caps
            .iter()
            .map(|cap| LocalProxyCapabilityJson {
                object_id: cap.object_id.clone(),
                peer_node_id: cap.peer_node_id.clone(),
                target_kind: match cap.target_kind {
                    crate::LocalProxyTargetKindRuntime::ExportId => "exportId",
                    crate::LocalProxyTargetKindRuntime::RemoteObjectId => "remoteObjectId",
                }
                .to_string(),
                target_id: cap.target_id.clone(),
                label: cap.label.clone(),
                kind: cap.kind.as_str().to_string(),
                type_tag: cap.type_tag.clone(),
                descriptor_json: cap.descriptor_json.clone(),
                enabled: cap.enabled,
                created_at_ms: cap.created_at_ms,
            })
            .collect::<Vec<_>>();
        let shared_caps = guard
            .shared_caps
            .iter()
            .map(|cap| SharedCapabilityJson {
                id: cap.id.clone(),
                saved_cap_id: cap.saved_cap.id.clone(),
                label: cap.label.clone(),
                kind: cap.kind.as_str().to_string(),
                type_tag: crate::shared_capability_type_tag(cap),
                enabled: cap.enabled,
                created_at_ms: cap.created_at_ms,
                saved_token: cap.saved_cap.saved_token.clone(),
                descriptor_json: cap.saved_cap.descriptor_json.clone(),
            })
            .collect::<Vec<_>>();

        serde_json::to_value(StateJson {
            powerbox_queries: PowerboxQueriesJson {
                api_session: crate::powerbox_query_for_interface(
                    crate::api_session_capnp::api_session::Client::TYPE_ID,
                )?,
                ip_network: crate::powerbox_query_for_interface(
                    crate::ip_capnp::ip_network::Client::TYPE_ID,
                )?,
                ip_interface: crate::powerbox_query_for_interface(
                    crate::ip_capnp::ip_interface::Client::TYPE_ID,
                )?,
            },
            powerbox_advertised_matches: advertised_powerbox_matches,
            iroh_node_id: guard.iroh_identity.node_id.clone(),
            iroh_endpoint: IrohEndpointJson {
                bound: guard.iroh_endpoint.is_some(),
                node_id: guard.iroh_endpoint_addr.node_id.clone(),
                relay_urls: guard.iroh_endpoint_addr.relay_urls.clone(),
                direct_addrs: guard.iroh_endpoint_addr.direct_addrs.clone(),
                custom_addrs: guard.iroh_endpoint_addr.custom_addrs.clone(),
                error: guard.iroh_endpoint_error.clone(),
                local_ticket: crate::format_local_ticket(&guard.iroh_endpoint_addr),
                raw_udp_interface,
            },
            pairing: PairingJson {
                status: pairing_status.as_str().to_string(),
                approved_peer_node_id: guard.approved_peer_node_id.clone(),
                pending_incoming_peer_node_id: guard.pending_incoming_peer_node_id.clone(),
                pending_outgoing_peer_node_id: guard.pending_outgoing_peer_node_id.clone(),
                tunnel_enabled: guard.tunnel_enabled,
            },
            peer_rpc,
            peer_rpc_error: guard.peer_rpc_error.clone(),
            local_proxy_caps,
            remote_ticket: guard.remote_ticket.clone(),
            saved_caps: saved_cap_rows,
            shared_caps,
        })
        .map_err(|err| format!("failed to encode state json: {err}"))
    }

    #[cfg(test)]
    pub(crate) fn render_state_json(&self) -> Result<String, String> {
        let state = self.render_state_value()?;
        serde_json::to_string(&state)
            .map_err(|err| format!("failed to serialize state json: {err}"))
    }

    fn persist_shared_capability_registry(
        &self,
        shared_caps: &[crate::SharedCapability],
    ) -> Result<(), String> {
        let records = shared_caps
            .iter()
            .map(|cap| SharedCapabilityRecord {
                id: cap.id.clone(),
                saved_cap_id: cap.saved_cap.id.clone(),
                label: cap.label.clone(),
                kind: cap.kind,
                type_tag: Some(crate::shared_capability_type_tag(cap)),
                enabled: cap.enabled,
                created_at_ms: cap.created_at_ms,
                descriptor_json: cap.saved_cap.descriptor_json.clone(),
            })
            .collect::<Vec<_>>();
        self.storage.persist_shared_capabilities(&records)
    }

    fn persist_local_proxy_capability_registry(
        &self,
        local_proxy_caps: &[crate::LocalProxyCapability],
    ) -> Result<(), String> {
        let records = local_proxy_caps
            .iter()
            .map(|cap| LocalProxyCapabilityRecord {
                object_id: cap.object_id.clone(),
                peer_node_id: cap.peer_node_id.clone(),
                target_kind: match cap.target_kind {
                    crate::LocalProxyTargetKindRuntime::ExportId => LocalProxyTargetKind::ExportId,
                    crate::LocalProxyTargetKindRuntime::RemoteObjectId => {
                        LocalProxyTargetKind::RemoteObjectId
                    }
                },
                target_id: cap.target_id.clone(),
                label: cap.label.clone(),
                kind: cap.kind,
                type_tag: Some(cap.type_tag.clone()),
                descriptor_json: cap.descriptor_json.clone(),
                enabled: cap.enabled,
                created_at_ms: cap.created_at_ms,
            })
            .collect::<Vec<_>>();
        self.storage
            .persist_local_proxy_capability_registry(&records)
    }

    fn close_peer_rpc_session_locked(&self, guard: &mut crate::AppState, reason: &'static [u8]) {
        if let Some(session) = guard.peer_rpc_session.take() {
            session.connection.close(0u32.into(), reason);
        }
        guard.pairing_status = if guard.tunnel_enabled {
            crate::PairingStatus::Disconnected
        } else {
            crate::PairingStatus::Disabled
        };
    }
}

#[derive(Clone)]
struct LocalProxyCapabilityServer {
    app: App,
    object_id: String,
    identity: std::sync::Arc<()>,
}

impl LocalProxyCapabilityServer {
    fn new(app: App, object_id: String) -> Self {
        Self {
            app,
            object_id,
            identity: std::sync::Arc::new(()),
        }
    }
}

impl CapServer for LocalProxyCapabilityServer {
    fn dispatch_call(
        self,
        interface_id: u64,
        method_id: u16,
        params: capnp::capability::Params<capnp::any_pointer::Owned>,
        mut results: capnp::capability::Results<capnp::any_pointer::Owned>,
    ) -> DispatchCallResult {
        let app = self.app.clone();
        let object_id = self.object_id.clone();
        DispatchCallResult::new(
            CapPromise::from_future(async move {
                let remote_client = app
                    .resolve_local_proxy_target_remote_capability(&object_id)
                    .await
                    .map_err(capnp::Error::failed)?;
                let mut request = remote_client
                    .new_call::<capnp::any_pointer::Owned, capnp::any_pointer::Owned>(
                        interface_id,
                        method_id,
                        None,
                    );
                request.get().set_as(params.get()?)?;
                let response = request.send().promise.await?;
                results.get().set_as(response.get()?)?;
                Ok(())
            }),
            false,
        )
    }

    fn as_ptr(&self) -> usize {
        std::sync::Arc::as_ptr(&self.identity) as usize
    }
}

#[derive(Clone)]
struct HostedLocalProxyCapabilityServer {
    app: App,
    object_id: String,
    backend_client: capnp::capability::Client,
    identity: std::sync::Arc<()>,
}

impl HostedLocalProxyCapabilityServer {
    fn new(app: App, object_id: String, backend_client: capnp::capability::Client) -> Self {
        Self {
            app,
            object_id,
            backend_client,
            identity: std::sync::Arc::new(()),
        }
    }
}

impl CapServer for HostedLocalProxyCapabilityServer {
    fn dispatch_call(
        self,
        interface_id: u64,
        method_id: u16,
        params: capnp::capability::Params<capnp::any_pointer::Owned>,
        mut results: capnp::capability::Results<capnp::any_pointer::Owned>,
    ) -> DispatchCallResult {
        if interface_id == crate::grain_capnp::app_persistent::Client::<capnp::text::Owned>::TYPE_ID
        {
            let object_id = self.object_id.clone();
            let app = self.app.clone();
            return DispatchCallResult::new(
                CapPromise::from_future(async move {
                    match method_id {
                        0 => {
                            let label = app
                                .state
                                .lock()
                                .map_err(|_| {
                                    capnp::Error::failed("app state lock poisoned".to_string())
                                })?
                                .local_proxy_caps
                                .iter()
                                .find(|record| record.object_id == object_id)
                                .map(|record| record.label.clone())
                                .ok_or_else(|| {
                                    capnp::Error::failed("unknown local proxy object".to_string())
                                })?;
                            let mut save_results = crate::grain_capnp::app_persistent::SaveResults::<
                                capnp::text::Owned,
                            >::new(results.hook);
                            let mut builder = save_results.get();
                            builder.set_object_id(&object_id).map_err(|err| {
                                capnp::Error::failed(format!(
                                    "failed to set proxy object id: {err}"
                                ))
                            })?;
                            let mut localized = builder.init_label();
                            localized.set_default_text(&label);
                            localized.init_localizations(0);
                            Ok(())
                        }
                        _ => Err(capnp::Error::unimplemented(
                            "unknown app persistent method".to_string(),
                        )),
                    }
                }),
                false,
            );
        }

        let backend_client = self.backend_client.clone();
        let object_id = self.object_id.clone();
        DispatchCallResult::new(
            CapPromise::from_future(async move {
                let mut request = backend_client
                    .new_call::<capnp::any_pointer::Owned, capnp::any_pointer::Owned>(
                        interface_id,
                        method_id,
                        None,
                    );
                request.get().set_as(params.get()?)?;
                let response = request.send().promise.await.map_err(|err| {
                    eprintln!(
                        "hosted_local_proxy.forward: object_id={object_id} interface_id=0x{interface_id:016x} method_id={} failed at_ms={} err={err}",
                        method_id,
                        crate::now_ms()
                    );
                    err
                })?;
                results.get().set_as(response.get()?)?;
                Ok(())
            }),
            false,
        )
    }

    fn as_ptr(&self) -> usize {
        std::sync::Arc::as_ptr(&self.identity) as usize
    }
}

#[derive(Clone)]
struct HostedStaticCapabilityServer {
    object_id: String,
    label: String,
    backend_client: capnp::capability::Client,
    identity: std::sync::Arc<()>,
}

impl HostedStaticCapabilityServer {
    fn new(object_id: String, label: String, backend_client: capnp::capability::Client) -> Self {
        Self {
            object_id,
            label,
            backend_client,
            identity: std::sync::Arc::new(()),
        }
    }
}

impl CapServer for HostedStaticCapabilityServer {
    fn dispatch_call(
        self,
        interface_id: u64,
        method_id: u16,
        params: capnp::capability::Params<capnp::any_pointer::Owned>,
        mut results: capnp::capability::Results<capnp::any_pointer::Owned>,
    ) -> DispatchCallResult {
        if interface_id == crate::grain_capnp::app_persistent::Client::<capnp::text::Owned>::TYPE_ID
        {
            let object_id = self.object_id.clone();
            let label = self.label.clone();
            return DispatchCallResult::new(
                CapPromise::from_future(async move {
                    match method_id {
                        0 => {
                            let mut save_results = crate::grain_capnp::app_persistent::SaveResults::<
                                capnp::text::Owned,
                            >::new(results.hook);
                            let mut builder = save_results.get();
                            builder.set_object_id(&object_id).map_err(|err| {
                                capnp::Error::failed(format!(
                                    "failed to set static object id: {err}"
                                ))
                            })?;
                            let mut localized = builder.init_label();
                            localized.set_default_text(&label);
                            localized.init_localizations(0);
                            Ok(())
                        }
                        _ => Err(capnp::Error::unimplemented(
                            "unknown app persistent method".to_string(),
                        )),
                    }
                }),
                false,
            );
        }

        let backend_client = self.backend_client.clone();
        let object_id = self.object_id.clone();
        DispatchCallResult::new(
            CapPromise::from_future(async move {
                eprintln!(
                    "hosted_static.dispatch: object_id={} interface_id=0x{interface_id:016x} method_id={} at_ms={}",
                    object_id,
                    method_id,
                    crate::now_ms()
                );
                let mut request = backend_client
                    .new_call::<capnp::any_pointer::Owned, capnp::any_pointer::Owned>(
                        interface_id,
                        method_id,
                        None,
                    );
                request.get().set_as(params.get()?)?;
                let response = request.send().promise.await.map_err(|err| {
                    eprintln!(
                        "hosted_static.dispatch: object_id={} interface_id=0x{interface_id:016x} method_id={} failed at_ms={} err={err}",
                        object_id,
                        method_id,
                        crate::now_ms()
                    );
                    err
                })?;
                results.get().set_as(response.get()?)?;
                eprintln!(
                    "hosted_static.dispatch: object_id={} interface_id=0x{interface_id:016x} method_id={} ok at_ms={}",
                    object_id,
                    method_id,
                    crate::now_ms()
                );
                Ok(())
            }),
            false,
        )
    }

    fn as_ptr(&self) -> usize {
        std::sync::Arc::as_ptr(&self.identity) as usize
    }
}

#[derive(Clone)]
struct LocalTestApiSessionBackend;

impl crate::grain_capnp::ui_session::Server for LocalTestApiSessionBackend {}
impl crate::api_session_capnp::api_session::Server for LocalTestApiSessionBackend {}

impl crate::web_session_capnp::web_session::Server for LocalTestApiSessionBackend {
    fn post(
        self: Rc<Self>,
        params: crate::web_session_capnp::web_session::PostParams,
        mut results: crate::web_session_capnp::web_session::PostResults,
    ) -> CapPromise<(), capnp::Error> {
        let params = pry!(params.get());
        let path = pry!(params.get_path()).to_str().unwrap_or("").to_string();
        if path != "preview" {
            return CapPromise::err(capnp::Error::failed(format!(
                "unexpected preview path: {path}"
            )));
        }

        let mut content = results.get().init_content();
        content.set_status_code(crate::web_session_capnp::web_session::response::SuccessCode::Ok);
        content.set_mime_type("application/pdf");
        content.init_body().set_bytes(b"%PDF-LOCAL-TEST\n");
        CapPromise::ok(())
    }
}
