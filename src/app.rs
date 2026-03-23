use std::collections::HashSet;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use capnp::capability::{
    DispatchCallResult, FromServer, Promise as CapPromise, Server as CapServer,
};
use capnp::traits::HasTypeId;
use capnp_rpc::{new_client, rpc_twoparty_capnp, twoparty, RpcSystem};
use capnp_rpc::pry;
use tokio::time::{sleep, Duration};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::backend::SandstormBackend;
use crate::storage::{
    LocalProxyCapabilityRecord, LocalProxyTargetKind, PersistedReceivedCapabilityRecord,
    ReceivedCapabilityKind, SharedCapabilityKind, SharedCapabilityRecord, Storage,
};

#[derive(Clone)]
pub(crate) struct App {
    state: Arc<Mutex<crate::AppState>>,
    storage: Storage,
}

pub(crate) const LOCAL_TEST_API_SESSION_OBJECT_ID: &str = "local-test-api-session";
pub(crate) const LOCAL_TEST_API_SESSION_LABEL: &str = "Local Test ApiSession";

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
        (*self.server).clone().dispatch_call(interface_id, method_id, params, results)
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
            let fut: std::pin::Pin<
                Box<
                    dyn std::future::Future<
                            Output = capnp::Result<
                                Vec<Option<Box<dyn capnp::private::capability::ClientHook>>>,
                            >,
                        > + 'static,
                >,
            > = Box::pin(async move {
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
            });
            fut
        })
    }

    fn local_proxy_request_transform(
        &self,
    ) -> crate::untyped_local::RequestCapTableTransform {
        let app = self.clone();
        std::rc::Rc::new(move |cap_table| {
            let app = app.clone();
            let fut: std::pin::Pin<
                Box<
                    dyn std::future::Future<
                            Output = capnp::Result<
                                Vec<Option<Box<dyn capnp::private::capability::ClientHook>>>,
                            >,
                        > + 'static,
                >,
            > = Box::pin(async move {
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
            });
            fut
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
            exported_ip_network: None,
            exported_api_session: None,
            exported_caps_live: std::collections::HashMap::new(),
            exported_ip_network_live: std::collections::HashMap::new(),
            exported_api_session_live: std::collections::HashMap::new(),
            peer_rpc_session: None,
            imported_remote_ip_network: None,
            imported_remote_api_session: None,
            imported_remote_caps: std::collections::HashMap::new(),
            persisted_received_caps: Vec::new(),
            local_proxy_caps: Vec::new(),
            registered_remote_caps: std::collections::HashMap::new(),
            registered_remote_hook_object_ids: std::collections::HashMap::new(),
            local_proxy_hook_object_ids: std::collections::HashMap::new(),
            next_peer_rpc_session_id: 0,
            next_imported_remote_cap_id: 0,
            next_local_proxy_cap_id: 0,
            next_registered_remote_cap_id: 0,
            peer_rpc_error: None,
            active_tcp_sessions: std::collections::HashMap::new(),
            next_tcp_session_id: 0,
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
        let tunnel_enabled = match storage.load_text_file(storage.tunnel_enabled_path().as_path())? {
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
        if local_proxy_records
            .iter()
            .any(|record| matches!(record.target_kind, crate::LocalProxyTargetKind::RemoteObjectId))
        {
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
                type_tag: record.type_tag.unwrap_or_else(|| "capnp/unknown".to_string()),
                descriptor_json: record.descriptor_json,
                created_at_ms: record.created_at_ms,
            })
            .collect::<Vec<_>>();
        let persisted_received_caps = storage
            .load_persisted_received_capabilities()?
            .into_iter()
            .map(|record| crate::PersistedReceivedCapability {
                object_id: record.object_id,
                export_id: record.export_id,
                label: record.label,
                kind: match record.kind {
                    crate::ReceivedCapabilityKind::IpNetwork => {
                        crate::ImportedRemoteCapabilityKind::IpNetwork
                    }
                    crate::ReceivedCapabilityKind::ApiSession => {
                        crate::ImportedRemoteCapabilityKind::ApiSession
                    }
                    crate::ReceivedCapabilityKind::Other => {
                        crate::ImportedRemoteCapabilityKind::Other
                    }
                },
                type_tag: record
                    .type_tag
                    .unwrap_or_else(|| "capnp/unknown".to_string()),
                descriptor_json: record.descriptor_json,
                enabled: record.enabled,
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
        let next_imported_remote_cap_id = persisted_received_caps
            .iter()
            .filter_map(|record| {
                record
                    .object_id
                    .strip_prefix("remote-cap-")
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
            exported_ip_network: None,
            exported_api_session: None,
            exported_caps_live: std::collections::HashMap::new(),
            exported_ip_network_live: std::collections::HashMap::new(),
            exported_api_session_live: std::collections::HashMap::new(),
            peer_rpc_session: None,
            imported_remote_ip_network: None,
            imported_remote_api_session: None,
            imported_remote_caps: std::collections::HashMap::new(),
            persisted_received_caps,
            local_proxy_caps,
            registered_remote_caps,
            registered_remote_hook_object_ids: std::collections::HashMap::new(),
            local_proxy_hook_object_ids: std::collections::HashMap::new(),
            next_peer_rpc_session_id: 0,
            next_imported_remote_cap_id,
            next_local_proxy_cap_id,
            next_registered_remote_cap_id,
            peer_rpc_error: None,
            active_tcp_sessions: std::collections::HashMap::new(),
            next_tcp_session_id: 0,
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
            guard.exported_api_session_live.clear();
        }
        guard.shared_caps.push(crate::SharedCapability {
            id: crate::make_shared_cap_id(),
            label: saved_cap.label.clone(),
            kind: SharedCapabilityKind::ApiSession,
            enabled: true,
            created_at_ms: saved_cap.created_at_ms,
            saved_cap: saved_cap.clone(),
        });
        guard.exported_api_session = Some(saved_cap.clone());
        guard
            .exported_caps_live
            .insert(saved_cap.id.clone(), client.client.clone());
        guard.exported_api_session_live.insert(
            saved_cap.id.clone(),
            crate::ExportedApiSessionState { client },
        );
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
        guard.exported_ip_network_live.clear();
        guard.shared_caps.push(crate::SharedCapability {
            id: crate::make_shared_cap_id(),
            label: saved_cap.label.clone(),
            kind: SharedCapabilityKind::IpNetwork,
            enabled: true,
            created_at_ms: saved_cap.created_at_ms,
            saved_cap: saved_cap.clone(),
        });
        guard.exported_ip_network = Some(saved_cap.clone());
        guard
            .exported_caps_live
            .insert(saved_cap.id.clone(), client.client.clone());
        guard.exported_ip_network_live.insert(
            saved_cap.id.clone(),
            crate::ExportedIpNetworkState { client },
        );
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
        guard.exported_api_session = None;
        let disabled_ids = guard
            .shared_caps
            .iter()
            .filter(|cap| cap.kind == SharedCapabilityKind::ApiSession)
            .map(|cap| cap.saved_cap.id.clone())
            .collect::<Vec<_>>();
        for disabled_id in disabled_ids {
            guard.exported_caps_live.remove(&disabled_id);
        }
        guard.exported_api_session_live.clear();
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
        let restored_cap = self.restore_object_capability(sandstorm_api, object_id).await?;
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
        let restored_cap = self.restore_object_capability(sandstorm_api, object_id).await?;
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
        let imported_remote_cap = {
            let guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            guard.imported_remote_caps.get(object_id).cloned()
        };
        if let Some(imported_remote_cap) = imported_remote_cap {
            return Ok(imported_remote_cap.client);
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
            return self.build_local_proxy_client(record.object_id);
        }
        {
            let guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            let persisted_match = guard
                .persisted_received_caps
                .iter()
                .find(|record| record.object_id == object_id)
                .cloned();
            if let Some(record) = persisted_match {
                return Err(format!(
                    "received {} object {} is known but not currently connected; reconnect the peer RPC session to restore it",
                    crate::imported_kind_label(record.kind),
                    record.object_id
                ));
            }
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
        guard.local_proxy_hook_object_ids.insert(hook_ptr, object_id);
        Ok(client)
    }

    fn build_local_test_api_session_client(&self) -> Result<capnp::capability::Client, String> {
        let backend_client =
            new_client::<crate::api_session_capnp::api_session::Client, _>(LocalTestApiSessionBackend)
                .client;
        Ok(new_client::<TypelessHostedClient, _>(HostedStaticCapabilityServer::new(
            LOCAL_TEST_API_SESSION_OBJECT_ID.to_string(),
            LOCAL_TEST_API_SESSION_LABEL.to_string(),
            backend_client,
        ))
        .0)
    }

    fn build_ephemeral_hosted_capability_client(
        &self,
        label: &str,
        backend_client: capnp::capability::Client,
    ) -> Result<capnp::capability::Client, String> {
        let object_id = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            guard.next_local_proxy_cap_id += 1;
            format!("ephemeral-hosted-cap-{}", guard.next_local_proxy_cap_id)
        };
        let transformed_backend = crate::untyped_local::new_client_with_transforms(
            ForwardingCapabilityServer::new(backend_client),
            Some(self.local_proxy_request_transform()),
            None,
        );
        Ok(new_client::<TypelessHostedClient, _>(HostedStaticCapabilityServer::new(
            object_id,
            label.to_string(),
            transformed_backend,
        ))
        .0)
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

    pub(crate) async fn create_local_proxy_for_received_object(
        &self,
        object_id: &str,
    ) -> Result<(String, String), String> {
        let (remote_node_id, export_id, label, kind, type_tag, descriptor_json) = {
            let guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            let session = guard
                .peer_rpc_session
                .as_ref()
                .ok_or_else(|| "peer rpc session is not connected".to_string())?;

            if let Some(cap) = guard.imported_remote_caps.get(object_id) {
                (
                    session.remote_node_id.clone(),
                    cap.export_id.clone(),
                    cap.label.clone(),
                    cap.kind,
                    cap.type_tag.clone(),
                    cap.descriptor_json.clone(),
                )
            } else if let Some(record) = guard
                .persisted_received_caps
                .iter()
                .find(|record| record.object_id == object_id)
            {
                (
                    session.remote_node_id.clone(),
                    record.export_id.clone(),
                    record.label.clone(),
                    record.kind,
                    record.type_tag.clone(),
                    record.descriptor_json.clone(),
                )
            } else {
                return Err(format!("object {} is not a received remote capability", object_id));
            }
        };

        self.create_local_proxy_record(
            remote_node_id,
            crate::LocalProxyTargetKindRuntime::ExportId,
            export_id,
            label,
            kind,
            type_tag,
            descriptor_json,
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
            (session.remote_bootstrap.clone(), session.remote_node_id.clone())
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
                .ok_or_else(|| {
                    format!("unknown local proxy object id: {local_proxy_object_id}")
                })?;
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
            (session.remote_bootstrap.clone(), session.remote_node_id.clone())
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
            Some(object_id) => self.resolve_local_proxy_target_remote_capability(&object_id).await,
            None => {
                let remote_bootstrap = {
                    let guard = self
                        .state
                        .lock()
                        .map_err(|_| "app state lock poisoned".to_string())?;
                    guard
                        .peer_rpc_session
                        .as_ref()
                        .ok_or_else(|| "peer rpc session is not connected".to_string())?
                        .remote_bootstrap
                        .clone()
                };
                let hosted_client =
                    self.build_ephemeral_hosted_capability_client("Forwarded capability", client)?;
                let remote_object_id = crate::register_local_ephemeral_capability(
                    &self.state,
                    hosted_client,
                    "Forwarded capability",
                    crate::ImportedRemoteCapabilityKind::Other,
                    "capnp/unknown",
                    None,
                )?;
                let (_label, _kind, _type_tag, _descriptor_json, localized_client) =
                    crate::fetch_remote_local_proxy_for_peer_registered_capability(
                        remote_bootstrap,
                        &remote_object_id,
                    )
                    .await?;
                Ok(localized_client)
            }
        }
    }

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
                .find(|record| record.peer_node_id == remote_node_id
                    && record.target_kind == target_kind
                    && record.target_id == target_id)
                .cloned()
            {
                return Ok((existing.label, existing.object_id));
            }

            guard.next_local_proxy_cap_id += 1;
            let object_id = format!("local-proxy-cap-{}", guard.next_local_proxy_cap_id);
            let now = crate::now_ms();
            let kind = match fetched_kind {
                crate::ImportedRemoteCapabilityKind::IpNetwork => crate::SharedCapabilityKind::IpNetwork,
                crate::ImportedRemoteCapabilityKind::ApiSession => crate::SharedCapabilityKind::ApiSession,
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
                created_at_ms: now,
            };
            guard.local_proxy_caps.push(record.clone());
            (record, guard.local_proxy_caps.clone())
        };

        self.persist_local_proxy_capability_registry(&persisted_records)?;
        Ok((record.label, record.object_id))
    }

    pub(crate) fn drop_received_remote_capability(
        &self,
        object_id: &str,
    ) -> Result<bool, String> {
        let (removed, persisted_caps) = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            let before = guard.persisted_received_caps.len();
            guard
                .persisted_received_caps
                .retain(|record| record.object_id != object_id);
            let removed = guard.persisted_received_caps.len() != before;
            guard.imported_remote_caps.remove(object_id);
            if guard
                .imported_remote_ip_network
                .as_ref()
                .map(|value| value.object_id == object_id)
                .unwrap_or(false)
            {
                guard.imported_remote_ip_network = None;
            }
            if guard
                .imported_remote_api_session
                .as_ref()
                .map(|value| value.object_id == object_id)
                .unwrap_or(false)
            {
                guard.imported_remote_api_session = None;
            }
            (removed, guard.persisted_received_caps.clone())
        };

        if removed {
            self.persist_received_capability_registry(&persisted_caps)?;
        }

        Ok(removed)
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
            if let Some(cap) = guard.imported_remote_caps.get(object_id) {
                (cap.label.clone(), cap.descriptor_json.clone())
            } else if let Some(record) = guard
                .persisted_received_caps
                .iter()
                .find(|record| record.object_id == object_id)
            {
                (record.label.clone(), record.descriptor_json.clone())
            } else {
                return Err(format!("unknown received capability object id: {object_id}"));
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
        let (changed, persisted_caps) = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            let mut changed = false;
            for record in &mut guard.persisted_received_caps {
                if record.object_id == object_id {
                    record.enabled = false;
                    changed = true;
                }
            }
            guard.imported_remote_caps.remove(object_id);
            if guard
                .imported_remote_ip_network
                .as_ref()
                .map(|value| value.object_id == object_id)
                .unwrap_or(false)
            {
                guard.imported_remote_ip_network = None;
            }
            if guard
                .imported_remote_api_session
                .as_ref()
                .map(|value| value.object_id == object_id)
                .unwrap_or(false)
            {
                guard.imported_remote_api_session = None;
            }
            (changed, guard.persisted_received_caps.clone())
        };
        if changed {
            self.persist_received_capability_registry(&persisted_caps)?;
        }
        Ok(changed)
    }

    pub(crate) async fn enable_received_remote_capability(
        &self,
        object_id: &str,
    ) -> Result<bool, String> {
        let record = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            let Some(record) = guard
                .persisted_received_caps
                .iter_mut()
                .find(|record| record.object_id == object_id)
            else {
                return Ok(false);
            };
            record.enabled = true;
            record.clone()
        };
        {
            let guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            self.persist_received_capability_registry(&guard.persisted_received_caps)?;
        }

        let maybe_remote_bootstrap = {
            let guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            guard
                .peer_rpc_session
                .as_ref()
                .map(|session| session.remote_bootstrap.clone())
        };
        let Some(remote_bootstrap) = maybe_remote_bootstrap else {
            return Ok(true);
        };

        let (label, kind, type_tag, descriptor_json, client) =
            crate::fetch_remote_capability_export(remote_bootstrap, &record.export_id).await?;
        self.activate_imported_remote_capability(
            record.object_id,
            record.export_id,
            label,
            kind,
            if type_tag.is_empty() {
                crate::imported_type_tag_for_kind(kind)
            } else {
                type_tag
            },
            descriptor_json.or_else(|| record.descriptor_json.clone()),
            client,
            true,
        )?;

        Ok(true)
    }

    pub(crate) async fn configure_exported_ip_network(
        &self,
        sandstorm_api: crate::grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
        saved_cap_id: &str,
    ) -> Result<(), String> {
        if let Some(saved_cap_id) = saved_cap_id.strip_prefix('!') {
            let path = crate::exported_ip_network_id_path();
            let (shared_caps, selected_export) = {
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
                guard.exported_ip_network_live.remove(saved_cap_id);
                guard.exported_ip_network = crate::first_enabled_shared_capability(
                    &guard.shared_caps,
                    SharedCapabilityKind::IpNetwork,
                );
                (guard.shared_caps.clone(), guard.exported_ip_network.clone())
            };
            self.persist_shared_capability_registry(&shared_caps)?;
            match selected_export {
                Some(saved_cap) => {
                    crate::persist_configured_exported_capability(path.as_path(), &saved_cap.id)?
                }
                None => crate::clear_configured_exported_capability(path.as_path())?,
            }
            return Ok(());
        }
        if saved_cap_id.trim().is_empty() {
            let path = crate::exported_ip_network_id_path();
            crate::clear_configured_exported_capability(path.as_path())?;
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
                guard.exported_ip_network = None;
                guard.exported_ip_network_live.clear();
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
        let path = crate::exported_ip_network_id_path();
        crate::persist_configured_exported_capability(path.as_path(), &saved_cap.id)?;

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
            guard.exported_ip_network = crate::first_enabled_shared_capability(
                &guard.shared_caps,
                SharedCapabilityKind::IpNetwork,
            );
            guard
                .exported_caps_live
                .insert(saved_cap.id.clone(), client.client.clone());
            guard.exported_ip_network_live.insert(
                saved_cap.id.clone(),
                crate::ExportedIpNetworkState { client },
            );
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
            let path = crate::exported_api_session_id_path();
            let (shared_caps, selected_export) = {
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
                guard.exported_api_session_live.remove(saved_cap_id);
                guard.exported_api_session = crate::first_enabled_shared_capability(
                    &guard.shared_caps,
                    SharedCapabilityKind::ApiSession,
                );
                (guard.shared_caps.clone(), guard.exported_api_session.clone())
            };
            self.persist_shared_capability_registry(&shared_caps)?;
            match selected_export {
                Some(saved_cap) => {
                    crate::persist_configured_exported_capability(path.as_path(), &saved_cap.id)?
                }
                None => crate::clear_configured_exported_capability(path.as_path())?,
            }
            return Ok(());
        }
        if saved_cap_id.trim().is_empty() {
            let path = crate::exported_api_session_id_path();
            crate::clear_configured_exported_capability(path.as_path())?;
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
                guard.exported_api_session = None;
                guard.exported_api_session_live.clear();
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
        let path = crate::exported_api_session_id_path();
        crate::persist_configured_exported_capability(path.as_path(), &saved_cap.id)?;

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
            guard.exported_api_session = crate::first_enabled_shared_capability(
                &guard.shared_caps,
                SharedCapabilityKind::ApiSession,
            );
            guard
                .exported_caps_live
                .insert(saved_cap.id.clone(), client.client.clone());
            guard.exported_api_session_live.insert(
                saved_cap.id.clone(),
                crate::ExportedApiSessionState { client },
            );
            guard.shared_caps.clone()
        };
        self.persist_shared_capability_registry(&shared_caps)?;
        Ok(())
    }

    pub(crate) async fn connect_peer_rpc_session(
        &self,
        sandstorm_api: crate::grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    ) -> Result<(Vec<crate::PeerRpcExport>, Vec<crate::PeerRpcExport>), String> {
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
            let old_connection = guard.peer_rpc_session.take().map(|session| session.connection);
            guard.imported_remote_ip_network = None;
            guard.imported_remote_api_session = None;
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
            let old_connection = guard.peer_rpc_session.take().map(|session| session.connection);
            guard.imported_remote_ip_network = None;
            guard.imported_remote_api_session = None;
            guard.imported_remote_caps.clear();
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
            match (
                guard.approved_peer_node_id.as_ref(),
                guard.pending_incoming_connection.as_ref(),
            ) {
                (Some(approved), Some(pending)) if approved == &pending.remote_node_id => true,
                _ => false,
            }
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
    ) -> Result<(Vec<crate::PeerRpcExport>, Vec<crate::PeerRpcExport>), String> {
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
        let remote_bootstrap = rpc_system
            .bootstrap::<crate::tunnel_capnp::peer_bootstrap::Client>(
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
                ip_network_exports: Vec::new(),
                api_session_exports: Vec::new(),
            });
            guard.pairing_status = crate::PairingStatus::Connected;
            session_id
        };

        tokio::task::spawn_local({
            let app = self.clone();
            let remote_bootstrap = remote_bootstrap.clone();
            async move {
                let capability_exports =
                    crate::list_remote_capability_exports(remote_bootstrap.clone()).await;
                let ip_network_exports =
                    crate::list_remote_ip_network_exports(remote_bootstrap.clone()).await;
                let api_session_exports =
                    crate::list_remote_api_session_exports(remote_bootstrap.clone()).await;
                match (capability_exports, ip_network_exports, api_session_exports) {
                    (Ok(capability_exports), Ok(ip_network_exports), Ok(api_session_exports)) => {
                        if let Ok(mut guard) = app.state.lock() {
                            let is_current = guard
                                .peer_rpc_session
                                .as_ref()
                                .map(|session| session.session_id == session_id)
                                .unwrap_or(false);
                            if is_current {
                                if let Some(session) = guard.peer_rpc_session.as_mut() {
                                    session.capability_exports = capability_exports;
                                    session.ip_network_exports = ip_network_exports;
                                    session.api_session_exports = api_session_exports;
                                }
                            }
                        }
                        if let Err(err) = app.reimport_persisted_received_capabilities().await {
                            if let Ok(mut guard) = app.state.lock() {
                                guard.peer_rpc_error = Some(err);
                            }
                        }
                    }
                    (Err(err), _, _) | (_, Err(err), _) | (_, _, Err(err)) => {
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
                        guard.imported_remote_ip_network = None;
                        guard.imported_remote_api_session = None;
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

        Ok((Vec::new(), Vec::new()))
    }

    async fn attach_client_peer_rpc_session_eager(
        &self,
        connection: iroh::endpoint::Connection,
        remote_node_id: String,
        sandstorm_api: crate::grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
    ) -> Result<(Vec<crate::PeerRpcExport>, Vec<crate::PeerRpcExport>), String> {
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
        let remote_bootstrap = rpc_system
            .bootstrap::<crate::tunnel_capnp::peer_bootstrap::Client>(
                rpc_twoparty_capnp::Side::Server,
            );
        tokio::task::spawn_local(async move {
            if let Err(err) = rpc_system.await {
                eprintln!("peer rpc system exited with error: {err}");
            }
        });

        let capability_exports =
            crate::list_remote_capability_exports(remote_bootstrap.clone()).await?;
        let ip_network_exports =
            crate::list_remote_ip_network_exports(remote_bootstrap.clone()).await?;
        let api_session_exports =
            crate::list_remote_api_session_exports(remote_bootstrap.clone()).await?;
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
                ip_network_exports: ip_network_exports.clone(),
                api_session_exports: api_session_exports.clone(),
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
                        guard.imported_remote_ip_network = None;
                        guard.imported_remote_api_session = None;
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

        self.reimport_persisted_received_capabilities().await?;

        Ok((ip_network_exports, api_session_exports))
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
        let remote_bootstrap = rpc_system
            .bootstrap::<crate::tunnel_capnp::peer_bootstrap::Client>(
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
                ip_network_exports: Vec::new(),
                api_session_exports: Vec::new(),
            });
            guard.pairing_status = crate::PairingStatus::Connected;
            session_id
        };

        tokio::task::spawn_local({
            let app = self.clone();
            let remote_bootstrap = remote_bootstrap.clone();
            async move {
                let capability_exports =
                    crate::list_remote_capability_exports(remote_bootstrap.clone()).await;
                let ip_network_exports =
                    crate::list_remote_ip_network_exports(remote_bootstrap.clone()).await;
                let api_session_exports =
                    crate::list_remote_api_session_exports(remote_bootstrap.clone()).await;
                match (capability_exports, ip_network_exports, api_session_exports) {
                    (Ok(capability_exports), Ok(ip_network_exports), Ok(api_session_exports)) => {
                        if let Ok(mut guard) = app.state.lock() {
                            let is_current = guard
                                .peer_rpc_session
                                .as_ref()
                                .map(|session| session.session_id == session_id)
                                .unwrap_or(false);
                            if is_current {
                                if let Some(session) = guard.peer_rpc_session.as_mut() {
                                    session.capability_exports = capability_exports;
                                    session.ip_network_exports = ip_network_exports;
                                    session.api_session_exports = api_session_exports;
                                }
                            }
                        }
                        if let Err(err) = app.reimport_persisted_received_capabilities().await {
                            if let Ok(mut guard) = app.state.lock() {
                                guard.peer_rpc_error = Some(err);
                            }
                        }
                    }
                    (Err(err), _, _) | (_, Err(err), _) | (_, _, Err(err)) => {
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
                        guard.imported_remote_ip_network = None;
                        guard.imported_remote_api_session = None;
                        guard.imported_remote_caps.clear();
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

    pub(crate) async fn reimport_persisted_received_capabilities(&self) -> Result<(), String> {
        let (remote_bootstrap, persisted_records, capability_exports, ip_exports, api_exports) = {
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
                guard.persisted_received_caps.clone(),
                session.capability_exports.clone(),
                session.ip_network_exports.clone(),
                session.api_session_exports.clone(),
            )
        };

        for record in persisted_records {
            if !record.enabled {
                continue;
            }
            let export_exists = capability_exports
                .iter()
                .any(|export| export.id == record.export_id)
                || match record.kind {
                    crate::ImportedRemoteCapabilityKind::IpNetwork => {
                        ip_exports.iter().any(|export| export.id == record.export_id)
                    }
                    crate::ImportedRemoteCapabilityKind::ApiSession => {
                        api_exports.iter().any(|export| export.id == record.export_id)
                    }
                    crate::ImportedRemoteCapabilityKind::Other => false,
                };
            if export_exists {
                let (label, kind, type_tag, descriptor_json, client) =
                    crate::fetch_remote_capability_export(
                        remote_bootstrap.clone(),
                        &record.export_id,
                    )
                    .await?;
                self.activate_imported_remote_capability(
                    record.object_id.clone(),
                    record.export_id.clone(),
                    label,
                    kind,
                    if type_tag.is_empty() {
                        record.type_tag.clone()
                    } else {
                        type_tag
                    },
                    descriptor_json.or_else(|| record.descriptor_json.clone()),
                    client,
                    true,
                )?;
            }
        }

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
        let ip_network_exports =
            crate::list_remote_ip_network_exports(remote_bootstrap.clone()).await?;
        let api_session_exports =
            crate::list_remote_api_session_exports(remote_bootstrap.clone()).await?;

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
                session.ip_network_exports = ip_network_exports;
                session.api_session_exports = api_session_exports;
            }
        }

        self.reimport_persisted_received_capabilities().await?;
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
            pending.connection.close(0u32.into(), b"peer rpc disconnected");
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

    pub(crate) async fn import_remote_ip_network_export(
        &self,
        export_id: &str,
    ) -> Result<(String, String), String> {
        let remote_bootstrap = {
            let guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            guard
                .peer_rpc_session
                .as_ref()
                .ok_or_else(|| "peer rpc session is not connected".to_string())?
                .remote_bootstrap
                .clone()
        };
        let (label, client) = crate::fetch_remote_ip_network_export(remote_bootstrap, export_id).await?;
        let object_id = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            guard
                .persisted_received_caps
                .iter()
                .find(|record| {
                    record.kind == crate::ImportedRemoteCapabilityKind::IpNetwork
                        && record.export_id == export_id
                })
                .map(|record| record.object_id.clone())
                .unwrap_or_else(|| self.allocate_imported_remote_object_id(&mut guard))
        };
        self.activate_imported_remote_ip_network(
            object_id.clone(),
            export_id.to_string(),
            label.clone(),
            client,
            true,
        )?;
        Ok((label, object_id))
    }

    pub(crate) async fn import_remote_capability_export(
        &self,
        export_id: &str,
    ) -> Result<(String, String, crate::ImportedRemoteCapabilityKind), String> {
        let remote_bootstrap = {
            let guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            guard
                .peer_rpc_session
                .as_ref()
                .ok_or_else(|| "peer rpc session is not connected".to_string())?
                .remote_bootstrap
                .clone()
        };
        let (label, kind, type_tag, descriptor_json, client) =
            crate::fetch_remote_capability_export(remote_bootstrap, export_id).await?;
        let object_id = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            guard
                .persisted_received_caps
                .iter()
                .find(|record| record.kind == kind && record.export_id == export_id)
                .map(|record| record.object_id.clone())
                .unwrap_or_else(|| self.allocate_imported_remote_object_id(&mut guard))
        };
        self.activate_imported_remote_capability(
            object_id.clone(),
            export_id.to_string(),
            label.clone(),
            kind,
            if type_tag.is_empty() {
                crate::imported_type_tag_for_kind(kind)
            } else {
                type_tag
            },
            descriptor_json,
            client,
            true,
        )?;
        Ok((label, object_id, kind))
    }

    pub(crate) async fn import_remote_api_session_export(
        &self,
        export_id: &str,
    ) -> Result<(String, String), String> {
        let remote_bootstrap = {
            let guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            guard
                .peer_rpc_session
                .as_ref()
                .ok_or_else(|| "peer rpc session is not connected".to_string())?
                .remote_bootstrap
                .clone()
        };
        let (label, client) =
            crate::fetch_remote_api_session_export(remote_bootstrap, export_id).await?;
        let object_id = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            guard
                .persisted_received_caps
                .iter()
                .find(|record| {
                    record.kind == crate::ImportedRemoteCapabilityKind::ApiSession
                        && record.export_id == export_id
                })
                .map(|record| record.object_id.clone())
                .unwrap_or_else(|| self.allocate_imported_remote_object_id(&mut guard))
        };
        self.activate_imported_remote_api_session(
            object_id.clone(),
            export_id.to_string(),
            label.clone(),
            client,
            true,
        )?;
        Ok((label, object_id))
    }

    pub(crate) async fn invoke_imported_remote_ip_network(
        &self,
        host: &str,
        port: u16,
    ) -> Result<crate::TcpProbeSummary, String> {
        let client = {
            let guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            guard
                .imported_remote_ip_network
                .as_ref()
                .ok_or_else(|| "no imported remote IpNetwork is loaded".to_string())?
                .client
                .clone()
        };

        let connection = crate::connect_ip_network_tcp_client(client, host, port)
            .await
            .map_err(|err| format!("remote IpNetwork TCP connect failed: {err}"))?;
        let payload = format!("GET / HTTP/1.0\r\nHost: {host}\r\n\r\n");
        let (response_bytes, trace) =
            crate::finish_saved_ip_network_tcp_exchange(connection, payload.as_bytes()).await?;

        Ok(crate::TcpProbeSummary {
            host: host.to_string(),
            port,
            response_bytes,
            trace,
        })
    }

    pub(crate) async fn invoke_imported_remote_api_session(
        &self,
        filename: &str,
        payload: &[u8],
    ) -> Result<crate::ApiSessionInvokeSummary, String> {
        let client = {
            let guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            guard
                .imported_remote_api_session
                .as_ref()
                .ok_or_else(|| "no imported remote ApiSession is loaded".to_string())?
                .client
                .clone()
        };

        let web_session = crate::api_session_as_web_session(client);
        let incoming = std::sync::Arc::new(std::sync::Mutex::new(crate::TcpSessionBuffer {
            bytes: Vec::new(),
            read_offset: 0,
            total_received_bytes: 0,
            write_calls: 0,
            saw_done: false,
        }));
        let trace = std::sync::Arc::new(std::sync::Mutex::new(vec![
            "remote-restore:ok".to_string(),
            "request:post-preview".to_string(),
        ]));
        let notify = std::sync::Arc::new(tokio::sync::Notify::new());
        let downstream: crate::util_capnp::byte_stream::Client =
            new_client(crate::ByteStreamCollector {
                incoming: incoming.clone(),
                trace: trace.clone(),
                notify: notify.clone(),
            });

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
                context.set_response_stream(downstream);
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
            .map_err(|err| format!("ApiSession.post(preview) failed: {err}"))?;
        let response = response
            .get()
            .map_err(|err| format!("failed to decode ApiSession.post(preview) response: {err}"))?;

        match response
            .which()
            .map_err(|err| format!("failed to decode ApiSession response union: {err}"))?
        {
            crate::web_session_capnp::web_session::response::Content(content) => {
                let status_code = crate::response_success_code_to_status(
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
                    crate::web_session_capnp::web_session::response::content::body::Bytes(bytes) => {
                        if let Ok(mut trace_guard) = trace.lock() {
                            trace_guard.push(format!(
                                "response-bytes:{}-bytes",
                                bytes.as_ref().map(|b| b.len()).unwrap_or(0)
                            ));
                        }
                        bytes
                            .map_err(|err| format!("failed to read response bytes: {err}"))?
                            .to_vec()
                    }
                    crate::web_session_capnp::web_session::response::content::body::Stream(_) => {
                        if let Ok(mut trace_guard) = trace.lock() {
                            trace_guard.push("response-stream:started".to_string());
                        }
                        crate::wait_for_byte_stream_completion(&incoming, &notify, 60_000).await?;
                        let guard = incoming
                            .lock()
                            .map_err(|_| "byte stream buffer lock poisoned".to_string())?;
                        guard.bytes.clone()
                    }
                };
                let trace = trace
                    .lock()
                    .map_err(|_| "api session trace lock poisoned".to_string())?
                    .join(" -> ");
                Ok(crate::ApiSessionInvokeSummary {
                    status_code,
                    content_type,
                    response_bytes,
                    trace,
                })
            }
            crate::web_session_capnp::web_session::response::ClientError(client_error) => {
                let status_code = crate::response_client_error_code_to_status(
                    client_error
                        .get_status_code()
                        .map_err(|err| format!("failed to decode client error status: {err}"))?,
                );
                let body = if client_error.has_non_html_body() {
                    let non_html = client_error
                        .get_non_html_body()
                        .map_err(|err| format!("failed to read client error body: {err}"))?;
                    String::from_utf8_lossy(
                        non_html
                            .get_data()
                            .map_err(|err| format!("failed to read client error bytes: {err}"))?
                            .as_ref(),
                    )
                    .to_string()
                } else if client_error.has_description_html() {
                    client_error
                        .get_description_html()
                        .map_err(|err| format!("failed to read client error html: {err}"))?
                        .to_str()
                        .unwrap_or("")
                        .to_string()
                } else {
                    String::new()
                };
                Err(format!(
                    "remote ApiSession returned HTTP {status_code}: {}",
                    body.trim()
                ))
            }
            crate::web_session_capnp::web_session::response::ServerError(server_error) => {
                let body = if server_error.has_non_html_body() {
                    let non_html = server_error
                        .get_non_html_body()
                        .map_err(|err| format!("failed to read server error body: {err}"))?;
                    String::from_utf8_lossy(
                        non_html
                            .get_data()
                            .map_err(|err| format!("failed to read server error bytes: {err}"))?
                            .as_ref(),
                    )
                    .to_string()
                } else if server_error.has_description_html() {
                    server_error
                        .get_description_html()
                        .map_err(|err| format!("failed to read server error html: {err}"))?
                        .to_str()
                        .unwrap_or("")
                        .to_string()
                } else {
                    String::new()
                };
                Err(format!("remote ApiSession returned server error: {}", body.trim()))
            }
            crate::web_session_capnp::web_session::response::NoContent(_) => Ok(
                crate::ApiSessionInvokeSummary {
                    status_code: 204,
                    content_type: String::new(),
                    response_bytes: Vec::new(),
                    trace: trace
                        .lock()
                        .map_err(|_| "api session trace lock poisoned".to_string())?
                        .join(" -> "),
                },
            ),
            crate::web_session_capnp::web_session::response::Redirect(redirect) => Err(format!(
                "remote ApiSession returned redirect to {}",
                redirect
                    .get_location()
                    .map_err(|err| format!("failed to read redirect location: {err}"))?
                    .to_str()
                    .unwrap_or("")
            )),
            crate::web_session_capnp::web_session::response::PreconditionFailed(_) => Err(
                "remote ApiSession rejected the request due to a precondition failure".to_string(),
            ),
        }
    }

    pub(crate) fn render_state_json(&self) -> Result<String, String> {
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
        let active_tcp_sessions = guard
            .active_tcp_sessions
            .iter()
            .map(|(session_id, session)| {
                let summary = session.snapshot()?;
                Ok(format!(
                    "{{\"sessionId\":\"{}\",\"host\":\"{}\",\"port\":{},\"bufferedBytes\":{},\"receivedBytes\":{},\"writeCalls\":{},\"done\":{},\"trace\":\"{}\"}}",
                    crate::json_escape(session_id),
                    crate::json_escape(&summary.host),
                    summary.port,
                    summary.buffered_bytes,
                    summary.received_bytes,
                    summary.write_calls,
                    if summary.done { "true" } else { "false" },
                    crate::json_escape(&summary.trace)
                ))
            })
            .collect::<Result<Vec<_>, String>>()?;
        let mut rows = Vec::new();
        for row in crate::load_saved_capabilities()? {
            rows.push(format!(
                "{{\"id\":\"{}\",\"objectId\":\"{}\",\"label\":\"{}\",\"savedToken\":\"{}\",\"createdAtMs\":{},\"descriptorJson\":{}}}",
                crate::json_escape(&row.id),
                crate::json_escape(&row.id),
                crate::json_escape(&row.label),
                crate::json_escape(&row.saved_token),
                row.created_at_ms,
                match &row.descriptor_json {
                    Some(value) => format!("\"{}\"", crate::json_escape(value)),
                    None => "null".to_string(),
                }
            ));
        }
        let mut advertised_match_descriptors = crate::load_saved_capabilities()?
            .into_iter()
            .filter_map(|row| {
                row.descriptor_json
                    .as_deref()
                    .and_then(crate::descriptor_json_to_match_request_b64)
            })
            .collect::<Vec<_>>();
        advertised_match_descriptors.extend(
            guard.imported_remote_caps
                .values()
                .filter_map(|cap| {
                    cap.descriptor_json
                        .as_deref()
                        .and_then(crate::descriptor_json_to_match_request_b64)
                }),
        );
        if let Some(session) = guard.peer_rpc_session.as_ref() {
            advertised_match_descriptors.extend(
                session
                    .capability_exports
                    .iter()
                    .filter_map(|cap| {
                        cap.descriptor_json
                            .as_deref()
                            .and_then(crate::descriptor_json_to_match_request_b64)
                    }),
            );
        }
        let mut seen_advertised_match_descriptors = HashSet::new();
        advertised_match_descriptors
            .retain(|descriptor| seen_advertised_match_descriptors.insert(descriptor.clone()));

        let relay_urls = crate::join_json_strings(&guard.iroh_endpoint_addr.relay_urls);
        let direct_addrs = crate::join_json_strings(&guard.iroh_endpoint_addr.direct_addrs);
        let custom_addrs = crate::join_json_strings(&guard.iroh_endpoint_addr.custom_addrs);
        let raw_udp_interface = match &guard.raw_udp_interface {
            Some(value) => format!(
                "{{\"label\":\"{}\",\"savedToken\":\"{}\",\"source\":\"{}\"}}",
                crate::json_escape(&value.label),
                crate::json_escape(&value.saved_token),
                crate::json_escape(guard.raw_udp_interface_source.as_deref().unwrap_or("unknown"))
            ),
            None => "null".to_string(),
        };
        let exported_ip_network = match &guard.exported_ip_network {
            Some(value) => format!(
                "{{\"id\":\"{}\",\"label\":\"{}\",\"savedToken\":\"{}\"}}",
                crate::json_escape(&value.id),
                crate::json_escape(&value.label),
                crate::json_escape(&value.saved_token)
            ),
            None => "null".to_string(),
        };
        let exported_api_session = match &guard.exported_api_session {
            Some(value) => format!(
                "{{\"id\":\"{}\",\"label\":\"{}\",\"savedToken\":\"{}\"}}",
                crate::json_escape(&value.id),
                crate::json_escape(&value.label),
                crate::json_escape(&value.saved_token)
            ),
            None => "null".to_string(),
        };
        let peer_rpc = match &guard.peer_rpc_session {
            Some(session) => {
                let ip_network_exports = session
                    .capability_exports
                    .iter()
                    .map(|export| {
                        format!(
                            "{{\"id\":\"{}\",\"label\":\"{}\",\"kind\":\"{}\",\"typeTag\":\"{}\",\"descriptorJson\":{}}}",
                            crate::json_escape(&export.id),
                            crate::json_escape(&export.label),
                            crate::json_escape(export.kind.as_str()),
                            crate::json_escape(&export.type_tag),
                            match &export.descriptor_json {
                                Some(value) => format!("\"{}\"", crate::json_escape(value)),
                                None => "null".to_string(),
                            }
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                let network_exports = session
                    .ip_network_exports
                    .iter()
                    .map(|export| {
                        format!(
                            "{{\"id\":\"{}\",\"label\":\"{}\"}}",
                            crate::json_escape(&export.id),
                            crate::json_escape(&export.label)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                let api_session_exports = session
                    .api_session_exports
                    .iter()
                    .map(|export| {
                        format!(
                            "{{\"id\":\"{}\",\"label\":\"{}\"}}",
                            crate::json_escape(&export.id),
                            crate::json_escape(&export.label)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                format!(
                    "{{\"connected\":true,\"sessionId\":{},\"remoteNodeId\":\"{}\",\"capabilityExports\":[{}],\"ipNetworkExports\":[{}],\"apiSessionExports\":[{}]}}",
                    session.session_id,
                    crate::json_escape(&session.remote_node_id),
                    ip_network_exports,
                    network_exports,
                    api_session_exports
                )
            }
            None => {
                "{\"connected\":false,\"capabilityExports\":[],\"ipNetworkExports\":[],\"apiSessionExports\":[]}".to_string()
            }
        };
        let imported_remote_ip_network = match &guard.imported_remote_ip_network {
            Some(value) => format!(
                "{{\"objectId\":\"{}\",\"exportId\":\"{}\",\"label\":\"{}\"}}",
                crate::json_escape(&value.object_id),
                crate::json_escape(&value.export_id),
                crate::json_escape(&value.label)
            ),
            None => "null".to_string(),
        };
        let imported_remote_api_session = match &guard.imported_remote_api_session {
            Some(value) => format!(
                "{{\"objectId\":\"{}\",\"exportId\":\"{}\",\"label\":\"{}\"}}",
                crate::json_escape(&value.object_id),
                crate::json_escape(&value.export_id),
                crate::json_escape(&value.label)
            ),
            None => "null".to_string(),
        };
        let imported_remote_caps = guard
            .imported_remote_caps
            .values()
            .map(|cap| {
                format!(
                    "{{\"objectId\":\"{}\",\"exportId\":\"{}\",\"label\":\"{}\",\"kind\":\"{}\",\"typeTag\":\"{}\",\"descriptorJson\":{}}}",
                    crate::json_escape(&cap.object_id),
                    crate::json_escape(&cap.export_id),
                    crate::json_escape(&cap.label),
                    crate::json_escape(crate::imported_kind_label(cap.kind)),
                    crate::json_escape(&cap.type_tag),
                    match &cap.descriptor_json {
                        Some(value) => format!("\"{}\"", crate::json_escape(value)),
                        None => "null".to_string(),
                    }
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let persisted_received_caps = guard
            .persisted_received_caps
            .iter()
        .map(|cap| {
            format!(
                "{{\"objectId\":\"{}\",\"exportId\":\"{}\",\"label\":\"{}\",\"kind\":\"{}\",\"typeTag\":\"{}\",\"descriptorJson\":{},\"enabled\":{}}}",
                crate::json_escape(&cap.object_id),
                crate::json_escape(&cap.export_id),
                crate::json_escape(&cap.label),
                crate::json_escape(crate::imported_kind_label(cap.kind)),
                crate::json_escape(&cap.type_tag),
                match &cap.descriptor_json {
                    Some(value) => format!("\"{}\"", crate::json_escape(value)),
                    None => "null".to_string(),
                },
                if cap.enabled { "true" } else { "false" }
            )
        })
        .collect::<Vec<_>>()
        .join(",");
        let local_proxy_caps = guard
            .local_proxy_caps
            .iter()
            .map(|cap| {
                format!(
                    "{{\"objectId\":\"{}\",\"peerNodeId\":\"{}\",\"targetKind\":\"{}\",\"targetId\":\"{}\",\"label\":\"{}\",\"kind\":\"{}\",\"typeTag\":\"{}\",\"descriptorJson\":{},\"createdAtMs\":{}}}",
                    crate::json_escape(&cap.object_id),
                    crate::json_escape(&cap.peer_node_id),
                    crate::json_escape(match cap.target_kind {
                        crate::LocalProxyTargetKindRuntime::ExportId => "exportId",
                        crate::LocalProxyTargetKindRuntime::RemoteObjectId => "remoteObjectId",
                    }),
                    crate::json_escape(&cap.target_id),
                    crate::json_escape(&cap.label),
                    crate::json_escape(cap.kind.as_str()),
                    crate::json_escape(&cap.type_tag),
                    match &cap.descriptor_json {
                        Some(value) => format!("\"{}\"", crate::json_escape(value)),
                        None => "null".to_string(),
                    },
                    cap.created_at_ms
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let remote_ticket = match &guard.remote_ticket {
            Some(value) => format!("\"{}\"", crate::json_escape(value)),
            None => "null".to_string(),
        };
        let shared_caps = guard
            .shared_caps
            .iter()
            .map(|cap| {
                format!(
                    "{{\"id\":\"{}\",\"savedCapId\":\"{}\",\"label\":\"{}\",\"kind\":\"{}\",\"typeTag\":\"{}\",\"enabled\":{},\"createdAtMs\":{},\"savedToken\":\"{}\",\"descriptorJson\":{}}}",
                    crate::json_escape(&cap.id),
                    crate::json_escape(&cap.saved_cap.id),
                    crate::json_escape(&cap.label),
                    crate::json_escape(cap.kind.as_str()),
                    crate::json_escape(&crate::shared_capability_type_tag(cap)),
                    if cap.enabled { "true" } else { "false" },
                    cap.created_at_ms,
                    crate::json_escape(&cap.saved_cap.saved_token),
                    match &cap.saved_cap.descriptor_json {
                        Some(value) => format!("\"{}\"", crate::json_escape(value)),
                        None => "null".to_string(),
                    }
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let approved_peer_node_id = match &guard.approved_peer_node_id {
            Some(value) => format!("\"{}\"", crate::json_escape(value)),
            None => "null".to_string(),
        };
        let pending_incoming_peer_node_id = match &guard.pending_incoming_peer_node_id {
            Some(value) => format!("\"{}\"", crate::json_escape(value)),
            None => "null".to_string(),
        };
        let pending_outgoing_peer_node_id = match &guard.pending_outgoing_peer_node_id {
            Some(value) => format!("\"{}\"", crate::json_escape(value)),
            None => "null".to_string(),
        };
        let endpoint_error = match &guard.iroh_endpoint_error {
            Some(value) => format!("\"{}\"", crate::json_escape(value)),
            None => "null".to_string(),
        };
        let peer_rpc_error = match &guard.peer_rpc_error {
            Some(value) => format!("\"{}\"", crate::json_escape(value)),
            None => "null".to_string(),
        };
        let local_ticket = format!(
            "\"{}\"",
            crate::json_escape(&crate::format_local_ticket(&guard.iroh_endpoint_addr))
        );
        let endpoint_bound = if guard.iroh_endpoint.is_some() {
            "true"
        } else {
            "false"
        };
        let advertised_powerbox_matches =
            crate::advertised_powerbox_matches_json_from_b64(&advertised_match_descriptors)?;

        Ok(format!(
            "{{\"powerboxQueries\":{{\"apiSession\":\"{}\",\"ipNetwork\":\"{}\",\"ipInterface\":\"{}\"}},\"powerboxAdvertisedMatches\":{},\"irohNodeId\":\"{}\",\"irohEndpoint\":{{\"bound\":{},\"nodeId\":\"{}\",\"relayUrls\":[{}],\"directAddrs\":[{}],\"customAddrs\":[{}],\"error\":{},\"localTicket\":{},\"rawUdpInterface\":{}}},\"pairing\":{{\"status\":\"{}\",\"approvedPeerNodeId\":{},\"pendingIncomingPeerNodeId\":{},\"pendingOutgoingPeerNodeId\":{},\"tunnelEnabled\":{}}},\"peerRpc\":{},\"peerRpcError\":{},\"exportedIpNetwork\":{},\"exportedApiSession\":{},\"importedRemoteIpNetwork\":{},\"importedRemoteApiSession\":{},\"importedRemoteCaps\":[{}],\"persistedReceivedCaps\":[{}],\"localProxyCaps\":[{}],\"transportAssessment\":\"{}\",\"remoteTicket\":{},\"savedCaps\":[{}],\"sharedCaps\":[{}],\"activeTcpSessions\":[{}]}}",
            crate::json_escape(&crate::powerbox_query_for_interface(
                crate::api_session_capnp::api_session::Client::TYPE_ID
            )?),
            crate::json_escape(&crate::powerbox_query_for_interface(
                crate::ip_capnp::ip_network::Client::TYPE_ID
            )?),
            crate::json_escape(&crate::powerbox_query_for_interface(
                crate::ip_capnp::ip_interface::Client::TYPE_ID
            )?),
            advertised_powerbox_matches,
            crate::json_escape(&guard.iroh_identity.node_id),
            endpoint_bound,
            crate::json_escape(&guard.iroh_endpoint_addr.node_id),
            relay_urls,
            direct_addrs,
            custom_addrs,
            endpoint_error,
            local_ticket,
            raw_udp_interface,
            crate::json_escape(pairing_status.as_str()),
            approved_peer_node_id,
            pending_incoming_peer_node_id,
            pending_outgoing_peer_node_id,
            if guard.tunnel_enabled { "true" } else { "false" },
            peer_rpc,
            peer_rpc_error,
            exported_ip_network,
            exported_api_session,
            imported_remote_ip_network,
            imported_remote_api_session,
            imported_remote_caps,
            persisted_received_caps,
            local_proxy_caps,
            crate::json_escape(crate::IROH_TRANSPORT_ASSESSMENT),
            remote_ticket,
            rows.join(","),
            shared_caps,
            active_tcp_sessions.join(",")
        ))
    }

    fn persist_received_capability_registry(
        &self,
        records: &[crate::PersistedReceivedCapability],
    ) -> Result<(), String> {
        let converted = records
            .iter()
            .map(|record| PersistedReceivedCapabilityRecord {
                object_id: record.object_id.clone(),
                export_id: record.export_id.clone(),
                label: record.label.clone(),
                enabled: record.enabled,
                kind: match record.kind {
                    crate::ImportedRemoteCapabilityKind::IpNetwork => {
                        ReceivedCapabilityKind::IpNetwork
                    }
                    crate::ImportedRemoteCapabilityKind::ApiSession => {
                        ReceivedCapabilityKind::ApiSession
                    }
                    crate::ImportedRemoteCapabilityKind::Other => ReceivedCapabilityKind::Other,
                },
                type_tag: Some(record.type_tag.clone()),
                descriptor_json: record.descriptor_json.clone(),
            })
            .collect::<Vec<_>>();
        self.storage.persist_received_capability_registry(&converted)
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
                created_at_ms: cap.created_at_ms,
            })
            .collect::<Vec<_>>();
        self.storage.persist_local_proxy_capability_registry(&records)
    }

    fn allocate_imported_remote_object_id(&self, guard: &mut crate::AppState) -> String {
        guard.next_imported_remote_cap_id += 1;
        format!("remote-cap-{}", guard.next_imported_remote_cap_id)
    }

    fn replace_imported_remote_capability(
        &self,
        guard: &mut crate::AppState,
        previous_object_id: Option<String>,
        capability: crate::ImportedRemoteCapability,
    ) {
        if let Some(previous_object_id) = previous_object_id {
            guard.imported_remote_caps.remove(&previous_object_id);
        }
        guard
            .imported_remote_caps
            .insert(capability.object_id.clone(), capability);
    }

    fn close_peer_rpc_session_locked(
        &self,
        guard: &mut crate::AppState,
        reason: &'static [u8],
    ) {
        if let Some(session) = guard.peer_rpc_session.take() {
            session.connection.close(0u32.into(), reason);
        }
        guard.imported_remote_ip_network = None;
        guard.imported_remote_api_session = None;
        guard.imported_remote_caps.clear();
        guard.pairing_status = if guard.tunnel_enabled {
            crate::PairingStatus::Disconnected
        } else {
            crate::PairingStatus::Disabled
        };
    }

    fn activate_imported_remote_ip_network(
        &self,
        object_id: String,
        export_id: String,
        label: String,
        client: crate::ip_capnp::ip_network::Client,
        persist_record: bool,
    ) -> Result<(), String> {
        self.activate_imported_remote_capability(
            object_id,
            export_id,
            label,
            crate::ImportedRemoteCapabilityKind::IpNetwork,
            crate::imported_type_tag_for_kind(crate::ImportedRemoteCapabilityKind::IpNetwork),
            None,
            client.client,
            persist_record,
        )
    }

    fn activate_imported_remote_api_session(
        &self,
        object_id: String,
        export_id: String,
        label: String,
        client: crate::api_session_capnp::api_session::Client,
        persist_record: bool,
    ) -> Result<(), String> {
        self.activate_imported_remote_capability(
            object_id,
            export_id,
            label,
            crate::ImportedRemoteCapabilityKind::ApiSession,
            crate::imported_type_tag_for_kind(crate::ImportedRemoteCapabilityKind::ApiSession),
            None,
            client.client,
            persist_record,
        )
    }

    fn activate_imported_remote_capability(
        &self,
        object_id: String,
        export_id: String,
        label: String,
        kind: crate::ImportedRemoteCapabilityKind,
        type_tag: String,
        descriptor_json: Option<String>,
        client: capnp::capability::Client,
        persist_record: bool,
    ) -> Result<(), String> {
        let persisted_caps = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            let previous_object_id = match kind {
                crate::ImportedRemoteCapabilityKind::IpNetwork => guard
                    .imported_remote_ip_network
                    .as_ref()
                    .map(|value| value.object_id.clone()),
                crate::ImportedRemoteCapabilityKind::ApiSession => guard
                    .imported_remote_api_session
                    .as_ref()
                    .map(|value| value.object_id.clone()),
                crate::ImportedRemoteCapabilityKind::Other => None,
            };
            let capability = crate::ImportedRemoteCapability {
                object_id: object_id.clone(),
                export_id: export_id.clone(),
                label: label.clone(),
                kind,
                type_tag: type_tag.clone(),
                descriptor_json: descriptor_json.clone(),
                client: client.clone(),
            };
            self.replace_imported_remote_capability(&mut guard, previous_object_id, capability);
            match kind {
                crate::ImportedRemoteCapabilityKind::IpNetwork => {
                    guard.imported_remote_ip_network = Some(crate::ImportedRemoteIpNetwork {
                        object_id: object_id.clone(),
                        export_id: export_id.clone(),
                        label: label.clone(),
                        client: crate::ip_capnp::ip_network::Client {
                            client: client.clone(),
                        },
                    });
                }
                crate::ImportedRemoteCapabilityKind::ApiSession => {
                    guard.imported_remote_api_session = Some(crate::ImportedRemoteApiSession {
                        object_id: object_id.clone(),
                        export_id: export_id.clone(),
                        label: label.clone(),
                        client: crate::api_session_capnp::api_session::Client {
                            client: client.clone(),
                        },
                    });
                }
                crate::ImportedRemoteCapabilityKind::Other => {}
            }
            if persist_record {
                guard
                    .persisted_received_caps
                    .retain(|record| record.object_id != object_id);
                guard.persisted_received_caps.push(crate::PersistedReceivedCapability {
                    object_id: object_id.clone(),
                    export_id: export_id.clone(),
                    label: label.clone(),
                    kind,
                    type_tag: type_tag.clone(),
                    descriptor_json: descriptor_json.clone(),
                    enabled: true,
                });
            }
            guard.peer_rpc_error = None;
            guard.persisted_received_caps.clone()
        };
        if persist_record {
            self.persist_received_capability_registry(&persisted_caps)?;
        }
        Ok(())
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
                request
                    .get()
                    .set_as(params.get().map_err(|err| err)?)?;
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
        if interface_id
            == crate::grain_capnp::app_persistent::Client::<capnp::text::Owned>::TYPE_ID
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
                            let mut save_results =
                                crate::grain_capnp::app_persistent::SaveResults::<
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
                        _ => {
                            Err(capnp::Error::unimplemented(
                                "unknown app persistent method".to_string(),
                            ))
                        }
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
                request.get().set_as(params.get().map_err(|err| err)?)?;
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

#[derive(Clone)]
struct ForwardingCapabilityServer {
    backend_client: capnp::capability::Client,
    identity: std::sync::Arc<()>,
}

impl ForwardingCapabilityServer {
    fn new(backend_client: capnp::capability::Client) -> Self {
        Self {
            backend_client,
            identity: std::sync::Arc::new(()),
        }
    }
}

impl CapServer for ForwardingCapabilityServer {
    fn dispatch_call(
        self,
        interface_id: u64,
        method_id: u16,
        params: capnp::capability::Params<capnp::any_pointer::Owned>,
        mut results: capnp::capability::Results<capnp::any_pointer::Owned>,
    ) -> DispatchCallResult {
        let backend_client = self.backend_client.clone();
        DispatchCallResult::new(
            CapPromise::from_future(async move {
                eprintln!(
                    "forwarding_capability.dispatch: interface_id=0x{interface_id:016x} method_id={} at_ms={}",
                    method_id,
                    crate::now_ms()
                );
                let mut request = backend_client
                    .new_call::<capnp::any_pointer::Owned, capnp::any_pointer::Owned>(
                        interface_id,
                        method_id,
                        None,
                    );
                request.get().set_as(params.get().map_err(|err| err)?)?;
                let response = request.send().promise.await.map_err(|err| {
                    eprintln!(
                        "forwarding_capability.dispatch: interface_id=0x{interface_id:016x} method_id={} failed at_ms={} err={err}",
                        method_id,
                        crate::now_ms()
                    );
                    err
                })?;
                results.get().set_as(response.get()?)?;
                eprintln!(
                    "forwarding_capability.dispatch: interface_id=0x{interface_id:016x} method_id={} ok at_ms={}",
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
        if interface_id
            == crate::grain_capnp::app_persistent::Client::<capnp::text::Owned>::TYPE_ID
        {
            let object_id = self.object_id.clone();
            let label = self.label.clone();
            return DispatchCallResult::new(
                CapPromise::from_future(async move {
                    match method_id {
                        0 => {
                            let mut save_results =
                                crate::grain_capnp::app_persistent::SaveResults::<
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
                request.get().set_as(params.get().map_err(|err| err)?)?;
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
        content.set_status_code(
            crate::web_session_capnp::web_session::response::SuccessCode::Ok,
        );
        content.set_mime_type("application/pdf");
        content.init_body().set_bytes(b"%PDF-LOCAL-TEST\n");
        CapPromise::ok(())
    }
}
