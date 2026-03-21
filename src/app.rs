use std::sync::{Arc, Mutex};

use capnp::traits::HasTypeId;
use capnp_rpc::{new_client, rpc_twoparty_capnp, twoparty, RpcSystem};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::backend::SandstormBackend;
use crate::storage::{PersistedReceivedCapabilityRecord, ReceivedCapabilityKind, Storage};

#[derive(Clone)]
pub(crate) struct App {
    state: Arc<Mutex<crate::AppState>>,
    storage: Storage,
}

impl App {
    pub(crate) fn new(state: Arc<Mutex<crate::AppState>>, storage: Storage) -> Self {
        Self { state, storage }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(storage: Storage, secret_key: crate::SecretKey) -> Self {
        let state = Arc::new(Mutex::new(crate::AppState {
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
            exported_ip_network: None,
            exported_api_session: None,
            exported_ip_network_live: None,
            exported_api_session_live: None,
            peer_rpc_session: None,
            imported_remote_ip_network: None,
            imported_remote_api_session: None,
            imported_remote_caps: std::collections::HashMap::new(),
            persisted_received_ip_network: None,
            persisted_received_api_session: None,
            next_peer_rpc_session_id: 0,
            next_imported_remote_cap_id: 0,
            peer_rpc_error: None,
            active_tcp_sessions: std::collections::HashMap::new(),
            next_tcp_session_id: 0,
        }));
        Self::new(state, storage)
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
            .alpns(vec![crate::IROH_ALPN.to_vec(), crate::IROH_RPC_ALPN.to_vec()])
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
        let mut guard = self
            .state
            .lock()
            .map_err(|_| "app state lock poisoned".to_string())?;
        guard.remote_ticket = Some(remote_ticket);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn seed_exported_api_session_for_test(
        &self,
        export_id: &str,
        label: &str,
        client: crate::api_session_capnp::api_session::Client,
    ) -> Result<(), String> {
        let saved_cap = crate::SavedCapability {
            id: export_id.to_string(),
            label: label.to_string(),
            saved_token: String::new(),
            created_at_ms: crate::now_ms(),
        };
        let mut guard = self
            .state
            .lock()
            .map_err(|_| "app state lock poisoned".to_string())?;
        guard.exported_api_session = Some(saved_cap.clone());
        guard.exported_api_session_live = Some(crate::ExportedApiSessionState { saved_cap, client });
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
        };
        let mut guard = self
            .state
            .lock()
            .map_err(|_| "app state lock poisoned".to_string())?;
        guard.exported_ip_network = Some(saved_cap.clone());
        guard.exported_ip_network_live = Some(crate::ExportedIpNetworkState { saved_cap, client });
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn clear_exported_api_session_for_test(&self) -> Result<(), String> {
        let mut guard = self
            .state
            .lock()
            .map_err(|_| "app state lock poisoned".to_string())?;
        guard.exported_api_session = None;
        guard.exported_api_session_live = None;
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
        {
            let guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            let persisted_match = [
                guard.persisted_received_ip_network.as_ref(),
                guard.persisted_received_api_session.as_ref(),
            ]
            .into_iter()
            .flatten()
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

    pub(crate) fn drop_received_remote_capability(
        &self,
        object_id: &str,
    ) -> Result<bool, String> {
        let (removed, ip_record, api_record) = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;

            let mut removed = false;
            if guard
                .persisted_received_ip_network
                .as_ref()
                .map(|record| record.object_id == object_id)
                .unwrap_or(false)
            {
                self.set_persisted_received_capability(
                    &mut guard,
                    None,
                    crate::ImportedRemoteCapabilityKind::IpNetwork,
                );
                self.remove_imported_remote_capability_by_kind(
                    &mut guard,
                    crate::ImportedRemoteCapabilityKind::IpNetwork,
                );
                removed = true;
            }

            if guard
                .persisted_received_api_session
                .as_ref()
                .map(|record| record.object_id == object_id)
                .unwrap_or(false)
            {
                self.set_persisted_received_capability(
                    &mut guard,
                    None,
                    crate::ImportedRemoteCapabilityKind::ApiSession,
                );
                self.remove_imported_remote_capability_by_kind(
                    &mut guard,
                    crate::ImportedRemoteCapabilityKind::ApiSession,
                );
                removed = true;
            }

            (
                removed,
                guard.persisted_received_ip_network.clone(),
                guard.persisted_received_api_session.clone(),
            )
        };

        if removed {
            self.persist_received_capability_registry(ip_record.as_ref(), api_record.as_ref())?;
        }

        Ok(removed)
    }

    pub(crate) async fn configure_exported_ip_network(
        &self,
        sandstorm_api: crate::grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
        saved_cap_id: &str,
    ) -> Result<(), String> {
        if saved_cap_id.trim().is_empty() {
            let path = crate::exported_ip_network_id_path();
            crate::clear_configured_exported_capability(path.as_path())?;
            let mut guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            guard.exported_ip_network = None;
            guard.exported_ip_network_live = None;
            return Ok(());
        }

        let saved_cap = crate::load_saved_capability_by_id(saved_cap_id)?
            .ok_or_else(|| format!("unknown saved capability id: {saved_cap_id}"))?;
        let client =
            crate::validate_saved_ip_network_capability(sandstorm_api, &saved_cap.saved_token)
                .await?;
        let path = crate::exported_ip_network_id_path();
        crate::persist_configured_exported_capability(path.as_path(), &saved_cap.id)?;

        let mut guard = self
            .state
            .lock()
            .map_err(|_| "app state lock poisoned".to_string())?;
        guard.exported_ip_network = Some(saved_cap.clone());
        guard.exported_ip_network_live = Some(crate::ExportedIpNetworkState { saved_cap, client });
        Ok(())
    }

    pub(crate) async fn configure_exported_api_session(
        &self,
        sandstorm_api: crate::grain_capnp::sandstorm_api::Client<capnp::any_pointer::Owned>,
        saved_cap_id: &str,
    ) -> Result<(), String> {
        if saved_cap_id.trim().is_empty() {
            let path = crate::exported_api_session_id_path();
            crate::clear_configured_exported_capability(path.as_path())?;
            let mut guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            guard.exported_api_session = None;
            guard.exported_api_session_live = None;
            return Ok(());
        }

        let saved_cap = crate::load_saved_capability_by_id(saved_cap_id)?
            .ok_or_else(|| format!("unknown saved capability id: {saved_cap_id}"))?;
        let client =
            crate::validate_saved_api_session_capability(sandstorm_api, &saved_cap.saved_token)
                .await?;
        let path = crate::exported_api_session_id_path();
        crate::persist_configured_exported_capability(path.as_path(), &saved_cap.id)?;

        let mut guard = self
            .state
            .lock()
            .map_err(|_| "app state lock poisoned".to_string())?;
        guard.exported_api_session = Some(saved_cap.clone());
        guard.exported_api_session_live = Some(crate::ExportedApiSessionState { saved_cap, client });
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
                ip_network_exports: ip_network_exports.clone(),
                api_session_exports: api_session_exports.clone(),
            });
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
                    }
                }
            }
        });

        self.reimport_persisted_received_capabilities().await?;

        Ok((ip_network_exports, api_session_exports))
    }

    pub(crate) async fn reimport_persisted_received_capabilities(&self) -> Result<(), String> {
        let (remote_bootstrap, ip_record, api_record, ip_exports, api_exports) = {
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
                guard.persisted_received_ip_network.clone(),
                guard.persisted_received_api_session.clone(),
                session.ip_network_exports.clone(),
                session.api_session_exports.clone(),
            )
        };

        if let Some(record) = ip_record {
            if ip_exports.iter().any(|export| export.id == record.export_id) {
                let (label, client) =
                    crate::fetch_remote_ip_network_export(remote_bootstrap.clone(), &record.export_id)
                        .await?;
                self.activate_imported_remote_ip_network(
                    record.object_id.clone(),
                    record.export_id.clone(),
                    label,
                    client,
                    true,
                )?;
            }
        }

        if let Some(record) = api_record {
            if api_exports.iter().any(|export| export.id == record.export_id) {
                let (label, client) =
                    crate::fetch_remote_api_session_export(remote_bootstrap, &record.export_id)
                        .await?;
                self.activate_imported_remote_api_session(
                    record.object_id.clone(),
                    record.export_id.clone(),
                    label,
                    client,
                    true,
                )?;
            }
        }

        Ok(())
    }

    pub(crate) fn disconnect_peer_rpc_session(&self) -> Result<(), String> {
        let mut guard = self
            .state
            .lock()
            .map_err(|_| "app state lock poisoned".to_string())?;
        self.close_peer_rpc_session_locked(&mut guard, b"peer rpc disconnected");
        guard.peer_rpc_error = None;
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
            self.persisted_received_capability_for_kind(
                &guard,
                crate::ImportedRemoteCapabilityKind::IpNetwork,
            )
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
            self.persisted_received_capability_for_kind(
                &guard,
                crate::ImportedRemoteCapabilityKind::ApiSession,
            )
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
                "{{\"id\":\"{}\",\"objectId\":\"{}\",\"label\":\"{}\",\"savedToken\":\"{}\",\"createdAtMs\":{}}}",
                crate::json_escape(&row.id),
                crate::json_escape(&row.id),
                crate::json_escape(&row.label),
                crate::json_escape(&row.saved_token),
                row.created_at_ms
            ));
        }

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
                    "{{\"connected\":true,\"sessionId\":{},\"remoteNodeId\":\"{}\",\"ipNetworkExports\":[{}],\"apiSessionExports\":[{}]}}",
                    session.session_id,
                    crate::json_escape(&session.remote_node_id),
                    ip_network_exports,
                    api_session_exports
                )
            }
            None => {
                "{\"connected\":false,\"ipNetworkExports\":[],\"apiSessionExports\":[]}".to_string()
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
                    "{{\"objectId\":\"{}\",\"exportId\":\"{}\",\"label\":\"{}\",\"kind\":\"{}\"}}",
                    crate::json_escape(&cap.object_id),
                    crate::json_escape(&cap.export_id),
                    crate::json_escape(&cap.label),
                    crate::json_escape(crate::imported_kind_label(cap.kind))
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let persisted_received_caps = [
            guard.persisted_received_ip_network.as_ref(),
            guard.persisted_received_api_session.as_ref(),
        ]
        .into_iter()
        .flatten()
        .map(|cap| {
            format!(
                "{{\"objectId\":\"{}\",\"exportId\":\"{}\",\"label\":\"{}\",\"kind\":\"{}\"}}",
                crate::json_escape(&cap.object_id),
                crate::json_escape(&cap.export_id),
                crate::json_escape(&cap.label),
                crate::json_escape(crate::imported_kind_label(cap.kind))
            )
        })
        .collect::<Vec<_>>()
        .join(",");
        let remote_ticket = match &guard.remote_ticket {
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

        Ok(format!(
            "{{\"powerboxQueries\":{{\"apiSession\":\"{}\",\"ipNetwork\":\"{}\",\"ipInterface\":\"{}\"}},\"irohNodeId\":\"{}\",\"irohEndpoint\":{{\"bound\":{},\"nodeId\":\"{}\",\"relayUrls\":[{}],\"directAddrs\":[{}],\"customAddrs\":[{}],\"error\":{},\"localTicket\":{},\"rawUdpInterface\":{}}},\"peerRpc\":{},\"peerRpcError\":{},\"exportedIpNetwork\":{},\"exportedApiSession\":{},\"importedRemoteIpNetwork\":{},\"importedRemoteApiSession\":{},\"importedRemoteCaps\":[{}],\"persistedReceivedCaps\":[{}],\"transportAssessment\":\"{}\",\"remoteTicket\":{},\"savedCaps\":[{}],\"activeTcpSessions\":[{}]}}",
            crate::json_escape(&crate::powerbox_query_for_interface(
                crate::api_session_capnp::api_session::Client::TYPE_ID
            )?),
            crate::json_escape(&crate::powerbox_query_for_interface(
                crate::ip_capnp::ip_network::Client::TYPE_ID
            )?),
            crate::json_escape(&crate::powerbox_query_for_interface(
                crate::ip_capnp::ip_interface::Client::TYPE_ID
            )?),
            crate::json_escape(&guard.iroh_identity.node_id),
            endpoint_bound,
            crate::json_escape(&guard.iroh_endpoint_addr.node_id),
            relay_urls,
            direct_addrs,
            custom_addrs,
            endpoint_error,
            local_ticket,
            raw_udp_interface,
            peer_rpc,
            peer_rpc_error,
            exported_ip_network,
            exported_api_session,
            imported_remote_ip_network,
            imported_remote_api_session,
            imported_remote_caps,
            persisted_received_caps,
            crate::json_escape(crate::IROH_TRANSPORT_ASSESSMENT),
            remote_ticket,
            rows.join(","),
            active_tcp_sessions.join(",")
        ))
    }

    fn persist_received_capability_registry(
        &self,
        ip_network: Option<&crate::PersistedReceivedCapability>,
        api_session: Option<&crate::PersistedReceivedCapability>,
    ) -> Result<(), String> {
        let convert =
            |record: &crate::PersistedReceivedCapability| PersistedReceivedCapabilityRecord {
                object_id: record.object_id.clone(),
                export_id: record.export_id.clone(),
                label: record.label.clone(),
                kind: match record.kind {
                    crate::ImportedRemoteCapabilityKind::IpNetwork => {
                        ReceivedCapabilityKind::IpNetwork
                    }
                    crate::ImportedRemoteCapabilityKind::ApiSession => {
                        ReceivedCapabilityKind::ApiSession
                    }
                },
            };
        self.storage.persist_received_capability_registry(
            ip_network.map(convert).as_ref(),
            api_session.map(convert).as_ref(),
        )
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

    fn persisted_received_capability_for_kind<'a>(
        &self,
        guard: &'a crate::AppState,
        kind: crate::ImportedRemoteCapabilityKind,
    ) -> Option<&'a crate::PersistedReceivedCapability> {
        match kind {
            crate::ImportedRemoteCapabilityKind::IpNetwork => {
                guard.persisted_received_ip_network.as_ref()
            }
            crate::ImportedRemoteCapabilityKind::ApiSession => {
                guard.persisted_received_api_session.as_ref()
            }
        }
    }

    fn set_persisted_received_capability(
        &self,
        guard: &mut crate::AppState,
        record: Option<crate::PersistedReceivedCapability>,
        kind: crate::ImportedRemoteCapabilityKind,
    ) {
        match kind {
            crate::ImportedRemoteCapabilityKind::IpNetwork => {
                guard.persisted_received_ip_network = record
            }
            crate::ImportedRemoteCapabilityKind::ApiSession => {
                guard.persisted_received_api_session = record
            }
        }
    }

    fn remove_imported_remote_capability_by_kind(
        &self,
        guard: &mut crate::AppState,
        kind: crate::ImportedRemoteCapabilityKind,
    ) -> bool {
        let object_id = match kind {
            crate::ImportedRemoteCapabilityKind::IpNetwork => guard
                .imported_remote_ip_network
                .as_ref()
                .map(|value| value.object_id.clone()),
            crate::ImportedRemoteCapabilityKind::ApiSession => guard
                .imported_remote_api_session
                .as_ref()
                .map(|value| value.object_id.clone()),
        };
        let Some(object_id) = object_id else {
            return false;
        };
        guard.imported_remote_caps.remove(&object_id);
        match kind {
            crate::ImportedRemoteCapabilityKind::IpNetwork => guard.imported_remote_ip_network = None,
            crate::ImportedRemoteCapabilityKind::ApiSession => {
                guard.imported_remote_api_session = None
            }
        }
        true
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
    }

    fn activate_imported_remote_ip_network(
        &self,
        object_id: String,
        export_id: String,
        label: String,
        client: crate::ip_capnp::ip_network::Client,
        persist_record: bool,
    ) -> Result<(), String> {
        let (ip_record, api_record) = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            let previous_object_id = guard
                .imported_remote_ip_network
                .as_ref()
                .map(|value| value.object_id.clone());
            let capability = crate::ImportedRemoteCapability {
                object_id: object_id.clone(),
                export_id: export_id.clone(),
                label: label.clone(),
                kind: crate::ImportedRemoteCapabilityKind::IpNetwork,
                client: client.client.clone(),
            };
            self.replace_imported_remote_capability(&mut guard, previous_object_id, capability);
            guard.imported_remote_ip_network = Some(crate::ImportedRemoteIpNetwork {
                object_id: object_id.clone(),
                export_id: export_id.clone(),
                label: label.clone(),
                client,
            });
            if persist_record {
                self.set_persisted_received_capability(
                    &mut guard,
                    Some(crate::PersistedReceivedCapability {
                        object_id: object_id.clone(),
                        export_id: export_id.clone(),
                        label: label.clone(),
                        kind: crate::ImportedRemoteCapabilityKind::IpNetwork,
                    }),
                    crate::ImportedRemoteCapabilityKind::IpNetwork,
                );
            }
            guard.peer_rpc_error = None;
            (
                guard.persisted_received_ip_network.clone(),
                guard.persisted_received_api_session.clone(),
            )
        };
        if persist_record {
            self.persist_received_capability_registry(ip_record.as_ref(), api_record.as_ref())?;
        }
        Ok(())
    }

    fn activate_imported_remote_api_session(
        &self,
        object_id: String,
        export_id: String,
        label: String,
        client: crate::api_session_capnp::api_session::Client,
        persist_record: bool,
    ) -> Result<(), String> {
        let (ip_record, api_record) = {
            let mut guard = self
                .state
                .lock()
                .map_err(|_| "app state lock poisoned".to_string())?;
            let previous_object_id = guard
                .imported_remote_api_session
                .as_ref()
                .map(|value| value.object_id.clone());
            let capability = crate::ImportedRemoteCapability {
                object_id: object_id.clone(),
                export_id: export_id.clone(),
                label: label.clone(),
                kind: crate::ImportedRemoteCapabilityKind::ApiSession,
                client: client.client.clone(),
            };
            self.replace_imported_remote_capability(&mut guard, previous_object_id, capability);
            guard.imported_remote_api_session = Some(crate::ImportedRemoteApiSession {
                object_id: object_id.clone(),
                export_id: export_id.clone(),
                label: label.clone(),
                client,
            });
            if persist_record {
                self.set_persisted_received_capability(
                    &mut guard,
                    Some(crate::PersistedReceivedCapability {
                        object_id: object_id.clone(),
                        export_id: export_id.clone(),
                        label: label.clone(),
                        kind: crate::ImportedRemoteCapabilityKind::ApiSession,
                    }),
                    crate::ImportedRemoteCapabilityKind::ApiSession,
                );
            }
            guard.peer_rpc_error = None;
            (
                guard.persisted_received_ip_network.clone(),
                guard.persisted_received_api_session.clone(),
            )
        };
        if persist_record {
            self.persist_received_capability_registry(ip_record.as_ref(), api_record.as_ref())?;
        }
        Ok(())
    }
}
