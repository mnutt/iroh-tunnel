use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

fn default_enabled() -> bool {
    true
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SavedCapabilityRecord {
    pub id: String,
    pub label: String,
    pub saved_token: String,
    pub created_at_ms: u64,
    pub descriptor_json: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReceivedCapabilityKind {
    IpNetwork,
    ApiSession,
    Other,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SharedCapabilityKind {
    IpNetwork,
    ApiSession,
    Other,
}

impl SharedCapabilityKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::IpNetwork => "IpNetwork",
            Self::ApiSession => "ApiSession",
            Self::Other => "Other",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SharedCapabilityRecord {
    pub id: String,
    pub saved_cap_id: String,
    pub label: String,
    pub kind: SharedCapabilityKind,
    pub type_tag: Option<String>,
    pub enabled: bool,
    pub created_at_ms: u64,
    pub descriptor_json: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PersistedReceivedCapabilityRecord {
    pub object_id: String,
    pub export_id: String,
    pub label: String,
    pub kind: ReceivedCapabilityKind,
    pub type_tag: Option<String>,
    pub descriptor_json: Option<String>,
    pub enabled: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LocalProxyTargetKind {
    ExportId,
    RemoteObjectId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LocalProxyCapabilityRecord {
    pub object_id: String,
    pub peer_node_id: String,
    pub target_kind: LocalProxyTargetKind,
    pub target_id: String,
    pub label: String,
    pub kind: SharedCapabilityKind,
    pub type_tag: Option<String>,
    pub descriptor_json: Option<String>,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    pub created_at_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RegisteredRemoteCapabilityDurability {
    Saveable,
    Ephemeral,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegisteredRemoteCapabilityRecord {
    pub remote_object_id: String,
    pub label: String,
    pub kind: ReceivedCapabilityKind,
    pub type_tag: Option<String>,
    pub descriptor_json: Option<String>,
    pub durability: RegisteredRemoteCapabilityDurability,
    pub saved_token: Option<String>,
    pub created_at_ms: u64,
}

#[derive(Clone, Debug)]
pub struct Storage {
    root: PathBuf,
}

impl Storage {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn saved_caps_path(&self) -> PathBuf {
        self.root.join("saved-caps.json")
    }

    pub fn shared_caps_path(&self) -> PathBuf {
        self.root.join("shared-caps.json")
    }

    pub fn received_caps_path(&self) -> PathBuf {
        self.root.join("received-caps.json")
    }

    pub fn local_proxy_caps_path(&self) -> PathBuf {
        self.root.join("local-proxy-caps.json")
    }

    pub fn registered_remote_caps_path(&self) -> PathBuf {
        self.root.join("registered-remote-caps.json")
    }

    fn read_json<T>(&self, path: PathBuf, read_error: &str) -> Result<Vec<T>, String>
    where
        T: for<'de> Deserialize<'de>,
    {
        match std::fs::read_to_string(&path) {
            Ok(contents) => serde_json::from_str(&contents)
                .map_err(|err| format!("{read_error}: failed to parse JSON {}: {err}", path.display())),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(err) => Err(format!("{read_error}: failed to read {}: {err}", path.display())),
        }
    }

    fn write_json<T: Serialize>(
        &self,
        path: PathBuf,
        records: &[T],
        write_error: &str,
    ) -> Result<(), String> {
        self.ensure_root()?;
        let body = serde_json::to_string_pretty(records)
            .map_err(|err| format!("{write_error}: failed to encode JSON: {err}"))?;
        std::fs::write(path, body).map_err(|err| format!("{write_error}: {err}"))
    }

    pub fn exported_ip_network_id_path(&self) -> PathBuf {
        self.root.join("exported-ip-network-id")
    }

    pub fn exported_api_session_id_path(&self) -> PathBuf {
        self.root.join("exported-api-session-id")
    }

    pub fn raw_udp_interface_token_path(&self) -> PathBuf {
        self.root.join("raw-udp-interface-token")
    }

    pub fn raw_udp_port_path(&self) -> PathBuf {
        self.root.join("raw-udp-port")
    }

    pub fn iroh_secret_key_path(&self) -> PathBuf {
        self.root.join("iroh-secret-key")
    }

    pub fn remote_ticket_path(&self) -> PathBuf {
        self.root.join("remote-ticket.txt")
    }

    pub fn approved_peer_node_id_path(&self) -> PathBuf {
        self.root.join("approved-peer-node-id")
    }

    pub fn tunnel_enabled_path(&self) -> PathBuf {
        self.root.join("tunnel-enabled")
    }

    pub fn ensure_root(&self) -> Result<(), String> {
        std::fs::create_dir_all(&self.root)
            .map_err(|err| format!("failed to create state directory {}: {err}", self.root.display()))
    }

    pub fn load_saved_capabilities(&self) -> Result<Vec<SavedCapabilityRecord>, String> {
        self.read_json(self.saved_caps_path(), "failed to load saved capability registry")
    }

    pub fn load_shared_capabilities(&self) -> Result<Vec<SharedCapabilityRecord>, String> {
        self.read_json(self.shared_caps_path(), "failed to load shared capability registry")
    }

    pub fn persist_shared_capabilities(
        &self,
        records: &[SharedCapabilityRecord],
    ) -> Result<(), String> {
        self.write_json(
            self.shared_caps_path(),
            records,
            "failed to persist shared capability registry",
        )
    }

    pub fn persist_saved_capability(&self, record: &SavedCapabilityRecord) -> Result<(), String> {
        let mut records = self.load_saved_capabilities()?;
        records.push(record.clone());
        self.write_json(
            self.saved_caps_path(),
            &records,
            "failed to persist saved capability registry",
        )
    }

    pub fn load_persisted_received_capabilities(
        &self,
    ) -> Result<Vec<PersistedReceivedCapabilityRecord>, String> {
        self.read_json(
            self.received_caps_path(),
            "failed to load received capability registry",
        )
    }

    pub fn load_local_proxy_capabilities(&self) -> Result<Vec<LocalProxyCapabilityRecord>, String> {
        self.read_json(
            self.local_proxy_caps_path(),
            "failed to load local proxy capability registry",
        )
    }

    pub fn load_registered_remote_capabilities(
        &self,
    ) -> Result<Vec<RegisteredRemoteCapabilityRecord>, String> {
        self.read_json(
            self.registered_remote_caps_path(),
            "failed to load registered remote capability registry",
        )
    }

    pub fn persist_received_capability_registry(
        &self,
        records: &[PersistedReceivedCapabilityRecord],
    ) -> Result<(), String> {
        self.write_json(
            self.received_caps_path(),
            records,
            "failed to persist received capability registry",
        )
    }

    pub fn persist_local_proxy_capability_registry(
        &self,
        records: &[LocalProxyCapabilityRecord],
    ) -> Result<(), String> {
        self.write_json(
            self.local_proxy_caps_path(),
            records,
            "failed to persist local proxy capability registry",
        )
    }

    pub fn persist_registered_remote_capability_registry(
        &self,
        records: &[RegisteredRemoteCapabilityRecord],
    ) -> Result<(), String> {
        self.write_json(
            self.registered_remote_caps_path(),
            records,
            "failed to persist registered remote capability registry",
        )
    }

    pub fn load_text_file(&self, path: &Path) -> Result<Option<String>, String> {
        match std::fs::read_to_string(path) {
            Ok(value) => {
                let trimmed = value.trim().to_string();
                if trimmed.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(trimmed))
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(format!("failed to read {}: {err}", path.display())),
        }
    }

    pub fn persist_text_file(&self, path: &Path, value: &str) -> Result<(), String> {
        self.ensure_root()?;
        std::fs::write(path, format!("{value}\n"))
            .map_err(|err| format!("failed to persist {}: {err}", path.display()))
    }

    pub fn clear_file(&self, path: &Path) -> Result<(), String> {
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(format!("failed to clear {}: {err}", path.display())),
        }
    }
}
