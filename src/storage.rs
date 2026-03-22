use std::fs::OpenOptions;
use std::io::Write as _;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub struct SavedCapabilityRecord {
    pub id: String,
    pub label: String,
    pub saved_token: String,
    pub created_at_ms: u64,
    pub descriptor_json: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReceivedCapabilityKind {
    IpNetwork,
    ApiSession,
    Other,
}

impl ReceivedCapabilityKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::IpNetwork => "IpNetwork",
            Self::ApiSession => "ApiSession",
            Self::Other => "Other",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "IpNetwork" => Some(Self::IpNetwork),
            "ApiSession" => Some(Self::ApiSession),
            "Other" => Some(Self::Other),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "IpNetwork" => Some(Self::IpNetwork),
            "ApiSession" => Some(Self::ApiSession),
            "Other" => Some(Self::Other),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
pub struct PersistedReceivedCapabilityRecord {
    pub object_id: String,
    pub export_id: String,
    pub label: String,
    pub kind: ReceivedCapabilityKind,
    pub type_tag: Option<String>,
    pub descriptor_json: Option<String>,
    pub enabled: bool,
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
        self.root.join("saved-caps.tsv")
    }

    pub fn shared_caps_path(&self) -> PathBuf {
        self.root.join("shared-caps.tsv")
    }

    pub fn received_caps_path(&self) -> PathBuf {
        self.root.join("received-caps.tsv")
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
        let contents = match std::fs::read_to_string(self.saved_caps_path()) {
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
                rows.push(SavedCapabilityRecord {
                    id: parts[0].to_string(),
                    label: parts[1].to_string(),
                    saved_token: parts[2].to_string(),
                    created_at_ms: parts[3].parse().unwrap_or(0),
                    descriptor_json: parts.get(4).map(|value| value.to_string()).filter(|value| !value.is_empty()),
                });
            }
        }
        Ok(rows)
    }

    pub fn load_shared_capabilities(&self) -> Result<Vec<SharedCapabilityRecord>, String> {
        let contents = match std::fs::read_to_string(self.shared_caps_path()) {
            Ok(contents) => contents,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(format!("failed to read shared capability registry: {err}")),
        };

        let mut rows = Vec::new();
        for line in contents.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let parts: Vec<_> = line.split('\t').collect();
            if parts.len() < 6 {
                continue;
            }
            let Some(kind) = SharedCapabilityKind::parse(parts[3]) else {
                continue;
            };
            rows.push(SharedCapabilityRecord {
                id: parts[0].to_string(),
                saved_cap_id: parts[1].to_string(),
                label: parts[2].to_string(),
                kind,
                type_tag: parts
                    .get(6)
                    .map(|value| value.to_string())
                    .filter(|value| !value.is_empty()),
                enabled: parts[4] == "true",
                created_at_ms: parts[5].parse().unwrap_or(0),
                descriptor_json: parts
                    .get(7)
                    .map(|value| value.to_string())
                    .filter(|value| !value.is_empty())
                    .or_else(|| {
                        parts.get(6).and_then(|value| {
                            if value.starts_with('{') || value.starts_with('[') || value.starts_with('"') {
                                Some((*value).to_string())
                            } else {
                                None
                            }
                        })
                    }),
            });
        }
        Ok(rows)
    }

    pub fn persist_shared_capabilities(
        &self,
        records: &[SharedCapabilityRecord],
    ) -> Result<(), String> {
        self.ensure_root()?;
        let mut rows = Vec::new();
        for record in records {
            rows.push(format!(
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                record.id,
                record.saved_cap_id,
                record.label,
                record.kind.as_str(),
                if record.enabled { "true" } else { "false" },
                record.created_at_ms,
                record.type_tag.as_deref().unwrap_or(""),
                record.descriptor_json.as_deref().unwrap_or("")
            ));
        }
        let body = if rows.is_empty() {
            String::new()
        } else {
            format!("{}\n", rows.join("\n"))
        };
        std::fs::write(self.shared_caps_path(), body)
            .map_err(|err| format!("failed to persist shared capability registry: {err}"))
    }

    pub fn persist_saved_capability(&self, record: &SavedCapabilityRecord) -> Result<(), String> {
        self.ensure_root()?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.saved_caps_path())
            .map_err(|err| format!("failed to open saved capability registry: {err}"))?;
        writeln!(
            file,
            "{}\t{}\t{}\t{}\t{}",
            record.id,
            record.label,
            record.saved_token,
            record.created_at_ms,
            record.descriptor_json.as_deref().unwrap_or("")
        )
        .map_err(|err| format!("failed to persist saved capability: {err}"))
    }

    pub fn load_persisted_received_capabilities(
        &self,
    ) -> Result<Vec<PersistedReceivedCapabilityRecord>, String> {
        let contents = match std::fs::read_to_string(self.received_caps_path()) {
            Ok(contents) => contents,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => return Err(format!("failed to read received capability registry: {err}")),
        };

        let mut rows = Vec::new();
        for line in contents.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let parts: Vec<_> = line.split('\t').collect();
            if parts.len() < 4 {
                continue;
            }
            let Some(kind) = ReceivedCapabilityKind::parse(parts[0]) else {
                continue;
            };
            let record = PersistedReceivedCapabilityRecord {
                object_id: parts[1].to_string(),
                export_id: parts[2].to_string(),
                label: parts[3].to_string(),
                kind,
                type_tag: parts
                    .get(5)
                    .map(|value| value.to_string())
                    .filter(|value| !value.is_empty()),
                descriptor_json: parts
                    .get(6)
                    .map(|value| value.to_string())
                    .filter(|value| !value.is_empty()),
                enabled: parts.get(4).map(|value| *value != "false").unwrap_or(true),
            };
            rows.push(record);
        }
        Ok(rows)
    }

    pub fn persist_received_capability_registry(
        &self,
        records: &[PersistedReceivedCapabilityRecord],
    ) -> Result<(), String> {
        self.ensure_root()?;
        let mut rows = Vec::new();
        for record in records {
            rows.push(format!(
                "{}\t{}\t{}\t{}\t{}\t{}\t{}",
                record.kind.as_str(),
                record.object_id,
                record.export_id,
                record.label,
                if record.enabled { "true" } else { "false" },
                record.type_tag.as_deref().unwrap_or(""),
                record.descriptor_json.as_deref().unwrap_or("")
            ));
        }
        let body = if rows.is_empty() {
            String::new()
        } else {
            format!("{}\n", rows.join("\n"))
        };
        std::fs::write(self.received_caps_path(), body)
            .map_err(|err| format!("failed to persist received capability registry: {err}"))
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
