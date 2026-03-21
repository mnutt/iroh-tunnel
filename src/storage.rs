use std::fs::OpenOptions;
use std::io::Write as _;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub struct SavedCapabilityRecord {
    pub id: String,
    pub label: String,
    pub saved_token: String,
    pub created_at_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReceivedCapabilityKind {
    IpNetwork,
    ApiSession,
}

impl ReceivedCapabilityKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::IpNetwork => "IpNetwork",
            Self::ApiSession => "ApiSession",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "IpNetwork" => Some(Self::IpNetwork),
            "ApiSession" => Some(Self::ApiSession),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct PersistedReceivedCapabilityRecord {
    pub object_id: String,
    pub export_id: String,
    pub label: String,
    pub kind: ReceivedCapabilityKind,
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
                });
            }
        }
        Ok(rows)
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
            "{}\t{}\t{}\t{}",
            record.id, record.label, record.saved_token, record.created_at_ms
        )
        .map_err(|err| format!("failed to persist saved capability: {err}"))
    }

    pub fn load_persisted_received_capabilities(
        &self,
    ) -> Result<
        (
            Option<PersistedReceivedCapabilityRecord>,
            Option<PersistedReceivedCapabilityRecord>,
        ),
        String,
    > {
        let contents = match std::fs::read_to_string(self.received_caps_path()) {
            Ok(contents) => contents,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok((None, None)),
            Err(err) => return Err(format!("failed to read received capability registry: {err}")),
        };

        let mut ip_network = None;
        let mut api_session = None;
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
            };
            match kind {
                ReceivedCapabilityKind::IpNetwork => ip_network = Some(record),
                ReceivedCapabilityKind::ApiSession => api_session = Some(record),
            }
        }
        Ok((ip_network, api_session))
    }

    pub fn persist_received_capability_registry(
        &self,
        ip_network: Option<&PersistedReceivedCapabilityRecord>,
        api_session: Option<&PersistedReceivedCapabilityRecord>,
    ) -> Result<(), String> {
        self.ensure_root()?;
        let mut rows = Vec::new();
        for record in [ip_network, api_session].into_iter().flatten() {
            rows.push(format!(
                "{}\t{}\t{}\t{}",
                record.kind.as_str(),
                record.object_id,
                record.export_id,
                record.label
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
