import { responseTextToStatus } from "./http.js";
import {
  acceptTunnel,
  connectTunnel,
  copyTicket,
  disableTunnel,
  disconnectTunnel,
  enableTunnel,
  forgetConnection,
  requestPowerboxCapability,
  rejectTunnel,
} from "./actions.js";
import { renderApp } from "./render.js";

const statusEl = document.getElementById("status");
const heroSectionEl = document.getElementById("hero-section");
const nodeIdEl = document.getElementById("node-id");
const endpointStatusEl = document.getElementById("endpoint-status");
const rawUdpInterfaceEl = document.getElementById("raw-udp-interface");
const customTransportPillEl = document.getElementById("custom-transport-pill");
const peerRpcPillEl = document.getElementById("peer-rpc-pill");
const tunnelStatusPillEl = document.getElementById("tunnel-status-pill");
const tunnelHelperEl = document.getElementById("tunnel-helper");
const shareInlineNoteEl = document.getElementById("share-inline-note");
const localTicketEl = document.getElementById("local-ticket");
const remoteTicketEl = document.getElementById("remote-ticket");
const peerRpcStatusEl = document.getElementById("peer-rpc-status");
const peerRpcErrorEl = document.getElementById("peer-rpc-error");
const sharedCapsEl = document.getElementById("shared-caps");
const sharePanelEl = document.getElementById("share-panel");
const receivedCapsEl = document.getElementById("received-caps");
const receivedPanelEl = document.getElementById("received-panel");
const receivedTitleEl = document.getElementById("received-title");
const receivedCopyEl = document.getElementById("received-copy");
const savedCapsEl = document.getElementById("saved-caps");
const requestButton = document.getElementById("request-cap");
const requestIpNetworkButton = document.getElementById("request-ip-network");
const requestIpInterfaceButton = document.getElementById("request-ip-interface");
const connectTunnelButton = document.getElementById("connect-tunnel");
const toggleTunnelButton = document.getElementById("toggle-tunnel");
const acceptTunnelButton = document.getElementById("accept-tunnel");
const rejectTunnelButton = document.getElementById("reject-tunnel");
const disconnectPeerRpcButton = document.getElementById("disconnect-peer-rpc");
const clearTicketButton = document.getElementById("clear-ticket");
const copyTicketButton = document.getElementById("copy-ticket");
const tunnelPanelEl = document.getElementById("tunnel-panel");

let powerboxQueries = {
  apiSession: "",
  ipNetwork: "",
  ipInterface: "",
};
let currentState = null;
let refreshTimerId = 0;
let refreshInFlight = false;

function setStatus(text) {
  statusEl.textContent = text;
}

const renderContext = {
  currentState,
  heroSectionEl,
  nodeIdEl,
  endpointStatusEl,
  rawUdpInterfaceEl,
  customTransportPillEl,
  peerRpcPillEl,
  tunnelStatusPillEl,
  tunnelHelperEl,
  shareInlineNoteEl,
  localTicketEl,
  remoteTicketEl,
  peerRpcStatusEl,
  peerRpcErrorEl,
  sharedCapsEl,
  sharePanelEl,
  receivedCapsEl,
  receivedPanelEl,
  receivedTitleEl,
  receivedCopyEl,
  savedCapsEl,
  requestIpInterfaceButton,
  connectTunnelButton,
  toggleTunnelButton,
  acceptTunnelButton,
  rejectTunnelButton,
  disconnectPeerRpcButton,
  clearTicketButton,
  tunnelPanelEl,
  setStatus,
  refreshState,
};

function renderCurrentState() {
  if (!currentState) {
    return;
  }
  renderContext.currentState = currentState;
  renderApp(renderContext, currentState);
}

function collectDescriptorStrings(value, out) {
  if (!value) {
    return;
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (trimmed && trimmed.length <= 80) {
      out.push(trimmed);
    }
    return;
  }
  if (Array.isArray(value)) {
    for (const entry of value) {
      collectDescriptorStrings(entry, out);
    }
    return;
  }
  if (typeof value === "object") {
    const preferredKeys = ["title", "name", "displayName", "defaultText", "text", "verbPhrase"];
    for (const key of preferredKeys) {
      if (Object.prototype.hasOwnProperty.call(value, key)) {
        collectDescriptorStrings(value[key], out);
      }
    }
    for (const [key, entry] of Object.entries(value)) {
      if (!preferredKeys.includes(key)) {
        collectDescriptorStrings(entry, out);
      }
    }
  }
}

function extractTagStringFromDescriptor(descriptor) {
  if (typeof descriptor !== "string" || !descriptor.trim()) {
    return null;
  }
  try {
    const bytes = atob(descriptor.trim());
    const filtered = Array.from(bytes)
      .filter((ch) => /[A-Za-z0-9._/-]/.test(ch))
      .join("");
    const matches = filtered.match(/org\.[A-Za-z0-9._-]+(?:\.[A-Za-z0-9._-]+)*\/v\d+/g);
    if (!matches || !matches.length) {
      return null;
    }
    return matches[0];
  } catch {
    return null;
  }
}

function deriveSuggestedCapabilityLabel(descriptor, fallbackLabel) {
  const tagString = extractTagStringFromDescriptor(descriptor);
  if (tagString) {
    return tagString;
  }
  const candidates = [];
  collectDescriptorStrings(descriptor, candidates);
  const preferred = candidates.find((value) => /[A-Za-z]/.test(value) && !/^https?:\/\//i.test(value));
  return preferred || fallbackLabel;
}

async function refreshState() {
  if (refreshInFlight) {
    return;
  }
  refreshInFlight = true;
  try {
    const response = await fetch("api/state");
    const data = await response.json();
    currentState = data;
    powerboxQueries = data.powerboxQueries || powerboxQueries;

    renderCurrentState();
  } finally {
    refreshInFlight = false;
    scheduleRefresh(currentState);
  }
}

function scheduleRefresh(data) {
  if (refreshTimerId) {
    window.clearTimeout(refreshTimerId);
    refreshTimerId = 0;
  }
  const isPowerboxRequestSession =
    !!(data && data.powerboxRequestSession && data.powerboxRequestSession.active);
  if (isPowerboxRequestSession) {
    return;
  }
  const pairing = (data && data.pairing) || {};
  const status = pairing.status || "";
  const delayMs = document.hidden
    ? 15000
    : (status === "awaitingRemoteAccept" || status === "connecting" || status === "incomingRequest")
      ? 1000
      : 5000;
  if (delayMs > 0) {
    refreshTimerId = window.setTimeout(() => {
      refreshState().catch((error) => {
        setStatus(`Failed to refresh state: ${error}`);
      });
    }, delayMs);
  }
}

const appContext = {
  localTicketEl,
  remoteTicketEl,
  setStatus,
  refreshState,
};

if (remoteTicketEl) {
  remoteTicketEl.addEventListener("input", () => {
    renderCurrentState();
  });
}

document.addEventListener("visibilitychange", () => {
  const isPowerboxRequestSession =
    !!(currentState && currentState.powerboxRequestSession && currentState.powerboxRequestSession.active);
  if (isPowerboxRequestSession) {
    return;
  }
  if (!document.hidden) {
    refreshState().catch((error) => {
      setStatus(`Failed to refresh state: ${error}`);
    });
  } else {
    scheduleRefresh(currentState);
  }
});

window.addEventListener("focus", () => {
  const isPowerboxRequestSession =
    !!(currentState && currentState.powerboxRequestSession && currentState.powerboxRequestSession.active);
  if (isPowerboxRequestSession) {
    return;
  }
  refreshState().catch((error) => {
    setStatus(`Failed to refresh state: ${error}`);
  });
});

if (copyTicketButton) {
  copyTicketButton.addEventListener("click", async () => {
    await copyTicket(appContext);
  });
}

if (clearTicketButton) {
  clearTicketButton.addEventListener("click", async () => {
    await forgetConnection(appContext);
  });
}

if (connectTunnelButton) {
  connectTunnelButton.addEventListener("click", async () => {
    await connectTunnel(appContext);
  });
}

if (toggleTunnelButton) {
  toggleTunnelButton.addEventListener("click", async () => {
    if ((currentState && currentState.pairing && currentState.pairing.status) === "disabled") {
      await enableTunnel(appContext);
    } else {
      await disableTunnel(appContext);
    }
  });
}

if (acceptTunnelButton) {
  acceptTunnelButton.addEventListener("click", async () => {
    await acceptTunnel(appContext);
  });
}

if (rejectTunnelButton) {
  rejectTunnelButton.addEventListener("click", async () => {
    await rejectTunnel(appContext);
  });
}

if (disconnectPeerRpcButton) {
  disconnectPeerRpcButton.addEventListener("click", async () => {
    await disconnectTunnel(appContext);
  });
}

if (requestButton) {
  requestButton.addEventListener("click", () => {
    if (!powerboxQueries.apiSession) {
      setStatus("ApiSession query is not loaded yet.");
      return;
    }
    const defaultLabel = "ApiSession capability";
    requestPowerboxCapability(appContext, powerboxQueries.apiSession, defaultLabel, {
      resolveSaveLabel: (descriptor) => {
        return deriveSuggestedCapabilityLabel(descriptor, defaultLabel);
      },
      afterClaim: async (result) => {
        setStatus("Configuring shared capability...");
        const exportResponse = await fetch("api/tunnel/exported-api-session", {
          method: "POST",
          headers: { "Content-Type": "text/plain" },
          body: result.id,
        });
        if (!exportResponse.ok) {
          const body = await exportResponse.text();
          throw new Error(responseTextToStatus("Shared capability setup failed", exportResponse, body));
        }
        setStatus("Capability is now shared.");
      },
    });
  });
}

if (requestIpNetworkButton) {
  requestIpNetworkButton.addEventListener("click", () => {
    if (!powerboxQueries.ipNetwork) {
      setStatus("IpNetwork query is not loaded yet.");
      return;
    }
    requestPowerboxCapability(appContext, powerboxQueries.ipNetwork, "IpNetwork capability");
  });
}

if (requestIpInterfaceButton) {
  requestIpInterfaceButton.addEventListener("click", () => {
    if (!powerboxQueries.ipInterface) {
      setStatus("IpInterface query is not loaded yet.");
      return;
    }
    requestPowerboxCapability(appContext, powerboxQueries.ipInterface, "IpInterface capability", {
      afterClaim: async (result) => {
        setStatus("Binding tunneling capability...");
        const bindResponse = await fetch("api/endpoint/raw-udp-interface", {
          method: "POST",
          headers: { "Content-Type": "text/plain" },
          body: result.savedToken,
        });
        if (!bindResponse.ok) {
          const body = await bindResponse.text();
          throw new Error(responseTextToStatus("Tunnel capability bind failed", bindResponse, body));
        }
        setStatus("Tunneling capability ready.");
      },
    });
  });
}

refreshState().catch((error) => {
  setStatus(`Failed to load current state: ${error}`);
});
