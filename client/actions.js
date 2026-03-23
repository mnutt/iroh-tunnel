import {
  legacyCopyText,
  postText,
  responseTextToStatus,
} from "./http.js";

export async function connectTunnel(context) {
  const { remoteTicketEl, setStatus, refreshState } = context;
  const remoteTicket = (remoteTicketEl.value || "").trim();
  if (!remoteTicket) {
    setStatus("Paste a remote grain ticket first.");
    return;
  }

  setStatus("Saving remote ticket...");
  let response = await postText("api/pairing/remote-ticket", remoteTicket);
  if (!response.ok) {
    const body = await response.text();
    setStatus(responseTextToStatus("Remote ticket save failed", response, body));
    return;
  }

  setStatus("Connecting tunnel...");
  response = await fetch("api/tunnel/connect", { method: "POST" });
  if (!response.ok) {
    const body = await response.text();
    setStatus(responseTextToStatus("Tunnel connect failed", response, body));
    await refreshState();
    return;
  }
  setStatus("Connection request sent. Waiting for remote to accept.");
  await refreshState();
}

export async function acceptTunnel(context) {
  const { setStatus, refreshState } = context;
  setStatus("Accepting incoming connection...");
  const response = await fetch("api/tunnel/accept", { method: "POST" });
  if (!response.ok) {
    const body = await response.text();
    setStatus(responseTextToStatus("Accept connection failed", response, body));
    await refreshState();
    return;
  }
  setStatus("Connection accepted.");
  await refreshState();
}

export async function rejectTunnel(context) {
  const { setStatus, refreshState } = context;
  setStatus("Rejecting incoming connection...");
  const response = await fetch("api/tunnel/reject", { method: "POST" });
  if (!response.ok) {
    const body = await response.text();
    setStatus(responseTextToStatus("Reject connection failed", response, body));
    await refreshState();
    return;
  }
  setStatus("Connection rejected.");
  await refreshState();
}

export async function disconnectTunnel(context) {
  const { setStatus, refreshState } = context;
  setStatus("Disconnecting tunnel...");
  const response = await fetch("api/tunnel/rpc/disconnect", { method: "POST" });
  if (!response.ok) {
    const body = await response.text();
    setStatus(responseTextToStatus("Tunnel disconnect failed", response, body));
    return;
  }
  setStatus("Tunnel disconnected.");
  await refreshState();
}

export async function enableTunnel(context) {
  const { setStatus, refreshState } = context;
  setStatus("Enabling tunnel...");
  const response = await fetch("api/tunnel/enable", { method: "POST" });
  if (!response.ok) {
    const body = await response.text();
    setStatus(responseTextToStatus("Enable tunnel failed", response, body));
    await refreshState();
    return;
  }
  setStatus("Tunnel enabled.");
  await refreshState();
}

export async function disableTunnel(context) {
  const { setStatus, refreshState } = context;
  setStatus("Disabling tunnel...");
  const response = await fetch("api/tunnel/disable", { method: "POST" });
  if (!response.ok) {
    const body = await response.text();
    setStatus(responseTextToStatus("Disable tunnel failed", response, body));
    await refreshState();
    return;
  }
  setStatus("Tunnel disabled.");
  await refreshState();
}

export async function forgetConnection(context) {
  const { setStatus, refreshState } = context;
  if (!window.confirm("Forget peer?\n\nThis will disconnect the tunnel, clear the approved peer, and remove the saved remote ticket.")) {
    return;
  }

  const clearResponse = await fetch("api/pairing/forget", { method: "POST" });
  if (!clearResponse.ok) {
    const body = await clearResponse.text();
    setStatus(responseTextToStatus("Forget peer failed", clearResponse, body));
    return;
  }

  setStatus("Peer forgotten.");
  await refreshState();
}

export async function copyTicket(context) {
  const { localTicketEl, setStatus } = context;
  const ticket = localTicketEl.value || "";
  if (!ticket.trim()) {
    setStatus("No local ticket is available yet.");
    return;
  }
  try {
    if (navigator.clipboard && typeof navigator.clipboard.writeText === "function") {
      await navigator.clipboard.writeText(ticket);
    } else if (!legacyCopyText(ticket)) {
      throw new Error("clipboard API unavailable");
    }
    setStatus("Copied local ticket.");
  } catch (error) {
    setStatus(`Copy failed: ${error}`);
  }
}

export function requestPowerboxCapability(context, query, saveLabel, options = {}) {
  const { setStatus, refreshState } = context;
  const { afterClaim, resolveSaveLabel } = options;
  const rpcId = `iroh-tunnel-${Date.now()}-${Math.random().toString(16).slice(2)}`;
  setStatus("Waiting for Powerbox selection...");

  const onMessage = async (event) => {
    const message = event.data || {};
    if (message.rpcId !== rpcId) return;
    window.removeEventListener("message", onMessage, false);

    if (message.error) {
      setStatus(`Powerbox error: ${message.error}`);
      return;
    }

    if (!message.token) {
      setStatus("Powerbox returned no token.");
      return;
    }

    let effectiveSaveLabel = saveLabel;
    if (resolveSaveLabel) {
      const resolved = resolveSaveLabel(message.descriptor);
      if (!resolved) {
        setStatus("Capability save canceled.");
        return;
      }
      effectiveSaveLabel = resolved;
    }

    setStatus("Claiming and saving capability...");
    const claimResponse = await fetch("api/powerbox/claim", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        token: message.token,
        label: effectiveSaveLabel,
        descriptor: message.descriptor ?? null,
      }),
    });
    if (!claimResponse.ok) {
      setStatus(`Claim failed with HTTP ${claimResponse.status}.`);
      return;
    }
    const result = await claimResponse.json();
    if (!result.ok) {
      setStatus("Claim failed.");
      return;
    }

    if (afterClaim) {
      try {
        await afterClaim(result);
      } catch (error) {
        setStatus(`Capability saved but follow-up setup failed: ${error}`);
        await refreshState();
        return;
      }
    }

    setStatus(`Saved capability token: ${result.savedToken}`);
    await refreshState();
  };

  window.addEventListener("message", onMessage, false);
  window.parent.postMessage({
    powerboxRequest: {
      rpcId,
      query: [query],
      saveLabel: { defaultText: saveLabel },
    },
  }, "*");
}
