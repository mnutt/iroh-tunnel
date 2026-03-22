import {
  legacyCopyText,
  postText,
  previewBase64Payload,
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

export async function probeTicket(context) {
  const { setStatus } = context;
  setStatus("Probing remote connection...");
  const response = await fetch("api/pairing/probe-connect", { method: "POST" });
  if (!response.ok) {
    const body = await response.text();
    setStatus(responseTextToStatus("Probe failed", response, body));
    return;
  }
  const result = await response.json();
  setStatus(`Probe succeeded with ${result.remoteNodeId}: ${result.response}`);
}

export async function importRemoteIpNetwork(context) {
  const { remoteIpNetworkExportSelectEl, setStatus, refreshState } = context;
  const exportId = remoteIpNetworkExportSelectEl.value;
  if (!exportId) {
    setStatus("Select a remote IpNetwork export first.");
    return;
  }
  setStatus(`Importing remote IpNetwork ${exportId}...`);
  const response = await postText("api/tunnel/rpc/import-ip-network", exportId);
  if (!response.ok) {
    const body = await response.text();
    setStatus(responseTextToStatus("Remote IpNetwork import failed", response, body));
    return;
  }
  const result = await response.json();
  setStatus(`Imported remote IpNetwork ${result.label} as object ${result.objectId}.`);
  await refreshState();
}

export async function importRemoteApiSession(context) {
  const { remoteApiSessionExportSelectEl, setStatus, refreshState } = context;
  const exportId = remoteApiSessionExportSelectEl.value;
  if (!exportId) {
    setStatus("Select a remote ApiSession export first.");
    return;
  }
  setStatus(`Importing remote ApiSession ${exportId}...`);
  const response = await postText("api/tunnel/rpc/import-api-session", exportId);
  if (!response.ok) {
    const body = await response.text();
    setStatus(responseTextToStatus("Remote ApiSession import failed", response, body));
    return;
  }
  const result = await response.json();
  setStatus(`Imported remote ApiSession ${result.label} as object ${result.objectId}.`);
  await refreshState();
}

export async function importRemoteCapability(context) {
  const { remoteCapabilityExportSelectEl, setStatus, refreshState } = context;
  const selected = remoteCapabilityExportSelectEl.value;
  if (!selected) {
    setStatus("Select a remote capability export first.");
    return;
  }

  const exportId = selected.trim();
  if (!exportId) {
    setStatus("Selected remote capability export is invalid.");
    return;
  }

  setStatus(`Importing remote capability ${exportId}...`);
  const response = await postText("api/tunnel/rpc/import-capability", exportId);
  if (!response.ok) {
    const body = await response.text();
    setStatus(responseTextToStatus("Remote capability import failed", response, body));
    return;
  }
  const result = await response.json();
  setStatus(`Imported remote capability ${result.label} as object ${result.objectId}.`);
  await refreshState();
}

export async function invokeRemoteIpNetwork(context) {
  const { remoteInvokeHostEl, remoteInvokePortEl, setStatus, refreshState } = context;
  const host = (remoteInvokeHostEl.value || "").trim() || "neverssl.com";
  const port = (remoteInvokePortEl.value || "").trim() || "80";
  setStatus(`Invoking imported remote IpNetwork for ${host}:${port}...`);
  const response = await postText("api/tunnel/rpc/invoke-ip-network", `${host}\n${port}`);
  if (!response.ok) {
    const body = await response.text();
    setStatus(responseTextToStatus("Remote IpNetwork invoke failed", response, body));
    return;
  }
  const result = await response.json();
  setStatus(`Remote IpNetwork invocation succeeded for ${result.host}:${result.port}: ${result.responsePreview} [${result.trace}]`);
  await refreshState();
}

export async function invokeRemoteApiSession(context) {
  const { remoteApiFileEl, remoteApiFilenameEl, setStatus, refreshState } = context;
  const file = remoteApiFileEl.files && remoteApiFileEl.files[0];
  if (!file) {
    setStatus("Choose a file to send to the imported ApiSession first.");
    return;
  }
  const filename = (remoteApiFilenameEl.value || "").trim() || file.name || "upload.bin";
  setStatus(`Invoking imported ApiSession with ${filename}...`);
  const response = await fetch("api/tunnel/rpc/invoke-api-session", {
    method: "POST",
    headers: {
      "Content-Type": "application/octet-stream",
      "x-sandstorm-app-filename": filename,
    },
    body: await file.arrayBuffer(),
  });
  if (!response.ok) {
    const body = await response.text();
    setStatus(responseTextToStatus("Remote ApiSession invoke failed", response, body));
    return;
  }
  const result = await response.json();
  const preview = result.responsePreview || previewBase64Payload(result.responsePreviewBase64 || "");
  setStatus(`Remote ApiSession invocation succeeded with HTTP ${result.status} ${result.contentType || ""}: ${preview} [${result.trace}]`);
  await refreshState();
}

export async function restoreImportedIpNetwork(context) {
  const { getImportedRemoteIpNetworkObjectId, setStatus } = context;
  const objectId = getImportedRemoteIpNetworkObjectId();
  if (!objectId) {
    setStatus("No imported remote IpNetwork object is loaded.");
    return;
  }
  setStatus(`Resolving imported remote IpNetwork object ${objectId}...`);
  const response = await fetch("api/saved-cap/resolve-object", {
    method: "PUT",
    headers: { "Content-Type": "text/plain" },
    body: objectId,
  });
  if (response.ok) {
    setStatus(`Imported remote IpNetwork object restore succeeded for ${objectId}`);
  } else {
    const body = await response.text();
    setStatus(responseTextToStatus("Imported remote IpNetwork object restore failed", response, body));
  }
}

export async function restoreImportedApiSession(context) {
  const { getImportedRemoteApiSessionObjectId, setStatus } = context;
  const objectId = getImportedRemoteApiSessionObjectId();
  if (!objectId) {
    setStatus("No imported remote ApiSession object is loaded.");
    return;
  }
  setStatus(`Resolving imported remote ApiSession object ${objectId}...`);
  const response = await fetch("api/saved-cap/resolve-object", {
    method: "PUT",
    headers: { "Content-Type": "text/plain" },
    body: objectId,
  });
  if (response.ok) {
    setStatus(`Imported remote ApiSession object restore succeeded for ${objectId}`);
  } else {
    const body = await response.text();
    setStatus(responseTextToStatus("Imported remote ApiSession object restore failed", response, body));
  }
}

export async function dropImportedIpNetwork(context) {
  const { getImportedRemoteIpNetworkObjectId, setStatus, refreshState } = context;
  const objectId = getImportedRemoteIpNetworkObjectId();
  if (!objectId) {
    setStatus("No imported remote IpNetwork object is loaded.");
    return;
  }
  setStatus(`Dropping imported remote IpNetwork object ${objectId}...`);
  const response = await fetch("api/saved-cap/drop-object", {
    method: "PUT",
    headers: { "Content-Type": "text/plain" },
    body: objectId,
  });
  if (response.ok) {
    setStatus(`Imported remote IpNetwork object dropped: ${objectId}`);
    await refreshState();
  } else {
    const body = await response.text();
    setStatus(responseTextToStatus("Imported remote IpNetwork object drop failed", response, body));
  }
}

export async function dropImportedApiSession(context) {
  const { getImportedRemoteApiSessionObjectId, setStatus, refreshState } = context;
  const objectId = getImportedRemoteApiSessionObjectId();
  if (!objectId) {
    setStatus("No imported remote ApiSession object is loaded.");
    return;
  }
  setStatus(`Dropping imported remote ApiSession object ${objectId}...`);
  const response = await fetch("api/saved-cap/drop-object", {
    method: "PUT",
    headers: { "Content-Type": "text/plain" },
    body: objectId,
  });
  if (response.ok) {
    setStatus(`Imported remote ApiSession object dropped: ${objectId}`);
    await refreshState();
  } else {
    const body = await response.text();
    setStatus(responseTextToStatus("Imported remote ApiSession object drop failed", response, body));
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
