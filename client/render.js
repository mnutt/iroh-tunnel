import {
  postText,
  responseTextToStatus,
} from "./http.js";

function createButton(label, className, onClick) {
  const button = document.createElement("button");
  button.textContent = label;
  if (className) {
    button.className = className;
  }
  button.addEventListener("click", onClick);
  return button;
}

function appendInlineText(parent, text, className = "cap-subtext") {
  const span = document.createElement("span");
  span.className = className;
  span.textContent = text;
  parent.appendChild(span);
}

function appendInlineCode(parent, prefix, value, className = "cap-subtext") {
  const span = document.createElement("span");
  span.className = className;
  if (prefix) {
    span.appendChild(document.createTextNode(prefix));
  }
  const code = document.createElement("code");
  code.textContent = value;
  span.appendChild(code);
  parent.appendChild(span);
}

function formatCapabilityType(kind, typeTag) {
  if (kind === "IpNetwork") {
    return "Type: Network";
  }
  if (kind === "ApiSession") {
    return "Type: API Session";
  }
  return `Type: ${typeTag || kind || "Unknown"}`;
}

function renderEmptyList(container, text) {
  container.innerHTML = "";
  const item = document.createElement("li");
  item.className = "empty";
  item.textContent = text;
  container.appendChild(item);
}

function renderSavedCaps(context, items, rawUdpInterface) {
  const {
    savedCapsEl,
    currentState,
    setStatus,
    refreshState,
  } = context;
  savedCapsEl.innerHTML = "";
  if (!items.length) {
    renderEmptyList(savedCapsEl, "No saved capabilities yet.");
    return;
  }

  for (const entry of items) {
    const item = document.createElement("li");
    item.className = "cap-card";

    const label = document.createElement("div");
    label.className = "cap-meta";
    const strong = document.createElement("strong");
    strong.textContent = entry.label;
    label.appendChild(strong);
    appendInlineCode(label, "", entry.savedToken);
    appendInlineCode(label, "object ", entry.objectId || entry.id);
    item.appendChild(label);

    const actions = document.createElement("div");
    actions.className = "cap-actions";

    const isPowerboxRequestSession =
      currentState
      && currentState.powerboxRequestSession
      && currentState.powerboxRequestSession.active;

    if (isPowerboxRequestSession) {
      actions.appendChild(createButton("Provide", "secondary", async () => {
        setStatus(`Providing ${entry.label} to requesting app...`);
        const response = await fetch("api/powerbox/fulfill-object", {
          method: "PUT",
          headers: { "Content-Type": "text/plain" },
          body: entry.objectId || entry.id,
        });
        if (!response.ok) {
          const body = await response.text();
          setStatus(responseTextToStatus("Powerbox fulfill failed", response, body));
          return;
        }
        setStatus(`Provided ${entry.label} to requesting app.`);
      }));
    }

    const isConfiguredRawUdp = rawUdpInterface && rawUdpInterface.savedToken === entry.savedToken;

    const bindButton = createButton(
      isConfiguredRawUdp ? "Tunnel Permission Active" : "Use for Tunnel Network",
      "secondary",
      async () => {
        setStatus(`Configuring tunnel network permission from ${entry.label}...`);
        const response = await postText("api/endpoint/raw-udp-interface", entry.savedToken);
        if (!response.ok) {
          const body = await response.text();
          setStatus(responseTextToStatus("Tunnel network update failed", response, body));
          return;
        }
        setStatus(`Tunnel network permission set from ${entry.label}.`);
        await refreshState();
      }
    );
    bindButton.disabled = !!isConfiguredRawUdp;
    actions.appendChild(bindButton);

    actions.appendChild(createButton("Share as Network Type", "secondary", async () => {
      setStatus(`Configuring shared network capability from ${entry.label}...`);
      const response = await postText("api/tunnel/exported-ip-network", entry.objectId || entry.id);
      if (!response.ok) {
        const body = await response.text();
        setStatus(responseTextToStatus("Shared capability setup failed", response, body));
        return;
      }
      setStatus(`Shared network capability set to ${entry.label}.`);
      await refreshState();
    }));

    actions.appendChild(createButton("Share as API Type", "secondary", async () => {
      setStatus(`Configuring shared API capability from ${entry.label}...`);
      const response = await postText("api/tunnel/exported-api-session", entry.objectId || entry.id);
      if (!response.ok) {
        const body = await response.text();
        setStatus(responseTextToStatus("Shared capability setup failed", response, body));
        return;
      }
      setStatus(`Shared API capability set to ${entry.label}.`);
      await refreshState();
    }));

    if (isConfiguredRawUdp) {
      actions.appendChild(createButton("Clear Tunnel Network", "secondary", async () => {
        setStatus("Clearing tunnel network permission...");
        const response = await fetch("api/endpoint/raw-udp-interface", { method: "PUT" });
        if (!response.ok) {
          const body = await response.text();
          setStatus(responseTextToStatus("Tunnel network clear failed", response, body));
          return;
        }
        setStatus("Tunnel network permission cleared.");
        await refreshState();
      }));
    }

    const advanced = document.createElement("details");
    advanced.className = "debug";
    const summary = document.createElement("summary");
    summary.textContent = "Advanced capability actions";
    advanced.appendChild(summary);

    const advancedRow = document.createElement("div");
    advancedRow.className = "button-row";

    advancedRow.appendChild(createButton("Restore Probe", "secondary", async () => {
      setStatus(`Restoring ${entry.savedToken}...`);
      const response = await fetch("api/saved-cap/restore", {
        method: "PUT",
        headers: { "Content-Type": "text/plain" },
        body: entry.savedToken,
      });
      if (response.ok) {
        setStatus(`Restore succeeded for ${entry.savedToken}`);
      } else {
        setStatus(`Restore failed with HTTP ${response.status}`);
      }
    }));

    advancedRow.appendChild(createButton("Object Probe", "secondary", async () => {
      const objectId = entry.objectId || entry.id;
      setStatus(`Resolving object ${objectId}...`);
      const response = await fetch("api/saved-cap/resolve-object", {
        method: "PUT",
        headers: { "Content-Type": "text/plain" },
        body: objectId,
      });
      if (response.ok) {
        setStatus(`Object restore succeeded for ${objectId}`);
      } else {
        setStatus(`Object restore failed with HTTP ${response.status}`);
      }
    }));

    advanced.appendChild(advancedRow);
    item.appendChild(actions);
    item.appendChild(advanced);
    savedCapsEl.appendChild(item);
  }
}

function renderSharedCapabilities(context, data) {
  const { sharedCapsEl, shareInlineNoteEl, setStatus, refreshState } = context;
  sharedCapsEl.innerHTML = "";

  const sharedCaps = data.sharedCaps || [];
  const localNodeId = data.irohNodeId || "";

  shareInlineNoteEl.textContent = sharedCaps.length
    ? "Known capability types currently supported here: Network and API Session."
    : "";

  if (!sharedCaps.length) {
    renderEmptyList(sharedCapsEl, "No shared capabilities yet.");
    return;
  }

  for (const row of sharedCaps) {
    const item = document.createElement("li");
    item.className = "cap-card";

    const meta = document.createElement("div");
    meta.className = "cap-meta";
    const strong = document.createElement("strong");
    strong.textContent = row.label;
    meta.appendChild(strong);
    appendInlineText(meta, row.enabled ? "Shared through this tunnel" : "Remembered, currently disabled");
    appendInlineText(meta, formatCapabilityType(row.kind, row.typeTag));
    item.appendChild(meta);

    const actions = document.createElement("div");
    actions.className = "cap-actions";

    const toggle = document.createElement("label");
    toggle.className = "toggle";
    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.checked = !!row.enabled;
    checkbox.addEventListener("change", async () => {
      const endpoint =
        row.kind === "IpNetwork" ? "api/tunnel/exported-ip-network" : "api/tunnel/exported-api-session";
      setStatus(`${checkbox.checked ? "Enabling" : "Stopping"} sharing for ${row.label}...`);
      const response = await postText(
        endpoint,
        checkbox.checked ? (row.savedCapId || "") : `!${row.savedCapId || ""}`
      );
      if (!response.ok) {
        const body = await response.text();
        setStatus(responseTextToStatus("Shared capability update failed", response, body));
        await refreshState();
        return;
      }
      setStatus(`${checkbox.checked ? "Enabled" : "Stopped"} sharing ${row.label}.`);
      await refreshState();
    });
    toggle.appendChild(checkbox);
    toggle.appendChild(document.createTextNode("Enabled"));
    actions.appendChild(toggle);

    item.appendChild(actions);

    const debugDetails = document.createElement("details");
    debugDetails.className = "debug";
    const summary = document.createElement("summary");
    summary.textContent = "Debug";
    debugDetails.appendChild(summary);

    const debugBody = document.createElement("div");
    debugBody.className = "stack";
    const exportLine = document.createElement("div");
    exportLine.className = "inline-note";
    exportLine.textContent = `Shared id: ${row.id || "unknown"}`;
    debugBody.appendChild(exportLine);
    const tokenLine = document.createElement("div");
    tokenLine.className = "inline-note";
    tokenLine.textContent = `Saved token: ${row.savedToken || "unknown"}`;
    debugBody.appendChild(tokenLine);
    const nodeLine = document.createElement("div");
    nodeLine.className = "inline-note";
    nodeLine.textContent = `Provider node: ${localNodeId || "unknown"}`;
    debugBody.appendChild(nodeLine);
    if (row.descriptorJson) {
      const descriptorLine = document.createElement("div");
      descriptorLine.className = "inline-note";
      descriptorLine.textContent = `Descriptor: ${row.descriptorJson}`;
      debugBody.appendChild(descriptorLine);
    }
    debugDetails.appendChild(debugBody);

    item.appendChild(debugDetails);
    sharedCapsEl.appendChild(item);
  }
}

function renderReceivedCapabilities(context, data) {
  const { receivedCapsEl, setStatus, refreshState } = context;
  receivedCapsEl.innerHTML = "";

  const peerRpc = data.peerRpc || { connected: false };
  const durableReceived = (data.localProxyCaps || []).filter(
    (entry) => entry.targetKind === "exportId"
  );
  const isPowerboxRequestSession =
    data.powerboxRequestSession && data.powerboxRequestSession.active;
  const remoteExports = peerRpc.capabilityExports || [];
  const durableByExportId = new Map();
  for (const entry of durableReceived) {
    durableByExportId.set(entry.targetId, entry);
  }

  if (isPowerboxRequestSession) {
    const savedCaps = data.savedCaps || [];
    const localProvideCaps =
      (data.powerboxRequestSession && data.powerboxRequestSession.localProvideCaps) || [];
    const itemsRendered = { count: 0 };

    for (const entry of localProvideCaps) {
      const item = document.createElement("li");
      item.className = "cap-card";

      const meta = document.createElement("div");
      meta.className = "cap-meta";
      const strong = document.createElement("strong");
      strong.textContent = entry.label;
      meta.appendChild(strong);
      appendInlineText(meta, "Provided directly by this grain");
      appendInlineText(meta, formatCapabilityType(entry.kind, entry.typeTag));
      item.appendChild(meta);

      const actions = document.createElement("div");
      actions.className = "cap-actions";
      actions.appendChild(createButton("Provide", "secondary", async () => {
        setStatus(`Providing ${entry.label} to requesting app...`);
        const response = await fetch("api/powerbox/fulfill-object", {
          method: "PUT",
          headers: { "Content-Type": "text/plain" },
          body: entry.objectId,
        });
        if (!response.ok) {
          const body = await response.text();
          setStatus(responseTextToStatus("Powerbox fulfill failed", response, body));
          await refreshState();
          return;
        }
        setStatus(`Provided ${entry.label} to requesting app.`);
      }));
      item.appendChild(actions);
      receivedCapsEl.appendChild(item);
      itemsRendered.count += 1;
    }

    for (const entry of savedCaps) {
      const item = document.createElement("li");
      item.className = "cap-card";

      const meta = document.createElement("div");
      meta.className = "cap-meta";
      const strong = document.createElement("strong");
      strong.textContent = entry.label;
      meta.appendChild(strong);
      appendInlineText(meta, "Saved in this grain");
      item.appendChild(meta);

      const actions = document.createElement("div");
      actions.className = "cap-actions";
      actions.appendChild(createButton("Provide", "secondary", async () => {
        setStatus(`Providing ${entry.label} to requesting app...`);
        const response = await fetch("api/powerbox/fulfill-object", {
          method: "PUT",
          headers: { "Content-Type": "text/plain" },
          body: entry.objectId || entry.id,
        });
        if (!response.ok) {
          const body = await response.text();
          setStatus(responseTextToStatus("Powerbox fulfill failed", response, body));
          await refreshState();
          return;
        }
        setStatus(`Provided ${entry.label} to requesting app.`);
      }));
      item.appendChild(actions);
      receivedCapsEl.appendChild(item);
      itemsRendered.count += 1;
    }

    for (const entry of durableReceived) {
      const isAvailable = peerRpc.connected && remoteExports.some((remote) => remote.id === entry.targetId);
      if (!isAvailable || entry.enabled === false) {
        continue;
      }
      const item = document.createElement("li");
      item.className = "cap-card";

      const meta = document.createElement("div");
      meta.className = "cap-meta";
      const strong = document.createElement("strong");
      strong.textContent = entry.label;
      meta.appendChild(strong);
      appendInlineText(meta, "Received through connected tunnel");
      appendInlineText(meta, formatCapabilityType(entry.kind, entry.typeTag));
      item.appendChild(meta);

      const actions = document.createElement("div");
      actions.className = "cap-actions";
      actions.appendChild(createButton("Provide", "secondary", async () => {
        setStatus(`Providing ${entry.label} to requesting app...`);
        const response = await fetch("api/powerbox/fulfill-object", {
          method: "PUT",
          headers: { "Content-Type": "text/plain" },
          body: entry.objectId,
        });
        if (!response.ok) {
          const body = await response.text();
          setStatus(responseTextToStatus("Powerbox fulfill failed", response, body));
          await refreshState();
          return;
        }
        setStatus(`Provided ${entry.label} to requesting app.`);
      }));
      item.appendChild(actions);
      receivedCapsEl.appendChild(item);
      itemsRendered.count += 1;
    }

    for (const entry of remoteExports) {
      if (durableByExportId.has(entry.id)) {
        continue;
      }
      const item = document.createElement("li");
      item.className = "cap-card";

      const meta = document.createElement("div");
      meta.className = "cap-meta";
      const strong = document.createElement("strong");
      strong.textContent = entry.label;
      meta.appendChild(strong);
      appendInlineText(meta, "Available from connected remote grain");
      appendInlineText(meta, formatCapabilityType(entry.kind, entry.typeTag));
      item.appendChild(meta);

      const actions = document.createElement("div");
      actions.className = "cap-actions";
      actions.appendChild(createButton("Provide", "secondary", async () => {
        setStatus(`Providing ${entry.label} to requesting app...`);
        const response = await fetch("api/powerbox/fulfill-export", {
          method: "PUT",
          headers: { "Content-Type": "text/plain" },
          body: entry.id,
        });
        if (!response.ok) {
          const body = await response.text();
          setStatus(responseTextToStatus("Powerbox fulfill failed", response, body));
          await refreshState();
          return;
        }
        setStatus(`Provided ${entry.label} to requesting app.`);
      }));
      item.appendChild(actions);
      receivedCapsEl.appendChild(item);
      itemsRendered.count += 1;
    }

    if (!itemsRendered.count) {
      renderEmptyList(receivedCapsEl, "No capabilities available to provide.");
    }
    return;
  }

  if (!durableReceived.length && !remoteExports.length) {
    renderEmptyList(receivedCapsEl, "No capabilities received.");
    return;
  }

  for (const entry of durableReceived) {
    const isAvailable = peerRpc.connected && remoteExports.some((remote) => remote.id === entry.targetId);
    const isEnabled = entry.enabled !== false;

    const item = document.createElement("li");
    item.className = "cap-card";

    const meta = document.createElement("div");
    meta.className = "cap-meta";
    const strong = document.createElement("strong");
    strong.textContent = entry.label;
    meta.appendChild(strong);
    appendInlineText(
      meta,
      isEnabled
        ? (isAvailable ? "from connected remote grain" : "Enabled, waiting for tunnel reconnect")
        : "Remembered, currently disabled"
    );
    appendInlineText(meta, formatCapabilityType(entry.kind, entry.typeTag));
    item.appendChild(meta);

    const actions = document.createElement("div");
    actions.className = "cap-actions";
    const toggle = document.createElement("label");
    toggle.className = "toggle";
    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.checked = isEnabled;
    checkbox.addEventListener("change", async () => {
      if (!checkbox.checked) {
        setStatus(`Disabling received capability ${entry.label}...`);
        const response = await fetch("api/received-cap/disable", {
          method: "PUT",
          headers: { "Content-Type": "text/plain" },
          body: entry.objectId,
        });
        if (!response.ok) {
          const body = await response.text();
          setStatus(responseTextToStatus("Received capability disable failed", response, body));
          await refreshState();
          return;
        }
        setStatus(`Disabled received capability ${entry.label}.`);
        await refreshState();
        return;
      }

      setStatus(`Re-enabling received capability ${entry.label}...`);
      const response = await fetch("api/received-cap/enable", {
        method: "PUT",
        headers: { "Content-Type": "text/plain" },
        body: entry.objectId,
      });
      if (!response.ok) {
        const body = await response.text();
        setStatus(responseTextToStatus("Received capability enable failed", response, body));
        await refreshState();
        return;
      }
      setStatus(`Re-enabled received capability ${entry.label}.`);
      await refreshState();
    });
    toggle.appendChild(checkbox);
    toggle.appendChild(document.createTextNode("Enabled"));
    actions.appendChild(toggle);

    if (isPowerboxRequestSession && isAvailable) {
      actions.appendChild(createButton("Provide", "secondary", async () => {
        setStatus(`Providing ${entry.label} to requesting app...`);
        const response = await fetch("api/powerbox/fulfill-object", {
          method: "PUT",
          headers: { "Content-Type": "text/plain" },
          body: entry.objectId,
        });
        if (!response.ok) {
          const body = await response.text();
          setStatus(responseTextToStatus("Powerbox fulfill failed", response, body));
          await refreshState();
          return;
        }
        setStatus(`Provided ${entry.label} to requesting app.`);
      }));
    }

    actions.appendChild(createButton("Save Locally", "secondary", async () => {
      setStatus(`Saving received capability ${entry.label} locally...`);
      const response = await fetch("api/received-cap/save-local", {
        method: "PUT",
        headers: { "Content-Type": "text/plain" },
        body: entry.objectId,
      });
      if (!response.ok) {
        const body = await response.text();
        setStatus(responseTextToStatus("Save local capability failed", response, body));
        await refreshState();
        return;
      }
      const result = await response.json();
      setStatus(`Saved local capability ${result.label} as ${result.id}.`);
      await refreshState();
    }));

    actions.appendChild(createButton("Forget", "secondary", async () => {
      setStatus(`Forgetting received capability ${entry.label}...`);
      const response = await fetch("api/saved-cap/drop-object", {
        method: "PUT",
        headers: { "Content-Type": "text/plain" },
        body: entry.objectId,
      });
      if (!response.ok) {
        const body = await response.text();
        setStatus(responseTextToStatus("Forget received capability failed", response, body));
        await refreshState();
        return;
      }
      setStatus(`Forgot received capability ${entry.label}.`);
      await refreshState();
    }));

    if (isEnabled && !isAvailable) {
      const note = document.createElement("span");
      note.className = "inline-note";
      note.textContent = "Unavailable until tunnel reconnects.";
      actions.appendChild(note);
    }

    item.appendChild(actions);
    receivedCapsEl.appendChild(item);
  }

  for (const entry of remoteExports) {
    if (durableByExportId.has(entry.id)) {
      continue;
    }

    const item = document.createElement("li");
    item.className = "cap-card";

    const meta = document.createElement("div");
    meta.className = "cap-meta";
    const strong = document.createElement("strong");
    strong.textContent = entry.label;
    meta.appendChild(strong);
    appendInlineText(meta, "Available from connected remote grain");
    appendInlineText(meta, formatCapabilityType(entry.kind, entry.typeTag));
    item.appendChild(meta);

    const actions = document.createElement("div");
    actions.className = "cap-actions";
    if (isPowerboxRequestSession) {
      actions.appendChild(createButton("Provide", "secondary", async () => {
        setStatus(`Providing ${entry.label} to requesting app...`);
        const response = await fetch("api/powerbox/fulfill-export", {
          method: "PUT",
          headers: { "Content-Type": "text/plain" },
          body: entry.id,
        });
        if (!response.ok) {
          const body = await response.text();
          setStatus(responseTextToStatus("Powerbox fulfill failed", response, body));
          await refreshState();
          return;
        }
        setStatus(`Provided ${entry.label} to requesting app.`);
      }));
    }
    actions.appendChild(createButton("Import", "secondary", async () => {
      setStatus(`Importing remote capability ${entry.label}...`);
      const response = await postText("api/tunnel/rpc/import-capability", entry.id);
      if (!response.ok) {
        const body = await response.text();
        setStatus(responseTextToStatus("Remote capability import failed", response, body));
        await refreshState();
        return;
      }
      const result = await response.json();
      setStatus(`Imported remote capability ${result.label} as object ${result.objectId}.`);
      await refreshState();
    }));

    item.appendChild(actions);
    receivedCapsEl.appendChild(item);
  }
}

function renderTunnel(context, data) {
  const {
    tunnelStatusPillEl,
    tunnelHelperEl,
    requestIpInterfaceButton,
    connectTunnelButton,
    toggleTunnelButton,
    acceptTunnelButton,
    rejectTunnelButton,
    disconnectPeerRpcButton,
    clearTicketButton,
  } = context;
  const endpoint = data.irohEndpoint || {};
  const peerRpc = data.peerRpc || { connected: false };
  const pairing = data.pairing || {};
  const hasNetworkPermission = !!endpoint.rawUdpInterface;
  const remoteTicket = (
    (context.remoteTicketEl && context.remoteTicketEl.value)
    || data.remoteTicket
    || ""
  ).trim();
  const pairingStatus = pairing.status || "disconnected";
  const pendingIncomingPeerNodeId = pairing.pendingIncomingPeerNodeId || "";
  const pendingOutgoingPeerNodeId = pairing.pendingOutgoingPeerNodeId || "";
  const approvedPeerNodeId = pairing.approvedPeerNodeId || "";

  let label = "Tunnel disconnected";
  let helper = "Paste a remote grain ticket and connect.";
  if (!hasNetworkPermission) {
    label = "Tunnel unavailable";
    helper = "This app needs a network capability before it can bind the tunnel.";
  } else if (pairingStatus === "incomingRequest") {
    label = "Incoming connection request";
    helper = pendingIncomingPeerNodeId
      ? `Another grain wants to connect: ${pendingIncomingPeerNodeId}.`
      : "Another grain wants to connect to this tunnel.";
  } else if (pairingStatus === "awaitingRemoteAccept" || pairingStatus === "connecting") {
    label = "Waiting for remote to accept";
    helper = pendingOutgoingPeerNodeId
      ? `Connection request sent to ${pendingOutgoingPeerNodeId}.`
      : "Connection request sent. The remote grain needs to accept it.";
  } else if (peerRpc.connected || pairingStatus === "connected") {
    label = "Tunnel connected";
    helper = `Connected to ${peerRpc.remoteNodeId || approvedPeerNodeId || "remote grain"}.`;
  } else if (pairingStatus === "disabled") {
    label = "Tunnel disabled";
    helper = pendingIncomingPeerNodeId
      ? `Approved peer ${pendingIncomingPeerNodeId} wants to reconnect. Enable the tunnel to accept.`
      : approvedPeerNodeId
        ? `This peer is approved but the tunnel is currently off: ${approvedPeerNodeId}.`
        : "This peer is approved but the tunnel is currently off.";
  } else if (pairingStatus === "error") {
    label = "Connection error";
    helper = data.peerRpcError || "The last connection attempt failed.";
  } else if (remoteTicket || approvedPeerNodeId) {
    label = "Tunnel disconnected";
    helper = approvedPeerNodeId
      ? `Peer approved: ${approvedPeerNodeId}. Connect to bring the tunnel up.`
      : "A remote ticket is saved. Connect to bring the tunnel up.";
  }

  tunnelStatusPillEl.textContent = label;
  tunnelHelperEl.textContent = helper;

  requestIpInterfaceButton.textContent = hasNetworkPermission
    ? "Tunneling Capability Ready"
    : "Request Tunneling Capability";
  requestIpInterfaceButton.disabled = hasNetworkPermission;
  const canToggleTunnel = hasNetworkPermission && !!approvedPeerNodeId && pairingStatus !== "incomingRequest";
  connectTunnelButton.disabled = !hasNetworkPermission
    || !remoteTicket
    || peerRpc.connected
    || pairingStatus === "connected"
    || pairingStatus === "incomingRequest"
    || pairingStatus === "awaitingRemoteAccept"
    || pairingStatus === "connecting"
    || pairingStatus === "disabled";
  toggleTunnelButton.textContent = pairingStatus === "disabled" ? "Enable Tunnel" : "Disable Tunnel";
  toggleTunnelButton.disabled = !canToggleTunnel;
  acceptTunnelButton.disabled = pairingStatus !== "incomingRequest";
  rejectTunnelButton.disabled = pairingStatus !== "incomingRequest";
  disconnectPeerRpcButton.disabled = !peerRpc.connected;
  clearTicketButton.disabled = !remoteTicket && !peerRpc.connected && !approvedPeerNodeId && pairingStatus !== "incomingRequest";

  connectTunnelButton.style.display = pairingStatus === "incomingRequest" || peerRpc.connected || pairingStatus === "connected" ? "none" : "";
  toggleTunnelButton.style.display = canToggleTunnel ? "" : "none";
  acceptTunnelButton.style.display = pairingStatus === "incomingRequest" ? "" : "none";
  rejectTunnelButton.style.display = pairingStatus === "incomingRequest" ? "" : "none";
}

export function renderApp(context, data) {
  const {
    heroSectionEl,
    nodeIdEl,
    endpointStatusEl,
    rawUdpInterfaceEl,
    customTransportPillEl,
    peerRpcPillEl,
    localTicketEl,
    remoteTicketEl,
    peerRpcStatusEl,
    peerRpcErrorEl,
    sharePanelEl,
    receivedPanelEl,
    receivedTitleEl,
    receivedCopyEl,
    tunnelPanelEl,
  } = context;
  const endpoint = data.irohEndpoint || {};
  const peerRpc = data.peerRpc || { connected: false, capabilityExports: [] };
  const pairing = data.pairing || {};
  const hasCustomTicket = (endpoint.localTicket || "").split("\n").some((line) => line.startsWith("custom:"));

  if (nodeIdEl) {
    nodeIdEl.textContent = data.irohNodeId || "unavailable";
  }
  if (endpointStatusEl) {
    endpointStatusEl.textContent = endpoint.error || (endpoint.bound ? "bound (relay disabled)" : "not bound");
  }
  if (rawUdpInterfaceEl) {
    rawUdpInterfaceEl.textContent = endpoint.rawUdpInterface
      ? `${endpoint.rawUdpInterface.label} [${endpoint.rawUdpInterface.source}]`
      : "not configured";
  }
  if (customTransportPillEl) {
    customTransportPillEl.textContent = hasCustomTicket ? "Custom Transport Ready" : "Custom Transport Missing";
  }
  if (peerRpcPillEl) {
    peerRpcPillEl.textContent = peerRpc.connected ? "Peer RPC Connected" : "Peer RPC Disconnected";
  }
  if (localTicketEl) {
    localTicketEl.value = endpoint.localTicket || "";
  }
  const serverRemoteTicket = data.remoteTicket || "";
  const currentRemoteTicket = remoteTicketEl ? (remoteTicketEl.value || "") : "";
  const preserveRemoteTicketDraft =
    document.activeElement === remoteTicketEl
    || (!!currentRemoteTicket.trim() && currentRemoteTicket !== serverRemoteTicket);
  if (!preserveRemoteTicketDraft && remoteTicketEl) {
    remoteTicketEl.value = serverRemoteTicket;
  }
  if (peerRpcStatusEl) {
    peerRpcStatusEl.textContent = peerRpc.connected
      ? `connected to ${peerRpc.remoteNodeId} [session ${peerRpc.sessionId}]`
      : pairing.status === "incomingRequest"
        ? `incoming request from ${pairing.pendingIncomingPeerNodeId || "remote grain"}`
        : pairing.status === "awaitingRemoteAccept" || pairing.status === "connecting"
          ? `waiting for ${pairing.pendingOutgoingPeerNodeId || "remote grain"}`
          : pairing.approvedPeerNodeId
            ? `approved peer ${pairing.approvedPeerNodeId}`
            : "not connected";
  }
  if (peerRpcErrorEl) {
    peerRpcErrorEl.textContent = data.peerRpcError || "";
  }

  const isPowerboxRequestSession =
    data.powerboxRequestSession && data.powerboxRequestSession.active;
  if (heroSectionEl) {
    heroSectionEl.style.display = isPowerboxRequestSession ? "none" : "";
  }
  if (sharePanelEl) {
    sharePanelEl.style.display = isPowerboxRequestSession ? "none" : "";
  }
  if (tunnelPanelEl) {
    tunnelPanelEl.style.display = isPowerboxRequestSession ? "none" : "";
  }
  if (receivedPanelEl) {
    receivedPanelEl.style.display = "";
  }
  if (receivedTitleEl) {
    receivedTitleEl.textContent = isPowerboxRequestSession
      ? "Provide Capability"
      : "Received Capabilities";
  }
  if (receivedCopyEl) {
    receivedCopyEl.textContent = isPowerboxRequestSession
      ? "Choose a capability to provide to the requesting grain."
      : "Capabilities remembered from the remote grain stay listed here, even when the tunnel is disconnected.";
  }

  renderSharedCapabilities(context, data);
  renderReceivedCapabilities(context, data);
  renderSavedCaps(context, data.savedCaps || [], endpoint.rawUdpInterface || null);
  renderTunnel(context, data);
}
