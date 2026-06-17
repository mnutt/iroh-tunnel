# Refactor Plan

## Target Model

Make the app revolve around four concepts only:

1. `shared_caps`
   Local Sandstorm capabilities this grain is willing to expose.

2. `peer session`
   One live `iroh` + `capnp-rpc` connection to the approved remote peer.

3. `remote exports`
   The current peer's advertised capabilities for this session.

4. `local proxy objects`
   Persistent app object IDs that represent "that remote capability through this peer".

Everything else should either disappear or become an internal cache.

## What To Remove

1. Remove typed export/import special cases.
   Delete the dedicated `IpNetwork` / `ApiSession` control plane from `src/tunnel.capnp`, `src/main.rs`, and the debug UI.
   Keep only generic capability listing and fetch.

2. Stop treating imported live objects as durable.
   `persisted_received_caps` should not be a first-class restore surface anymore.
   A raw imported object ID is only valid while the peer session exists.

3. Remove "registered remote capability" as a user-visible persistence concept.
   Keep it only if it is still needed internally for nested capability localization.
   It should not be part of the main product model.

4. Drop most of the transport/debug probe UI.
   The `/api/network/*` and typed invoke endpoints were for feasibility.
   Move them behind a dev-only flag or remove them.

## What To Keep

1. Keep the Sandstorm `RawUdp` `iroh` transport path.
   That is the right Sandstorm primitive for QUIC/`iroh`.

2. Keep the untyped local proxy machinery in `src/untyped_local.rs`.
   That is the key to arbitrary capability forwarding, especially nested caps.

3. Keep `MainView.restore()` as the durable boundary.
   That is where reconnect/resume must be rebuilt.

## New Core Invariant

Every durable received capability should be represented by a local app object ID like:

- `peer/<approved-peer-node-id>/export/<remote-export-id>`

or, if peer-registered nested objects are still needed:

- `peer/<approved-peer-node-id>/remote/<remote-object-id>`

Then `MainView.restore()` always does this:

1. Parse object ID.
2. Verify current approved peer matches.
3. If the peer session is connected, fetch or rebuild the live remote capability.
4. Wrap it in the local proxy membrane as needed.
5. Return it.
6. If disconnected, fail clearly with `peer not connected`.

That gives restart and resume without pretending the remote object is locally persistent.

## Concrete Phases

1. Permissions fix first.
   In `src/main.rs`, stop using empty `requiredPermissions` for both `claimRequest()` and `fulfillRequest()`.
   Use `manageTunnel` for claiming and sharing setup actions.
   Use `useReceivedCaps` when fulfilling a tunneled capability into another grain.

2. Collapse protocol to generic exports.
   Replace the typed methods in `src/tunnel.capnp` with:
   - `listCapabilityExports`
   - `getCapabilityExport`
   - optionally `registerCapability` only for nested-cap localization

3. Simplify `AppState`.
   Remove:
   - `exported_ip_network`
   - `exported_api_session`
   - `exported_ip_network_live`
   - `exported_api_session_live`
   - `imported_remote_ip_network`
   - `imported_remote_api_session`

   Keep:
   - `shared_caps`
   - `exported_caps_live`
   - `peer_rpc_session`
   - `local_proxy_caps`
   - maybe `registered_remote_caps` if still needed internally

4. Redefine persistence around local proxies.
   `local_proxy_caps.json` becomes the durable received-capability registry.
   Each record should contain:
   - `object_id`
   - `peer_node_id`
   - `target_kind`
   - `target_id`
   - `label`
   - `descriptor_json`
   - `enabled`

   `received-caps.json` likely becomes unnecessary.

5. Make restore path proxy-only for remote capabilities.
   Stop restoring persisted received capabilities directly.
   If the object is remote, it should always go through `build_local_proxy_client()`.

6. Use `shareCap()` on outbound shares.
   Before exporting a local capability to the peer, wrap it with `SandstormApi.shareCap()` so the tunneled authority is an explicit Sandstorm share membrane, not just a raw restored capability.
   Persist whatever local metadata is needed to revoke or disable it cleanly.

7. Narrow UI to product flows.
   Left side:
   - shared capabilities
   - received capabilities

   Right side:
   - tunnel permission
   - pairing
   - connected/disconnected state

   Remove typed import and invoke buttons from the main UI.

8. Reconnect semantics.
   On startup:
   - restore `iroh` identity
   - restore raw UDP interface
   - restore approved peer and ticket
   - reconnect if enabled

   Do not try to eagerly restore all remote capabilities.
   Rebuild them lazily from local proxy object IDs when needed.

9. Nested capability forwarding.
   Keep `registerCapability` only as an internal mechanism for capabilities that appear inside params/results.
   Do not expose it in the product model or UI.

10. Tests to add before cleanup lands.
   - restart with saved local proxy object IDs, then reconnect, then restore succeeds
   - restart without reconnect, then restore fails clearly
   - peer changes node ID, old object IDs fail closed
   - revoked `manageTunnel` permission breaks future claim/share actions
   - revoked `useReceivedCaps` permission breaks future fulfill/use actions
   - nested returned capability still localizes through proxy after reconnect

## Recommended File Order

1. `src/main.rs`
   Fix permissions and remove typed state.

2. `src/tunnel.capnp`
   Collapse protocol.

3. `src/app.rs`
   Make proxy objects the only durable remote restore path.

4. `src/storage.rs`
   Merge or remove duplicated received-capability registries.

5. `client/app.js` and related UI files
   Remove debug-first flows from the main surface.

## Net Result

After this refactor, the system becomes:

- Sandstorm persists local capabilities.
- `iroh` carries live RPC.
- local proxy object IDs are the only durable representation of remote capabilities.
- reconnect rebuilds live authority from those IDs.
- typed capability special cases disappear.
