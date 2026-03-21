# Plan

## Phase 0: feasibility spikes

### Goal

Reduce uncertainty before building the full app.

### Tasks

1. Package a minimal Rust Sandstorm app that can talk to `/tmp/sandstorm-api`.
2. Validate raw Cap'n Proto calls to `sandstorm-http-bridge`.
3. Prototype a Powerbox request from browser JS and server-side claim flow.
4. Test whether an empty Powerbox query is accepted.
5. Test requesting `IpNetwork` through the Powerbox.
6. Inspect whether vendored Sandstorm networking schemas expose any UDP receive surface richer than `UdpPort`.
7. Build a standalone Rust prototype of `capnp-rpc` over an `iroh` stream outside Sandstorm.
8. Repeat the transport test inside a packaged grain.

### Exit criteria

- one capability can be claimed and saved
- one saved capability can be restored again
- one `IpNetwork` capability can be claimed and saved
- one peer connection can be established through Sandstorm's capability-gated networking surface
- one live capability can cross the transport boundary

Current status:

- capability claim/save/restore is complete
- Sandstorm networking feasibility is now proven for `iroh` via low-level `RawUdp`
- live capability transfer over the transport boundary is proven

## Phase 1: package skeleton

### Goal

Create a Sandstorm app shell that starts reliably and survives restarts.

### Tasks

1. Create `.sandstorm/box.toml` and runtime scripts.
2. Add a Rust binary entry point.
3. Run a raw Sandstorm `UiView` / `WebSession` server on the inherited RPC socket.
4. Add persistent storage under `/var`.
5. Add a draft `sandstorm-pkgdef.capnp`.
6. Implement health and startup logging.

### Exit criteria

- the grain boots in dev mode
- the raw `UiView` renders a packaged HTML page
- state survives restart

## Phase 2: local capability intake

### Goal

Let a user acquire and manage local capabilities.

### Tasks

1. Add UI button for Powerbox request.
2. Add browser-side `postMessage()` flow.
3. Add claim-token POST endpoint.
4. Redeem and save capabilities server-side.
5. Store metadata in the registry.
6. Add restore probe against saved tokens.
7. Add typed `IpNetwork` request path.
8. Add list and enable/disable controls in the UI.

### Exit criteria

- user can add a capability
- capability metadata is persisted
- saved capability can be restored on demand
- `IpNetwork` can be requested, claimed, and saved
- user can mark it for sharing

## Phase 3: peer connectivity

### Goal

Pair two grains and establish a tunnel.

### Tasks

1. Generate and persist an `iroh` node identity.
2. Show a local pairing ticket in the UI.
3. Accept a remote ticket in the UI.
4. Connect to the remote peer.
5. Expose detailed connection state in the UI.
6. Persist peer configuration.
7. Replace the ambient-socket probe with one that actually uses the saved Sandstorm raw UDP capability path.

### Exit criteria

- two grains can exchange a local ticket and complete one probe round trip through the intended Sandstorm networking capability path
- reconnect works after process restart

Current status:

- local ticket display, remote ticket persistence, and peer probing are working
- the probe now runs over Sandstorm `RawUdp` custom transport rather than ambient sockets
- restart and reconnect behavior still need more validation

## Phase 4: capability tunnel

### Goal

Send live capabilities over the peer connection.

### Narrow success target

Prove one end-to-end remote invocation with the smallest possible surface area:

- grain A exports one already-saved capability
- grain B imports that live capability over one `iroh` session
- grain B invokes one known-good method on it successfully
- disconnect leaves the imported capability clearly stale rather than silently wrong

### Tasks

1. Adapt one bidirectional `iroh` stream to the transport needed by `capnp-rpc`.
2. Create one RPC connection for one peer session.
3. Define one minimal debug protocol for capability exchange:
   - list or advertise one exported capability
   - request one exported capability by simple id
4. Export one already-saved local capability without designing the final sharing UX yet.
5. Import that remote capability client on the peer and keep it in an in-memory map keyed by a simple remote share id.
6. Add one manual invocation path for the imported capability:
   - debug HTTP endpoint, debug UI action, or both
7. Make disconnect behavior explicit:
   - tear down the RPC session
   - mark imported capabilities stale
   - fail subsequent invocations clearly
8. Defer all persistence and Sandstorm re-export behavior to Phase 5.

### Exit criteria

- one selected capability can be invoked remotely through the tunnel

Current status:

- raw-UDP `iroh` peer connectivity is working
- there is now a live `capnp-rpc` session over that peer path
- one saved `IpNetwork` can be exported, imported, and invoked remotely
- one saved HTTP bridge `ApiSession` can be exported, imported, and invoked remotely
- imported remote capabilities are tracked in memory and fail closed when the peer session drops

Implementation order:

1. Build the `iroh` stream <-> `capnp-rpc` adapter.
2. Stand up one peer RPC session with explicit connect/disconnect lifecycle.
3. Export one saved capability from one grain.
4. Import it from the other grain.
5. Invoke it once through a debug path.
6. Confirm disconnect fails safely.

Out of scope for this phase:

- final sharing/received UX
- persistent `received_caps` storage
- app object ID mapping for remote capabilities
- `MainView.restore()` of imported capabilities
- multi-peer coordination
- automatic reconnect

Result:

- complete
- the remaining work moved to durability and productization rather than tunnel feasibility

## Phase 5: re-export into Sandstorm

### Goal

Make received capabilities available from the destination grain.

### Tasks

1. Introduce `MainView(AppObjectId)` as the persistent export surface.
2. Define app object IDs for exported remote capabilities.
3. Map received capability records to app object IDs.
4. Implement `restore()` to return imported capabilities.
5. Implement `drop()` cleanup behavior.
6. Surface received capabilities in the UI.

### Exit criteria

- a capability received from the remote grain can be restored locally by Sandstorm

Current status:

- imported remote `IpNetwork` and `ApiSession` capabilities are now assigned in-memory app object IDs
- `MainView.restore(objectId)` can now return those imported live capabilities
- the proof has been exercised end to end with a remote `ApiSession` restored as `remote-cap-1`
- this is not durable yet: imported remote object IDs are not persisted across disconnect or restart

Remaining work:

1. persist received-capability metadata instead of keeping it only in memory
2. rebuild imported live capabilities after reconnect
3. decide which received capabilities should appear in normal app flows rather than only debug UI
4. implement any needed re-export or forwarding UX on top of the restore path
5. make `drop()` meaningful for received remote capabilities

## Phase 6: UX and safety

### Goal

Make the app understandable and safe to use.

### Tasks

1. Add share labels and descriptions.
2. Separate "sharing" and "received" views.
3. Add peer identity verification UI.
4. Add confirmation before enabling sharing.
5. Add error states for disconnected or stale capabilities.
6. Add structured logs for connection and capability events.

### Exit criteria

- the UI clearly communicates what authority is flowing where

## Phase 7: verification

### Goal

Validate the model under realistic grain lifecycle events.

### Tasks

1. Restart grains and ensure state recovers.
2. Suspend and reopen tabs.
3. Break and restore the peer connection.
4. Confirm stale capabilities fail safely.
5. Test with multiple shared capabilities.
6. Test package rebuild and upgrade behavior.

### Exit criteria

- reconnect and recovery behavior is predictable

## Risk register

### High risk

- Raw capability re-export may have edge cases around persistence and lifetimes.
- Durable session management across grain suspension may be more difficult than the current one-shot peer probe.

### Medium risk

- Grain suspension may make the product feel less "tunnel-like" than expected.
- Cap'n Proto RPC transport adaptation over `iroh` may need custom glue.
- Sandstorm RawUdp binding/configuration may remain awkward until the app owns that setup flow cleanly.
- Empty Powerbox queries appear unreliable enough that typed queries are likely required in practice.

### Low risk

- The minimal HTTP UI is straightforward once the capability path is proven.

## Immediate next steps

1. Keep the raw `UiView` / `MainView` baseline stable.
2. Keep the current persisted `iroh` identity, raw UDP binding, and remote-ticket layer stable.
3. Persist imported remote capability descriptors and object IDs.
4. Rebuild imported live capabilities after peer reconnect or grain restart.
5. Move the successful debug restore path into a more normal received-capability UX.

## Progress notes

- The first useful capability-gated probe is an outbound TCP/HTTP exchange over a restored `IpNetwork`.
- Keep that probe separate from the ambient-socket `iroh` path until the Sandstorm network surface is proven on its own terms.
- The saved `IpNetwork` capability is now proven in a real outbound network operation, and the next useful step is a generic TCP byte-stream probe rather than an HTTP-only check.
- The current transport boundary now includes stateful saved-`IpNetwork` TCP sessions with explicit open/send/receive/close operations, which is closer to the shape a transport adapter will need than the earlier one-shot exchange path.
- The current transport boundary also includes a saved-`IpNetwork` UDP probe using `IpRemoteHost.getUdpPort()` and a local `UdpPort` callback capability, which is a more relevant slice for `iroh` than TCP-only experiments.
- The vendored Sandstorm networking schema now includes `RawUdpSocket`, `RawUdpReceiver`, and packet metadata sufficient for low-level QUIC-shaped transport work.
- Native `iroh 0.97.0` now exposes custom transports behind `unstable-custom-transports`, and the app has a Sandstorm-backed custom transport path wired in.
- Two grains can now exchange peer requests over Sandstorm raw UDP after saving an `IpInterface`, binding `RawUdp`, and sharing tickets with `custom:` transport addresses.
- The tunnel now carries live capabilities over one peer RPC session, including both `IpNetwork` and HTTP bridge `ApiSession`.
- Imported remote capabilities can now be assigned local object IDs and restored through `MainView.restore()`.
- The next meaningful boundary is no longer live transport or restore feasibility; it is durability across disconnect, reconnect, and restart.
