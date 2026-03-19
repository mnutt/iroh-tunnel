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
6. Test whether `IpInterface` is also needed for inbound UDP behavior.
7. Build a standalone Rust prototype of `capnp-rpc` over an `iroh` stream outside Sandstorm.
8. Repeat the transport test inside a packaged grain.

### Exit criteria

- one capability can be claimed and saved
- one peer connection can be established
- one live capability can cross the transport boundary

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
6. Add list and enable/disable controls in the UI.

### Exit criteria

- user can add a capability
- capability metadata is persisted
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

### Exit criteria

- two grains can pair manually
- reconnect works after process restart

## Phase 4: capability tunnel

### Goal

Send live capabilities over the peer connection.

### Tasks

1. Adapt an `iroh` stream to the transport needed by `capnp-rpc`.
2. Create one RPC connection per peer session.
3. Export enabled local capabilities.
4. Import remote capabilities.
5. Maintain a live mapping from remote share IDs to imported capability clients.
6. Handle disconnect and reconnection.

### Exit criteria

- one selected capability can be invoked remotely through the tunnel

## Phase 5: re-export into Sandstorm

### Goal

Make received capabilities available from the destination grain.

### Tasks

1. Implement `AppHooks`.
2. Define app object IDs for exported remote capabilities.
3. Map received capability records to app object IDs.
4. Implement `restore()` to return imported capabilities.
5. Implement `drop()` cleanup behavior.
6. Surface received capabilities in the UI.

### Exit criteria

- a capability received from the remote grain can be restored locally by Sandstorm

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

- Sandstorm networking capability may not be sufficient for `iroh`, especially if inbound UDP behavior requires more than `IpNetwork`.
- The Powerbox may require typed queries rather than an empty generic query.
- Raw capability re-export may have edge cases around persistence and lifetimes.

### Medium risk

- Grain suspension may make the product feel less "tunnel-like" than expected.
- Cap'n Proto RPC transport adaptation over `iroh` may need custom glue.

### Low risk

- The minimal HTTP UI is straightforward once the capability path is proven.

## Immediate next steps

1. Bootstrap the Sandstorm package and Rust app.
2. Keep the raw `UiView` smoke test passing while adding Sandstorm API calls.
3. Build the first Powerbox intake path.
4. Run an `iroh` transport spike before investing in full UI work.
