# Architecture

## Overview

`iroh-tunnel` is currently a raw Sandstorm app:

- a human-facing browser UI served through raw `WebSession`
- raw Cap'n Proto integration with the grain's bootstrapped supervisor capabilities
- an `iroh` transport layer between paired grains using Sandstorm `RawUdp`
- an app-managed registry of locally saved and remotely received capabilities

The app should not attempt to encode arbitrary interface semantics itself. It should transport live capabilities using Cap'n Proto RPC over an `iroh` stream.

## Current architecture

### 1. Raw UiView / WebSession server

The grain boots directly as a raw Sandstorm RPC server on fd `3`.

Responsibilities:

- serve packaged browser assets from `/opt/app/client`
- render status and saved capability state
- start Powerbox requests from browser JS
- receive request tokens back through the grain's own HTTP surface
- trigger save and restore probes through Sandstorm

This path is already working.

### 2. Sandstorm session/supervisor integration

Each browser session receives:

- the bootstrapped `SandstormApi`
- a per-session `SessionContext`

Responsibilities:

- claim Powerbox request tokens
- save durable local capabilities
- restore saved capabilities by token
- request and save privileged networking capabilities like `IpNetwork`

The current implementation keeps the bootstrapped `SandstormApi` on `UiViewImpl` and clones the session `SessionContext` into each `WebSessionImpl`.

This is now proven for capability acquisition:

- browser-side Powerbox requests can ask for `IpNetwork`
- the returned request token can be claimed server-side
- the resulting `IpNetwork` capability can be saved like any other capability

### 3. Saved capability registry

The app persists a simple registry under `/var/iroh-tunnel`.

Current record shape:

- `id`
- `label`
- `saved_token`
- `created_at_ms`

Current storage:

- [saved-caps.tsv](/var/iroh-tunnel/saved-caps.tsv) at runtime

This registry is intentionally small but already shaped to evolve into app-managed persistent object IDs.

## Next Sandstorm step

### MainView-backed persistent exports

The next major Sandstorm milestone is to turn the now-working in-memory remote object mapping into a durable Sandstorm export layer through `MainView(AppObjectId)`.

Responsibilities:

- define stable app object IDs that survive reconnect and restart
- persist metadata for received capabilities, not just local saved capabilities
- implement `restore(objectId)` for app-exported capabilities after reconnect
- implement `drop(objectId)` cleanup

This is the critical piece for durable re-export of remote capabilities into the local Sandstorm environment.

## Future transport components

### 4. Iroh endpoint manager

Responsibilities:

- initialize or load the grain's persistent `iroh` node identity
- bind a local endpoint
- display local addressing state
- accept and persist a remote ticket
- later establish and maintain a peer session
- surface transport state to the UI

State should be written under `/var`. Identity and pairing state must survive restarts.

Current implementation:

- persists the secret key at `/var/iroh-tunnel/iroh-secret-key`
- restores a saved `IpInterface` capability for raw UDP binding when configured
- binds a Sandstorm `RawUdpSocket` via `IpInterface.bindRawUdp()`
- injects a Sandstorm-backed custom transport into a relay-disabled `iroh::Endpoint`
- exposes local direct addresses and custom transport addresses through `GET /api/state`
- persists a raw remote ticket string at `/var/iroh-tunnel/remote-ticket.txt`
- persists the selected raw UDP interface token and bound port under `/var/iroh-tunnel`
- runs a background echo accept loop for the probe ALPN
- exposes a one-shot connect probe that dials the stored remote ticket and performs an echo round trip

This is no longer only an ambient-socket spike. It proves that paired grains can exchange `iroh` traffic over Sandstorm's low-level raw UDP interface. It is still not a durable peer-session manager yet.

Current integration assessment:

- saved `IpNetwork` is now proven for outbound TCP and outbound UDP reply flow
- vendored Sandstorm networking definitions now include a `RawUdpSocket` surface with packet metadata
- native `iroh 0.97.0` now exposes custom transports behind `unstable-custom-transports`
- the app now restores a saved `IpInterface`, binds `RawUdp`, and injects a Sandstorm-backed custom transport into `iroh::Endpoint`
- paired grains can now exchange tickets containing `custom:` transport addresses and complete a peer probe over that path
- the remaining work is application plumbing and session management rather than transport feasibility:
- restore and configure the right `IpInterface` automatically and early in startup
- make Sandstorm-mode transport selection explicit rather than prototype-shaped
- evolve the probe path into a durable peer session suitable for capability exchange

### 5. Cap'n Proto RPC session over iroh

This is the actual tunnel.

Proposed model:

- open a bidirectional `iroh` stream
- adapt it to an async transport usable by `capnp-rpc`
- run one RPC connection per peer pairing
- export local capabilities into that RPC connection
- import remote capabilities from the peer

This is preferable to inventing a custom proxy protocol because Cap'n Proto RPC already models capability references, pipelining, method calls, and cancellation.

This is now proven in the current implementation:

- paired grains can establish one live `capnp-rpc` session over an `iroh` stream carried by Sandstorm `RawUdp`
- one grain can export a saved `IpNetwork` or `ApiSession`
- the other grain can import that live capability and invoke it successfully
- imported remote capabilities can now be assigned local app object IDs and restored through `MainView.restore(objectId)`

### 6. Remote capability registry

This is the durable metadata layer for exported/imported capabilities.

Suggested records:

- `shared_caps`
  - `id`
  - `label`
  - `saved_token`
  - `descriptor_summary`
  - `local_object_id`
  - `enabled`
  - `created_at`
- `received_caps`
  - `remote_share_id`
  - `label`
  - `local_export_object_id`
  - `last_seen_connection`
  - `status`

## Data flow

### Local save flow

1. User clicks `Request Capability`.
2. Browser sends a Powerbox request via `window.parent.postMessage(...)`.
3. Sandstorm returns a claim token.
4. Browser POSTs the token to the grain.
5. App redeems the token using the current session's `SessionContext`.
6. App saves the returned capability using `SandstormApi.save()`.
7. App writes registry metadata and the durable save token to `/var`.

### Local restore probe flow

1. User selects a saved capability in the UI.
2. Browser sends the saved token back to the grain.
3. App decodes the token and calls `SandstormApi.restore()`.
4. Success proves the durable token is still usable.

### Remote export flow

1. Peer connection comes up over `iroh`.
2. Local grain exports enabled capabilities into the RPC connection.
3. Remote grain imports those capabilities and records them in an in-memory imported-cap registry.
4. Remote grain maps each imported capability to a local app object ID.
5. When Sandstorm asks `MainView.restore(objectId)`, the app returns the imported live capability.

### Local consumption flow

1. A user in the receiving grain selects a received capability in the UI.
2. The app offers or otherwise exposes the capability through the local grain.
3. Sandstorm invokes `restore()` for the selected app object ID.
4. The app returns the imported remote capability.

## Security model

- Pairing is explicit and user-driven.
- Only capabilities that a local user explicitly claims and enables are shared.
- The tunnel should authenticate the remote peer by persistent `iroh` node identity.
- The UI should clearly distinguish:
  - capabilities this grain is sharing
  - capabilities received from the other side
- The app should never silently broaden authority.

Future hardening:

- peer allowlist
- per-capability share toggles
- confirmation prompts before re-offering a newly received capability
- audit log of share and use events

## Lifecycle and restart behavior

Sandstorm grains are suspended aggressively. The app must assume:

- the process can stop whenever no tab is open
- active tunnel connections will drop
- reconnect is normal

Therefore:

- persist node identity and pairing config
- persist capability metadata
- rebuild live imported capabilities after reconnect
- show disconnected state honestly in the UI

The first version should not promise always-on transport.

## Current proven baseline

The following are already demonstrated in this repo:

1. packaged grain boots as a raw `UiView` / `WebSession`
2. packaged browser assets render through that raw session
3. browser `postMessage` Powerbox request returns a token
4. server-side `claimRequest()` works
5. `SandstormApi.save()` persists the selected capability
6. `SandstormApi.restore()` can probe the persisted token later
7. `MainView.restore(objectId)` can resolve saved local capabilities by app object ID
8. local `iroh` identity and relay-disabled endpoint state survive restart
9. Sandstorm `RawUdp` can bind through a saved `IpInterface` capability
10. paired grains can run one live `capnp-rpc` session over `iroh` on Sandstorm `RawUdp`
11. one grain can import and invoke a remote `IpNetwork` over that session
12. one grain can import and invoke a remote HTTP bridge `ApiSession` over that session
13. imported remote capabilities can be assigned app object IDs like `remote-cap-1`
14. `MainView.restore(objectId)` can resolve those imported remote live capabilities in memory
10. two grains can exchange `iroh` traffic over Sandstorm raw UDP custom transport addresses

## Open questions

1. What is the cleanest lifecycle for restoring and rebinding the chosen `IpInterface` capability on every startup without fragile manual setup?
2. How should the app represent and persist peer/session state once the current one-shot `iroh` probe becomes a durable RPC connection?
3. What is the cleanest query model for the “pick a capability” UX: empty queries, typed queries, or a curated set?
4. What is the cleanest UX for exposing a received capability back into Sandstorm: direct app object links, offers, or both?
5. Do we need relay-only `iroh` mode as a compatibility fallback if direct UDP is unavailable?

## Feasibility gates

Do not commit to the full app until these are proven:

1. packaged grain can obtain the required Sandstorm networking capability
2. `iroh` can connect acceptably through that capability surface rather than through assumed ambient sockets
3. `capnp-rpc` works over the chosen `iroh` stream abstraction
4. a received capability can be re-exported via `MainView.restore()`
