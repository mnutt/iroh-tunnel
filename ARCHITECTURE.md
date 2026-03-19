# Architecture

## Overview

`iroh-tunnel` is currently a raw Sandstorm app:

- a human-facing browser UI served through raw `WebSession`
- raw Cap'n Proto integration with the grain's bootstrapped supervisor capabilities
- a future `iroh` transport layer between paired grains
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
- later request privileged networking capabilities if needed

The current implementation keeps the bootstrapped `SandstormApi` on `UiViewImpl` and clones the session `SessionContext` into each `WebSessionImpl`.

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

The next major Sandstorm milestone is to extend the current bootstrap to support app-managed persistent exports through `MainView(AppObjectId)`.

Responsibilities:

- define stable app object IDs
- map saved and received capabilities onto those IDs
- implement `restore(objectId)` for app-exported capabilities
- implement `drop(objectId)` cleanup

This is the critical piece for re-exporting remote capabilities into the local Sandstorm environment.

## Future transport components

### 4. Iroh endpoint manager

Responsibilities:

- initialize or load the grain's persistent `iroh` node identity
- display a local connection ticket
- accept a remote ticket
- establish and maintain a peer session
- surface transport state to the UI

State should be written under `/var`. Identity and pairing state must survive restarts.

### 5. Cap'n Proto RPC session over iroh

This is the actual tunnel.

Proposed model:

- open a bidirectional `iroh` stream
- adapt it to an async transport usable by `capnp-rpc`
- run one RPC connection per peer pairing
- export local capabilities into that RPC connection
- import remote capabilities from the peer

This is preferable to inventing a custom proxy protocol because Cap'n Proto RPC already models capability references, pipelining, method calls, and cancellation.

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
3. Remote grain imports those capabilities and records them in `received_caps`.
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

## Open questions

1. Is `IpNetwork` alone enough for `iroh`, or does practical operation inside Sandstorm also require `IpInterface` for inbound UDP?
2. What is the cleanest query model for the “pick a capability” UX: empty queries, typed queries, or a curated set?
3. What is the cleanest UX for exposing a received capability back into Sandstorm: direct app object links, offers, or both?
4. Do we need relay-only `iroh` mode as a compatibility fallback if direct UDP is unavailable?

## Feasibility gates

Do not commit to the full app until these are proven:

1. packaged grain can obtain the required Sandstorm networking capability
2. `iroh` can connect acceptably inside that sandbox
3. `capnp-rpc` works over the chosen `iroh` stream abstraction
4. a received capability can be re-exported via `MainView.restore()`
