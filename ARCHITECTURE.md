# Architecture

## Overview

`iroh-tunnel` is a hybrid Sandstorm app:

- a human-facing HTTP UI served behind `sandstorm-http-bridge`
- a raw Cap'n Proto integration path to Sandstorm's local API socket
- an `iroh` transport layer between paired grains
- an app-managed registry of locally shared and remotely received capabilities

The app should not attempt to encode arbitrary interface semantics itself. Instead, it should transport live capabilities using Cap'n Proto RPC over an `iroh` stream.

## Why hybrid instead of raw-only

The UI is simpler to build as a normal web app. The capability operations are not. Sandstorm's Powerbox, session context, durable capability handling, and application hooks all require raw Cap'n Proto access. A hybrid design keeps the UI simple while preserving full Sandstorm capability support.

## Components

### 1. HTTP UI server

Responsibilities:

- render tunnel status
- show local node identity and pairing ticket
- accept a remote ticket
- start Powerbox requests from browser JS
- list capabilities being shared
- list capabilities received from the remote grain

The UI should be intentionally small. It only orchestrates user actions and shows state.

### 2. Sandstorm API bridge client

This component connects to `/tmp/sandstorm-api` and speaks the raw interfaces exposed by `sandstorm-http-bridge`.

Responsibilities:

- fetch `SandstormApi`
- fetch `SessionContext`
- claim Powerbox request tokens
- save and restore durable local capabilities
- support receiving Powerbox offers if needed later

This component is also where the app eventually requests special capabilities like network access.

For networking, the current assumption should be:

- request `IpNetwork` for outbound peer connectivity
- evaluate whether `IpInterface` is required for inbound UDP listener behavior
- treat both as privileged Powerbox requests likely requiring server-admin approval

### 3. AppHooks server

The app should set `bridgeConfig.expectAppHooks = true` and provide an `AppHooks` bootstrap object.

Responsibilities:

- implement `getViewInfo()`
- implement `restore(objectId)` for app-exported objects
- implement `drop(objectId)`

This is the mechanism that allows the grain to expose capabilities backed by app-managed object IDs. It is the critical piece for re-exporting remote capabilities into the local Sandstorm environment.

### 4. Iroh endpoint manager

Responsibilities:

- initialize or load the grain's persistent `iroh` node identity
- display a local connection ticket
- accept a remote ticket
- establish and maintain a peer session
- surface transport state to the UI

State should be written under `/var`. Identity and pairing state must survive restarts.

### 5. Capability registry

This is the app's durable metadata layer.

It should track:

- locally claimed capabilities that the user chose to share
- remotely received capabilities that are currently available
- stable app object IDs used by `AppHooks.restore()`
- labels and UI metadata
- whether a capability is enabled for export

Suggested records:

- `shared_caps`
  - `share_id`
  - `label`
  - `descriptor_summary`
  - `local_object_id`
  - `enabled`
- `received_caps`
  - `remote_share_id`
  - `label`
  - `local_export_object_id`
  - `last_seen_connection`
  - `status`

### 6. Cap'n Proto RPC session over iroh

This is the actual tunnel.

Proposed model:

- open a bidirectional `iroh` stream
- adapt it to an async transport usable by `capnp-rpc`
- run one RPC connection per peer pairing
- export local capabilities into that RPC connection
- import remote capabilities from the peer

This is preferable to inventing a custom proxy protocol because Cap'n Proto RPC already models capability references, pipelining, method calls, and cancellation.

## Data flow

### Local share flow

1. User clicks "Request capability".
2. Browser sends a Powerbox request via `window.parent.postMessage(...)`.
3. Sandstorm returns a claim token.
4. Browser POSTs the token to the app.
5. App redeems the token using the session ID and raw bridge API.
6. App stores metadata and a durable object reference for the chosen capability.
7. App marks the capability as shareable over the tunnel.

### Remote export flow

1. Peer connection comes up over `iroh`.
2. Local grain exports enabled capabilities into the RPC connection.
3. Remote grain imports those capabilities and records them in `received_caps`.
4. Remote grain maps each imported capability to a local app object ID.
5. When Sandstorm asks `AppHooks.restore(objectId)`, the app returns the imported live capability.

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

## Open questions

1. Is `IpNetwork` alone enough for `iroh`, or does practical operation inside Sandstorm also require `IpInterface` for inbound UDP?
2. Can an empty Powerbox query be used to present a generic "pick any capability" flow?
3. What is the cleanest UX for exposing a received capability back into Sandstorm: direct app object links, offers, or both?
4. Do we need relay-only `iroh` mode as a compatibility fallback if direct UDP is unavailable?

## Recommended repo layout

Once implementation starts:

```text
src/
  main.rs
  http/
  sandstorm/
  app_hooks/
  iroh_transport/
  registry/
  rpc_bridge/
static/
templates/
capnp/
.sandstorm/
  box.toml
```

## Feasibility gates

Do not commit to the full app until these are proven:

1. packaged grain can obtain the required Sandstorm networking capability
2. `iroh` can connect acceptably inside that sandbox
3. `capnp-rpc` works over the chosen `iroh` stream abstraction
4. a received capability can be re-exported via `AppHooks.restore()`
