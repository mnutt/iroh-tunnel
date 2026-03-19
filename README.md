# iroh-tunnel

`iroh-tunnel` is a Sandstorm app for tunneling Sandstorm Cap'n Proto capabilities between two grains running on different Sandstorm instances.

The long-term model is:

- each grain runs an `iroh` endpoint
- a user pairs two `iroh-tunnel` grains
- a user imports one or more capabilities through the Sandstorm Powerbox
- selected capabilities are sent over an `iroh` transport using `capnp-rpc`
- the receiving grain re-exports them back into Sandstorm

This repository already contains a working raw Sandstorm baseline, not just design notes.

## Current status

Implemented today:

- raw Sandstorm bootstrap as `UiView` / `WebSession`
- raw `MainView(Text)` bootstrap with `restore()` / `drop()` hooks
- packaged static UI served from `/opt/app/client`
- browser-side Powerbox request flow via `window.parent.postMessage(...)`
- server-side `SessionContext.claimRequest()`
- server-side `SandstormApi.save()`
- typed `IpNetwork` Powerbox request generation and save path
- persisted saved-capability registry under `/var/iroh-tunnel`
- restore probing through `SandstormApi.restore()`
- persisted `iroh` node identity under `/var/iroh-tunnel/iroh-secret-key`
- relay-disabled local `iroh` endpoint bind on startup
- persisted remote-ticket field under `/var/iroh-tunnel/remote-ticket.txt`
- local ticket display plus one `iroh` echo probe over a bidi stream
- saved `IpNetwork` capability threaded into real outbound TCP operations
- capability-gated HTTP probe over restored `IpNetwork`
- capability-gated raw TCP byte probe over restored `IpNetwork`
- generic capability-gated binary exchange endpoint for transport experiments
- capability-gated UDP probe over restored `IpNetwork`

Not implemented yet:

- long-lived peer session management over `iroh`
- `capnp-rpc` over an `iroh` stream
- remote capability import/export
- re-export of received remote capabilities back into Sandstorm

## Goals

- Run as a Sandstorm app written in Rust.
- Use Sandstorm's Powerbox to gather capabilities from the current user.
- Persist selected capabilities in the local grain.
- Tunnel live Cap'n Proto capabilities over an `iroh` connection.
- Re-expose received capabilities from the destination grain back into Sandstorm.
- Provide a minimal UI for connection state and shared capability management.

## Non-goals for the first version

- Automatic peer discovery.
- Fully unattended background tunneling while no grain tab is open.
- Broad capability-type-specific UX.
- App market polish before feasibility is proven.

## Main technical risks

1. Sandstorm network access is capability-gated. The app does need `IpNetwork` or a related raw Cap'n Proto capability from the Powerbox, and the remaining question is how to use that capability surface for real transport.
2. `IpNetwork` acquisition is now proven, but `iroh` still is not using that capability surface. It is not yet proven that Sandstorm's capability-gated networking is sufficient for `iroh` in a packaged grain.
3. Empty generic Powerbox queries appear unreliable enough that typed queries are likely required in practice.
4. Re-exporting imported remote capabilities back into Sandstorm depends on wiring the current `MainView.restore()` / `drop()` baseline to live imported capabilities rather than saved local ones.

Current `iroh` integration assessment:

- In local source for `iroh 0.96.1`, `Endpoint::builder()` stores native `TransportConfig` entries and `bind()` passes them into `socket::Socket::spawn(...)`.
- The public `iroh::Endpoint` builder surface exposes native IP binding, relay config, QUIC config, DNS, and hooks, but not a custom packet I/O or socket backend.
- However, lower-level `iroh-quinn 0.16.1` does expose `Endpoint::new_with_abstract_socket(...)` and an `AsyncUdpSocket` trait.
- That means the blocker has narrowed: native `iroh::Endpoint` still cannot be adapted directly, and the lower-level seam is only partial.
- `iroh-quinn::AsyncUdpSocket::poll_recv(...)` expects per-datagram metadata including the remote source address. Sandstorm's current `UdpPort` callback only hands the app message bytes, not source address, destination IP, interface index, or ECN.
- So the next concrete blocker is packet metadata loss on the Sandstorm UDP callback surface, not lack of a custom socket seam in QUIC.

## Current stack

- Rust app server
- raw Sandstorm `UiView` / `WebSession` bootstrap on fd `3`
- browser UI served from packaged assets under `/opt/app/client`
- raw Sandstorm APIs via bootstrapped `SandstormApi` and per-session `SessionContext`
- `iroh` planned for peer transport
- `capnp` / `capnp-rpc` for forwarding live capabilities
- persistent state under `/var`

## Sandstorm networking notes

The `ip.capnp` model is encouraging:

- `IpNetwork` is the capability for full outbound network access.
- `IpRemoteHost.getTcpPort()` and `getUdpPort()` suggest both outbound TCP and UDP are part of the intended model.
- `IpInterface.listenTcp()` and `listenUdp()` cover inbound listeners.
- but inbound and outbound UDP both still bottom out at `UdpPort`, whose only method is `send(message, returnPort)`.
- the comments explicitly say these capabilities are usually admin-controlled and requested through the Powerbox.

For `iroh-tunnel`, that means:

- outbound `iroh` connectivity likely maps to `IpNetwork`
- inbound listener authority may require `IpInterface`, but the vendored schema does not make UDP receive any richer than the existing `UdpPort` callback shape
- this app should be treated as an admin-approved driver app until proven otherwise

Current status of that networking work:

- the grain can now request `IpNetwork` through the Powerbox
- the returned capability can be claimed and saved successfully
- the saved `IpNetwork` can now perform real outbound TCP exchanges
- the saved `IpNetwork` can now drive a UDP probe through `IpRemoteHost.getUdpPort()`
- the current native `iroh` probe still uses ambient sockets rather than the saved `IpNetwork` capability
- inspection of local `iroh 0.96.1` source indicates the native builder is blocked by missing custom transport injection
- inspection of vendored `sandstorm/ip.capnp` plus local `iroh-quinn 0.16.1` shows the lower-level path is additionally blocked because `UdpPort` only exposes `send(message, returnPort)` and does not provide packet source/destination metadata on receive

## Current transport boundary

The most important new boundary in the code is that saved-capability outbound TCP now goes through reusable helpers in [src/main.rs](/home/michael/tmp/iroh-tunnel/src/main.rs):

- `connect_saved_ip_network_tcp(...)`
- `finish_saved_ip_network_tcp_exchange(...)`
- `send_tcp_session_bytes(...)`
- `read_tcp_session_bytes(...)`

The browser UI also exposes a generic `Binary Exchange` action backed by `POST /api/network/exchange`.

For UDP-shaped experiments, the app also exposes `POST /api/network/udp-probe`, which:

- restores a saved `IpNetwork`
- resolves a remote host
- obtains a `UdpPort`
- sends a base64 payload
- waits for one reply packet via a local `UdpPort` callback capability

For stateful transport experiments, the app now also exposes a session-shaped control surface:

- `POST /api/network/session/open`
- `POST /api/network/session/send`
- `POST /api/network/session/receive`
- `POST /api/network/session/close`

Request format:

1. saved token hex
2. host
3. port
4. base64 payload

Response payload:

- JSON including `responseBase64`, `responseByteCount`, and a connection trace

The browser UI renders active capability-backed TCP sessions, lets you send chunks, poll for received chunks, and close the writer without forcing a one-shot request/response pattern, and now also offers a raw UDP probe path. This is the current control surface for the next transport-shaped experiments.

## Packaging status

This repo has been bootstrapped with:

- `spktool setupvm diy`
- `spktool vm create`
- `spktool init`

The active package definition is [`.sandstorm/sandstorm-pkgdef.capnp`](/home/michael/tmp/iroh-tunnel/.sandstorm/sandstorm-pkgdef.capnp). The app currently runs as a raw Sandstorm RPC server rather than an HTTP-bridge app.

## Documents

- [ARCHITECTURE.md](/home/michael/tmp/iroh-tunnel/ARCHITECTURE.md)
- [plan.md](/home/michael/tmp/iroh-tunnel/plan.md)
- [`.sandstorm/sandstorm-pkgdef.capnp`](/home/michael/tmp/iroh-tunnel/.sandstorm/sandstorm-pkgdef.capnp)

## Recommended next milestone

1. Keep the raw `UiView` baseline stable.
2. Keep the current app-owned object ID / `MainView.restore()` baseline stable.
3. Thread the saved `IpNetwork` capability into real network operations instead of assuming ambient sockets.
4. Use the capability-gated TCP and UDP experiment boundaries to identify the minimum Sandstorm transport surface `iroh` actually needs.
5. Treat native `iroh::Endpoint` transport injection as blocked, and treat Sandstorm UDP callback metadata as the next lower-level blocker for `iroh-quinn::AsyncUdpSocket`.
6. Send one live capability over one `iroh` RPC connection.

If that works, the rest is mostly persistence, UX, and operational hardening.
