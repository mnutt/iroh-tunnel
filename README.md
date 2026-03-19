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
- persisted saved-capability registry under `/var/iroh-tunnel`
- restore probing through `SandstormApi.restore()`
- persisted `iroh` node identity under `/var/iroh-tunnel/iroh-secret-key`
- relay-disabled local `iroh` endpoint bind on startup
- persisted remote-ticket field under `/var/iroh-tunnel/remote-ticket.txt`

Not implemented yet:

- live peer dialing over `iroh`
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

1. Sandstorm network access is capability-gated. The app likely needs `IpNetwork` or a related raw Cap'n Proto capability from the Powerbox. This must be validated in practice.
2. `iroh` depends on QUIC and normally benefits from UDP. It is not yet proven that Sandstorm's networking capability surface is sufficient for `iroh` in a packaged grain.
3. Empty generic Powerbox queries appear unreliable enough that typed queries may be required in practice.
4. Re-exporting imported remote capabilities back into Sandstorm depends on wiring the current `MainView.restore()` / `drop()` baseline to live imported capabilities rather than saved local ones.

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
- the comments explicitly say these capabilities are usually admin-controlled and requested through the Powerbox.

For `iroh-tunnel`, that means:

- outbound `iroh` connectivity likely maps to `IpNetwork`
- inbound UDP may require `IpInterface` if `iroh` needs explicit listener binding
- this app should be treated as an admin-approved driver app until proven otherwise

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
2. Replace the temporary `ApiSession` Powerbox query with the intended query model.
3. Keep the current app-owned object ID / `MainView.restore()` baseline stable.
4. Move from persisted pairing state to a real `iroh` dial/accept path.
5. Send one live capability over one `iroh` RPC connection.
6. Re-export it on the remote side through app-managed persistent object IDs.

If that works, the rest is mostly persistence, UX, and operational hardening.
