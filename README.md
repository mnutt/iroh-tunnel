# iroh-tunnel

`iroh-tunnel` is a planned Sandstorm app for tunneling Sandstorm Cap'n Proto capabilities between two grains running on different Sandstorm instances.

The core idea is:

- each grain runs an `iroh` endpoint
- a user pairs two `iroh-tunnel` grains
- a user can import a capability into one grain through the Sandstorm Powerbox
- that capability is made available to the paired grain over an `iroh` transport
- the receiving grain re-exports the capability back into Sandstorm so local users can use it

This repository currently contains the design and implementation plan, not a working app.

## Goals

- Run as a Sandstorm app written in Rust.
- Use Sandstorm's Powerbox to gather capabilities from the current user.
- Persist selected capabilities in the local grain.
- Tunnel live Cap'n Proto capabilities over an `iroh` connection.
- Re-expose received capabilities from the destination grain back into Sandstorm.
- Provide a minimal web UI for connection state and shared capability management.

## Non-goals for the first version

- Automatic peer discovery.
- Fully unattended background tunneling while no grain tab is open.
- Broad capability-type-specific UX.
- App market polish before feasibility is proven.

## Main technical risks

1. Sandstorm network access is capability-gated. The app likely needs `IpNetwork` or a related raw Cap'n Proto capability from the Powerbox. This must be validated in practice.
2. `iroh` depends on QUIC and normally benefits from UDP. It is not yet proven that Sandstorm's networking capability surface is sufficient for `iroh` in a packaged grain.
3. The "empty Powerbox query returns everything I can access" behavior is not confirmed by the docs and must be tested.
4. Exporting imported remote capabilities back into Sandstorm depends on a correct `AppHooks` implementation.

## Proposed stack

- Rust app server
- minimal HTTP UI behind `sandstorm-http-bridge`
- raw Cap'n Proto access over `/tmp/sandstorm-api`
- `iroh` for peer transport
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

`spktool` created the `.sandstorm/` project scaffold, but in this environment it did not emit a managed `sandstorm-pkgdef.capnp` file into `.sandstorm/`. The root-level [sandstorm-pkgdef.capnp](/home/michael/tmp/iroh-tunnel/sandstorm-pkgdef.capnp) is therefore a draft design artifact for now, not a generated source of truth.

## Documents

- [ARCHITECTURE.md](/home/michael/tmp/iroh-tunnel/ARCHITECTURE.md)
- [plan.md](/home/michael/tmp/iroh-tunnel/plan.md)
- [sandstorm-pkgdef.capnp](/home/michael/tmp/iroh-tunnel/sandstorm-pkgdef.capnp)

## Recommended first milestone

Build the smallest vertical slice:

1. Package a Rust app with `sandstorm-http-bridge` and `AppHooks`.
2. Display a local `iroh` node ticket in the UI and allow manual peer ticket entry.
3. Request one capability through the Powerbox.
4. Save it locally.
5. Send it over one live `iroh` connection using `capnp-rpc`.
6. Re-export it on the remote side through `AppHooks.restore()`.

If that works, the rest is mainly UX, persistence, and operational hardening.
