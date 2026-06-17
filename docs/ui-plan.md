# UI Plan

## Goal

Turn the current debug-first interface into a simple product UI centered on one idea:

- choose capabilities to share
- connect a tunnel to another grain
- use capabilities received through that tunnel

The main UI should avoid transport jargon and Cap'n Proto type names wherever possible. Advanced transport and debugging details should move behind a secondary `Debug` surface.

## Primary Layout

Use a two-column layout with a vertical split:

- left column: `Capabilities`
- right column: `Tunnel`

Desktop:

- two fixed sections side by side
- `Capabilities` wider than `Tunnel`

Mobile:

- stack the same sections vertically
- keep the same internal grouping and wording

## Left Column: Capabilities

This column contains two sections:

- `Share Capabilities`
- `Received Capabilities`

### Share Capabilities

Purpose:

- manage the capabilities this grain is offering into the tunnel

Default state:

- empty list
- helper text: `No shared capabilities yet.`
- primary button: `Find Capability to Share`

Interaction model:

- clicking `Find Capability to Share` opens the Powerbox picker
- the initial query should be broad / empty so the user can choose from any accessible capability
- after selection, the capability is added to the list immediately

Each shared capability row should show:

- capability label
- source grain or app name if available
- secondary fallback text if a source name is not available
- `Shared` toggle
- `Remove` action

Row example:

- `Office Preview`
- `from Files Grain`
- toggle: on/off
- button: `Remove`

Semantics:

- `Shared` on:
  - the capability is advertised to the connected peer
- `Shared` off:
  - the capability remains saved locally but is not advertised
- `Remove`:
  - removes the capability from this app's shared list
  - requires confirmation

Notes:

- source grain name is desirable but not required for MVP
- acceptable fallback:
  - saved Powerbox label
  - app title if exposed
  - generic text like `from another grain`

### Received Capabilities

Purpose:

- show the capabilities the remote grain has made available to this grain

Default state:

- empty list
- helper text: `No capabilities received.`

Each received capability row should show:

- capability label
- source grain or app name if available
- `Enabled` toggle

Row example:

- `Office Preview`
- `from Remote Grain`
- toggle: on/off

Semantics:

- `Enabled` on:
  - the capability is active locally and can be restored / used
- `Enabled` off:
  - the capability remains remembered but is not active locally

MVP choice:

- do not expose `Forget` or `Delete` in the main row yet
- keep that for a later iteration or debug flow if needed

Reason:

- end-user model stays simpler
- `Received` is primarily about availability, not low-level lifecycle management

## Right Column: Tunnel

Purpose:

- request the networking capability needed for the tunnel
- display tunnel status
- connect to a remote grain
- approve or reject new pairing requests
- disconnect, disable, or forget an approved pairing

Main states:

- `Tunnel unavailable`
- `Tunnel disabled`
- `Tunnel disconnected`
- `Tunnel connecting`
- `Waiting for remote to accept`
- `Incoming connection request`
- `Tunnel connected`

### Tunnel Unavailable

Shown when the app does not yet have the raw UDP / `IpInterface` capability needed to bind the tunnel.

UI:

- status label: `Tunnel unavailable`
- short helper text explaining that the tunnel needs network permission
- primary button: `Request Tunneling Capability`

Behavior:

- clicking `Request Tunneling Capability` requests the needed raw UDP capability
- if granted, the app automatically saves it and activates it for raw UDP
- there should be no separate manual `Use for RawUdp` step in the product UI

### Tunnel Disabled

Shown when this grain already has an approved peer relationship, but the tunnel is intentionally off.

UI:

- status label: `Tunnel disabled`
- helper text explaining that the peer is approved but the tunnel is not currently active
- primary control: `Enable Tunnel`
- secondary action: `Forget Connection`

Semantics:

- this is different from missing network permission
- enabling the tunnel should not ask for network permission again
- first implementation should return to `Tunnel disconnected`
- later implementation may optionally auto-reconnect if a saved remote ticket exists

### Tunnel Disconnected

Shown after the tunnel capability exists, the peer is approved or a remote ticket has been entered, but there is no active peer connection.

UI:

- status label: `Tunnel disconnected`
- local ticket display
- copy button: `Copy Ticket`
- input: `Remote Grain Ticket`
- primary button: `Connect`

Behavior:

- entering a remote ticket and clicking `Connect` should:
  - save the remote ticket
  - attempt connection immediately

### Tunnel Connecting

Shown while a connection attempt is in progress.

UI:

- status label: `Tunnel connecting`
- remote ticket remains visible
- disable repeated connect presses
- show connection error inline if the attempt fails

### Waiting For Remote To Accept

Shown on the outbound side after transport connection succeeds but before the remote grain has approved the pairing.

UI:

- status label: `Waiting for remote to accept`
- helper text: `Connection request sent. The remote grain needs to accept it.`
- remote ticket remains visible
- secondary action: `Cancel`

Behavior:

- this is the first-contact state for a new or not-yet-approved peer
- no shared or received capabilities should become active yet

### Incoming Connection Request

Shown on the inbound side when another grain has connected at the transport layer but has not yet been approved.

UI:

- status label: `Incoming connection request`
- show remote peer node id
- primary action: `Accept Connection`
- secondary action: `Reject`

Semantics:

- `Accept Connection`:
  - approves this peer relationship
  - transitions to `Tunnel connected`
- `Reject`:
  - closes the pending connection
  - does not create an approved pairing
- this is not the same as `Enable Tunnel`

### Tunnel Connected

Shown when the tunnel is live.

UI:

- status label: `Tunnel connected`
- remote grain name or peer label if available
- toggle: `Tunnel enabled`
- secondary action: `Forget Connection`

Semantics:

- `Tunnel enabled` on:
  - active connection remains up
- `Tunnel enabled` off:
  - disconnect now, but keep the approved peer relationship and saved remote ticket
- `Forget Connection`:
  - confirm first
  - disconnect and clear the saved remote ticket
  - clear the approved peer relationship

## Pairing And Trust Model

The product should distinguish between trusting a peer and merely running the tunnel.

Important distinction:

- `Accept Connection` means:
  - approve a new peer relationship
- `Enable Tunnel` means:
  - re-activate an already approved relationship

Recommended MVP model:

- only one approved peer at a time
- first contact from an unknown peer requires explicit `Accept Connection`
- once a peer is approved, later reconnects can skip the accept prompt
- `Disable Tunnel` should disconnect without forgetting the approved peer
- `Forget Connection` should clear both the saved ticket and the approved peer identity

Suggested internal state concepts:

- approved peer
- pending incoming peer
- saved remote ticket
- tunnel enabled
- connection state

This avoids conflating:

- missing permission
- pending approval
- known-but-disabled
- connected

## Product Language

Use end-user language in the main UI.

Preferred terms:

- `Tunnel`
- `Share Capabilities`
- `Received Capabilities`
- `Request Tunneling Capability`
- `Enable Tunnel`
- `Accept Connection`
- `Reject`
- `Connected`
- `Disconnected`
- `Shared`
- `Enabled`

Avoid in the main UI:

- `RawUdp`
- `Peer RPC`
- `ApiSession`
- `IpNetwork`
- `MainView`
- `restore(objectId)`

Those terms are acceptable in a debug view only.

## Empty, Loading, and Error States

### Share Capabilities

Empty:

- `No shared capabilities yet.`

Powerbox failure:

- inline error banner near `Find Capability to Share`

Toggle failure:

- revert toggle and show inline error

### Received Capabilities

Empty:

- `No capabilities received.`

Disconnected but remembered:

- keep item visible
- show muted subtext like `Unavailable until tunnel reconnects`

Enable failure:

- revert toggle and show inline error

### Tunnel

Missing network capability:

- `Request Tunneling Capability`

Connection failure:

- show inline error under the tunnel controls

Pending incoming connection:

- show peer id and `Accept Connection` / `Reject`

Waiting for remote approval:

- show helper text instead of falling back to plain disconnected

Remote export disappeared:

- received item remains listed but marked unavailable

## Confirmations

Require confirmation for:

- removing a shared capability
- forgetting a connection

Recommended copy:

- remove shared capability:
  - title: `Remove shared capability?`
  - body: `This will stop offering the capability through the tunnel.`
- forget connection:
  - title: `Forget connection?`
  - body: `This will disconnect the tunnel and remove the saved remote ticket.`

No confirmation needed for:

- turning `Shared` on/off
- turning `Enabled` on/off
- turning `Tunnel enabled` off/on

## Debug Surface

Provide a `Debug` button in the `Tunnel` area.

It should open a modal or drawer with:

- local and remote peer ids
- raw local ticket
- raw saved remote ticket
- current connection error details
- advanced transport state
- existing low-level probe tools
- any capability-type-specific diagnostics

This preserves developer usefulness without polluting the main UI.

## Data / Behavior Decisions

These choices define the MVP product behavior.

### Shared capability persistence

- shared capabilities remain in the list across disconnect and restart
- `Shared` state persists

### Received capability persistence

- received capabilities remain remembered across disconnect and restart
- `Enabled` state persists
- capabilities with no currently live remote export remain visible but unavailable

### Connection persistence

- saved remote ticket persists across restart
- approved peer identity should persist across restart once that feature exists
- `Tunnel enabled` off disconnects but keeps the saved remote ticket
- `Forget Connection` clears the ticket

### Capability labels

- prefer capability label + source grain/app name
- if source grain/app name is not reliably available, fall back gracefully

## Information Hierarchy

The screen should tell this story at a glance:

1. is the tunnel available?
2. is there a connection request waiting for approval?
3. is it connected or disabled?
4. what am I sharing?
5. what have I received?

The user should not have to understand:

- transport mode
- capability schemas
- object ids
- persistence internals

## MVP Implementation Plan

### Phase A: Main Layout

1. Replace the current top-level layout with the two-column split.
2. Create the three visible product sections:
   - `Share Capabilities`
   - `Received Capabilities`
   - `Tunnel`
3. Add a `Debug` button in `Tunnel`.

### Phase B: Tunnel Product Flow

1. Replace the current raw-UDP setup flow with:
   - `Request Tunneling Capability`
2. Auto-bind the raw UDP capability after request.
3. Show:
   - local ticket
   - copy action
   - remote ticket input
   - connect action
4. Add connection-request states:
   - `Waiting for remote to accept`
   - `Incoming connection request`
5. Add:
   - `Accept Connection`
   - `Reject`
6. Add connected/disconnected/disabled state rendering.
7. Add `Tunnel enabled` toggle and `Forget Connection`.

### Phase C: Share Capabilities

1. Replace the current saved-cap debug list with a product list of shared capabilities.
2. Add `Find Capability to Share`.
3. Persist list membership and `Shared` state.
4. Add remove confirmation.

### Phase D: Received Capabilities

1. Show received capabilities as a product list.
2. Add `Enabled` toggle semantics.
3. Show unavailable-but-remembered state when disconnected or missing remotely.

### Phase E: Move Debug Tools Out of the Main Flow

1. Move transport lab and probe-heavy controls into the debug modal.
2. Remove transport jargon from the main screen.
3. Keep all currently useful diagnostics available in debug.

## Out of Scope For This UI Pass

- multi-peer UI
- per-capability permissions beyond on/off
- aliasing remote peers
- sorting and grouping customization
- rich source-grain profile display
- polished design system work beyond clarity and usability

## Open Questions

These do not block the plan, but should be answered during implementation.

1. How reliably can we display the source grain or app name for a capability?
2. Should a received capability row eventually support `Forget` in the main UI?
3. Should disabling a received capability only hide it from restore/use, or should it also stop auto-reimport until re-enabled?
4. Should the tunnel panel display a user-editable alias for the remote grain in addition to the raw ticket?
5. When re-enabling an already approved tunnel, should the app auto-reconnect or return to disconnected first?

## Success Criteria

The UI is successful if a new user can:

1. enable the tunnel
2. copy their ticket
3. paste the remote ticket and connect
4. share one capability
5. see one received capability
6. disable or re-enable sharing without needing debug tools

If a user needs to understand `RawUdp`, `ApiSession`, `IpNetwork`, or `Peer RPC` to complete the basic flow, the UI is still too complicated.
