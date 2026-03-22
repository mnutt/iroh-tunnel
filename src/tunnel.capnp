@0x95f1f692b377f0d1;

using Ip = import "/sandstorm/ip.capnp";
using ApiSession = import "/sandstorm/api-session.capnp";

struct ExportedIpNetwork {
  id @0 :Text;
  label @1 :Text;
}

struct ExportedApiSession {
  id @0 :Text;
  label @1 :Text;
}

enum CapabilityKind {
  ipNetwork @0;
  apiSession @1;
  other @2;
}

struct ExportedCapability {
  id @0 :Text;
  label @1 :Text;
  kind @2 :CapabilityKind;
  typeTag @3 :Text;
  descriptorJson @4 :Text;
}

struct CapabilityExport {
  cap @0 :Capability;
  label @1 :Text;
  kind @2 :CapabilityKind;
  typeTag @3 :Text;
  descriptorJson @4 :Text;
}

enum PairDecision {
  accepted @0;
  rejected @1;
}

struct PairRequest {
  version @0 :UInt16;
}

struct PairResponse {
  version @0 :UInt16;
  decision @1 :PairDecision;
}

struct PairControl {
  union {
    request @0 :PairRequest;
    response @1 :PairResponse;
  }
}

interface PeerBootstrap {
  listIpNetworkExports @0 () -> (exports :List(ExportedIpNetwork));
  getIpNetworkExport @1 (id :Text) -> (cap :Ip.IpNetwork, label :Text);
  listApiSessionExports @2 () -> (exports :List(ExportedApiSession));
  getApiSessionExport @3 (id :Text) -> (cap :ApiSession.ApiSession, label :Text);
  listCapabilityExports @4 () -> (exports :List(ExportedCapability));
  getCapabilityExport @5 (id :Text) -> CapabilityExport;
}
