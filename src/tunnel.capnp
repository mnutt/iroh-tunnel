@0x95f1f692b377f0d1;

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

struct CapabilityRegistration {
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
  listCapabilityExports @0 () -> (exports :List(ExportedCapability));
  getCapabilityExport @1 (id :Text) -> CapabilityExport;
  registerCapability @2 CapabilityRegistration -> (remoteObjectId :Text);
  getRegisteredCapability @3 (remoteObjectId :Text) -> CapabilityExport;
}
