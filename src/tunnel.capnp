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

interface PeerBootstrap {
  listIpNetworkExports @0 () -> (exports :List(ExportedIpNetwork));
  getIpNetworkExport @1 (id :Text) -> (cap :Ip.IpNetwork, label :Text);
  listApiSessionExports @2 () -> (exports :List(ExportedApiSession));
  getApiSessionExport @3 (id :Text) -> (cap :ApiSession.ApiSession, label :Text);
}
