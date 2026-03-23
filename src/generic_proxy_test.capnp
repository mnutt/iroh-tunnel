@0xcbd7d8ef2ff4b1e1;

interface Echo {
  ping @0 (text :Text) -> (text :Text);
}

interface EchoFactory {
  getEcho @0 (prefix :Text) -> (echo :Echo);
}

interface EchoRelay {
  callEcho @0 (echo :Echo, text :Text) -> (text :Text);
}
