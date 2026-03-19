use std::fs;
use std::path::Path;

fn main() {
    let schema_dir = Path::new("vendor/include/sandstorm");
    println!("cargo:rerun-if-changed=vendor/include");

    let mut command = capnpc::CompilerCommand::new();
    command.import_path("vendor/include");
    command.src_prefix("vendor/include");
    command.file("vendor/include/capnp/stream.capnp");

    let mut entries = fs::read_dir(schema_dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    entries.sort();

    for entry in entries {
        if entry.extension().and_then(|ext| ext.to_str()) == Some("capnp") {
            command.file(entry);
        }
    }

    command.run().expect(
        "capnp schema compilation failed; ensure `capnp` is installed in the build environment",
    );
}
