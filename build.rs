fn main() {
    // Re-run if Cargo.toml changes
    println!("cargo:rerun-if-changed=Cargo.toml");

    let metadata = cargo_metadata::MetadataCommand::new()
        .exec()
        .expect("Failed to get cargo metadata");

    // Find the turso package in the dependency list
    if let Some(turso) = metadata.packages.iter().find(|p| p.name == "turso") {
        println!("cargo:rustc-env=TURSO_VERSION={}", turso.version);
    } else {
        println!("cargo:rustc-env=TURSO_VERSION=unknown");
    }
}
