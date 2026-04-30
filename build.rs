fn main() {
    // Re-run if Cargo.toml changes
    println!("cargo:rerun-if-changed=Cargo.toml");
    println!("cargo:rerun-if-changed=.git/HEAD");

    let metadata = cargo_metadata::MetadataCommand::new()
        .exec()
        .expect("Failed to get cargo metadata");

    // Find the turso package in the dependency list
    if let Some(turso) = metadata.packages.iter().find(|p| p.name == "turso") {
        println!("cargo:rustc-env=TURSO_VERSION={}", turso.version);
    } else {
        println!("cargo:rustc-env=TURSO_VERSION=unknown");
    }

    // Get git hash
    let git_hash = std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=GIT_HASH={}", git_hash);
}
