fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Try workspace-relative path first (local dev), fall back to crate-local copy (crates.io).
    let (proto, include) = if std::path::Path::new("../../proto/rune/wire/v1/rune.proto").exists() {
        ("../../proto/rune/wire/v1/rune.proto", "../../proto")
    } else {
        ("proto/rune/wire/v1/rune.proto", "proto")
    };
    println!("cargo:rerun-if-changed={proto}");
    println!("cargo:rerun-if-changed={include}");
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&[proto], &[include])?;
    Ok(())
}
