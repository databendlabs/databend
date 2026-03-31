//! Build WASM binaries from source.

use anyhow::{Context, Result};
use std::path::PathBuf;
use std::process::Command;

/// Build a wasm binary from a Rust UDF script.
///
/// The `manifest` is a TOML string that will be appended to the generated `Cargo.toml`.
/// The `script` is a Rust source code string that will be written to `src/lib.rs`.
///
/// This function will block the current thread until the build is finished.
///
/// # Example
///
/// ```ignore
/// let manifest = r#"
/// [dependencies]
/// chrono = "0.4"
/// "#;
///
/// let script = r#"
/// use arrow_udf::function;
///
/// #[function("gcd(int, int) -> int")]
/// fn gcd(mut a: i32, mut b: i32) -> i32 {
///     while b != 0 {
///         (a, b) = (b, a % b);
///     }
///     a
/// }
/// "#;
///
/// let binary = arrow_udf_runtime::wasm::build::build(manifest, script).unwrap();
/// ```
pub fn build(manifest: &str, script: &str) -> Result<Vec<u8>> {
    let opts = BuildOpts {
        manifest: manifest.to_string(),
        script: script.to_string(),
        ..Default::default()
    };
    build_with(&opts)
}

/// Options for building wasm binaries.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct BuildOpts {
    /// A TOML string that will be appended to the generated `Cargo.toml`.
    pub manifest: String,
    /// A Rust source code string that will be written to `src/lib.rs`.
    pub script: String,
    /// Whether to build offline.
    pub offline: bool,
    /// The toolchain to use.
    pub toolchain: Option<String>,
    /// The version of the `arrow-udf` crate to use.
    /// If not specified, 0.2 will be used.
    pub arrow_udf_version: Option<String>,
    /// The temporary directory to use.
    /// If not specified, a random temporary directory will be used.
    pub tempdir: Option<PathBuf>,
}

/// Build a wasm binary with options.
pub fn build_with(opts: &BuildOpts) -> Result<Vec<u8>> {
    // install wasm32-wasip1 target
    if !opts.offline {
        let mut command = Command::new("rustup");
        if let Some(toolchain) = &opts.toolchain {
            command.arg(format!("+{}", toolchain));
        }
        let output = command
            .arg("target")
            .arg("add")
            .arg("wasm32-wasip1")
            .output()
            .context("failed to run `rustup target add wasm32-wasip1`")?;
        if !output.status.success() {
            return Err(anyhow::anyhow!(
                "failed to install wasm32-wasip1 target. ({})\n--- stdout\n{}\n--- stderr\n{}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ));
        }
    }

    let manifest = format!(
        r#"
[package]
name = "udf"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[dependencies.arrow-udf]
version = "{}"

{}"#,
        opts.arrow_udf_version.as_deref().unwrap_or("0.2"),
        opts.manifest
    );

    // create a temporary directory if not specified
    let tempdir = if opts.tempdir.is_some() {
        None
    } else {
        Some(tempfile::tempdir().context("failed to create tempdir")?)
    };
    let dir = match &opts.tempdir {
        Some(dir) => dir,
        None => tempdir.as_ref().unwrap().path(),
    };
    std::fs::create_dir_all(dir.join("src"))?;
    std::fs::write(dir.join("src/lib.rs"), &opts.script)?;
    std::fs::write(dir.join("Cargo.toml"), manifest)?;

    let mut command = Command::new("cargo");
    if let Some(toolchain) = &opts.toolchain {
        command.arg(format!("+{}", toolchain));
    }
    command
        .arg("build")
        .arg("--release")
        .arg("--target")
        .arg("wasm32-wasip1")
        .current_dir(dir);
    if opts.offline {
        command.arg("--offline");
    }
    let output = command.output().context("failed to run cargo build")?;
    if !output.status.success() {
        return Err(anyhow::anyhow!(
            "failed to build wasm ({})\n--- stdout\n{}\n--- stderr\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let binary_path = dir.join("target/wasm32-wasip1/release/udf.wasm");
    // strip the wasm binary if wasm-strip exists
    if Command::new("wasm-strip").arg("--version").output().is_ok() {
        let output = Command::new("wasm-strip")
            .arg(&binary_path)
            .output()
            .context("failed to strip wasm")?;
        if !output.status.success() {
            return Err(anyhow::anyhow!(
                "failed to strip wasm. ({})\n--- stdout\n{}\n--- stderr\n{}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ));
        }
    }
    let binary = std::fs::read(binary_path).context("failed to read wasm binary")?;
    Ok(binary)
}
