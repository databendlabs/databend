// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![feature(os_str_display)]

use std::ffi::OsStr;
use std::ffi::OsString;
use std::process::Command;

const FLATC_BUILD_PATH: Option<&str> = option_env!("FLATC_PATH");

pub const SUPPORTED_FLATC_VERSION: &str = "24.3.25";

fn main() {
    compile(vec![OsString::from("schemas/geo.fbs")]).expect("flatbuffer compilation failed");
}

fn compile(files: Vec<OsString>) -> Result {
    let compiler = if let Some(build_flatc) = FLATC_BUILD_PATH {
        build_flatc.to_owned()
    } else {
        std::env::var("FLATC_PATH").unwrap_or("flatc".into())
    };
    let output_path = std::env::var_os("OUT_DIR")
        .ok_or(Error::OutputDirNotSet)
        .map(|mut s| {
            s.push(OsString::from("/flatbuffers"));
            s
        })?;

    confirm_flatc_version(&compiler)?;

    let mut args = vec![
        OsString::from("--rust"),
        OsString::from("-o"),
        output_path.clone(),
    ];
    args.extend(files.iter().cloned());
    run_flatc(&compiler, &args)?;

    for file in files {
        println!("cargo::rerun-if-changed={}", file.display());
    }
    println!("cargo:rerun-if-changed=build.rs");
    Ok(())
}

fn confirm_flatc_version(compiler: &str) -> Result {
    const FLATC_VERSION_PREFIX: &str = "flatc version ";

    // Output shows up in stdout
    let output = run_flatc(compiler, ["--version"])?;
    if output.stdout.starts_with(FLATC_VERSION_PREFIX) {
        let version_str = output.stdout[FLATC_VERSION_PREFIX.len()..].trim_end();
        if version_str == SUPPORTED_FLATC_VERSION {
            Ok(())
        } else {
            Err(Error::UnsupportedFlatcVersion(version_str.into()))
        }
    } else {
        Err(Error::InvalidFlatcOutput(output.stdout))
    }
}

struct ProgramOutput {
    pub stdout: String,
    pub _stderr: String,
}

fn run_flatc<I: IntoIterator<Item = S>, S: AsRef<OsStr>>(
    compiler: &str,
    args: I,
) -> Result<ProgramOutput> {
    let output = Command::new(compiler)
        .args(args)
        .output()
        .map_err(Error::FlatcSpawnFailure)?;
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    if output.status.success() {
        Ok(ProgramOutput {
            stdout,
            _stderr: stderr,
        })
    } else {
        Err(Error::FlatcErrorCode {
            status_code: output.status.code(),
            stdout,
            stderr,
        })
    }
}

/// Alias for a Result that uses [`Error`] as the default error type.
pub type Result<T = (), E = Error> = std::result::Result<T, E>;

/// Primary error type returned when you compile your flatbuffer specifications to Rust.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Returned when `flatc` returns with an non-zero status code for a reason not covered
    /// elsewhere in this enum.
    #[error(
        "flatc exited unexpectedly with status code {status_code:?}\n-- stdout:\n{stdout}\n-- stderr:\n{stderr}\n"
    )]
    FlatcErrorCode {
        /// Status code returned by `flatc` (none if program was terminated by a signal).
        status_code: Option<i32>,
        /// Standard output stream contents of the program
        stdout: String,
        /// Standard error stream contents of the program
        stderr: String,
    },
    /// Returned if `flatc --version` generates output we cannot parse. Usually means that the
    /// binary requested is not, in fact, flatc.
    #[error("flatc returned invalid output for --version: {0}")]
    InvalidFlatcOutput(String),
    /// Returned if the version of `flatc` does not match the supported version. Please refer to
    /// [`SUPPORTED_FLATC_VERSION`] for that.
    #[error(
        "flatc version '{0}' is unsupported by this version of the library. Please match your library with your flatc version"
    )]
    UnsupportedFlatcVersion(String),
    /// Returned if we fail to spawn a process with `flatc`. Usually means the supplied path to
    /// flatc does not exist.
    #[error("flatc failed to spawn: {0}")]
    FlatcSpawnFailure(#[source] std::io::Error),
    /// Returned if you failed to set either the output path or the `OUT_DIR` environment variable.
    #[error(
        "output directory was not set. Either call .set_output_path() or set the `OUT_DIR` env var"
    )]
    OutputDirNotSet,
    /// Returned when an issue arises when creating the symlink. Typically this will be things
    /// like permissions, a directory existing already at the file location, or other filesystem
    /// errors.
    #[error("failed to create symlink path requested: {0}")]
    SymlinkCreationFailure(#[source] std::io::Error),
}
