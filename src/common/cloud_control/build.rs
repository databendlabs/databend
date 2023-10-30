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

// https://github.com/rust-lang/rustfmt/blob/e1ab878ccb24cda1b9e1c48865b375230385fede/build.rs

#![allow(clippy::uninlined_format_args)]

use std::env;
use std::fs;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::path::Path;
use std::process::Command;

use semver::Version;

fn main() -> Result<()> {
    build_proto()
}

fn build_proto() -> Result<()> {
    let pwd = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR env variable unset");
    let proto_path = Path::new(&pwd).join("proto");
    let proto_defs = fs::read_dir(&proto_path)?
        .map(|v| proto_path.join(v.expect("read dir must success").path()))
        .collect::<Vec<_>>();

    for proto in proto_defs.iter() {
        println!("cargo:rerun-if-changed={}", proto.to_str().unwrap());
    }

    // Check protoc version.
    let mut cmd = Command::new(prost_build::protoc_from_env());
    // get protoc version.
    cmd.arg("--version");
    let output = cmd.output()?;
    let version = if output.status.success() {
        let content = String::from_utf8_lossy(&output.stdout);
        let content = content.trim().split(' ').last().ok_or_else(|| {
            Error::new(
                ErrorKind::Other,
                format!("protoc --version got unexpected output: {}", content),
            )
        })?;
        lenient_semver::parse(content).map_err(|err| {
            Error::new(
                ErrorKind::Other,
                format!("protoc --version doesn't return valid version: {:?}", err),
            )
        })?
    } else {
        return Err(Error::new(
            ErrorKind::Other,
            format!("protoc failed: {}", String::from_utf8_lossy(&output.stderr)),
        ));
    };

    let mut config = prost_build::Config::new();
    config.btree_map(["."]);

    // Version before 3.12 doesn't support allow_proto3_optional
    if version < Version::new(3, 12, 0) {
        return Err(Error::new(
            ErrorKind::Other,
            format!(
                "protoc version is outdated, expect: >= 3.12.0, actual: {version}, reason: need feature --experimental_allow_proto3_optional"
            ),
        ));
    }
    // allow_proto3_optional has been enabled by default since 3.15.0
    if version < Version::new(3, 15, 0) {
        config.protoc_arg("--experimental_allow_proto3_optional");
    }

    tonic_build::configure().compile_with_config(config, &proto_defs, &[proto_path])
}
