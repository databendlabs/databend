// Copyright 2021 Datafuse Labs.
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
//
// https://github.com/rust-lang/rustfmt/blob/e1ab878ccb24cda1b9e1c48865b375230385fede/build.rs

use std::env;
use std::io::Result;
use std::path::Path;

fn main() -> Result<()> {
    build_proto()
}

fn build_proto() -> Result<()> {
    let pwd = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR env variable unset");
    let proto_path = Path::new(&pwd).join("proto");

    let proto_defs = [
        &Path::new(&proto_path).join(Path::new("datatype.proto")),
        &Path::new(&proto_path).join(Path::new("metadata.proto")),
        &Path::new(&proto_path).join(Path::new("user.proto")),
    ];

    for proto in proto_defs.iter() {
        println!("cargo:rerun-if-changed={}", proto.to_str().unwrap());
    }

    let mut config = prost_build::Config::new();
    config.btree_map(&["."]);
    config.protoc_arg("--experimental_allow_proto3_optional");
    tonic_build::configure()
        .type_attribute("IntervalKind", "#[derive(num_derive::FromPrimitive)]")
        .compile_with_config(config, &proto_defs, &[&proto_path])
}
