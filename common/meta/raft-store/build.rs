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
use std::path::Path;

fn main() {
    common_building::setup();
    build_proto();
}

fn build_proto() {
    let manifest_dir =
        env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR env variable unset");

    let proto_dir = Path::new(&manifest_dir).join("proto");
    let protos = [&Path::new(&proto_dir).join(Path::new("meta.proto"))];

    for proto in protos.iter() {
        println!("cargo:rerun-if-changed={}", proto.to_str().unwrap());
    }
    tonic_build::configure()
        .compile(&protos, &[&proto_dir])
        .unwrap();
}
