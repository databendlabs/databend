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

use std::env;
use std::path::Path;
use std::path::PathBuf;

fn main() {
    databend_common_building::setup();
    build_proto();
}

fn build_proto() {
    let manifest_dir =
        env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR env variable unset");

    let proto_dir = Path::new(&manifest_dir).join("proto");
    let protos = [
        &Path::new(&proto_dir).join(Path::new("raft.proto")),
        &Path::new(&proto_dir).join(Path::new("meta.proto")),
        &Path::new(&proto_dir).join(Path::new("request.proto")),
    ];

    for proto in protos.iter() {
        println!("cargo:rerun-if-changed={}", proto.to_str().unwrap());
    }

    println!("cargo:rerun-if-changed=build.rs");

    let mut config = prost_build::Config::new();
    config.protoc_arg("--experimental_allow_proto3_optional");

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    tonic_build::configure()
        .file_descriptor_set_path(out_dir.join("meta_descriptor.bin"))
        .type_attribute(
            "SeqV",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnGetRequest",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnPutRequest",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnDeleteRequest",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnDeleteByPrefixRequest",
            "#[derive(Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnCondition.ConditionResult",
            "#[derive(serde::Serialize, serde::Deserialize, num_derive::FromPrimitive, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnCondition.target",
            "#[derive(Eq,serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnOp.request",
            "#[derive(Eq,serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnCondition",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnOp",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnRequest",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnGetResponse",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnPutResponse",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnDeleteResponse",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnDeleteByPrefixResponse",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnOpResponse.response",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, derive_more::TryInto, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnOpResponse",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "TxnReply",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "WatchRequest",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "WatchResponse",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "Event",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .type_attribute(
            "KVMeta",
            "#[derive(Eq, serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]",
        )
        .field_attribute(
            "TxnPutRequest.ttl_ms",
            r#"#[serde(skip_serializing_if = "Option::is_none")]"#,
        )
        .compile_with_config(config, &protos, &[&proto_dir])
        .unwrap();
}
