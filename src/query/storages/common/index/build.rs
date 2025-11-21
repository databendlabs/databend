// Copyright Qdrant
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

use std::env;

fn main() {
    println!("cargo:rerun-if-changed=cpp");
    let mut builder = cc::Build::new();

    let target_arch = env::var("CARGO_CFG_TARGET_ARCH")
        .expect("CARGO_CFG_TARGET_ARCH env-var is not defined or is not UTF-8");

    // TODO: Is `CARGO_CFG_TARGET_FEATURE` *always* defined?
    //
    // Cargo docs says that "boolean configurations are present if they are set,
    // and not present otherwise", so, what about "target features"?
    //
    // https://doc.rust-lang.org/cargo/reference/environment-variables.html (Ctrl-F CARGO_CFG_<cfg>)
    let target_feature = env::var("CARGO_CFG_TARGET_FEATURE")
        .expect("CARGO_CFG_TARGET_FEATURE env-var is not defined or is not UTF-8");

    if target_arch == "x86_64" {
        builder.file("cpp/sse.c");
        builder.file("cpp/avx2.c");

        if builder.get_compiler().is_like_msvc() {
            builder.flag("/arch:AVX");
            builder.flag("/arch:AVX2");
            builder.flag("/arch:SSE");
            builder.flag("/arch:SSE2");
        } else {
            builder.flag("-march=haswell");
        }

        // O3 optimization level
        builder.flag("-O3");
        // Use popcnt instruction
        builder.flag("-mpopcnt");
    } else if target_arch == "aarch64" && target_feature.split(',').any(|feat| feat == "neon") {
        builder.file("cpp/neon.c");
        builder.flag("-O3");
    }

    builder.compile("simd_utils");
}
