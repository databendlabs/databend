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
    // Keep build script rerun behavior explicit for the feature list we expose.
    println!("cargo:rerun-if-env-changed=PROFILE");
    println!("cargo:rerun-if-env-changed=OPT_LEVEL");

    // Features declared in `src/query/service/Cargo.toml`.
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_SIMD");
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_PYTHON_UDF");
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_DISABLE_INITIAL_EXEC_TLS");
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_JEMALLOC");
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_MEMORY_PROFILING");
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_STORAGE_HDFS");
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_IO_URING");

    let mut features = env::vars()
        .filter_map(|(k, v)| {
            if !k.starts_with("CARGO_FEATURE_") {
                return None;
            }
            if v != "1" {
                return None;
            }
            let name = k
                .trim_start_matches("CARGO_FEATURE_")
                .to_ascii_lowercase()
                .replace('_', "-");
            Some(name)
        })
        .collect::<Vec<_>>();

    features.sort();
    features.dedup();

    let features = features.join(",");
    println!("cargo:rustc-env=DATABEND_QUERY_CARGO_FEATURES={features}");
}
