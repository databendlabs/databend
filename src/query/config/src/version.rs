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

use std::sync::LazyLock;

use semver::Version;

pub static DATABEND_COMMIT_VERSION: LazyLock<String> = LazyLock::new(|| {
    let semver = option_env!("DATABEND_GIT_SEMVER");
    let git_sha = option_env!("VERGEN_GIT_SHA");
    let rustc_semver = option_env!("VERGEN_RUSTC_SEMVER");
    let timestamp = option_env!("VERGEN_BUILD_TIMESTAMP");

    match (semver, git_sha, rustc_semver, timestamp) {
        #[cfg(not(feature = "simd"))]
        (Some(semver), Some(git_sha), Some(rustc_semver), Some(timestamp)) => {
            format!("{semver}-{git_sha}(rust-{rustc_semver}-{timestamp})")
        }
        #[cfg(feature = "simd")]
        (Some(semver), Some(git_sha), Some(rustc_semver), Some(timestamp)) => {
            format!("{semver}-{git_sha}-simd(rust-{rustc_semver}-{timestamp})")
        }
        _ => String::new(),
    }
});

pub static QUERY_GIT_SEMVER: LazyLock<String> =
    LazyLock::new(|| match option_env!("DATABEND_GIT_SEMVER") {
        Some(v) => v.to_string(),
        None => "unknown".to_string(),
    });

pub static QUERY_GIT_SHA: LazyLock<String> =
    LazyLock::new(|| match option_env!("VERGEN_GIT_SHA") {
        Some(sha) => sha.to_string(),
        None => "unknown".to_string(),
    });

pub static QUERY_SEMVER: LazyLock<Version> = LazyLock::new(|| {
    //
    let build_semver = option_env!("DATABEND_GIT_SEMVER");
    let semver = build_semver.expect("DATABEND_GIT_SEMVER can not be None");

    let semver = semver.strip_prefix('v').unwrap_or(semver);

    Version::parse(semver).unwrap_or_else(|e| panic!("Invalid semver: {:?}: {}", semver, e))
});
