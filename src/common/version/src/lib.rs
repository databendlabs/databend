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

//! The compilation artefact of this crate cannot be cached, so to avoid long ci compilation times,
//! you should avoid introducing this crate in the underlying crate.

use std::sync::LazyLock;

use databend_common_base::base::BuildInfo;
use semver::Version;

pub const VERGEN_GIT_SHA: Option<&'static str> = option_env!("VERGEN_GIT_SHA");

pub const VERGEN_RUSTC_SEMVER: Option<&'static str> = option_env!("VERGEN_RUSTC_SEMVER");

pub const VERGEN_BUILD_TIMESTAMP: Option<&'static str> = option_env!("VERGEN_BUILD_TIMESTAMP");

pub const VERGEN_CARGO_FEATURES: Option<&'static str> = option_env!("VERGEN_CARGO_FEATURES");

pub const DATABEND_GIT_SEMVER: &str = env!("DATABEND_GIT_SEMVER");

pub const DATABEND_COMMIT_AUTHORS: &str = env!("DATABEND_COMMIT_AUTHORS");

pub const DATABEND_CREDITS_NAMES: &str = env!("DATABEND_CREDITS_NAMES");

pub const DATABEND_CREDITS_VERSIONS: &str = env!("DATABEND_CREDITS_VERSIONS");

pub const DATABEND_CREDITS_LICENSES: &str = env!("DATABEND_CREDITS_LICENSES");

pub const DATABEND_ENTERPRISE_LICENSE_EMBEDDED: &str = env!("DATABEND_ENTERPRISE_LICENSE_EMBEDDED");

pub const DATABEND_ENTERPRISE_LICENSE_PUBLIC_KEY: &str =
    env!("DATABEND_ENTERPRISE_LICENSE_PUBLIC_KEY");

pub const DATABEND_CARGO_CFG_TARGET_FEATURE: &str = env!("DATABEND_CARGO_CFG_TARGET_FEATURE");

pub const DATABEND_TELEMETRY_ENDPOINT: &str = env!("DATABEND_TELEMETRY_ENDPOINT");

pub const DATABEND_TELEMETRY_API_KEY: &str = env!("DATABEND_TELEMETRY_API_KEY");

pub static DATABEND_SEMVER: LazyLock<Version> = LazyLock::new(|| {
    let semver = DATABEND_GIT_SEMVER
        .strip_prefix('v')
        .unwrap_or(DATABEND_GIT_SEMVER);

    Version::parse(semver).unwrap_or_else(|e| panic!("Invalid semver: {:?}: {}", semver, e))
});

pub static DATABEND_COMMIT_VERSION: LazyLock<String> = LazyLock::new(|| {
    let git_sha = VERGEN_GIT_SHA;
    let rustc_semver = VERGEN_RUSTC_SEMVER;
    let timestamp = VERGEN_BUILD_TIMESTAMP;

    match (git_sha, rustc_semver, timestamp) {
        (Some(git_sha), Some(rustc_semver), Some(timestamp)) => {
            format!("{DATABEND_GIT_SEMVER}-{git_sha}(rust-{rustc_semver}-{timestamp})")
        }
        _ => String::new(),
    }
});

pub static DATABEND_GIT_SHA: LazyLock<String> = LazyLock::new(|| match VERGEN_GIT_SHA {
    Some(sha) => sha.to_string(),
    None => "unknown".to_string(),
});

pub static METASRV_COMMIT_VERSION: LazyLock<String> = LazyLock::new(|| {
    let git_sha = VERGEN_GIT_SHA;
    let rustc_semver = VERGEN_RUSTC_SEMVER;
    let timestamp = VERGEN_BUILD_TIMESTAMP;

    // simd is enabled by default now
    match (git_sha, rustc_semver, timestamp) {
        (Some(v2), Some(v3), Some(v4)) => {
            format!("{DATABEND_GIT_SEMVER}-{}-simd({}-{})", v2, v3, v4)
        }
        _ => String::new(),
    }
});

pub static BUILD_INFO: LazyLock<BuildInfo> = LazyLock::new(|| BuildInfo {
    semantic: DATABEND_SEMVER.clone(),
    commit_detail: DATABEND_COMMIT_VERSION.clone(),
    embedded_license: DATABEND_ENTERPRISE_LICENSE_EMBEDDED.to_string(),
});
