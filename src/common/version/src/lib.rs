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

use semver::Version;

pub const VERGEN_GIT_SHA: Option<&'static str> = option_env!("VERGEN_GIT_SHA");

pub const VERGEN_RUSTC_SEMVER: Option<&'static str> = option_env!("VERGEN_RUSTC_SEMVER");

pub const VERGEN_BUILD_TIMESTAMP: Option<&'static str> = option_env!("VERGEN_BUILD_TIMESTAMP");

pub const VERGEN_CARGO_FEATURES: Option<&'static str> = option_env!("VERGEN_CARGO_FEATURES");

pub const VERGEN_BUILD_SEMVER: Option<&'static str> = option_env!("VERGEN_BUILD_SEMVER");

pub const DATABEND_GIT_SEMVER: Option<&'static str> = option_env!("DATABEND_GIT_SEMVER");

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
    let build_semver = DATABEND_GIT_SEMVER;
    let semver = build_semver.expect("DATABEND_GIT_SEMVER can not be None");

    let semver = semver.strip_prefix('v').unwrap_or(semver);

    Version::parse(semver).unwrap_or_else(|e| panic!("Invalid semver: {:?}: {}", semver, e))
});

pub static UDF_CLIENT_USER_AGENT: LazyLock<String> =
    LazyLock::new(|| format!("databend-query/{}", *DATABEND_SEMVER));

pub static DATABEND_COMMIT_VERSION: LazyLock<String> = LazyLock::new(|| {
    let semver = DATABEND_GIT_SEMVER;
    let git_sha = VERGEN_GIT_SHA;
    let rustc_semver = VERGEN_RUSTC_SEMVER;
    let timestamp = VERGEN_BUILD_TIMESTAMP;

    match (semver, git_sha, rustc_semver, timestamp) {
        (Some(semver), Some(git_sha), Some(rustc_semver), Some(timestamp)) => {
            format!("{semver}-{git_sha}(rust-{rustc_semver}-{timestamp})")
        }
        _ => String::new(),
    }
});

pub static DATABEND_GIT_SHA: LazyLock<String> = LazyLock::new(|| match VERGEN_GIT_SHA {
    Some(sha) => sha.to_string(),
    None => "unknown".to_string(),
});
