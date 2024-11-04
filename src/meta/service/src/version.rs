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

use feature_set::FeatureSet;
use semver::Version;

pub static METASRV_COMMIT_VERSION: LazyLock<String> = LazyLock::new(|| {
    let build_semver = option_env!("DATABEND_GIT_SEMVER");
    let git_sha = option_env!("VERGEN_GIT_SHA");
    let rustc_semver = option_env!("VERGEN_RUSTC_SEMVER");
    let timestamp = option_env!("VERGEN_BUILD_TIMESTAMP");

    match (build_semver, git_sha, rustc_semver, timestamp) {
        #[cfg(not(feature = "simd"))]
        (Some(v1), Some(v2), Some(v3), Some(v4)) => format!("{}-{}({}-{})", v1, v2, v3, v4),
        #[cfg(feature = "simd")]
        (Some(v1), Some(v2), Some(v3), Some(v4)) => {
            format!("{}-{}-simd({}-{})", v1, v2, v3, v4)
        }
        _ => String::new(),
    }
});

pub static METASRV_GIT_SEMVER: LazyLock<String> =
    LazyLock::new(|| match option_env!("DATABEND_GIT_SEMVER") {
        Some(v) => v.to_string(),
        None => "unknown".to_string(),
    });

pub static METASRV_GIT_SHA: LazyLock<String> =
    LazyLock::new(|| match option_env!("VERGEN_GIT_SHA") {
        Some(sha) => sha.to_string(),
        None => "unknown".to_string(),
    });

pub static METASRV_SEMVER: LazyLock<Version> = LazyLock::new(|| {
    let build_semver = option_env!("DATABEND_GIT_SEMVER");
    let semver = build_semver.expect("DATABEND_GIT_SEMVER can not be None");

    let semver = semver.strip_prefix('v').unwrap_or(semver);

    Version::parse(semver).unwrap_or_else(|e| panic!("Invalid semver: {:?}: {}", semver, e))
});

/// Oldest compatible nightly meta-client version
pub static MIN_METACLI_SEMVER: Version = Version::new(0, 9, 41);

/// The min meta-server version that can be deployed together in a cluster,
/// i.e., the network APIs are compatible.
///
/// - since 0.9.41
///   Add vote_v0
///   Add append_v0
///   Add install_snapshot_v0
///
/// - 2023-11-16: since 1.2.212:
///   Add install_snapshot_v1
pub static MIN_META_SEMVER: Version = Version::new(0, 9, 41);

/// Defines the feature set provided and required by raft server and client.
///
/// - The server depends on a sub set of the features provided by the client.
/// - The client depends on a sub set of the features provided by the server.
///
/// For example, an RPC call may look like the following:
///
/// - Server provides features S1, S2, S3, and the client requires S1, S3,
///   which is a subset of S1, S2, S3, so the call can be made.
///
/// - The client provides features C1, C2, C3, and the server requires C2, C3,
///   which is a subset of C1, C2, C3, so the response can be read by client.
///
/// ```text
///                  request
/// Client calls:  ------------> Server API provides:
/// - S1                         - S1
///                              - S2
/// - S3                         - S3
///
///                       response
/// Client can receives: <------ Server replies with:
/// - C1
/// - C2                         - C2
/// - C3                         - C3
/// ```
pub(crate) mod raft {
    pub(crate) mod server {
        use feature_set::add_provide;
        use feature_set::del_provide;
        use feature_set::Action;
        use feature_set::Provide;

        /// Feature set provided by raft server.
        ///
        /// This is a change-log of the features that raft server provides,
        /// and can be built into a BTreeMap of features with `FeatureSet::from_provides`
        #[rustfmt::skip]
        pub const PROVIDES: &[Action<Provide>] = &[
            add_provide(("vote",             0), "2023-02-16", (0,  9,  41)),
            add_provide(("append",           0), "2023-02-16", (0,  9,  41)),
            add_provide(("install_snapshot", 0), "2023-02-16", (0,  9,  41)),
            add_provide(("install_snapshot", 1), "2023-11-16", (1,  2, 212)),
            add_provide(("install_snapshot", 2), "2024-05-06", (1,  2, 453)),
            del_provide(("install_snapshot", 0), "2024-05-21", (1,  2, 479)),
            del_provide(("install_snapshot", 2), "2024-07-02", (1,  2, 552)),
            add_provide(("install_snapshot", 3), "2024-07-02", (1,  2, 552)),
        ];

        /// The client features that raft server depends on.
        #[allow(dead_code)]
        #[rustfmt::skip]
        pub const REQUIRES: &[(&str, u8, &str)] = &[
        ];
    }

    pub(crate) mod client {
        use feature_set::add_optional;
        use feature_set::add_require;
        use feature_set::del_require;
        use feature_set::Action;
        use feature_set::Require;

        /// The server features that raft client depends on.
        ///
        /// This is a change-log of the features that raft client depends on,
        /// and can be built into a BTreeMap of features with `FeatureSet::from_required`
        #[rustfmt::skip]
        pub const REQUIRES: &[Action<Require>] = &[
            add_require( ("vote",             0), "2023-02-16", (0,  9,  41)),
            add_require( ("append",           0), "2023-02-16", (0,  9,  41)),
            add_require( ("install_snapshot", 0), "2023-02-16", (0,  9,  41)),
            add_optional(("install_snapshot", 1), "2023-11-16", (1,  2, 212)),
            add_require( ("install_snapshot", 1), "2023-05-21", (1,  2, 479)),
            del_require( ("install_snapshot", 0), "2024-05-21", (1,  2, 479)),
            del_require( ("install_snapshot", 1), "2024-07-02", (1,  2, 552)),
            add_require( ("install_snapshot", 3), "2024-07-02", (1,  2, 552)),
            
        ];

        /// Feature set provided by raft client.
        #[allow(dead_code)]
        #[rustfmt::skip]
        pub const PROVIDES: &[(&str, u8, &str)] = &[
        ];
    }
}

pub fn raft_server_provides() -> FeatureSet {
    FeatureSet::from_provides(raft::server::PROVIDES)
}

pub fn raft_client_requires() -> FeatureSet {
    FeatureSet::from_required(raft::client::REQUIRES, false)
}

pub fn to_digit_ver(v: &Version) -> u64 {
    v.major * 1_000_000 + v.minor * 1_000 + v.patch
}

pub fn from_digit_ver(u: u64) -> Version {
    Version::new(u / 1_000_000, u / 1_000 % 1_000, u % 1_000)
}
