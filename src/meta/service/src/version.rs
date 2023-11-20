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

use std::collections::BTreeSet;
use std::fmt;
use std::fmt::Formatter;

use once_cell::sync::Lazy;
use semver::BuildMetadata;
use semver::Prerelease;
use semver::Version;

pub static METASRV_COMMIT_VERSION: Lazy<String> = Lazy::new(|| {
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

pub static METASRV_SEMVER: Lazy<Version> = Lazy::new(|| {
    let build_semver = option_env!("DATABEND_GIT_SEMVER");
    let semver = build_semver.expect("DATABEND_GIT_SEMVER can not be None");

    let semver = semver.strip_prefix('v').unwrap_or(semver);

    Version::parse(semver).unwrap_or_else(|e| panic!("Invalid semver: {:?}: {}", semver, e))
});

/// Oldest compatible nightly meta-client version
pub static MIN_METACLI_SEMVER: Version = Version {
    major: 0,
    minor: 9,
    patch: 41,
    pre: Prerelease::EMPTY,
    build: BuildMetadata::EMPTY,
};

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

pub const REQUIRE: u8 = 0b11;
pub const OPTIONAL: u8 = 0b01;
pub const NOT_REQUIRE: u8 = 0b00;

pub const PROVIDE: u8 = 0b11;
pub const NOT_PROVIDE: u8 = 0b00;

/// Feature set provided by raft server.
#[rustfmt::skip]
pub const RAFT_SERVER_PROVIDES: &[(&str, u8, &str)] = &[
    ("vote_v0",             PROVIDE,     "2023-02-16 0.9.41"),
    ("append_v0",           PROVIDE,     "2023-02-16 0.9.41"),
    ("install_snapshot_v0", PROVIDE,     "2023-02-16 0.9.41"),
    ("install_snapshot_v1", PROVIDE,     "2023-11-16 1.2.212"),
];

/// The server features that raft client depends on.
#[rustfmt::skip]
pub const RAFT_CLIENT_REQUIRES: &[(&str, u8, &str)] = &[
    ("vote_v0",             REQUIRE,     "2023-02-16 0.9.41"),
    ("append_v0",           REQUIRE,     "2023-02-16 0.9.41"),
    ("install_snapshot_v0", REQUIRE,     "2023-02-16 0.9.41"),
    ("install_snapshot_v1", OPTIONAL,    "2023-11-16 1.2.212"),
];

/// Feature set provided by raft client.
#[rustfmt::skip]
pub const RAFT_CLIENT_PROVIDES: &[(&str, u8, &str)] = &[
];

/// The client features that raft server depends on.
#[rustfmt::skip]
pub const RAFT_SERVER_DEPENDS: &[(&str, u8, &str)] = &[
];

pub struct FeatureSet {
    features: BTreeSet<String>,
}

impl FeatureSet {
    pub fn new(fs: impl IntoIterator<Item = String>) -> Self {
        Self {
            features: fs.into_iter().collect(),
        }
    }
}

impl fmt::Display for FeatureSet {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            self.features.iter().cloned().collect::<Vec<_>>().join(", ")
        )
    }
}

pub fn raft_server_provides() -> FeatureSet {
    let mut set = BTreeSet::new();

    for (name, state, _) in RAFT_SERVER_PROVIDES {
        if *state == PROVIDE {
            set.insert(name.to_string());
        }
    }

    FeatureSet::new(set)
}

pub fn raft_client_requires() -> FeatureSet {
    let mut set = BTreeSet::new();

    for (name, state, _) in RAFT_CLIENT_REQUIRES {
        if *state == REQUIRE {
            set.insert(name.to_string());
        }
    }

    FeatureSet::new(set)
}

pub fn to_digit_ver(v: &Version) -> u64 {
    v.major * 1_000_000 + v.minor * 1_000 + v.patch
}

pub fn from_digit_ver(u: u64) -> Version {
    Version::new(u / 1_000_000, u / 1_000 % 1_000, u % 1_000)
}
