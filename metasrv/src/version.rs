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

use once_cell::sync::Lazy;
use semver::BuildMetadata;
use semver::Prerelease;
use semver::Version;

pub static METASRV_COMMIT_VERSION: Lazy<String> = Lazy::new(|| {
    let build_semver = option_env!("VERGEN_GIT_SEMVER");
    let git_sha = option_env!("VERGEN_GIT_SHA_SHORT");
    let rustc_semver = option_env!("VERGEN_RUSTC_SEMVER");
    let timestamp = option_env!("VERGEN_BUILD_TIMESTAMP");

    let ver = match (build_semver, git_sha, rustc_semver, timestamp) {
        #[cfg(not(feature = "simd"))]
        (Some(v1), Some(v2), Some(v3), Some(v4)) => format!("{}-{}({}-{})", v1, v2, v3, v4),
        #[cfg(feature = "simd")]
        (Some(v1), Some(v2), Some(v3), Some(v4)) => {
            format!("{}-{}-simd({}-{})", v1, v2, v3, v4)
        }
        _ => String::new(),
    };
    ver
});

pub static METASRV_SEMVER: Lazy<Version> = Lazy::new(|| {
    let build_semver = option_env!("VERGEN_GIT_SEMVER");
    let semver = build_semver.expect("VERGEN_GIT_SEMVER can not be None");

    let semver = if semver.starts_with("v") {
        &semver[1..]
    } else {
        semver
    };

    Version::parse(semver).expect(&format!("Invalid semver: {:?}", semver))
});

/// Oldest compatible nightly meta-client version
pub static MIN_METACLI_SEMVER: Version = Version {
    major: 0,
    minor: 7,
    patch: 57,
    pre: Prerelease::EMPTY,
    build: BuildMetadata::EMPTY,
};

pub fn to_digit_ver(v: &Version) -> u64 {
    v.major * 1_000_000 + v.minor * 1_000 + v.patch
}

pub fn from_digit_ver(u: u64) -> Version {
    println!("{}", u);
    Version::new(u / 1_000_000, u / 1_000 % 1_000, u % 1_000)
}
