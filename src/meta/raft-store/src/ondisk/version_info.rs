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

use std::collections::BTreeMap;
use std::fmt;

use maplit::btreemap;

use crate::ondisk::DataVersion;

#[derive(Debug, Clone)]
pub struct VersionInfo {
    /// The commit hash that introduced a data version.
    #[allow(dead_code)]
    commit: &'static str,

    /// The date this commit was made.
    #[allow(dead_code)]
    commit_date: &'static str,

    /// The first build version hash that includes a data version.
    build: semver::Version,

    /// Additional description
    desc: &'static str,
}

impl fmt::Display for VersionInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}: {}", self.build, self.desc)
    }
}

impl VersionInfo {
    pub(crate) const fn new(
        commit: &'static str,
        commit_date: &'static str,
        build: semver::Version,
        desc: &'static str,
    ) -> Self {
        Self {
            commit,
            commit_date,
            build,
            desc,
        }
    }

    pub(crate) fn download_url(&self) -> String {
        format!(
            "https://github.com/datafuselabs/databend/releases/tag/v{}-nightly",
            self.build
        )
    }

    #[allow(dead_code)]
    const fn openraft_0708() -> Self {
        Self::new(
            "ea6002919d100c3880916e937b864be9f2f4cc38",
            "2023-02-28",
            new_semver(1, 0, 6),
            "Add compatible with openraft v07 and v08",
        )
    }

    const fn v0() -> Self {
        Self::new(
            "6a4181d8320a700d0c3d103e4d21188f451557a3",
            "2023-04-21",
            new_semver(1, 1, 13),
            "Add data version V0",
        )
    }

    const fn v001() -> Self {
        Self::new(
            "869cc74a2ca3d31d76c18314752d729d45580c15",
            "2023-05-18",
            new_semver(1, 1, 40),
            "Get rid of compat, use only openraft v08 data types",
        )
    }

    const fn v002() -> Self {
        Self::new(
            "3694e259c8e7c227fadfac5faa881cd2f2af6bbe",
            "2023-08-08",
            new_semver(1, 2, 53),
            "Persistent snapshot, in-memory state-machine",
        )
    }
}

pub static VERSION_INFOS: std::sync::LazyLock<BTreeMap<DataVersion, VersionInfo>> =
    std::sync::LazyLock::new(|| {
        btreemap! {
            DataVersion::V0 => VersionInfo::v0(),
            DataVersion::V001 => VersionInfo::v001() ,
            DataVersion::V002 => VersionInfo::v002() ,
        }
    });

const fn new_semver(major: u64, minor: u64, patch: u64) -> semver::Version {
    semver::Version {
        major,
        minor,
        patch,
        pre: semver::Prerelease::EMPTY,
        build: semver::BuildMetadata::EMPTY,
    }
}
