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

//! User-facing binary version compatibility constants.
//!
//! These define the minimum compatible *binary* versions (SemVer) for display
//! in `--cmd ver`. The actual protocol version negotiation (CalVer) happens
//! in the external `databend-meta` crate and is unaffected by these values.

use std::sync::LazyLock;

use semver::Version;

/// Minimum meta-service binary version that this query binary requires.
pub static MIN_METASRV_VER_FOR_QUERY: LazyLock<Version> = LazyLock::new(|| Version::new(1, 2, 770));

/// Minimum query binary version that this meta-service binary requires.
pub static MIN_QUERY_VER_FOR_METASRV: LazyLock<Version> = LazyLock::new(|| Version::new(1, 2, 676));
