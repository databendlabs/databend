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

use databend_common_base::runtime::metrics::register_gauge_family;
use databend_common_base::runtime::metrics::FamilyGauge;

pub static SYSTEM_VERSION_GAUGE: LazyLock<FamilyGauge<Vec<(&'static str, String)>>> =
    LazyLock::new(|| register_gauge_family("system_version"));

pub fn set_system_version(component: &str, semver: &str, sha: &str) {
    let labels = &vec![
        ("component", component.to_string()),
        ("semver", semver.to_string()),
        ("sha", sha.to_string()),
    ];

    SYSTEM_VERSION_GAUGE.get_or_create(labels).set(1);
}
