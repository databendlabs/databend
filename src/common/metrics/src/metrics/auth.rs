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
use std::time::Duration;

use databend_common_base::runtime::metrics::FamilyCounter;
use databend_common_base::runtime::metrics::FamilyHistogram;
use databend_common_base::runtime::metrics::register_counter_family;
use databend_common_base::runtime::metrics::register_histogram_family_in_milliseconds;

use crate::VecLabels;

static AUTH_JWKS_REQUESTS_COUNT: LazyLock<FamilyCounter<VecLabels>> =
    LazyLock::new(|| register_counter_family("auth_jwks_requests_count"));
static AUTH_JWKS_REFRESH_DURATION: LazyLock<FamilyHistogram<VecLabels>> =
    LazyLock::new(|| register_histogram_family_in_milliseconds("auth_jwks_refresh_duration_ms"));

pub fn metrics_incr_auth_jwks_requests_count(url: String, reason: String, status: u16) {
    let labels = vec![
        ("url", url),
        ("reason", reason),
        ("status", status.to_string()),
    ];
    AUTH_JWKS_REQUESTS_COUNT.get_or_create(&labels).inc();
}

pub fn metrics_observe_auth_jwks_refresh_duration(url: String, duration: Duration) {
    let labels = vec![("url", url)];
    AUTH_JWKS_REFRESH_DURATION
        .get_or_create(&labels)
        .observe(duration.as_millis() as f64);
}
