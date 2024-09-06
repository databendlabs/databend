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

use databend_common_base::runtime::metrics::register_histogram_family_in_seconds;
use databend_common_base::runtime::metrics::FamilyHistogram;

use crate::VecLabels;

const METRIC_REQUEST_EXTERNAL_DURATION: &str = "external_request_duration";
const METRIC_CONNECT_EXTERNAL_DURATION: &str = "external_connect_duration";

static REQUEST_EXTERNAL_DURATION: LazyLock<FamilyHistogram<VecLabels>> =
    LazyLock::new(|| register_histogram_family_in_seconds(METRIC_REQUEST_EXTERNAL_DURATION));

static CONNECT_EXTERNAL_DURATION: LazyLock<FamilyHistogram<VecLabels>> =
    LazyLock::new(|| register_histogram_family_in_seconds(METRIC_CONNECT_EXTERNAL_DURATION));

const LABEL_FUNCTION_NAME: &str = "function_name";

pub fn record_connect_external_duration(function_name: String, duration: Duration) {
    let labels = &vec![(LABEL_FUNCTION_NAME, function_name)];
    CONNECT_EXTERNAL_DURATION
        .get_or_create(labels)
        .observe(duration.as_millis_f64());
}

pub fn record_request_external_duration(function_name: String, duration: Duration) {
    let labels = &vec![(LABEL_FUNCTION_NAME, function_name)];
    REQUEST_EXTERNAL_DURATION
        .get_or_create(labels)
        .observe(duration.as_millis_f64());
}
