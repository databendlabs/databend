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
use databend_common_base::runtime::metrics::register_histogram_family_in_rows;

use crate::VecLabels;

const METRIC_REQUEST_EXTERNAL_DURATION: &str = "external_request_duration";
const METRIC_CONNECT_EXTERNAL_DURATION: &str = "external_connect_duration";
const METRIC_RETRY: &str = "external_retry";
const METRIC_ERROR: &str = "external_error";
const METRIC_RUNNING_REQUESTS: &str = "external_running_requests";
const METRIC_REQUESTS: &str = "external_requests";
const METRIC_EXTERNAL_BATCH_ROWS: &str = "external_batch_rows";

static REQUEST_EXTERNAL_DURATION: LazyLock<FamilyHistogram<VecLabels>> =
    LazyLock::new(|| register_histogram_family_in_milliseconds(METRIC_REQUEST_EXTERNAL_DURATION));

static CONNECT_EXTERNAL_DURATION: LazyLock<FamilyHistogram<VecLabels>> =
    LazyLock::new(|| register_histogram_family_in_milliseconds(METRIC_CONNECT_EXTERNAL_DURATION));

static RETRY_EXTERNAL: LazyLock<FamilyCounter<VecLabels>> =
    LazyLock::new(|| register_counter_family(METRIC_RETRY));

static ERROR_EXTERNAL: LazyLock<FamilyCounter<VecLabels>> =
    LazyLock::new(|| register_counter_family(METRIC_ERROR));

static RUNNING_REQUESTS_EXTERNAL: LazyLock<FamilyCounter<VecLabels>> =
    LazyLock::new(|| register_counter_family(METRIC_RUNNING_REQUESTS));

static REQUESTS_EXTERNAL_EXTERNAL: LazyLock<FamilyCounter<VecLabels>> =
    LazyLock::new(|| register_counter_family(METRIC_REQUESTS));

static EXTERNAL_BATCH_ROWS: LazyLock<FamilyHistogram<VecLabels>> =
    LazyLock::new(|| register_histogram_family_in_rows(METRIC_EXTERNAL_BATCH_ROWS));

const LABEL_FUNCTION_NAME: &str = "function_name";
const LABEL_ERROR_KIND: &str = "error_kind";

pub fn record_connect_external_duration(function_name: impl Into<String>, duration: Duration) {
    let labels = &vec![(LABEL_FUNCTION_NAME, function_name.into())];
    CONNECT_EXTERNAL_DURATION
        .get_or_create(labels)
        .observe(duration.as_millis_f64());
}

pub fn record_request_external_duration(function_name: impl Into<String>, duration: Duration) {
    let labels = &vec![(LABEL_FUNCTION_NAME, function_name.into())];
    REQUEST_EXTERNAL_DURATION
        .get_or_create(labels)
        .observe(duration.as_millis_f64());
}

pub fn record_request_external_batch_rows(function_name: impl Into<String>, rows: usize) {
    let labels = &vec![(LABEL_FUNCTION_NAME, function_name.into())];
    EXTERNAL_BATCH_ROWS
        .get_or_create(labels)
        .observe(rows as f64);
}

pub fn record_retry_external(function_name: impl Into<String>, error_kind: impl Into<String>) {
    let labels = &vec![
        (LABEL_FUNCTION_NAME, function_name.into()),
        (LABEL_ERROR_KIND, error_kind.into()),
    ];
    RETRY_EXTERNAL.get_or_create(labels).inc();
}

pub fn record_error_external(function_name: impl Into<String>, error_kind: impl Into<String>) {
    let labels = &vec![
        (LABEL_FUNCTION_NAME, function_name.into()),
        (LABEL_ERROR_KIND, error_kind.into()),
    ];
    ERROR_EXTERNAL.get_or_create(labels).inc();
}

pub fn record_running_requests_external_start(function_name: impl Into<String>, cnt: u64) {
    let labels = &vec![(LABEL_FUNCTION_NAME, function_name.into())];
    RUNNING_REQUESTS_EXTERNAL.get_or_create(labels).inc_by(cnt);

    REQUESTS_EXTERNAL_EXTERNAL.get_or_create(labels).inc_by(cnt);
}

pub fn record_running_requests_external_finish(function_name: impl Into<String>, cnt: u64) {
    let labels = &vec![(LABEL_FUNCTION_NAME, function_name.into())];
    RUNNING_REQUESTS_EXTERNAL.get_or_create(labels).sub_by(cnt);
}
