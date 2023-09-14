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

use std::time::Duration;

use common_metrics::register_histogram_in_milliseconds;
use common_metrics::Histogram;
use lazy_static::lazy_static;
use metrics::histogram;

// TODO: to be removed
pub static METRIC_MYSQL_PROCESSOR_REQUEST_DURATION: &str = "mysql.process_request_duration";
pub static METRIC_INTERPRETER_USEDTIME: &str = "interpreter.usedtime";

lazy_static! {
    static ref MYSQL_PROCESSOR_REQUEST_DURATION: Histogram =
        register_histogram_in_milliseconds("mysql_process_request_duration_ms");
    static ref MYSQL_INTERPRETER_USEDTIME: Histogram =
        register_histogram_in_milliseconds("mysql_interpreter_usedtime_ms");
}

pub fn observe_mysql_process_request_duration(duration: Duration) {
    histogram!(
        METRIC_MYSQL_PROCESSOR_REQUEST_DURATION,
        duration.as_millis() as u64
    );
    MYSQL_PROCESSOR_REQUEST_DURATION.observe(duration.as_millis() as f64);
}

pub fn observe_mysql_interpreter_used_time(duration: Duration) {
    histogram!(METRIC_INTERPRETER_USEDTIME, duration.as_millis() as u64);
    MYSQL_INTERPRETER_USEDTIME.observe(duration.as_millis() as f64);
}
