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

use lazy_static::lazy_static;

use crate::register_counter_family;
use crate::register_histogram_family_in_milliseconds;
use crate::Counter;
use crate::Family;
use crate::Histogram;
use crate::VecLabels;

const METRIC_QUERY_START: &str = "query_start";
const METRIC_QUERY_ERROR: &str = "query_error";
const METRIC_QUERY_SUCCESS: &str = "query_success";
const METRIC_QUERY_FAILED: &str = "query_failed";

const METRIC_QUERY_DURATION_MS: &str = "query_duration_ms";
const METRIC_QUERY_WRITE_ROWS: &str = "query_write_rows";
const METRIC_QUERY_WRITE_BYTES: &str = "query_write_bytes";
const METRIC_QUERY_WRITE_IO_BYTES: &str = "query_write_io_bytes";
const METRIC_QUERY_WRITE_IO_BYTES_COST_MS: &str = "query_write_io_bytes_cost_ms";
const METRIC_QUERY_SCAN_ROWS: &str = "query_scan_rows";
const METRIC_QUERY_SCAN_BYTES: &str = "query_scan_bytes";
const METRIC_QUERY_SCAN_IO_BYTES: &str = "query_scan_io_bytes";
const METRIC_QUERY_SCAN_IO_BYTES_COST_MS: &str = "query_scan_io_bytes_cost_ms";
const METRIC_QUERY_SCAN_PARTITIONS: &str = "query_scan_partitions";
const METRIC_QUERY_TOTAL_PARTITIONS: &str = "query_total_partitions";
const METRIC_QUERY_RESULT_ROWS: &str = "query_result_rows";
const METRIC_QUERY_RESULT_BYTES: &str = "query_result_bytes";

lazy_static! {
    pub static ref QUERY_START: Family<VecLabels, Counter> =
        register_counter_family(METRIC_QUERY_START);
    pub static ref QUERY_ERROR: Family<VecLabels, Counter> =
        register_counter_family(METRIC_QUERY_ERROR);
    pub static ref QUERY_SUCCESS: Family<VecLabels, Counter> =
        register_counter_family(METRIC_QUERY_SUCCESS);
    pub static ref QUERY_FAILED: Family<VecLabels, Counter> =
        register_counter_family(METRIC_QUERY_FAILED);
    pub static ref QUERY_DURATION_MS: Family<VecLabels, Histogram> =
        register_histogram_family_in_milliseconds(METRIC_QUERY_DURATION_MS);
    pub static ref QUERY_WRITE_ROWS: Family<VecLabels, Counter> =
        register_counter_family(METRIC_QUERY_WRITE_ROWS);
    pub static ref QUERY_WRITE_BYTES: Family<VecLabels, Counter> =
        register_counter_family(METRIC_QUERY_WRITE_BYTES);
    pub static ref QUERY_WRITE_IO_BYTES: Family<VecLabels, Counter> =
        register_counter_family(METRIC_QUERY_WRITE_IO_BYTES);
    pub static ref QUERY_WRITE_IO_BYTES_COST_MS: Family<VecLabels, Histogram> =
        register_histogram_family_in_milliseconds(METRIC_QUERY_WRITE_IO_BYTES_COST_MS);
    pub static ref QUERY_SCAN_ROWS: Family<VecLabels, Counter> =
        register_counter_family(METRIC_QUERY_SCAN_ROWS);
    pub static ref QUERY_SCAN_BYTES: Family<VecLabels, Counter> =
        register_counter_family(METRIC_QUERY_SCAN_BYTES);
    pub static ref QUERY_SCAN_IO_BYTES: Family<VecLabels, Counter> =
        register_counter_family(METRIC_QUERY_SCAN_IO_BYTES);
    pub static ref QUERY_SCAN_IO_BYTES_COST_MS: Family<VecLabels, Histogram> =
        register_histogram_family_in_milliseconds(METRIC_QUERY_SCAN_IO_BYTES_COST_MS);
    pub static ref QUERY_SCAN_PARTITIONS: Family<VecLabels, Counter> =
        register_counter_family(METRIC_QUERY_SCAN_PARTITIONS);
    pub static ref QUERY_TOTAL_PARTITIONS: Family<VecLabels, Counter> =
        register_counter_family(METRIC_QUERY_TOTAL_PARTITIONS);
    pub static ref QUERY_RESULT_ROWS: Family<VecLabels, Counter> =
        register_counter_family(METRIC_QUERY_RESULT_ROWS);
    pub static ref QUERY_RESULT_BYTES: Family<VecLabels, Counter> =
        register_counter_family(METRIC_QUERY_RESULT_BYTES);
}
