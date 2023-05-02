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
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use common_config::GlobalConfig;
use common_exception::ErrorCode;
use common_metrics::label_counter_with_val_and_labels;
use common_metrics::label_histogram_with_val;

use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct InterpreterMetrics;

const QUERY_START: &str = "query_start";
const QUERY_ERROR: &str = "query_error";
const QUERY_SUCCESS: &str = "query_success";
const QUERY_FAILED: &str = "query_failed";

const QUERY_DURATION_MS: &str = "query_duration_ms";
const QUERY_WRITE_ROWS: &str = "query_write_rows";
const QUERY_WRITE_BYTES: &str = "query_write_bytes";
const QUERY_WRITE_IO_BYTES: &str = "query_write_io_bytes";
const QUERY_WRITE_IO_BYTES_COST_MS: &str = "query_write_io_bytes_cost_ms";
const QUERY_SCAN_ROWS: &str = "query_scan_rows";
const QUERY_SCAN_BYTES: &str = "query_scan_bytes";
const QUERY_SCAN_IO_BYTES: &str = "query_scan_io_bytes";
const QUERY_SCAN_IO_BYTES_COST_MS: &str = "query_scan_io_bytes_cost_ms";
const QUERY_SCAN_PARTITIONS: &str = "query_scan_partitions";
const QUERY_TOTAL_PARTITIONS: &str = "query_total_partitions";
const QUERY_RESULT_ROWS: &str = "query_result_rows";
const QUERY_RESULT_BYTES: &str = "query_result_bytes";

const LABEL_HANDLER: &str = "handler";
const LABEL_KIND: &str = "kind";
const LABEL_TENANT: &str = "tenant";
const LABEL_CLUSTER: &str = "cluster";
const LABEL_CODE: &str = "code";

impl InterpreterMetrics {
    fn common_labels(ctx: &QueryContext) -> Vec<(&'static str, String)> {
        let handler_type = ctx.get_current_session().get_type().to_string();
        let query_kind = ctx.get_query_kind();
        let tenant_id = ctx.get_tenant();
        let cluster_id = GlobalConfig::instance().query.cluster_id.clone();

        vec![
            (LABEL_HANDLER, handler_type),
            (LABEL_KIND, query_kind),
            (LABEL_TENANT, tenant_id),
            (LABEL_CLUSTER, cluster_id),
        ]
    }

    fn record_query_detail(ctx: &QueryContext, labels: &Vec<(&'static str, String)>) {
        let event_time = convert_query_timestamp(SystemTime::now());
        let query_start_time = convert_query_timestamp(ctx.get_created_time());
        let query_duration_ms = (event_time - query_start_time) as f64 / 1_000.0;

        let data_metrics = ctx.get_data_metrics();

        let written_rows = ctx.get_write_progress_value().rows as u64;
        let written_bytes = ctx.get_write_progress_value().bytes as u64;
        let written_io_bytes = data_metrics.get_write_bytes() as u64;
        let written_io_bytes_cost_ms = data_metrics.get_write_bytes_cost();

        let scan_rows = ctx.get_scan_progress_value().rows as u64;
        let scan_bytes = ctx.get_scan_progress_value().bytes as u64;
        let scan_io_bytes = data_metrics.get_read_bytes() as u64;
        let scan_io_bytes_cost_ms = data_metrics.get_read_bytes_cost();

        let scan_partitions = data_metrics.get_partitions_scanned();
        let total_partitions = data_metrics.get_partitions_total();

        let result_rows = ctx.get_result_progress_value().rows as u64;
        let result_bytes = ctx.get_result_progress_value().bytes as u64;

        label_histogram_with_val(QUERY_DURATION_MS, labels, query_duration_ms);

        label_counter_with_val_and_labels(QUERY_WRITE_ROWS, labels, written_rows);
        label_counter_with_val_and_labels(QUERY_WRITE_BYTES, labels, written_bytes);
        label_counter_with_val_and_labels(QUERY_WRITE_IO_BYTES, labels, written_io_bytes);
        if written_io_bytes_cost_ms > 0 {
            label_histogram_with_val(
                QUERY_WRITE_IO_BYTES_COST_MS,
                labels,
                written_io_bytes_cost_ms as f64,
            );
        }

        label_counter_with_val_and_labels(QUERY_SCAN_ROWS, labels, scan_rows);
        label_counter_with_val_and_labels(QUERY_SCAN_BYTES, labels, scan_bytes);
        label_counter_with_val_and_labels(QUERY_SCAN_IO_BYTES, labels, scan_io_bytes);
        if scan_io_bytes_cost_ms > 0 {
            label_histogram_with_val(
                QUERY_SCAN_IO_BYTES_COST_MS,
                labels,
                scan_io_bytes_cost_ms as f64,
            );
        }

        label_counter_with_val_and_labels(QUERY_SCAN_PARTITIONS, labels, scan_partitions);
        label_counter_with_val_and_labels(QUERY_TOTAL_PARTITIONS, labels, total_partitions);
        label_counter_with_val_and_labels(QUERY_RESULT_ROWS, labels, result_rows);
        label_counter_with_val_and_labels(QUERY_RESULT_BYTES, labels, result_bytes);
    }

    pub fn record_query_start(ctx: &QueryContext) {
        let labels = Self::common_labels(ctx);
        label_counter_with_val_and_labels(QUERY_START, &labels, 1);
    }

    pub fn record_query_finished(ctx: &QueryContext, err: Option<ErrorCode>) {
        let mut labels = Self::common_labels(ctx);
        Self::record_query_detail(ctx, &labels);
        match err {
            None => {
                label_counter_with_val_and_labels(QUERY_SUCCESS, &labels, 1);
            }
            Some(err) => {
                labels.push((LABEL_CODE, err.code().to_string()));
                label_counter_with_val_and_labels(QUERY_FAILED, &labels, 1);
            }
        };
    }

    pub fn record_query_error(ctx: &QueryContext) {
        let labels = Self::common_labels(ctx);
        label_counter_with_val_and_labels(QUERY_ERROR, &labels, 1);
    }
}

fn convert_query_timestamp(time: SystemTime) -> u128 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::new(0, 0))
        .as_micros()
}
