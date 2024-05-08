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

use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_metrics::interpreter::*;

use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct InterpreterMetrics;

const LABEL_HANDLER: &str = "handler";
const LABEL_KIND: &str = "kind";
const LABEL_TENANT: &str = "tenant";
const LABEL_CLUSTER: &str = "cluster";
const LABEL_CODE: &str = "code";

impl InterpreterMetrics {
    fn common_labels(ctx: &QueryContext) -> Vec<(&'static str, String)> {
        let handler_type = ctx.get_current_session().get_type().to_string();
        let query_kind = ctx.get_query_kind().to_string();
        let tenant_id = ctx.get_tenant();
        let cluster_id = GlobalConfig::instance().query.cluster_id.clone();

        vec![
            (LABEL_HANDLER, handler_type),
            (LABEL_KIND, query_kind),
            (LABEL_TENANT, tenant_id.tenant_name().to_string()),
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

        QUERY_DURATION_MS
            .get_or_create(labels)
            .observe(query_duration_ms);

        QUERY_WRITE_ROWS.get_or_create(labels).inc_by(written_rows);
        QUERY_WRITE_BYTES
            .get_or_create(labels)
            .inc_by(written_bytes);
        QUERY_WRITE_IO_BYTES
            .get_or_create(labels)
            .inc_by(written_io_bytes);

        if written_io_bytes_cost_ms > 0 {
            QUERY_WRITE_IO_BYTES_COST_MS
                .get_or_create(labels)
                .observe(written_io_bytes_cost_ms as f64);
        }

        QUERY_SCAN_ROWS.get_or_create(labels).inc_by(scan_rows);
        QUERY_SCAN_BYTES.get_or_create(labels).inc_by(scan_bytes);
        QUERY_SCAN_IO_BYTES
            .get_or_create(labels)
            .inc_by(scan_io_bytes);
        if scan_io_bytes_cost_ms > 0 {
            QUERY_SCAN_IO_BYTES_COST_MS
                .get_or_create(labels)
                .observe(scan_io_bytes_cost_ms as f64);
        }

        QUERY_SCAN_PARTITIONS
            .get_or_create(labels)
            .inc_by(scan_partitions);
        QUERY_TOTAL_PARTITIONS
            .get_or_create(labels)
            .inc_by(total_partitions);
        QUERY_RESULT_ROWS.get_or_create(labels).inc_by(result_rows);
        QUERY_RESULT_BYTES
            .get_or_create(labels)
            .inc_by(result_bytes);
    }

    pub fn record_query_start(ctx: &QueryContext) {
        let labels = Self::common_labels(ctx);
        QUERY_START.get_or_create(&labels).inc();
    }

    pub fn record_query_finished(ctx: &QueryContext, err: Option<ErrorCode>) {
        let mut labels = Self::common_labels(ctx);
        Self::record_query_detail(ctx, &labels);
        match err {
            None => {
                QUERY_SUCCESS.get_or_create(&labels).inc();
            }
            Some(err) => {
                labels.push((LABEL_CODE, err.code().to_string()));
                QUERY_FAILED.get_or_create(&labels).inc();
            }
        };
    }
}

fn convert_query_timestamp(time: SystemTime) -> u128 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::new(0, 0))
        .as_micros()
}
