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

use std::sync::Arc;

use databend_common_base::base::ProgressValues;
use databend_common_metrics::interpreter::METRIC_QUERY_SCAN_PROGRESS_BYTES;
use databend_common_metrics::interpreter::METRIC_QUERY_SCAN_PROGRESS_ROWS;
use databend_common_metrics::interpreter::METRIC_QUERY_SPILL_PROGRESS_BYTES;
use databend_common_metrics::interpreter::METRIC_QUERY_SPILL_PROGRESS_ROWS;
use databend_common_metrics::interpreter::METRIC_QUERY_WRITE_PROGRESS_BYTES;
use databend_common_metrics::interpreter::METRIC_QUERY_WRITE_PROGRESS_ROWS;
use parking_lot::Mutex;
use prometheus_client::collector::Collector;
use prometheus_client::encoding::EncodeMetric;
use prometheus_client::metrics::counter::ConstCounter;

use crate::sessions::SessionManager;

/// [`SessionManagerMetricsCollector`] dumps the progress metrics of scan/write/spills
/// from the [`SessionManager`]'s running queries to the prometheus. To avoid the progress
/// metrics being decreased, we also need to accumulate these progress values after the query
/// is finished.
#[derive(Clone)]
pub struct SessionManagerMetricsCollector {
    inner: Arc<Mutex<SessionManagerMetricsCollectorInner>>,
}

pub(crate) struct SessionManagerMetricsCollectorInner {
    session_mgr: Option<Arc<SessionManager>>,
    finished_scan_total: ProgressValues,
    finished_write_total: ProgressValues,
    finished_spill_total: ProgressValues,
}

impl SessionManagerMetricsCollector {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(SessionManagerMetricsCollectorInner {
                session_mgr: None,
                finished_scan_total: ProgressValues::default(),
                finished_write_total: ProgressValues::default(),
                finished_spill_total: ProgressValues::default(),
            })),
        }
    }

    pub fn attach_session_manager(&self, session_mgr: Arc<SessionManager>) {
        let mut guard = self.inner.lock();
        guard.session_mgr.replace(session_mgr);
    }

    pub fn track_finished_query(
        &self,
        scan: ProgressValues,
        write: ProgressValues,
        join_spill: ProgressValues,
        aggregate_spill: ProgressValues,
        group_by_spill: ProgressValues,
        window_partition_spill: ProgressValues,
    ) {
        let mut guard = self.inner.lock();
        guard.finished_scan_total = guard.finished_scan_total.add(&scan);
        guard.finished_write_total = guard.finished_write_total.add(&write);
        guard.finished_spill_total = guard
            .finished_spill_total
            .add(&join_spill)
            .add(&aggregate_spill)
            .add(&group_by_spill)
            .add(&window_partition_spill);
    }
}

impl Default for SessionManagerMetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for SessionManagerMetricsCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SessionMetricsCollector")
    }
}

impl Collector for SessionManagerMetricsCollector {
    fn encode(
        &self,
        mut encoder: prometheus_client::encoding::DescriptorEncoder,
    ) -> Result<(), std::fmt::Error> {
        let processes = {
            match self.inner.lock().session_mgr.as_ref() {
                Some(mgr) => mgr.processes_info(),
                None => return Ok(()),
            }
        };

        let (mut scan_progress, mut write_progress, mut spill_progress) = {
            let guard = self.inner.lock();
            (
                guard.finished_scan_total.clone(),
                guard.finished_write_total.clone(),
                guard.finished_spill_total.clone(),
            )
        };
        for process in processes {
            if let Some(scan) = &process.scan_progress_value {
                scan_progress = scan_progress.add(scan);
            }
            if let Some(write) = &process.write_progress_value {
                write_progress = write_progress.add(write);
            }
            if let Some(spill) = &process.spill_progress_value {
                spill_progress = spill_progress.add(spill);
            }
        }

        let metrics = vec![
            (
                METRIC_QUERY_SCAN_PROGRESS_ROWS,
                scan_progress.rows as f64,
                "Total scan rows in progress.",
            ),
            (
                METRIC_QUERY_SCAN_PROGRESS_BYTES,
                scan_progress.bytes as f64,
                "Total scan bytes in progress.",
            ),
            (
                METRIC_QUERY_WRITE_PROGRESS_ROWS,
                write_progress.rows as f64,
                "Total write rows in progress.",
            ),
            (
                METRIC_QUERY_WRITE_PROGRESS_BYTES,
                write_progress.bytes as f64,
                "Total write bytes in progress.",
            ),
            (
                METRIC_QUERY_SPILL_PROGRESS_ROWS,
                spill_progress.rows as f64,
                "Total spill rows in progress.",
            ),
            (
                METRIC_QUERY_SPILL_PROGRESS_BYTES,
                spill_progress.bytes as f64,
                "Total spill bytes in progress.",
            ),
        ];

        for (name, value, help) in metrics {
            let counter = ConstCounter::new(value);
            let counter_encoder =
                encoder.encode_descriptor(name, help, None, counter.metric_type())?;
            counter.encode(counter_encoder)?;
        }

        Ok(())
    }
}
