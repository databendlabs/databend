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

use super::SessionManager;

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
        &mut self,
        scan: ProgressValues,
        write: ProgressValues,
        spill: ProgressValues,
    ) {
        let mut guard = self.inner.lock();
        guard.finished_scan_total = guard.finished_scan_total.add(&scan);
        guard.finished_write_total = guard.finished_write_total.add(&write);
        guard.finished_spill_total = guard.finished_spill_total.add(&spill);
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

        let mut scan_bytes = 0;
        let mut scan_rows = 0;
        let mut write_bytes = 0;
        let mut write_rows = 0;
        let mut spill_bytes = 0;
        let mut spill_rows = 0;
        // TODO: ensure the process is RUNNING
        for process in processes {
            if let Some(scan) = &process.scan_progress_value {
                scan_bytes += scan.bytes;
                scan_rows += scan.rows;
            }
            if let Some(write) = &process.write_progress_value {
                write_bytes += write.bytes;
                write_rows += write.rows;
            }
            if let Some(spill) = &process.spill_progress_value {
                spill_bytes += spill.bytes;
                spill_rows += spill.rows;
            }
        }

        let scan_rows_counter = ConstCounter::new(scan_rows as f64);
        let scan_rows_encoder = encoder.encode_descriptor(
            METRIC_QUERY_SCAN_PROGRESS_ROWS,
            "Total scan rows in progress.",
            None,
            scan_rows_counter.metric_type(),
        )?;
        scan_rows_counter.encode(scan_rows_encoder)?;

        let scan_bytes_counter = ConstCounter::new(scan_bytes as f64);
        let scan_bytes_encoder = encoder.encode_descriptor(
            METRIC_QUERY_SCAN_PROGRESS_BYTES,
            "Total scan bytes in progress.",
            None,
            scan_bytes_counter.metric_type(),
        )?;
        scan_bytes_counter.encode(scan_bytes_encoder)?;

        let write_rows_counter = ConstCounter::new(write_rows as f64);
        let write_rows_encoder = encoder.encode_descriptor(
            METRIC_QUERY_WRITE_PROGRESS_ROWS,
            "Total write rows in progress.",
            None,
            write_rows_counter.metric_type(),
        )?;
        write_rows_counter.encode(write_rows_encoder)?;

        let write_bytes_counter = ConstCounter::new(write_bytes as f64);
        let write_bytes_encoder = encoder.encode_descriptor(
            METRIC_QUERY_WRITE_PROGRESS_BYTES,
            "Total write bytes in progress.",
            None,
            write_bytes_counter.metric_type(),
        )?;
        write_bytes_counter.encode(write_bytes_encoder)?;

        let spill_rows_counter = ConstCounter::new(spill_rows as f64);
        let spill_rows_encoder = encoder.encode_descriptor(
            METRIC_QUERY_SPILL_PROGRESS_ROWS,
            "Total spill rows in progress.",
            None,
            spill_rows_counter.metric_type(),
        )?;
        spill_rows_counter.encode(spill_rows_encoder)?;

        let spill_bytes_counter = ConstCounter::new(spill_bytes as f64);
        let spill_bytes_encoder = encoder.encode_descriptor(
            METRIC_QUERY_SPILL_PROGRESS_BYTES,
            "Total spill bytes in progress.",
            None,
            spill_bytes_counter.metric_type(),
        )?;
        spill_bytes_counter.encode(spill_bytes_encoder)?;
        Ok(())
    }
}
