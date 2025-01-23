use std::sync::Arc;

use databend_common_base::base::ProgressValues;
use databend_common_metrics::interpreter::METRIC_QUERY_SCAN_PROGRESS_BYTES;
use databend_common_metrics::interpreter::METRIC_QUERY_SCAN_PROGRESS_ROWS;
use databend_common_metrics::interpreter::METRIC_QUERY_SPILL_PROGRESS_BYTES;
use databend_common_metrics::interpreter::METRIC_QUERY_SPILL_PROGRESS_ROWS;
use databend_common_metrics::interpreter::METRIC_QUERY_WRITE_PROGRESS_BYTES;
use databend_common_metrics::interpreter::METRIC_QUERY_WRITE_PROGRESS_ROWS;
use prometheus_client::collector::Collector;
use prometheus_client::encoding::EncodeMetric;
use prometheus_client::metrics::counter::ConstCounter;

use super::SessionManager;

pub struct SessionManagerMetricsCollector {
    session_mgr: Arc<SessionManager>,
    finished_scan_total: ProgressValues,
    finished_write_total: ProgressValues,
    finished_spill_total: ProgressValues,
}

impl SessionManagerMetricsCollector {
    pub fn new(session_mgr: Arc<SessionManager>) -> Self {
        Self {
            session_mgr,
            finished_scan_total: ProgressValues::default(),
            finished_write_total: ProgressValues::default(),
            finished_spill_total: ProgressValues::default(),
        }
    }

    pub fn track_finished_query(
        &mut self,
        scan: ProgressValues,
        write: ProgressValues,
        spill: ProgressValues,
    ) {
        self.finished_scan_total = self.finished_scan_total.add(&scan);
        self.finished_write_total = self.finished_write_total.add(&write);
        self.finished_spill_total = self.finished_spill_total.add(&spill);
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
        let processes = self.session_mgr.processes_info();
        let mut scan_bytes = 0;
        let mut scan_rows = 0;
        let mut write_bytes = 0;
        let mut write_rows = 0;
        let mut spill_bytes = 0;
        let mut spill_rows = 0;
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
