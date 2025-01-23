use std::sync::Arc;

use databend_common_metrics::interpreter::METRIC_QUERY_SCAN_PROGRESS_BYTES;
use databend_common_metrics::interpreter::METRIC_QUERY_SCAN_PROGRESS_ROWS;
use databend_common_metrics::interpreter::METRIC_QUERY_WRITE_PROGRESS_BYTES;
use databend_common_metrics::interpreter::METRIC_QUERY_WRITE_PROGRESS_ROWS;
use prometheus_client::collector::Collector;
use prometheus_client::encoding::EncodeMetric;
use prometheus_client::metrics::counter::ConstCounter;

use super::SessionManager;

pub struct SessionManagerMetricsCollector {
    session_mgr: Arc<SessionManager>,
}

impl SessionManagerMetricsCollector {
    pub fn new(session_mgr: Arc<SessionManager>) -> Self {
        Self { session_mgr }
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
        for process in processes {
            if let Some(scan) = &process.scan_progress_value {
                scan_bytes += scan.bytes;
                scan_rows += scan.rows;
            }
            if let (write) = &process.write_progress_value {
                write_bytes += write.bytes;
                write_rows += write.rows;
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
        Ok(())
    }
}
