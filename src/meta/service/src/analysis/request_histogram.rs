use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::time::Duration;
use std::time::SystemTime;

use chrono::DateTime;
use chrono::Utc;
use databend_base::histogram::Histogram;
use databend_base::histogram::PercentileStats;
use databend_common_meta_client::MetaGrpcReadReq;
use databend_common_meta_types::Cmd;
use databend_common_meta_types::LogEntry;
use databend_common_meta_types::TxnRequest;
use databend_common_meta_types::UpsertKV;

static REQUEST_LATENCY_TRACKER: LazyLock<LatencyRecorder> = LazyLock::new(LatencyRecorder::new);

/// Return the histogram label for an UpsertKV request.
pub fn label_for_upsert(_upsert: &UpsertKV) -> Cow<'static, str> {
    Cow::Borrowed("UpsertKV")
}

/// Return the histogram label for a MetaGrpcReadReq, matching the script's categorization.
pub fn label_for_read(req: &MetaGrpcReadReq) -> Cow<'static, str> {
    match req {
        MetaGrpcReadReq::GetKV(_) => Cow::Borrowed("GetKV"),
        MetaGrpcReadReq::MGetKV(_) => Cow::Borrowed("MGetKV"),
        MetaGrpcReadReq::ListKV(list) => {
            if list.prefix.is_empty() {
                return Cow::Borrowed("ListKV");
            }
            match list.prefix.split('/').next() {
                Some(cat) if !cat.is_empty() => Cow::Owned(format!("ListKV-{}", cat)),
                _ => Cow::Borrowed("ListKV"),
            }
        }
    }
}

/// Return the histogram label for a transaction request.
pub fn label_for_txn(_txn: &TxnRequest) -> Cow<'static, str> {
    Cow::Borrowed("TxnRequest")
}

/// Return the histogram label for a forwarded write request.
pub fn label_for_write(entry: &LogEntry) -> Cow<'static, str> {
    match &entry.cmd {
        Cmd::AddNode { .. } => Cow::Borrowed("Write-AddNode"),
        Cmd::RemoveNode { .. } => Cow::Borrowed("Write-RemoveNode"),
        Cmd::SetFeature { feature, .. } => Cow::Owned(format!("Write-SetFeature-{}", feature)),
        Cmd::UpsertKV(_) => Cow::Borrowed("Write-UpsertKV"),
        Cmd::Transaction(_) => Cow::Borrowed("Write-Transaction"),
    }
}

/// Record a request latency sample.
pub fn record(label: &str, duration: Duration) {
    REQUEST_LATENCY_TRACKER.record(label, duration);
}

/// Return a snapshot of current histogram data without resetting.
pub fn report() -> impl fmt::Display {
    REQUEST_LATENCY_TRACKER.report()
}

/// Reset all histograms and start a new collection window.
pub fn reset() {
    REQUEST_LATENCY_TRACKER.reset();
}

/// Tracks request latencies over fixed windows and emits label_summaries.
struct LatencyRecorder {
    /// Interior state shared by recorders.
    inner: Mutex<LatencyState>,
}

/// Mutable state that backs the tracker.
struct LatencyState {
    /// Start timestamp of the current reporting window.
    window_start: SystemTime,
    /// Histograms keyed by request label.
    label_hists: BTreeMap<String, Histogram>,
}

impl LatencyRecorder {
    /// Construct a tracker using the default interval.
    fn new() -> Self {
        Self {
            inner: Mutex::new(LatencyState {
                window_start: SystemTime::now(),
                label_hists: BTreeMap::new(),
            }),
        }
    }

    /// Record a new latency sample for a given label.
    ///
    /// Avoids allocation when the label already exists in the map.
    fn record(&self, label: &str, duration: Duration) {
        let mut inner = self.inner.lock().unwrap();
        let micros = clamp_duration_to_micros(duration);

        if let Some(hist) = inner.label_hists.get_mut(label) {
            hist.record(micros);
        } else {
            let mut hist = Histogram::new();
            hist.record(micros);
            inner.label_hists.insert(label.to_string(), hist);
        }
    }

    /// Return a snapshot of current histogram data without resetting.
    fn report(&self) -> LatencyReport {
        let inner = self.inner.lock().unwrap();
        let elapsed = SystemTime::now()
            .duration_since(inner.window_start)
            .unwrap_or(Duration::ZERO);

        let mut label_summaries: Vec<_> = inner
            .label_hists
            .iter()
            .map(|(label, histogram)| LabeledPercentile::new(label.clone(), histogram))
            .collect();

        label_summaries.sort_by(|a, b| a.label.cmp(&b.label));
        LatencyReport {
            duration: elapsed,
            window_start: inner.window_start,
            percentiles: label_summaries,
        }
    }

    /// Reset all histograms and start a new collection window.
    fn reset(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.label_hists.clear();
        inner.window_start = SystemTime::now();
    }
}

/// Captures the label_summaries for a completed interval.
struct LatencyReport {
    /// Length of the interval.
    duration: Duration,
    /// Wall-clock start time for display.
    window_start: SystemTime,
    /// Per-label label_summaries.
    percentiles: Vec<LabeledPercentile>,
}

impl fmt::Display for LatencyReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let timestamp: DateTime<Utc> = self.window_start.into();
        write!(
            f,
            "RequestLatencyHistogram[start={} interval={:?} entries={}]",
            timestamp.format("%Y-%m-%d %H:%M:%S"),
            self.duration,
            self.percentiles.len()
        )?;

        for summary in &self.percentiles {
            let p50 = us_to_duration(summary.percentiles.p50);
            let p90 = us_to_duration(summary.percentiles.p90);
            let p99 = us_to_duration(summary.percentiles.p99);
            let p999 = us_to_duration(summary.percentiles.p99_9);
            write!(
                f,
                ", {} qps={:.2} p50={:?} p90={:?} p99={:?} p99.9={:?} count={}",
                summary.label,
                summary.qps(self.duration),
                p50,
                p90,
                p99,
                p999,
                summary.sample_count()
            )?;
        }

        Ok(())
    }
}

#[derive(Debug)]
/// Aggregated statistics for a single label within the interval.
struct LabeledPercentile {
    /// Human-friendly request label (e.g. `ListKV-settings`).
    label: String,
    /// Percentile data pulled from the histogram.
    percentiles: PercentileStats,
}

impl LabeledPercentile {
    fn new(label: String, histogram: &Histogram) -> LabeledPercentile {
        let percentiles = histogram.percentile_stats();

        LabeledPercentile { label, percentiles }
    }

    fn sample_count(&self) -> u64 {
        self.percentiles.samples
    }

    /// Compute QPS for this summary.
    fn qps(&self, interval: Duration) -> f64 {
        let secs = interval.as_secs_f64();
        if secs == 0.0 {
            0.0
        } else {
            self.sample_count() as f64 / secs
        }
    }
}

fn us_to_duration(us: u64) -> Duration {
    Duration::from_micros(us)
}

/// Convert a duration into microseconds and clamp to `u64`.
fn clamp_duration_to_micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_label_for_list_prefix() {
        let req = MetaGrpcReadReq::ListKV(databend_common_meta_kvapi::kvapi::ListKVReq {
            prefix: "__fd_settings/tenant/".to_string(),
        });
        assert_eq!(label_for_read(&req), "ListKV-__fd_settings");

        let req = MetaGrpcReadReq::ListKV(databend_common_meta_kvapi::kvapi::ListKVReq {
            prefix: String::new(),
        });
        assert_eq!(label_for_read(&req), "ListKV");
    }

    #[test]
    fn test_tracker_report() {
        let tracker = LatencyRecorder::new();
        tracker.record("GetKV", Duration::from_millis(2));
        tracker.record("GetKV", Duration::from_millis(4));
        tracker.record("ListKV", Duration::from_millis(8));

        // report() returns snapshot without clearing
        let report = tracker.report();
        assert_eq!(report.percentiles.len(), 2);
        let get_stats = report
            .percentiles
            .iter()
            .find(|s| s.label == "GetKV")
            .unwrap();
        assert_eq!(get_stats.sample_count(), 2);

        // report() again still has data
        assert_eq!(tracker.report().percentiles.len(), 2);

        // reset() clears the data
        tracker.reset();
        assert!(tracker.report().percentiles.is_empty());
    }
}
