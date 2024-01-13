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

use std::collections::HashMap;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use prometheus_client::registry::Registry;

use crate::render_prometheus_metrics;

#[derive(Debug)]
pub struct MetricSample {
    pub name: String,
    pub labels: HashMap<String, String>,
    pub value: MetricValue,
}

#[derive(Debug, PartialEq, serde::Serialize)]
pub enum MetricValue {
    Counter(f64),
    Gauge(f64),
    Untyped(f64),
    Histogram(Vec<HistogramCount>),
    Summary(Vec<SummaryCount>),
}

impl MetricValue {
    pub fn kind(&self) -> String {
        match self {
            MetricValue::Counter(_) => "counter",
            MetricValue::Gauge(_) => "gauge",
            MetricValue::Untyped(_) => "untyped",
            MetricValue::Histogram(_) => "histogram",
            MetricValue::Summary(_) => "summary",
        }
        .to_string()
    }
}

impl From<prometheus_parse::Value> for MetricValue {
    fn from(value: prometheus_parse::Value) -> Self {
        use prometheus_parse::Value;
        match value {
            Value::Counter(v) => MetricValue::Counter(v),
            Value::Gauge(v) => MetricValue::Gauge(v),
            Value::Untyped(v) => MetricValue::Untyped(v),
            Value::Histogram(v) => MetricValue::Histogram(
                v.iter()
                    .map(|h| HistogramCount {
                        less_than: h.less_than,
                        count: h.count,
                    })
                    .collect(),
            ),
            Value::Summary(v) => MetricValue::Summary(
                v.iter()
                    .map(|s| SummaryCount {
                        quantile: s.quantile,
                        count: s.count,
                    })
                    .collect(),
            ),
        }
    }
}

#[derive(Debug, PartialEq, serde::Serialize)]
pub struct HistogramCount {
    pub less_than: f64,
    pub count: f64,
}

#[derive(Debug, PartialEq, serde::Serialize)]
pub struct SummaryCount {
    pub quantile: f64,
    pub count: f64,
}

pub fn dump_metric_samples(registry: &Registry) -> Result<Vec<MetricSample>> {
    let text = render_prometheus_metrics(registry);
    let lines = text.lines().map(|s| Ok(s.to_owned()));
    let mut samples = prometheus_parse::Scrape::parse(lines)
        .map_err(|err| ErrorCode::Internal(format!("Dump prometheus metrics failed: {:?}", err)))?
        .samples
        .into_iter()
        .map(|s| {
            let value: MetricValue = s.value.into();
            let metric_name = s
                .metric
                .strip_prefix("databend_")
                .map(|s| s.to_string())
                .unwrap_or(s.metric);
            MetricSample {
                name: metric_name,
                value,
                labels: (*s.labels).clone(),
            }
        })
        .collect::<Vec<_>>();

    let proc_stats = dump_proc_stats().unwrap_or_default();
    samples.extend(proc_stats);
    Ok(samples)
}

#[cfg(not(target_os = "linux"))]
pub fn dump_proc_stats() -> Result<Vec<MetricSample>> {
    Ok(vec![])
}

#[cfg(target_os = "linux")]
pub fn dump_proc_stats() -> procfs::ProcResult<Vec<MetricSample>> {
    let me = procfs::process::Process::myself()?;
    let io = me.io()?;
    // ❯ cat /proc/thread-self/io
    // rchar: 4092
    // wchar: 8
    // syscr: 8
    // syscw: 1
    // read_bytes: 0
    // write_bytes: 0
    // cancelled_write_bytes: 0

    let results = vec![
        MetricSample {
            // "Number of bytes read from disks or block devices. Doesn't include bytes read from page cache."
            name: "os_read_bytes".to_string(),
            value: MetricValue::Counter(io.read_bytes as f64),
            labels: HashMap::new(),
        },
        MetricSample {
            // "Number of bytes written to disks or block devices. Doesn't include bytes that are in page cache dirty pages."
            name: "os_write_bytes".to_string(),
            value: MetricValue::Counter(io.write_bytes as f64),
            labels: HashMap::new(),
        },
        MetricSample {
            name: "os_read_chars".to_string(),
            value: MetricValue::Counter(io.rchar as f64),
            labels: HashMap::new(),
        },
        MetricSample {
            name: "os_write_chars".to_string(),
            value: MetricValue::Counter(io.wchar as f64),
            labels: HashMap::new(),
        },
    ];

    Ok(results)
}
