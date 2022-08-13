// Copyright 2021 Datafuse Labs.
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

use common_exception::ErrorCode;
use common_exception::Result;
use metrics_exporter_prometheus::PrometheusHandle;

#[derive(Debug)]
pub struct MetricSample {
    pub name: String,
    pub kind: String,
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

pub fn dump_metric_samples(handle: PrometheusHandle) -> Result<Vec<MetricSample>> {
    let text = handle.render();
    let lines = text.lines().map(|s| Ok(s.to_owned()));
    let samples = prometheus_parse::Scrape::parse(lines)
        .map_err(|err| {
            ErrorCode::UnexpectedError(format!("Dump prometheus metrics failed: {:?}", err))
        })?
        .samples
        .into_iter()
        .map(|s| {
            let value: MetricValue = s.value.into();
            MetricSample {
                name: s.metric,
                kind: value.kind(),
                value,
                labels: (*s.labels).clone(),
            }
        })
        .collect::<Vec<_>>();
    Ok(samples)
}
