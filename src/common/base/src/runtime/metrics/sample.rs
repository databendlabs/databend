use std::collections::HashMap;

#[derive(Debug)]
pub struct MetricSample {
    pub name: String,
    pub labels: HashMap<String, String>,
    pub value: MetricValue,
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
