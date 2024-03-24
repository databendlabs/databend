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

#![allow(clippy::uninlined_format_args)]
#![allow(dead_code)]
#![recursion_limit = "256"]
#![feature(lazy_cell)]

pub mod count;
pub mod counter;
mod dump;
pub mod histogram;
mod metrics;
pub mod registry;

pub type VecLabels = Vec<(&'static str, String)>;

pub use counter::Counter;
pub use dump::dump_metric_samples;
pub use dump::HistogramCount;
pub use dump::MetricSample;
pub use dump::MetricValue;
pub use dump::SummaryCount;
pub use histogram::Histogram;
pub use metrics_exporter_prometheus::PrometheusHandle;
pub use prometheus_client::metrics::family::Family;
pub use prometheus_client::metrics::gauge::Gauge;
pub use registry::load_global_prometheus_registry;
pub use registry::register_counter;
pub use registry::register_counter_family;
pub use registry::register_gauge;
pub use registry::register_gauge_family;
pub use registry::register_histogram_family_in_milliseconds;
pub use registry::register_histogram_family_in_seconds;
pub use registry::register_histogram_in_milliseconds;
pub use registry::register_histogram_in_seconds;
pub use registry::render_prometheus_metrics;
pub use registry::reset_global_prometheus_registry;

pub use crate::metrics::cache;
pub use crate::metrics::cluster;
/// Metrics.
pub use crate::metrics::http;
pub use crate::metrics::interpreter;
pub use crate::metrics::lock;
pub use crate::metrics::mysql;
pub use crate::metrics::openai;
pub use crate::metrics::session;
pub use crate::metrics::storage;
