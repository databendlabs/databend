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

use std::sync::Mutex;
use std::sync::MutexGuard;

use lazy_static::lazy_static;
use prometheus_client::encoding::text::encode as prometheus_encode;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Metric;
use prometheus_client::registry::Registry;

use crate::histogram::BUCKET_MILLISECONDS;
use crate::histogram::BUCKET_SECONDS;

lazy_static! {
    pub static ref REGISTRY: Mutex<Registry> = Mutex::new(Registry::with_prefix("databend"));
}

pub fn load_global_prometheus_registry() -> MutexGuard<'static, Registry> {
    REGISTRY.lock().unwrap()
}

pub fn reset_global_prometheus_registry() {
    // TODO(liyz): do nothing yet. This function would be trivial once prometheus_client
    // supports iterating metrics. However it's not supported yet. I've raised an issue about
    // this: https://github.com/prometheus/client_rust/issues/163 . If this feature request
    // got denied, we can still wrap a customized Registry which record the metrics by itself.
}

pub struct WrappedRegistry {
    inner: Registry,
    metrics: Vec<Box<dyn Metric>>,
}

impl WrappedRegistry {
    pub fn register(&mut self, name: &str, help: &str, metric: impl Metric + Clone) {
        self.metrics.push(Box::new(metric.clone()));
        self.inner.register(name, help, metric);
    }

    pub fn inner(&self) -> &Registry {
        &self.inner
    }
}

pub fn render_prometheus_metrics(registry: &Registry) -> String {
    let mut text = String::new();
    match prometheus_encode(&mut text, registry) {
        Ok(_) => text,
        Err(err) => format!("Failed to encode metrics: {}", err),
    }
}

pub fn register_counter(name: &str) -> Counter {
    let counter = Counter::default();
    let mut registry = load_global_prometheus_registry();
    registry.register(name, "", counter.clone());
    counter
}

pub fn register_gauge(name: &str) -> Gauge {
    let gauge = Gauge::default();
    let mut registry = load_global_prometheus_registry();
    registry.register(name, "", gauge.clone());
    gauge
}

pub fn register_histogram(name: &str, buckets: impl Iterator<Item = f64>) -> Histogram {
    let histogram = Histogram::new(buckets);
    let mut registry = load_global_prometheus_registry();
    registry.register(name, "", histogram.clone());
    histogram
}

pub fn register_histogram_in_milliseconds(name: &str) -> Histogram {
    register_histogram(name, BUCKET_MILLISECONDS.iter().copied())
}

pub fn register_histogram_in_seconds(name: &str) -> Histogram {
    register_histogram(name, BUCKET_SECONDS.iter().copied())
}

pub fn register_counter_family<T>(name: &str) -> Family<T, Counter>
where T: EncodeLabelSet + std::hash::Hash + Eq + Clone + std::fmt::Debug + Send + Sync + 'static {
    let family = Family::<T, Counter>::default();
    let mut registry = load_global_prometheus_registry();
    registry.register(name, "", family.clone());
    family
}

pub fn register_gauge_family<T>(name: &str) -> Family<T, Gauge>
where T: EncodeLabelSet + std::hash::Hash + Eq + Clone + std::fmt::Debug + Send + Sync + 'static {
    let family = Family::<T, Gauge>::default();
    let mut registry = load_global_prometheus_registry();
    registry.register(name, "", family.clone());
    family
}

pub fn register_histogram_family_in_milliseconds<T>(name: &str) -> Family<T, Histogram>
where T: EncodeLabelSet + std::hash::Hash + Eq + Clone + std::fmt::Debug + Send + Sync + 'static {
    let family = Family::<T, Histogram>::new_with_constructor(move || {
        Histogram::new(BUCKET_MILLISECONDS.iter().copied())
    });
    let mut registry = load_global_prometheus_registry();
    registry.register(name, "", family.clone());
    family
}

pub fn register_histogram_family_in_seconds<T>(name: &str) -> Family<T, Histogram>
where T: EncodeLabelSet + std::hash::Hash + Eq + Clone + std::fmt::Debug + Send + Sync + 'static {
    let family = Family::<T, Histogram>::new_with_constructor(move || {
        Histogram::new(BUCKET_MILLISECONDS.iter().copied())
    });
    let mut registry = load_global_prometheus_registry();
    registry.register(name, "", family.clone());
    family
}
