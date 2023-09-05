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
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;

use crate::histogram::BUCKET_MILLISECONDS;
use crate::histogram::BUCKET_SECONDS;

lazy_static! {
    pub static ref REGISTRY: Mutex<Registry> = Mutex::new(Registry::default());
}

pub fn load_global_prometheus_registry() -> MutexGuard<'static, Registry> {
    REGISTRY.lock().unwrap()
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
