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

//! This mod provides a thin wrapper over the original prometheus_client's Histogram.
//! The original histogram did not provide an implementation on Default by design, but
//! histogram is used every where on observing time durations. This wrapper provide
//! a default bucket configuration for the common use cases.

use prometheus_client::encoding::EncodeMetric;
use prometheus_client::encoding::MetricEncoder;
use prometheus_client::metrics::histogram::Histogram as HistogramInner;
use prometheus_client::metrics::MetricType;
use prometheus_client::metrics::TypedMetric;

#[derive(Clone, Debug)]
pub struct Histogram {
    inner: HistogramInner,
}

impl Histogram {
    pub fn new(buckets: impl Iterator<Item = f64>) -> Self {
        Self {
            inner: HistogramInner::new(buckets),
        }
    }

    pub fn observe(&self, v: f64) {
        self.inner.observe(v);
    }

    pub fn inner(&self) -> &HistogramInner {
        &self.inner
    }
}

impl Default for Histogram {
    fn default() -> Self {
        let default_buckets = [
            0.02, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0, 30.0, 60.0, 300.0, 600.0, 1800.0,
        ];
        let inner = HistogramInner::new(default_buckets.into_iter());
        Self { inner }
    }
}

impl TypedMetric for Histogram {
    const TYPE: MetricType = MetricType::Histogram;
}

impl EncodeMetric for Histogram {
    fn encode(&self, encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
        self.inner().encode(encoder)
    }

    fn metric_type(&self) -> MetricType {
        Self::TYPE
    }
}
