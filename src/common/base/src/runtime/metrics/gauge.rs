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

use std::sync::atomic::AtomicI64;

use prometheus_client::encoding::EncodeGaugeValue;
use prometheus_client::encoding::EncodeMetric;
use prometheus_client::encoding::MetricEncoder;
use prometheus_client::metrics::gauge::Atomic;
use prometheus_client::metrics::gauge::Gauge as PGauge;
use prometheus_client::metrics::MetricType;
use prometheus_client::metrics::TypedMetric;
use crate::runtime::metrics::registry::{SampleMetric};
use crate::runtime::metrics::sample::{MetricSample, MetricValue};

#[derive(Debug)]
pub struct Gauge<N = i64, A = AtomicI64> {
    inner: PGauge<N, A>,
    index: usize,
}

impl<N, A> Clone for Gauge<N, A> {
    fn clone(&self) -> Self {
        Gauge {
            index: self.index,
            inner: self.inner.clone(),
        }
    }
}

// TODO: Maybe impl deref is better
impl<N, A: Atomic<N>> Gauge<N, A> {
    pub fn create(index: usize) -> Gauge {
        Gauge {
            index,
            inner: PGauge::default(),
        }
    }

    pub fn inc(&self) -> N {
        self.inner.inc()
    }

    pub fn inc_by(&self, v: N) -> N {
        self.inner.inc_by(v)
    }

    pub fn dec(&self) -> N {
        self.inner.dec()
    }

    pub fn dec_by(&self, v: N) -> N {
        self.inner.dec_by(v)
    }

    pub fn set(&self, v: N) -> N {
        self.inner.set(v)
    }

    pub fn get(&self) -> N {
        self.inner.get()
    }

    pub fn inner(&self) -> &A {
        self.inner.inner()
    }
}

impl<N, A: Atomic<N>> TypedMetric for Gauge<N, A> {
    const TYPE: MetricType = MetricType::Gauge;
}

impl<N: EncodeGaugeValue, A: Atomic<N>> EncodeMetric for Gauge<N, A> {
    fn encode(&self, encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
        self.inner.encode(encoder)
    }

    fn metric_type(&self) -> MetricType {
        Self::TYPE
    }
}

impl SampleMetric for Gauge<i64, AtomicI64> {
    fn sample(&self, name: &str, samples: &mut Vec<MetricSample>) {
        samples.push(MetricSample {
            name: name.to_string(),
            labels: Default::default(),
            value: MetricValue::Gauge(self.get() as f64),
        })
    }
}
// impl<N:EncodeGaugeValue, A> Metric for Gauge<N, A> {
//
// }
