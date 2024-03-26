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

use parking_lot::MappedRwLockReadGuard;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::encoding::EncodeMetric;
use prometheus_client::encoding::MetricEncoder;
use prometheus_client::metrics::family::Family as PFamily;
use prometheus_client::metrics::family::MetricConstructor;
use prometheus_client::metrics::MetricType;
use prometheus_client::metrics::TypedMetric;
use crate::runtime::metrics::registry::SampleMetric;
use crate::runtime::metrics::sample::{MetricSample, MetricValue};

#[derive(Debug)]
pub struct Family<S, M, C = fn() -> M> {
    index: usize,
    inner: PFamily<S, M, C>,
}

impl<S: Clone + std::hash::Hash + Eq, M: Default> Family<S, M> {
    pub fn create(index: usize) -> Self {
        Self {
            index,
            inner: PFamily::<S, M>::default(),
        }
    }
}

impl<S: Clone + std::hash::Hash + Eq, M, C> Family<S, M, C> {
    pub fn create_with_constructor(index: usize, constructor: C) -> Self {
        Self {
            index,
            inner: PFamily::<S, M, C>::new_with_constructor(constructor),
        }
    }
}

impl<S: Clone + std::hash::Hash + Eq, M, C: MetricConstructor<M>> Family<S, M, C> {
    pub fn get_or_create(&self, label_set: &S) -> MappedRwLockReadGuard<M> {
        // RwLockReadGuard::map()
        self.inner.get_or_create(label_set)
    }

    pub fn remove(&self, label_set: &S) -> bool {
        self.inner.remove(label_set)
    }

    pub fn clear(&self) {
        self.inner.clear()
    }
}

impl<S, M, C: Clone> Clone for Family<S, M, C> {
    fn clone(&self) -> Self {
        Family {
            index: self.index,
            inner: self.inner.clone(),
        }
    }
}

impl<S, M: TypedMetric, C> TypedMetric for Family<S, M, C> {
    const TYPE: MetricType = <M as TypedMetric>::TYPE;
}

impl<S, M, C> EncodeMetric for Family<S, M, C>
    where
        S: Clone + std::hash::Hash + Eq + EncodeLabelSet,
        M: EncodeMetric + TypedMetric,
        C: MetricConstructor<M>,
{
    fn encode(&self, encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
        self.inner.encode(encoder)
    }

    fn metric_type(&self) -> MetricType {
        M::TYPE
    }
}

impl<S, M, C> SampleMetric for Family<S, M, C>
    where
        S: Clone + std::hash::Hash + Eq + EncodeLabelSet,
        M: EncodeMetric + TypedMetric,
        C: MetricConstructor<M>,
{
    fn sample(&self, name: &str, samples: &mut Vec<MetricSample>) {
        samples.push(MetricSample {
            name: name.to_string(),
            labels: Default::default(),
            value: MetricValue::Untyped(0 as f64),
        })
    }
}

// #[derive(Debug)]
// pub struct FamilyCount {
//     value: Arc<AtomicU64>,
//     index: usize,
// }
//
// impl Clone for FamilyCount {
//     fn clone(&self) -> Self {
//         Self {
//             index: self.index,
//             value: self.value.clone(),
//         }
//     }
// }
//
// impl FamilyCount {
//     pub fn create(index: usize) -> FamilyCount {
//         FamilyCount {
//             index,
//             value: Arc::new(AtomicU64::new(0)),
//         }
//     }
//
//     /// Increase the [`FamilyCount`] by 1, returning the previous value.
//     pub fn inc(&self) -> u64 {
//         ScopedRegistry::op(self.index, |m: &Family<>| {
//             m.value.fetch_add(1, Ordering::Relaxed);
//         });
//
//         self.value.fetch_add(1, Ordering::Relaxed)
//     }
//
//     /// Increase the [`FamilyCount`] by `v`, returning the previous value.
//     pub fn inc_by(&self, v: u64) -> u64 {
//         ScopedRegistry::op(self.index, |m: &Self| {
//             m.value.fetch_add(v, Ordering::Relaxed);
//         });
//
//         self.value.fetch_add(v, Ordering::Relaxed)
//     }
//
//     /// Get the current value of the [`FamilyCount`].
//     pub fn get(&self) -> u64 {
//         self.value.load(Ordering::Relaxed)
//     }
//
//     /// Reset the [`FamilyCount`] to 0.
//     pub fn reset(&self) {
//         self.value.store(0, Ordering::Release)
//     }
// }
//
// impl TypedMetric for FamilyCount {
//     const TYPE: MetricType = MetricType::Counter;
// }
//
// impl EncodeMetric for FamilyCount {
//     fn encode(&self, mut encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
//         encoder.encode_counter::<(), _, u64>(&self.get(), None)
//     }
//
//     fn metric_type(&self) -> MetricType {
//         Self::TYPE
//     }
// }
