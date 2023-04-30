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

use std::borrow::Borrow;

use super::Meter;

/// Size limit based on a simple count of cache items.
pub struct Count;

impl<K, V> Meter<K, V> for Count {
    /// Don't store anything, the measurement can be derived from the map.
    type Measure = ();

    /// Don't actually count anything either.
    fn measure<Q: ?Sized>(&self, _: &Q, _: &V)
    where K: Borrow<Q> {
    }
}

/// A trait to allow the default `Count` measurement to not store an
/// extraneous counter.
pub trait CountableMeter<K, V>: Meter<K, V> {
    /// Add `amount` to `current` and return the sum.
    fn add(&self, current: Self::Measure, amount: Self::Measure) -> Self::Measure;
    /// Subtract `amount` from `current` and return the difference.
    fn sub(&self, current: Self::Measure, amount: Self::Measure) -> Self::Measure;
    /// Return `current` as a `usize` if possible, otherwise return `None`.
    ///
    /// If this method returns `None` the cache will use the number of cache entries as
    /// its size.
    fn size(&self, current: Self::Measure) -> Option<u64>;
}

/// `Count` is all no-ops, the number of entries in the map is the size.
impl<K, V, T: Meter<K, V>> CountableMeter<K, V> for T
where T: CountableMeterWithMeasure<K, V, <T as Meter<K, V>>::Measure>
{
    fn add(&self, current: Self::Measure, amount: Self::Measure) -> Self::Measure {
        CountableMeterWithMeasure::meter_add(self, current, amount)
    }
    fn sub(&self, current: Self::Measure, amount: Self::Measure) -> Self::Measure {
        CountableMeterWithMeasure::meter_sub(self, current, amount)
    }
    fn size(&self, current: Self::Measure) -> Option<u64> {
        CountableMeterWithMeasure::meter_size(self, current)
    }
}

pub trait CountableMeterWithMeasure<K, V, M> {
    /// Add `amount` to `current` and return the sum.
    fn meter_add(&self, current: M, amount: M) -> M;
    /// Subtract `amount` from `current` and return the difference.
    fn meter_sub(&self, current: M, amount: M) -> M;
    /// Return `current` as a `usize` if possible, otherwise return `None`.
    ///
    /// If this method returns `None` the cache will use the number of cache entries as
    /// its size.
    fn meter_size(&self, current: M) -> Option<u64>;
}

/// For any other `Meter` with `Measure=usize`, just do the simple math.
impl<K, V, T> CountableMeterWithMeasure<K, V, usize> for T
where T: Meter<K, V>
{
    fn meter_add(&self, current: usize, amount: usize) -> usize {
        current + amount
    }
    fn meter_sub(&self, current: usize, amount: usize) -> usize {
        current - amount
    }
    fn meter_size(&self, current: usize) -> Option<u64> {
        Some(current as u64)
    }
}

impl<K, V> CountableMeterWithMeasure<K, V, ()> for Count {
    fn meter_add(&self, _current: (), _amount: ()) {}
    fn meter_sub(&self, _current: (), _amount: ()) {}
    fn meter_size(&self, _current: ()) -> Option<u64> {
        None
    }
}
