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

pub mod bytes_meter;
pub mod count_meter;
pub mod file_meter;
#[cfg(feature = "heapsize")]
#[cfg(not(target_os = "macos"))]
pub mod heap_meter;

use std::borrow::Borrow;

/// A trait for measuring the size of a cache entry.
///
/// If you implement this trait, you should use `usize` as the `Measure` type, otherwise you will
/// also have to implement [`CountableMeter`][countablemeter].
///
/// [countablemeter]: trait.Meter.html
pub trait Meter<K, V> {
    /// The type used to store measurements.
    type Measure: Default + Copy;
    /// Calculate the size of `key` and `value`.
    fn measure<Q: ?Sized>(&self, key: &Q, value: &V) -> Self::Measure
    where K: Borrow<Q>;
}
