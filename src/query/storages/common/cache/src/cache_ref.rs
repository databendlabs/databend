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

use std::any::Any;
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

pub type ErasedValue = Arc<dyn Any + Send + Sync>;

/// Byte counters shared by a pool or a kind.
#[derive(Default, Debug)]
pub struct Usage {
    resident: AtomicUsize,
    in_cache: AtomicUsize,
    items: AtomicUsize,
}

impl Usage {
    /// Bytes of admitted values still alive, including those already evicted but held by readers.
    pub fn resident(&self) -> usize {
        self.resident.load(Ordering::Relaxed)
    }

    /// Bytes of values currently held by the cache.
    pub fn in_cache(&self) -> usize {
        self.in_cache.load(Ordering::Relaxed)
    }

    pub fn items(&self) -> usize {
        self.items.load(Ordering::Relaxed)
    }

    pub(crate) fn enter(&self, bytes: usize) {
        self.in_cache.fetch_add(bytes, Ordering::Relaxed);
        self.items.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn leave(&self, bytes: usize) {
        self.in_cache.fetch_sub(bytes, Ordering::Relaxed);
        self.items.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Charge for one admitted value, released when the last `CacheRef` drops.
pub struct Reservation {
    usages: [Arc<Usage>; 2],
    bytes: usize,
}

impl Reservation {
    pub(crate) fn charge(pool: Arc<Usage>, kind: Arc<Usage>, bytes: usize) -> Self {
        for usage in [&pool, &kind] {
            usage.resident.fetch_add(bytes, Ordering::Relaxed);
        }
        Reservation {
            usages: [pool, kind],
            bytes,
        }
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        for usage in &self.usages {
            usage.resident.fetch_sub(self.bytes, Ordering::Relaxed);
        }
    }
}

struct Charged<T> {
    value: T,
    reservation: OnceLock<Reservation>,
}

/// Shared handle to a cached value. Values that were not admitted carry no reservation.
pub struct CacheRef<T>(Arc<Charged<T>>);

impl<T: Send + Sync + 'static> CacheRef<T> {
    pub fn new(value: T) -> Self {
        CacheRef(Arc::new(Charged {
            value,
            reservation: OnceLock::new(),
        }))
    }

    pub fn is_charged(&self) -> bool {
        self.0.reservation.get().is_some()
    }

    pub(crate) fn attach(&self, reservation: Reservation) {
        let _ = self.0.reservation.set(reservation);
    }

    pub(crate) fn erase(&self) -> ErasedValue {
        self.0.clone()
    }

    pub(crate) fn from_erased(value: ErasedValue) -> Option<Self> {
        value.downcast::<Charged<T>>().ok().map(CacheRef)
    }
}

impl<T> Clone for CacheRef<T> {
    fn clone(&self) -> Self {
        CacheRef(self.0.clone())
    }
}

impl<T> Deref for CacheRef<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.0.value
    }
}

impl<T: fmt::Debug> fmt::Debug for CacheRef<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.value.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reservation_released_on_last_drop() {
        let pool = Arc::new(Usage::default());
        let kind = Arc::new(Usage::default());
        let value = CacheRef::new(7u32);
        value.attach(Reservation::charge(pool.clone(), kind.clone(), 100));
        let held = value.clone();
        assert_eq!((pool.resident(), kind.resident()), (100, 100));
        drop(value);
        assert_eq!(pool.resident(), 100);
        drop(held);
        assert_eq!((pool.resident(), kind.resident()), (0, 0));
    }

    #[test]
    fn test_erase_roundtrip() {
        let value = CacheRef::new(String::from("v"));
        let erased = value.erase();
        assert!(CacheRef::<u32>::from_erased(erased.clone()).is_none());
        let back = CacheRef::<String>::from_erased(erased).unwrap();
        assert_eq!(&*back, "v");
    }
}
