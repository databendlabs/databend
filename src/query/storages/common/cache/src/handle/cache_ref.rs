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

/// The budget of a pool or a kind: bytes charged by the values it admitted that are still
/// alive, in the cache or held by readers after their eviction. What is in the cache, the
/// backends count from their own structures.
#[derive(Default, Debug)]
pub struct Usage {
    resident: AtomicUsize,
}

impl Usage {
    pub fn resident(&self) -> usize {
        self.resident.load(Ordering::Relaxed)
    }

    /// Charges `bytes`, given back when the charge drops.
    pub(crate) fn charge(self: &Arc<Self>, bytes: usize) -> Charge {
        self.resident.fetch_add(bytes, Ordering::Relaxed);
        Charge {
            usage: self.clone(),
            bytes,
        }
    }

    /// Charges `bytes` if that keeps `resident` within `max`; atomic against concurrent
    /// charges.
    pub(crate) fn try_charge(self: &Arc<Self>, bytes: usize, max: usize) -> Option<Charge> {
        self.resident
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |resident| {
                resident.checked_add(bytes).filter(|total| *total <= max)
            })
            .ok()?;
        Some(Charge {
            usage: self.clone(),
            bytes,
        })
    }
}

/// Bytes charged to one `Usage`, given back on drop: a charge is owned from the moment it is
/// taken, so an insert that fails or unwinds halfway leaks nothing.
pub(crate) struct Charge {
    usage: Arc<Usage>,
    bytes: usize,
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.usage.resident.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

/// Charge for one admitted value, to its pool and its kind, released when the last
/// `CacheRef` drops.
pub struct Reservation {
    _charges: [Charge; 2],
}

impl Reservation {
    pub(crate) fn new(pool: Charge, kind: Charge) -> Self {
        Reservation {
            _charges: [pool, kind],
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
        value.attach(Reservation::new(pool.charge(100), kind.charge(100)));
        let held = value.clone();
        assert_eq!((pool.resident(), kind.resident()), (100, 100));
        drop(value);
        assert_eq!(pool.resident(), 100);
        drop(held);
        assert_eq!((pool.resident(), kind.resident()), (0, 0));
    }

    #[test]
    fn test_try_charge_stays_within_max() {
        let usage = Arc::new(Usage::default());
        let first = usage.try_charge(60, 100).unwrap();
        assert!(usage.try_charge(50, 100).is_none());
        assert_eq!(usage.resident(), 60);
        drop(first);
        assert!(usage.try_charge(100, 100).is_some());
        assert_eq!(usage.resident(), 0);
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
