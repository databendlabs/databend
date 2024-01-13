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

//! This mod provides mechanism to track the count of active instances of some type `T`.
//! The count is maintained by a `Count` implementation and will be increased or decreased when a wrapper of `T` `WithCounter` is created or dropped.
//!
//! Example:
//!
//! ```ignore
//! struct Connection{}
//! impl Connection {
//!     fn ping() {}
//! }
//!
//! struct MyCounter{ identifier: String, }
//! impl Count for MyCounter {/*...*/}
//!
//! {
//!     let conn = WithCounter::new(Connection{}, MyCounter{}); // increase count with `MyCounter`
//!     conn.ping();
//! } // decrease count with `MyCounter`
//! ```

use std::ops::Deref;
use std::ops::DerefMut;

/// Defines how to report counter metrics.
pub trait Count {
    fn incr_count(&mut self, n: i64);

    /// Create a guard instance that increases the counter when created, and decreases the counter when dropped.
    fn guard() -> WithCount<Self, ()>
    where Self: Default + Sized {
        WithCount::new((), Self::default())
    }

    fn counter_guard(self) -> WithCount<Self, ()>
    where Self: Sized {
        WithCount::new((), self)
    }
}

/// To enable using a function as a counter: `let _guard = (|i| incr_count(i)).counter_guard();`.
impl<F> Count for F
where F: FnMut(i64)
{
    fn incr_count(&mut self, n: i64) {
        self(n)
    }
}

/// Binds a counter to a `T`.
///
/// It counts the number of instances of `T` with the provided counter `Count`.
#[derive(Debug)]
pub struct WithCount<C, T>
where C: Count
{
    counter: C,
    inner: T,
}

impl<C, T> WithCount<C, T>
where C: Count
{
    pub fn new(t: T, counter: C) -> Self {
        let mut s = Self { counter, inner: t };
        s.counter.incr_count(1);
        s
    }

    pub fn counter(&self) -> &C {
        &self.counter
    }
}

/// When being dropped, decreases the count.
impl<C, T> Drop for WithCount<C, T>
where C: Count
{
    fn drop(&mut self) {
        self.counter.incr_count(-1);
    }
}

/// Let an app use `WithCount` the same as using `T`.
impl<C, T> Deref for WithCount<C, T>
where C: Count
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Let an app use `WithCount` the same as using `T`.
impl<C, T> DerefMut for WithCount<C, T>
where C: Count
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicI64;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use crate::count::Count;
    use crate::count::WithCount;

    struct Foo {}
    struct Counter {
        n: Arc<AtomicI64>,
    }
    impl Count for Counter {
        fn incr_count(&mut self, n: i64) {
            self.n.fetch_add(n, Ordering::Relaxed);
        }
    }

    #[test]
    fn test_with_count() -> anyhow::Result<()> {
        use Ordering::Relaxed;

        let count = Arc::new(AtomicI64::new(0));
        assert_eq!(0, count.load(Relaxed));

        {
            let _a = WithCount::new(Foo {}, Counter { n: count.clone() });
            assert_eq!(1, count.load(Relaxed));
            {
                let _b = WithCount::new(Foo {}, Counter { n: count.clone() });
                assert_eq!(2, count.load(Relaxed));
            }
            assert_eq!(1, count.load(Relaxed));
        }
        assert_eq!(0, count.load(Relaxed));
        Ok(())
    }

    #[test]
    fn test_new_guard() -> anyhow::Result<()> {
        use Ordering::Relaxed;

        let count = Arc::new(AtomicI64::new(0));
        assert_eq!(0, count.load(Relaxed));

        {
            let _a = (|i: i64| {
                count.fetch_add(i, Relaxed);
            })
            .counter_guard();
            assert_eq!(1, count.load(Relaxed));
            {
                let _b = (|i: i64| {
                    count.fetch_add(i * 2, Relaxed);
                })
                .counter_guard();
                assert_eq!(3, count.load(Relaxed));
            }
            assert_eq!(1, count.load(Relaxed));
        }
        assert_eq!(0, count.load(Relaxed));
        Ok(())
    }
}
