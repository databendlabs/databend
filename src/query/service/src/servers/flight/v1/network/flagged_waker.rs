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

//! FlaggedWaker: wraps an executor waker with batched wake coalescing.
//!
//! Used by synchronous pipeline processors (ThreadChannelReader, ThreadChannelWriter)
//! to poll async futures without `async_process`.
//!
//! Multiple concurrent wakes for the same processor are coalesced: only the
//! first wake (false→true transition) calls the executor waker. Subsequent
//! wakes are no-ops until `reset()` is called when the processor runs.

use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Wake;
use std::task::Waker;

/// Inner wake implementation. Shared via Arc between FlaggedWaker and Waker.
struct FlaggedWakerInner {
    inner: Waker,
    flag: Arc<AtomicBool>,
}

impl Wake for FlaggedWakerInner {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        // Only call executor waker on false→true transition.
        // This coalesces multiple wakes into one executor scheduling.
        if !self.flag.swap(true, Ordering::AcqRel) {
            self.inner.wake_by_ref();
        }
    }
}

/// Processor-side handle for a coalescing waker.
///
/// Derefs to `Waker` for use with `Context::from_waker`. Call `reset()` at
/// the start of `event()` so subsequent wakes will trigger the executor again.
pub struct FlaggedWaker {
    waker: Waker,
    flag: Arc<AtomicBool>,
}

impl FlaggedWaker {
    pub fn create(executor_waker: Waker) -> Self {
        let flag = Arc::new(AtomicBool::new(false));
        let waker = Waker::from(Arc::new(FlaggedWakerInner {
            inner: executor_waker,
            flag: flag.clone(),
        }));
        Self { waker, flag }
    }

    /// Reset the flag so the next wake will call the executor waker again.
    /// Call this at the start of `event()`.
    pub fn reset(&self) {
        self.flag.store(false, Ordering::Release);
    }
}

impl Deref for FlaggedWaker {
    type Target = Waker;

    fn deref(&self) -> &Waker {
        &self.waker
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Wake;
    use std::task::Waker;

    use super::FlaggedWaker;

    /// A mock waker that counts how many times it's been woken.
    struct CountingWaker {
        count: Arc<AtomicUsize>,
    }

    impl Wake for CountingWaker {
        fn wake(self: Arc<Self>) {
            self.count.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn counting_waker() -> (Waker, Arc<AtomicUsize>) {
        let count = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(CountingWaker {
            count: count.clone(),
        }));
        (waker, count)
    }

    #[test]
    fn test_first_wake_calls_executor() {
        let (inner, count) = counting_waker();
        let fw = FlaggedWaker::create(inner);

        fw.wake_by_ref();
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_duplicate_wakes_coalesced() {
        let (inner, count) = counting_waker();
        let fw = FlaggedWaker::create(inner);

        fw.wake_by_ref();
        fw.wake_by_ref();
        fw.wake_by_ref();
        // Only the first wake should call the executor
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_reset_allows_next_wake() {
        let (inner, count) = counting_waker();
        let fw = FlaggedWaker::create(inner);

        fw.wake_by_ref();
        assert_eq!(count.load(Ordering::SeqCst), 1);

        fw.reset();

        fw.wake_by_ref();
        assert_eq!(count.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn test_reset_without_wake_is_noop() {
        let (inner, count) = counting_waker();
        let fw = FlaggedWaker::create(inner);

        fw.reset();
        fw.reset();
        assert_eq!(count.load(Ordering::SeqCst), 0);

        fw.wake_by_ref();
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_clone_shares_coalescing() {
        let (inner, count) = counting_waker();
        let fw = FlaggedWaker::create(inner);

        let cloned: Waker = (*fw).clone();

        fw.wake_by_ref();
        cloned.wake_by_ref();
        // Both share the same flag — second wake is coalesced
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_concurrent_wakes() {
        let (inner, count) = counting_waker();
        let fw = FlaggedWaker::create(inner);
        let waker: Waker = (*fw).clone();

        let barrier = Arc::new(std::sync::Barrier::new(16));
        let handles: Vec<_> = (0..16)
            .map(|_| {
                let w = waker.clone();
                let b = barrier.clone();
                databend_common_base::runtime::Thread::spawn(move || {
                    b.wait();
                    for _ in 0..1000 {
                        w.wake_by_ref();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Exactly 1 executor wake (all 16k wakes coalesced)
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_concurrent_wake_reset_cycles() {
        // Core invariant: each reset() enables at most one executor wake.
        // With N resets, total executor wakes <= N + 1 (initial + one per reset).
        let num_resets = 1000;
        let num_producers = 4;
        let wakes_per_producer = 1000;

        let (inner, count) = counting_waker();
        let fw = Arc::new(FlaggedWaker::create(inner));
        let waker: Waker = (**fw).clone();

        let barrier = Arc::new(std::sync::Barrier::new(num_producers + 1));

        // Simulate executor: reset in a loop
        let fw_clone = fw.clone();
        let b = barrier.clone();
        let executor = databend_common_base::runtime::Thread::spawn(move || {
            b.wait();
            for _ in 0..num_resets {
                fw_clone.reset();
                std::thread::yield_now();
            }
        });

        // Simulate multiple producers waking concurrently
        let handles: Vec<_> = (0..num_producers)
            .map(|_| {
                let w = waker.clone();
                let b = barrier.clone();
                databend_common_base::runtime::Thread::spawn(move || {
                    b.wait();
                    for _ in 0..wakes_per_producer {
                        w.wake_by_ref();
                    }
                })
            })
            .collect();

        executor.join().unwrap();
        for h in handles {
            h.join().unwrap();
        }

        let final_count = count.load(Ordering::SeqCst);
        // Must have at least 2 wakes (initial + at least one reset enabled another)
        assert!(final_count >= 2, "expected >= 2 wakes, got {final_count}");
        // Cannot exceed resets + 1: each reset enables at most one wake
        assert!(
            final_count <= num_resets + 1,
            "too many wakes: {final_count} > {} (resets + 1), coalescing broken",
            num_resets + 1
        );
    }
}
