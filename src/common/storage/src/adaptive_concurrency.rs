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

use std::io;
use std::time::Duration;
use std::time::Instant;

use parking_lot::Mutex;

/// Bounds and pacing for additive-increase / multiplicative-decrease concurrency control.
/// Defaults preserve the policy used by vacuuming temporary spill directories.
#[derive(Debug, Clone, Copy)]
pub struct AdaptiveConcurrencyConfig {
    pub initial: usize,
    pub min: usize,
    pub max: usize,
    /// Consecutive successful, retry-free outcomes needed to increase concurrency by one.
    pub increase_after: usize,
    /// Minimum interval between decreases, so overlapping retries count as one episode.
    pub decrease_cooldown: Duration,
}

impl Default for AdaptiveConcurrencyConfig {
    fn default() -> Self {
        Self {
            initial: 8,
            min: 1,
            max: 32,
            increase_after: 16,
            decrease_cooldown: Duration::from_secs(10),
        }
    }
}

/// Feedback for one completed unit of work, independent of its business-level result.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ConcurrencyFeedback {
    pub failed: bool,
    /// The operation returned a rate-limiting error after retries were exhausted.
    pub rate_limited: bool,
    /// Retries hidden by the retry layer while the work was in flight, of any error kind:
    /// not every backend flags throttling as `RateLimited`, so use [`crate::RetryStats::retried`]
    /// rather than the rate-limited subset. With shared retry statistics, overlapping work can
    /// observe the same retries. This is a pressure signal, not an exact per-request count; do
    /// not sum it as a retry metric.
    pub retried: usize,
}

/// A shared concurrency policy, not an executor or a semaphore.
///
/// The scheduler admits work while its in-flight count is below [`Self::current`], then calls
/// [`Self::observe`] when work completes. Reducing the bound does not cancel existing work.
/// Use one admission scheduler per bound: sharing this value alone does not enforce a global
/// request limit across independent schedulers.
///
/// ```
/// use std::time::Instant;
///
/// use databend_common_storage::AdaptiveConcurrency;
/// use databend_common_storage::ConcurrencyFeedback;
///
/// let concurrency = AdaptiveConcurrency::default();
/// let before = concurrency.current();
/// let after = concurrency.observe(
///     ConcurrencyFeedback {
///         failed: true,
///         rate_limited: true,
///         retried: 0,
///     },
///     Instant::now(),
/// );
/// assert_eq!(after, before / 2);
/// ```
#[derive(Debug)]
pub struct AdaptiveConcurrency {
    config: AdaptiveConcurrencyConfig,
    state: Mutex<ConcurrencyState>,
}

#[derive(Debug)]
struct ConcurrencyState {
    current: usize,
    clear_streak: usize,
    last_decrease: Option<Instant>,
}

impl Default for AdaptiveConcurrency {
    fn default() -> Self {
        Self::new(AdaptiveConcurrencyConfig::default()).expect("default bounds must be valid")
    }
}

impl AdaptiveConcurrency {
    pub fn new(config: AdaptiveConcurrencyConfig) -> io::Result<Self> {
        if config.min == 0
            || config.min > config.initial
            || config.initial > config.max
            || config.increase_after == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "adaptive concurrency requires 0 < min <= initial <= max and increase_after > 0",
            ));
        }
        Ok(Self {
            config,
            state: Mutex::new(ConcurrencyState {
                current: config.initial,
                clear_streak: 0,
                last_decrease: None,
            }),
        })
    }

    pub fn current(&self) -> usize {
        self.state.lock().current
    }

    /// Apply feedback and return the resulting concurrency bound. The supplied clock makes
    /// cooldown behavior testable without sleeping. Ordinary failures and sparse retries
    /// reset the success streak but do not decrease the bound.
    pub fn observe(&self, feedback: ConcurrencyFeedback, now: Instant) -> usize {
        self.observe_change(feedback, now).1
    }

    /// Like [`Self::observe`], also returning the bound before this feedback. The pair is
    /// read under one lock, so concurrent observers each see a distinct, consistent step.
    pub fn observe_change(&self, feedback: ConcurrencyFeedback, now: Instant) -> (usize, usize) {
        let mut state = self.state.lock();
        let before = state.current;
        if self.throttled(feedback, state.current) {
            state.clear_streak = 0;
            let cooled = state.last_decrease.is_none_or(|last| {
                now.saturating_duration_since(last) >= self.config.decrease_cooldown
            });
            if cooled && state.current > self.config.min {
                state.last_decrease = Some(now);
                state.current = (state.current / 2).max(self.config.min);
            }
        } else if feedback.failed || feedback.retried > 0 {
            state.clear_streak = 0;
        } else {
            state.clear_streak += 1;
            if state.clear_streak >= self.config.increase_after {
                state.clear_streak = 0;
                if state.current < self.config.max {
                    state.current += 1;
                }
            }
        }
        (before, state.current)
    }

    /// Sporadic retries hold the bound; a burst of at least half the current concurrency,
    /// and never fewer than one initial round, counts as throttling.
    fn throttled(&self, feedback: ConcurrencyFeedback, current: usize) -> bool {
        feedback.rate_limited || feedback.retried >= current.div_ceil(2).max(self.config.initial)
    }
}
