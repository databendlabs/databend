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

use std::time::Duration;

/// Retry state for a history worker. Errors must never exhaust its retry budget:
/// even schema or resource errors can be resolved while the query process is alive.
#[derive(Debug, Default)]
pub struct RetryBackoff {
    failures: u64,
    delay: Duration,
}

impl RetryBackoff {
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    pub fn next_delay(&mut self) -> Duration {
        self.failures = self.failures.saturating_add(1);
        self.delay = if self.delay.is_zero() {
            Duration::from_secs(5)
        } else {
            self.delay
                .saturating_mul(2)
                .min(Duration::from_secs(30 * 60))
        };
        self.delay
    }

    pub fn failures(&self) -> u64 {
        self.failures
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::RetryBackoff;

    #[test]
    fn retries_remain_bounded_after_prolonged_failure() {
        let mut backoff = RetryBackoff::default();
        let first = backoff.next_delay();
        let mut previous = first;
        for _ in 0..1000 {
            let delay = backoff.next_delay();
            assert!(delay >= previous);
            assert_eq!(delay, (previous * 2).min(Duration::from_secs(30 * 60)));
            previous = delay;
        }
        assert_eq!(previous, Duration::from_secs(30 * 60));

        backoff.reset();
        assert_eq!(backoff.failures(), 0);
        assert_eq!(backoff.next_delay(), first);
    }

    #[test]
    fn failure_count_saturates_without_stopping_retries() {
        let mut backoff = RetryBackoff {
            failures: u64::MAX,
            delay: Duration::from_secs(30 * 60),
        };
        assert_eq!(backoff.next_delay(), Duration::from_secs(30 * 60));
        assert_eq!(backoff.failures(), u64::MAX);
    }
}
