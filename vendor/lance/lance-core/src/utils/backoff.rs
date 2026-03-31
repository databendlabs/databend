use rand::{Rng, SeedableRng};
use std::time::Duration;

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

/// Computes backoff as
///
/// ```text
/// backoff = base^attempt * unit + jitter
/// ```
///
/// The defaults are base=2, unit=50ms, jitter=50ms, min=0ms, max=5s. This gives
/// a backoff of 50ms, 100ms, 200ms, 400ms, 800ms, 1.6s, 3.2s, 5s, (not including jitter).
///
/// You can have non-exponential backoff by setting base=1.
pub struct Backoff {
    base: u32,
    unit: u32,
    jitter: i32,
    min: u32,
    max: u32,
    attempt: u32,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            base: 2,
            unit: 50,
            jitter: 50,
            min: 0,
            max: 5000,
            attempt: 0,
        }
    }
}

impl Backoff {
    pub fn with_base(self, base: u32) -> Self {
        Self { base, ..self }
    }

    pub fn with_unit(self, unit: u32) -> Self {
        Self { unit, ..self }
    }

    pub fn with_jitter(self, jitter: i32) -> Self {
        Self { jitter, ..self }
    }

    pub fn with_min(self, min: u32) -> Self {
        Self { min, ..self }
    }

    pub fn with_max(self, max: u32) -> Self {
        Self { max, ..self }
    }

    pub fn next_backoff(&mut self) -> Duration {
        let backoff = self
            .base
            .saturating_pow(self.attempt)
            .saturating_mul(self.unit);
        let jitter = rand::rng().random_range(-self.jitter..=self.jitter);
        let backoff = (backoff.saturating_add_signed(jitter)).clamp(self.min, self.max);
        self.attempt += 1;
        Duration::from_millis(backoff as u64)
    }

    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    pub fn reset(&mut self) {
        self.attempt = 0;
    }
}

/// SlotBackoff is a backoff strategy that randomly chooses a time slot to retry.
///
/// This is useful when you have multiple tasks that can't overlap, and each
/// task takes roughly the same amount of time.
///
/// The `unit` represents the time it takes to complete one attempt. Future attempts
/// are divided into time slots, and a random slot is chosen for the retry. The number
/// of slots increases exponentially with each attempt. Initially, there are 4 slots,
/// then 8, then 16, and so on.
///
/// Example:
/// Suppose you have 10 tasks that can't overlap, each taking 1 second. The tasks
/// don't know about each other and can't coordinate. Each task randomly picks a
/// time slot to retry. Here's how it might look:
///
/// First round (4 slots):
/// ```text
/// task id   | 1, 2, 3 | 4, 5, 6 | 7, 8, 9 | 10 |
/// status    | x, x, ✓ | x, x, ✓ | x, x, ✓ | ✓  |
/// timeline  | 0s      | 1s      | 2s      | 3s |
/// ```
/// Each slot can have one success. Here, tasks 3, 6, 9, and 10 succeed.
/// In the next round, the number of slots doubles (8):
///
/// Second round (8 slots):
/// ```text
/// task id   |  1 |  2 |    | 4, 5 |  7 |  8 |    |    |
/// status    |  ✓ |  ✓ |    | x, ✓ |  ✓ |  ✓ |    |    |
/// timeline  | 0s | 1s | 2s | 3s   | 4s | 5s | 6s | 7s |
/// ```
/// Most tasks are done now, except for task 4. It will succeed in the next round.
pub struct SlotBackoff {
    base: u32,
    unit: u32,
    starting_i: u32,
    attempt: u32,
    rng: rand::rngs::SmallRng,
}

impl Default for SlotBackoff {
    fn default() -> Self {
        Self {
            base: 2,
            unit: 50,
            starting_i: 2, // start with 4 slots
            attempt: 0,
            rng: rand::rngs::SmallRng::from_os_rng(),
        }
    }
}

impl SlotBackoff {
    pub fn with_unit(self, unit: u32) -> Self {
        Self { unit, ..self }
    }

    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    pub fn next_backoff(&mut self) -> Duration {
        let num_slots = self.base.saturating_pow(self.attempt + self.starting_i);
        let slot_i = self.rng.random_range(0..num_slots);
        self.attempt += 1;
        Duration::from_millis((slot_i * self.unit) as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff() {
        let mut backoff = Backoff::default().with_jitter(0);
        assert_eq!(backoff.next_backoff().as_millis(), 50);
        assert_eq!(backoff.attempt(), 1);
        assert_eq!(backoff.next_backoff().as_millis(), 100);
        assert_eq!(backoff.attempt(), 2);
        assert_eq!(backoff.next_backoff().as_millis(), 200);
        assert_eq!(backoff.attempt(), 3);
        assert_eq!(backoff.next_backoff().as_millis(), 400);
        assert_eq!(backoff.attempt(), 4);
    }

    #[test]
    fn test_slot_backoff() {
        fn assert_in(value: u128, expected: &[u128]) {
            assert!(
                expected.contains(&value),
                "value {} not in {:?}",
                value,
                expected
            );
        }

        for _ in 0..10 {
            let mut backoff = SlotBackoff::default().with_unit(100);
            assert_in(backoff.next_backoff().as_millis(), &[0, 100, 200, 300]);
            assert_eq!(backoff.attempt(), 1);
            assert_in(
                backoff.next_backoff().as_millis(),
                &[0, 100, 200, 300, 400, 500, 600, 700],
            );
            assert_eq!(backoff.attempt(), 2);
            assert_in(
                backoff.next_backoff().as_millis(),
                &(0..16).map(|i| i * 100).collect::<Vec<_>>(),
            );
            assert_eq!(backoff.attempt(), 3);
        }
    }
}
