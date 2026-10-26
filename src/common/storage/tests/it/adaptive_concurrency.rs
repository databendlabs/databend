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

use databend_common_storage::AdaptiveConcurrency;
use databend_common_storage::AdaptiveConcurrencyConfig;
use databend_common_storage::ConcurrencyFeedback;

fn clear() -> ConcurrencyFeedback {
    ConcurrencyFeedback::default()
}

fn failed() -> ConcurrencyFeedback {
    ConcurrencyFeedback {
        failed: true,
        ..clear()
    }
}

fn rate_limited() -> ConcurrencyFeedback {
    ConcurrencyFeedback {
        rate_limited: true,
        ..failed()
    }
}

fn retried(n: usize) -> ConcurrencyFeedback {
    ConcurrencyFeedback {
        retried: n,
        ..clear()
    }
}

#[test]
fn test_ramps_up_once_per_full_streak() {
    let config = AdaptiveConcurrencyConfig::default();
    let c = AdaptiveConcurrency::default();
    let now = Instant::now();
    assert_eq!(c.current(), config.initial);
    for _ in 0..config.increase_after - 1 {
        c.observe(clear(), now);
    }
    assert_eq!(c.current(), config.initial);
    assert_eq!(c.observe(clear(), now), config.initial + 1);
    for _ in 0..config.increase_after - 1 {
        c.observe(clear(), now);
    }
    assert_eq!(c.current(), config.initial + 1);
    for _ in 0..config.max * config.increase_after {
        c.observe(clear(), now);
    }
    assert_eq!(c.current(), config.max);
}

#[test]
fn test_holds_on_failed_or_retried_work() {
    let config = AdaptiveConcurrencyConfig::default();
    let c = AdaptiveConcurrency::default();
    let now = Instant::now();
    for feedback in [failed(), retried(1)] {
        for _ in 0..config.max * config.increase_after {
            c.observe(feedback, now);
        }
        assert_eq!(c.current(), config.initial);
    }
    for _ in 0..config.increase_after - 1 {
        c.observe(clear(), now);
    }
    c.observe(failed(), now);
    c.observe(clear(), now);
    assert_eq!(c.current(), config.initial);
    for _ in 0..config.increase_after - 1 {
        c.observe(clear(), now);
    }
    assert_eq!(c.current(), config.initial + 1);
}

#[test]
fn test_halves_at_most_once_per_cooldown() {
    let config = AdaptiveConcurrencyConfig::default();
    let c = AdaptiveConcurrency::default();
    let now = Instant::now();
    let cooldown = config.decrease_cooldown;
    c.observe(rate_limited(), now);
    c.observe(rate_limited(), now);
    c.observe(rate_limited(), now + Duration::from_secs(3));
    c.observe(rate_limited(), now + cooldown - Duration::from_millis(1));
    assert_eq!(c.current(), config.initial / 2);
    c.observe(rate_limited(), now + cooldown);
    assert_eq!(c.current(), config.initial / 4);
    for i in 2..20 {
        c.observe(rate_limited(), now + cooldown * i);
    }
    assert_eq!(c.current(), config.min);
    for _ in 0..config.increase_after - 1 {
        c.observe(clear(), now);
    }
    c.observe(rate_limited(), now + cooldown * 30);
    for _ in 0..config.increase_after - 1 {
        c.observe(clear(), now);
    }
    assert_eq!(c.current(), config.min);
    assert_eq!(c.observe(clear(), now), config.min + 1);
}

#[test]
fn test_ignores_sporadic_retries() {
    let config = AdaptiveConcurrencyConfig::default();
    let now = Instant::now();
    let cooldown = config.decrease_cooldown;

    // Below a full initial round of retries is noise: the bound holds, whatever the pressure.
    let c = AdaptiveConcurrency::default();
    assert_eq!(c.observe(retried(config.initial - 1), now), config.initial);
    assert_eq!(
        c.observe(retried(config.initial), now + cooldown),
        config.initial / 2
    );

    // Above the initial bound the threshold scales with the current bound.
    let c = AdaptiveConcurrency::default();
    for _ in 0..(config.max - config.initial) * config.increase_after {
        c.observe(clear(), now);
    }
    assert_eq!(c.current(), config.max);
    assert_eq!(c.observe(retried(config.max / 2 - 1), now), config.max);
    assert_eq!(c.observe(retried(config.max / 2), now), config.max / 2);
}

#[test]
fn test_custom_bounds_and_pacing() -> io::Result<()> {
    let c = AdaptiveConcurrency::new(AdaptiveConcurrencyConfig {
        initial: 4,
        min: 2,
        max: 5,
        increase_after: 2,
        decrease_cooldown: Duration::from_secs(3),
    })?;
    let now = Instant::now();
    assert_eq!(c.observe(clear(), now), 4);
    assert_eq!(c.observe(clear(), now), 5);
    assert_eq!(c.observe(rate_limited(), now), 2);
    assert_eq!(c.observe(rate_limited(), now + Duration::from_secs(3)), 2);
    assert_eq!(c.observe(clear(), now), 2);
    assert_eq!(c.observe(clear(), now), 3);
    Ok(())
}

#[test]
fn test_rejects_invalid_configuration() {
    let default = AdaptiveConcurrencyConfig::default();
    for config in [
        AdaptiveConcurrencyConfig { min: 0, ..default },
        AdaptiveConcurrencyConfig {
            initial: 0,
            ..default
        },
        AdaptiveConcurrencyConfig {
            min: default.initial + 1,
            ..default
        },
        AdaptiveConcurrencyConfig {
            max: default.initial - 1,
            ..default
        },
        AdaptiveConcurrencyConfig {
            increase_after: 0,
            ..default
        },
    ] {
        assert_eq!(
            AdaptiveConcurrency::new(config).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
}
