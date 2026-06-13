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

use std::fmt::Display;
use std::time::Duration;

use databend_common_meta_app::app_error::TxnRetryMaxTimes;
use futures::future::BoxFuture;
use log::info;
use log::warn;
use rand::Rng;

const TXN_MAX_RETRY_TIMES: u32 = 60;

/// Creates an iterator providing a series of backoff durations for transaction retries,
/// followed by an error upon reaching the maximum number of retries.
///
/// The iterator yields `n` `Ok` items, each containing a `Sleep` future representing
/// the backoff duration to wait before the next transaction attempt. After `n` successful
/// yields, the iterator will produce an `Err` containing an `AppError` to signify that
/// the maximum number of retries has been exceeded.
///
/// The backoff strategy is determined by a predefined list of durations, and the backoff
/// time for each subsequent retry increases up to the last value in this list, after which
/// it remains constant for any further retries. Additionally, a random jitter is applied
/// to each backoff duration to prevent synchronization issues in distributed systems.
///
/// # Examples
///
/// ```ignore
/// fn update_table() -> Result<(), AppError> {
///     let mut trials = txn_trials(3, "update_table");
///     loop {
///         trials.next().unwrap()?.await;
///         // do something
///     }
/// }
/// ```
pub fn txn_backoff(
    n: Option<u32>,
    ctx: impl Display,
) -> impl Iterator<Item = Result<BoxFuture<'static, ()>, TxnRetryMaxTimes>> {
    let n = n.unwrap_or(TXN_MAX_RETRY_TIMES);

    let mut rnd = rand::thread_rng();
    let scale = 1.0 + rnd.gen_range(-0.2..0.2);

    let ctx = ctx.to_string();

    (1..=(n + 1)).map(move |i| {
        if i <= n {
            let backoff = ith_backoff(i as usize);
            let sleep = Duration::from_secs_f64(backoff * scale);
            let ctx2 = ctx.clone();

            let msg = format!("{}: txn-retry for {}th time, sleep {:?}", &ctx2, i, sleep);

            let fu = async move {
                if i >= 5 {
                    warn!("{}, start", msg);
                } else if i >= 2 {
                    info!("{}, start", msg);
                }

                tokio::time::sleep(sleep).await;

                if i >= 5 {
                    warn!("{}, end", msg);
                } else if i >= 2 {
                    info!("{}, end", msg);
                }
            };

            let fu: BoxFuture<'static, ()> = Box::pin(fu);

            Ok(fu)
        } else {
            let err = TxnRetryMaxTimes::new(&ctx.to_string(), n);
            Err(err)
        }
    })
}

/// Return ith backoff in seconds
fn ith_backoff(i: usize) -> f64 {
    let len = BACKOFF.len();

    if i < len {
        BACKOFF[i]
    } else {
        BACKOFF[len - 1]
    }
}

const fn from_millis(millis: u64) -> f64 {
    (millis as f64) / 1000.0
}

// backoff time in ms
// 0-th item is not used.
// 1-th item for the first attempt should always 0.
const BACKOFF: &[f64] = &[
    from_millis(0),
    from_millis(0),
    from_millis(2),
    from_millis(5),
    from_millis(10),
    from_millis(14),
    from_millis(20),
    from_millis(29),
    from_millis(42),
    from_millis(61),
    from_millis(89),
    from_millis(128),
    from_millis(184),
    from_millis(266),
    from_millis(383),
    from_millis(552),
    from_millis(794),
];

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_backoff_first() {
        let mut trials = super::txn_backoff(Some(4), "test");

        let now = std::time::Instant::now();
        let _ = trials.next().unwrap().unwrap().await;
        let elapsed = now.elapsed().as_secs_f64();

        assert!(elapsed < 0.100, "{} is expected to be 0", elapsed);
    }

    #[tokio::test]
    async fn test_backoff() {
        let now = std::time::Instant::now();
        let mut trials = super::txn_backoff(Some(6), "test");
        for _ in 0..6 {
            let _ = trials.next().unwrap().unwrap().await;
        }

        let elapsed = now.elapsed().as_secs_f64();
        println!("elapsed: {elapsed}");
        assert!(
            (0.041..0.120).contains(&elapsed),
            "{} is expected to be 2 + 5 + 10 + 14 + 20 milliseconds",
            elapsed
        );

        assert!(trials.next().unwrap().is_err());
    }

    #[tokio::test]
    async fn test_backoff_many() {
        let mut trials = super::txn_backoff(Some(100), "test");
        for _ in 0..100 {
            #[allow(clippy::let_underscore_future)]
            let _ = trials.next().unwrap().unwrap();
        }

        assert!(trials.next().unwrap().is_err());
    }
}
