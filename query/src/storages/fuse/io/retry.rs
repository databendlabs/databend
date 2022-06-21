//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

/// Temporary crate
/// migrating to crate `backon` whenever `retry_notify` is ready
use std::future::Future;
use std::time::Duration;

use backoff::default;
use backoff::future::Retry;
use backoff::future::Sleeper;
use backoff::ExponentialBackoff;
use backoff::Notify;
use common_base::base::tokio;
use common_exception::ErrorCode;

pub fn from_io_error(e: std::io::Error) -> backoff::Error<std::io::Error> {
    if e.kind() == std::io::ErrorKind::NotFound {
        // shall we count `PermissionDenied` as permanent too?
        backoff::Error::permanent(e)
    } else {
        backoff::Error::transient(e)
    }
}

pub fn from_error_code(e: ErrorCode) -> backoff::Error<ErrorCode> {
    // Range of Storage errors: [3001, 4000].
    if e.code() >= 3001 && e.code() <= 4000 {
        if e.code() == ErrorCode::storage_not_found_code() {
            backoff::Error::permanent(e)
        } else {
            backoff::Error::transient(e)
        }
    } else {
        backoff::Error::permanent(e)
    }
}

pub trait Retryable<T, E, Fut, Fn, N> {
    fn retry_with_notify(self, notify: N) -> Retry<TokioSleeper, ExponentialBackoff, N, Fn, Fut>;
}

impl<T, E, Fut, Fn, N> Retryable<T, E, Fut, Fn, N> for Fn
where
    Fut: Future<Output = std::result::Result<T, backoff::Error<E>>>,
    Fn: FnMut() -> Fut,
    N: Notify<E>,
{
    fn retry_with_notify(self, notify: N) -> Retry<TokioSleeper, ExponentialBackoff, N, Fn, Fut> {
        let backoff = default_backoff();
        Retry::new(TokioSleeper, backoff, notify, self)
    }
}

pub struct TokioSleeper;
impl Sleeper for TokioSleeper {
    type Sleep = tokio::time::Sleep;
    fn sleep(&self, dur: Duration) -> Self::Sleep {
        tokio::time::sleep(dur)
    }
}

/// default backoff
/// -
/// The default initial interval value in milliseconds (0.2 seconds).
/// The default randomization factor (0.5 which results in a random period ranging between 50%
/// below and 50% above the retry interval).
/// The default multiplier value (1.5 which is 50% increase per back off).
/// The default maximum back off time in milliseconds (0.2 minute).
/// The default maximum elapsed time in milliseconds (0.5 minutes).
fn default_backoff() -> backoff::ExponentialBackoff {
    let mut eb = ExponentialBackoff {
        current_interval: Duration::from_millis(200),
        initial_interval: Duration::from_millis(200),
        randomization_factor: default::RANDOMIZATION_FACTOR,
        multiplier: default::MULTIPLIER,
        ..Default::default()
    };

    eb.max_interval = Duration::from_millis(12 * 1000);
    eb.max_elapsed_time = Some(Duration::from_millis(30 * 1000));
    eb
}
