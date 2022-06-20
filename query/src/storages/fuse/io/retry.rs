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

use std::future::Future;
use std::time::Duration;

use backoff::backoff::Backoff;
use backoff::future::Retry;
use backoff::future::Sleeper;
use backoff::ExponentialBackoff;
use backoff::Notify;
use common_base::base::tokio;
use common_exception::ErrorCode;

pub fn to_backoff_err(e: std::io::Error) -> backoff::Error<std::io::Error> {
    if e.kind() == std::io::ErrorKind::NotFound {
        backoff::Error::permanent(e)
    } else {
        backoff::Error::transient(e)
    }
}

pub fn to_backoff_err1(e: ErrorCode) -> backoff::Error<ErrorCode> {
    if e.code() == ErrorCode::storage_not_found_code() {
        backoff::Error::permanent(e)
    } else {
        backoff::Error::transient(e)
    }
}

pub trait Retryable<T, E, Fut, Fn, N> {
    fn retry_notify1(self, notify: N) -> Retry<TokioSleeper, ExponentialBackoff, N, Fn, Fut>;
}

impl<T, E, Fut, Fn, N> Retryable<T, E, Fut, Fn, N> for Fn
where
    Fut: Future<Output = std::result::Result<T, backoff::Error<E>>>,
    Fn: FnMut() -> Fut,
    N: Notify<E>,
{
    fn retry_notify1(self, notify: N) -> Retry<TokioSleeper, ExponentialBackoff, N, Fn, Fut> {
        let mut backoff = backoff::ExponentialBackoff::default();
        backoff.reset();
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
