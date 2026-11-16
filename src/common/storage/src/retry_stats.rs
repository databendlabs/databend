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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use opendal::ErrorKind;

/// Retries absorbed by the retry layer of an observed operator.
///
/// Pass this counter to [`crate::init_operator_observed`] to distinguish throttling of one
/// activity from node-wide storage retries. Sample around a request or a batch of work;
/// even a successful result can have encountered retries.
///
/// Two counters are kept because only some backends report throttling as `RateLimited`: S3
/// `SlowDown`, GCS and B2 `429` do, while OSS, COS, OBS and Azure answer `503` that OpenDAL
/// classifies as a temporary `Unexpected` error. [`Self::retried`] therefore counts every
/// retried error, whatever its kind, and is the signal to use for back-pressure decisions;
/// [`Self::rate_limited`] is the subset the backend explicitly flagged.
///
/// All operators sharing this counter belong to the same observation scope. Overlapping
/// samples can include the same retries, so their deltas are pressure signals rather than
/// additive per-request metrics. The counters are never reset by a reader.
#[derive(Debug, Default)]
pub struct RetryStats {
    retried: AtomicU64,
    rate_limited: AtomicU64,
}

impl RetryStats {
    /// Number of retries observed so far, of any error kind.
    pub fn retried(&self) -> u64 {
        self.retried.load(Ordering::Relaxed)
    }

    /// Number of `RateLimited` retries observed so far.
    pub fn rate_limited(&self) -> u64 {
        self.rate_limited.load(Ordering::Relaxed)
    }

    pub(crate) fn record_retry(&self, kind: ErrorKind) {
        self.retried.fetch_add(1, Ordering::Relaxed);
        if kind == ErrorKind::RateLimited {
            self.rate_limited.fetch_add(1, Ordering::Relaxed);
        }
    }
}
