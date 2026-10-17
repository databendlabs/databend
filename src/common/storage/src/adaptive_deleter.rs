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

use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream::FuturesUnordered;
use opendal::EntryMode;
use opendal::ErrorKind;
use opendal::Operator;

use crate::AdaptiveConcurrency;
use crate::AdaptiveConcurrencyConfig;
use crate::ConcurrencyFeedback;
use crate::RetryStats;

/// Delete whole prefixes with as many concurrent delete requests as the object store accepts.
///
/// [`Operator::remove_all`] lists the prefix and issues one batch delete after another; one
/// large table then takes as long as its number of batches times the request latency. This
/// deleter keeps up to [`AdaptiveConcurrency::current`] batches in flight and lets the policy
/// move that bound on the backend's feedback: throttling halves it, a streak of clean batches
/// raises it.
///
/// One deleter is one observation scope. Create it with the operator of the storage whose
/// prefixes it removes (through [`crate::init_operator_observed`] so retries the retry layer
/// absorbs reach it) and share it across every prefix on that storage, so the bound reflects
/// that storage and not another one. Calls to [`Self::remove_all`] on the same deleter run
/// under the shared bound.
///
/// Memory is bounded by the batches in flight, not by the size of the prefix.
///
/// ```ignore
/// let stats = Arc::new(RetryStats::default());
/// let operator = init_operator_observed(&storage_params, stats.clone())?;
/// let deleter = AdaptiveDeleter::new(operator, stats, AdaptiveConcurrencyConfig::default())?;
///
/// for table in dropped_tables {
///     // Succeeds only when every object under the prefix is gone; keep the table's metadata
///     // on error so the next vacuum retries it.
///     deleter.remove_all(&table_prefix(&table)).await?;
/// }
/// ```
pub struct AdaptiveDeleter {
    operator: Operator,
    retry_stats: Arc<RetryStats>,
    concurrency: AdaptiveConcurrency,
    batch_size: usize,
}

/// What one [`AdaptiveDeleter::remove_all`] did, for progress reporting.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RemoveAllReport {
    pub removed: usize,
    pub batches: usize,
    /// Retries the retry layer absorbed while this prefix was being removed. A pressure
    /// signal shared with concurrent work on the same deleter, not an exact count.
    pub retried: usize,
}

impl AdaptiveDeleter {
    pub fn new(
        operator: Operator,
        retry_stats: Arc<RetryStats>,
        config: AdaptiveConcurrencyConfig,
    ) -> std::io::Result<Self> {
        let batch_size = operator
            .info()
            .full_capability()
            .delete_max_size
            .unwrap_or(1);
        Ok(Self {
            operator,
            retry_stats,
            concurrency: AdaptiveConcurrency::new(config)?,
            batch_size,
        })
    }

    /// Concurrent batches currently allowed; moves with the backend's feedback.
    pub fn concurrency(&self) -> usize {
        self.concurrency.current()
    }

    /// Remove every object under `prefix`, and `prefix` itself if it is an object.
    ///
    /// Returns only when the listing is exhausted and every batch is done. On the first
    /// failed batch or listing error, no new batch is started, the batches already in flight
    /// are awaited, and the error is returned: the prefix is then only partially removed and
    /// the caller must keep whatever points at it.
    pub async fn remove_all(&self, prefix: &str) -> opendal::Result<RemoveAllReport> {
        match self.operator.stat(prefix).await {
            Ok(meta) if meta.mode() != EntryMode::DIR => {
                self.operator.delete(prefix).await?;
                // Other objects may still share the prefix on object stores.
            }
            Ok(_) => {}
            // A prefix with no object of its own, the normal case on object stores.
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }

        let mut lister = self.operator.lister_with(prefix).recursive(true).await?;
        let mut in_flight = FuturesUnordered::new();
        let mut report = RemoveAllReport::default();
        let mut batch = Vec::with_capacity(self.batch_size);
        let mut listing_done = false;
        let mut first_error = None;

        loop {
            // Fill the admitted slots from the listing; stop producing after any error.
            while first_error.is_none()
                && !listing_done
                && in_flight.len() < self.concurrency.current()
            {
                match lister.try_next().await {
                    Ok(Some(entry)) => {
                        if entry.metadata().mode() == EntryMode::DIR {
                            continue;
                        }
                        batch.push(entry.path().to_owned());
                        if batch.len() == self.batch_size {
                            in_flight.push(self.delete_batch(std::mem::take(&mut batch)));
                        }
                    }
                    Ok(None) => {
                        listing_done = true;
                        if !batch.is_empty() {
                            in_flight.push(self.delete_batch(std::mem::take(&mut batch)));
                        }
                    }
                    Err(e) => {
                        listing_done = true;
                        first_error = Some(e);
                    }
                }
            }

            let Some((outcome, retried)) = in_flight.next().await else {
                break;
            };
            let (failed, rate_limited) = match &outcome {
                Ok(n) => {
                    report.removed += n;
                    (false, false)
                }
                Err(e) => (true, e.kind() == ErrorKind::RateLimited),
            };
            report.batches += 1;
            report.retried += retried;
            self.concurrency.observe(
                ConcurrencyFeedback {
                    failed,
                    rate_limited,
                    retried,
                },
                Instant::now(),
            );
            if let Err(e) = outcome
                && first_error.is_none()
            {
                first_error = Some(e);
            }
        }

        match first_error {
            Some(e) => Err(e),
            None => Ok(report),
        }
    }

    /// One delete request's worth of paths; the result and the retries seen while it ran.
    async fn delete_batch(&self, paths: Vec<String>) -> (opendal::Result<usize>, usize) {
        let n = paths.len();
        let before = self.retry_stats.retried();
        let result = async {
            let mut deleter = self.operator.deleter().await?;
            deleter.delete_iter(paths).await?;
            deleter.close().await?;
            Ok(n)
        }
        .await;
        let retried = self.retry_stats.retried().saturating_sub(before) as usize;
        (result, retried)
    }
}
