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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Instant;

use futures::StreamExt;
use futures::TryStreamExt;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use opendal::EntryMode;
use opendal::ErrorKind;
use opendal::Operator;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

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
/// absorbs reach it) and share it, by cloning, across every prefix on that storage so the
/// bound reflects that storage and not another one. All clones admit batches against the
/// same bound: it caps the batches in flight across every concurrent [`Self::remove_all`],
/// not per call.
///
/// Memory is bounded by the batches in flight plus, on filesystem-style backends, the
/// directory entries under the prefix, which are removed after their contents.
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
#[derive(Clone)]
pub struct AdaptiveDeleter {
    shared: Arc<Shared>,
}

struct Shared {
    operator: Operator,
    retry_stats: Arc<RetryStats>,
    concurrency: AdaptiveConcurrency,
    /// One permit per batch the policy currently allows, across every `remove_all` on this
    /// deleter. A batch holds its permit until it is done or dropped, so a cancelled call
    /// gives its slots back; waiters sleep on the semaphore instead of polling.
    slots: Arc<Semaphore>,
    /// Permits the bound has shrunk by that were held at the time and still have to be
    /// forgotten as they come back, so `slots` settles at exactly `concurrency.current()`.
    deficit: AtomicUsize,
    batch_size: usize,
}

/// What one [`AdaptiveDeleter::remove_all`] did, for progress reporting.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RemoveAllReport {
    /// Objects removed; directory entries on filesystem-style backends are not counted.
    pub removed: usize,
    /// Delete requests issued, directory removals included.
    pub batches: usize,
    /// Retries the retry layer absorbed while this prefix was being removed. A pressure
    /// signal shared with concurrent work on the same deleter, not an exact count.
    pub retried: usize,
}

/// The result of one batch (its object count on success) and the retries seen while it ran.
type BatchOutcome = (opendal::Result<usize>, usize);

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
        let concurrency = AdaptiveConcurrency::new(config)?;
        let slots = Arc::new(Semaphore::new(concurrency.current()));
        Ok(Self {
            shared: Arc::new(Shared {
                operator,
                retry_stats,
                concurrency,
                slots,
                deficit: AtomicUsize::new(0),
                batch_size,
            }),
        })
    }

    /// Concurrent batches currently allowed; moves with the backend's feedback.
    pub fn concurrency(&self) -> usize {
        self.shared.concurrency.current()
    }

    /// Remove every object under `prefix`, and `prefix` itself if it is an object.
    ///
    /// Returns only when the listing is exhausted and every batch is done. On the first
    /// failed batch or listing error, no new batch is started, the batches already in flight
    /// are awaited, and the error is returned: the prefix is then only partially removed and
    /// the caller must keep whatever points at it.
    ///
    /// On filesystem-style backends the directories under the prefix, and the prefix itself,
    /// are removed after their contents, as [`Operator::remove_all`] does.
    pub async fn remove_all(&self, prefix: &str) -> opendal::Result<RemoveAllReport> {
        let shared = &self.shared;
        let mut run = Run::new(self.shared.clone());

        // Listings may or may not include an entry for the prefix itself, so it is handled
        // here and skipped there.
        let prefix_is_dir = match shared.operator.stat(prefix).await {
            Ok(meta) if meta.mode() == EntryMode::DIR => true,
            // An object at the prefix itself; other objects may still share the prefix.
            Ok(_) => {
                run.push(vec![prefix.to_owned()]).await;
                false
            }
            // A prefix with no object of its own, the normal case on object stores.
            Err(e) if e.kind() == ErrorKind::NotFound => false,
            Err(e) => return Err(e),
        };

        let mut lister = match shared.operator.lister_with(prefix).recursive(true).await {
            Ok(lister) => lister,
            Err(e) => {
                run.first_error = Some(e);
                run.drain().await;
                return Err(run.first_error.take().unwrap());
            }
        };
        let mut batch = Vec::with_capacity(shared.batch_size);
        let mut dirs = Vec::new();
        while run.first_error.is_none() {
            match lister.try_next().await {
                Ok(Some(entry)) => {
                    if entry.path() == prefix {
                        continue;
                    }
                    if entry.metadata().mode() == EntryMode::DIR {
                        dirs.push(entry.path().to_owned());
                        continue;
                    }
                    batch.push(entry.path().to_owned());
                    if batch.len() == shared.batch_size {
                        run.push(std::mem::take(&mut batch)).await;
                    }
                }
                Ok(None) => {
                    if !batch.is_empty() {
                        run.push(std::mem::take(&mut batch)).await;
                    }
                    break;
                }
                Err(e) => run.first_error = Some(e),
            }
        }
        run.drain().await;

        // Directories only exist on filesystem-style backends: deepest first, the prefix last.
        if run.first_error.is_none() {
            dirs.sort_unstable_by(|a, b| b.len().cmp(&a.len()).then_with(|| b.cmp(a)));
            if prefix_is_dir {
                dirs.push(prefix.to_owned());
            }
            for dir in dirs {
                if run.first_error.is_some() {
                    break;
                }
                run.push_dir(dir).await;
                run.drain().await;
            }
        }

        match run.first_error {
            Some(e) => Err(e),
            None => Ok(run.report),
        }
    }
}

impl Shared {
    /// One delete request's worth of paths; the result and the retries seen while it ran.
    /// The permit is released when this future completes or is dropped.
    async fn delete_batch(
        self: Arc<Self>,
        _permit: OwnedSemaphorePermit,
        paths: Vec<String>,
        counted: bool,
    ) -> BatchOutcome {
        let n = if counted { paths.len() } else { 0 };
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

    /// Feed a batch outcome to the policy and move the permit count with the bound.
    ///
    /// Called with the batch's permit already released. Growing adds permits at once. When
    /// the bound shrinks, only free permits can be forgotten now; the rest are held by other
    /// batches and are forgotten as they return, through the deficit.
    fn observe(&self, feedback: ConcurrencyFeedback) {
        let (before, after) = self.concurrency.observe_change(feedback, Instant::now());
        if after > before {
            self.slots.add_permits(after - before);
        } else if after < before {
            self.deficit.fetch_add(before - after, Ordering::AcqRel);
        }
        self.settle_deficit();
    }

    /// Forget as much of the deficit as the free permits allow.
    fn settle_deficit(&self) {
        let owed = self.deficit.load(Ordering::Acquire);
        if owed == 0 {
            return;
        }
        let forgotten = self.slots.forget_permits(owed);
        if forgotten > 0 {
            self.deficit.fetch_sub(forgotten, Ordering::AcqRel);
        }
    }

    /// Wait for a slot under the current bound.
    async fn acquire(&self) -> OwnedSemaphorePermit {
        self.slots
            .clone()
            .acquire_owned()
            .await
            .expect("the slot semaphore is never closed")
    }
}

/// The batches one `remove_all` has in flight, admitted under the deleter's shared bound.
struct Run {
    shared: Arc<Shared>,
    batches: FuturesUnordered<BoxFuture<'static, BatchOutcome>>,
    report: RemoveAllReport,
    first_error: Option<opendal::Error>,
}

impl Run {
    fn new(shared: Arc<Shared>) -> Self {
        Self {
            shared,
            batches: FuturesUnordered::new(),
            report: RemoveAllReport::default(),
            first_error: None,
        }
    }

    /// Start deleting the objects at `paths` once a slot is free.
    async fn push(&mut self, paths: Vec<String>) {
        self.admit(paths, true).await
    }

    /// Start removing a directory entry once a slot is free; not counted as an object.
    async fn push_dir(&mut self, dir: String) {
        self.admit(vec![dir], false).await
    }

    /// Wait for a slot, settling this call's own finished batches meanwhile so their outcome
    /// reaches the policy promptly. Nothing is admitted once a batch has failed.
    async fn admit(&mut self, paths: Vec<String>, counted: bool) {
        let permit = loop {
            if self.first_error.is_some() {
                return;
            }
            tokio::select! {
                biased;
                permit = self.shared.acquire() => break permit,
                settled = self.batches.next() => match settled {
                    Some(outcome) => self.settle(outcome),
                    // Nothing of ours in flight: wait for another call to release a slot.
                    None => break self.shared.acquire().await,
                },
            }
        };
        if self.first_error.is_some() {
            return;
        }
        self.batches.push(Box::pin(
            self.shared.clone().delete_batch(permit, paths, counted),
        ));
    }

    /// Wait for every batch of this call.
    async fn drain(&mut self) {
        while self.settle_one().await {}
    }

    /// Settle one finished batch; false when none is in flight.
    async fn settle_one(&mut self) -> bool {
        match self.batches.next().await {
            Some(outcome) => {
                self.settle(outcome);
                true
            }
            None => false,
        }
    }

    /// Record a finished batch in the report and feed it to the policy.
    fn settle(&mut self, (outcome, retried): BatchOutcome) {
        let (failed, rate_limited) = match &outcome {
            Ok(n) => {
                self.report.removed += n;
                (false, false)
            }
            Err(e) => (true, e.kind() == ErrorKind::RateLimited),
        };
        self.report.batches += 1;
        self.report.retried += retried;
        self.shared.observe(ConcurrencyFeedback {
            failed,
            rate_limited,
            retried,
        });
        if let Err(e) = outcome
            && self.first_error.is_none()
        {
            self.first_error = Some(e);
        }
    }
}
