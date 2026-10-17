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
use std::sync::Mutex;
use std::sync::MutexGuard;
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

/// Delete prefixes or selected paths under a shared adaptive request bound.
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
/// same bound: it caps batches across every concurrent [`Self::remove_all`] and
/// [`Self::remove_files`], not per call.
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
    /// deleter. A batch holds its permit until its outcome has been fed to the policy, or
    /// until it is dropped, so a cancelled call gives its slots back; waiters sleep on the
    /// semaphore instead of polling.
    slots: Arc<Semaphore>,
    /// Permits the bound has shrunk by that other batches held at the time and that still
    /// have to be retired as they come back, so `slots` settles at `concurrency.current()`.
    ///
    /// The lock is held across a policy update and the permit adjustment it calls for, so
    /// two observers on different threads cannot interleave a decrease that is not yet
    /// booked with an increase that adds a permit.
    deficit: Mutex<usize>,
    batch_size: usize,
    progress: Mutex<Progress>,
}

#[derive(Default)]
struct Progress {
    removed: usize,
    report: Option<Arc<dyn Fn(usize) + Send + Sync>>,
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

/// What one [`AdaptiveDeleter::remove_files`] did, including progress before an error.
#[derive(Debug, Default)]
#[must_use = "check the error and attempted count before advancing a manifest or deletion budget"]
pub struct RemoveFilesReport {
    /// Confirmed deletions form a contiguous prefix of the input. A failed request may have
    /// deleted additional paths, which are unconfirmed and must remain eligible for retry.
    /// Paths beyond `attempted` have not been submitted.
    pub removed: usize,
    /// Paths accounted for as attempted, including all paths of a failed request that may
    /// have partially succeeded. A deletion budget must use this rather than just `removed`.
    pub attempted: usize,
    pub batches: usize,
    /// Retry pressure sampled around each request, not an additive per-request metric.
    pub retried: usize,
    pub error: Option<opendal::Error>,
}

impl RemoveFilesReport {
    fn absorb(&mut self, other: Self) {
        self.removed += other.removed;
        self.attempted += other.attempted;
        self.batches += other.batches;
        self.retried += other.retried;
        if self.error.is_none() {
            self.error = other.error;
        }
    }
}

/// A finished batch and the slot it still holds until the policy has seen the outcome.
struct BatchOutcome {
    result: opendal::Result<usize>,
    attempted: usize,
    retried: usize,
    slot: Slot,
}

/// A slot under the deleter's bound. However it ends, by `observe` or by being dropped with
/// a cancelled call, it is retired against the deficit first and returned to the semaphore
/// only when nothing is owed, so cancellation cannot expose slots above the bound.
struct Slot {
    permit: Option<OwnedSemaphorePermit>,
    shared: Arc<Shared>,
}

impl Slot {
    /// Give the slot back: retire it if the bound owes a permit, release it otherwise.
    fn give_back(&mut self) {
        if let Some(permit) = self.permit.take() {
            let mut deficit = self.shared.deficit.lock().unwrap();
            Shared::give_back(&mut deficit, permit);
        }
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        self.give_back();
    }
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
            .unwrap_or(1)
            .max(1);
        let concurrency = AdaptiveConcurrency::new(config)?;
        let slots = Arc::new(Semaphore::new(concurrency.current()));
        Ok(Self {
            shared: Arc::new(Shared {
                operator,
                retry_stats,
                concurrency,
                slots,
                deficit: Mutex::new(0),
                batch_size,
                progress: Mutex::new(Progress::default()),
            }),
        })
    }

    /// Publish the running total of confirmed deletions after each successful request.
    /// Configure before starting work; the callback and total are shared by all clones.
    /// Directory cleanup by `remove_all` is excluded from the total.
    pub fn with_progress(self, report: Arc<dyn Fn(usize) + Send + Sync>) -> Self {
        self.shared.progress.lock().unwrap().report = Some(report);
        self
    }

    /// Concurrent batches currently allowed; moves with the backend's feedback.
    pub fn concurrency(&self) -> usize {
        self.shared.concurrency.current()
    }

    /// Remove selected paths in input order, stopping at the first failed request.
    ///
    /// Requests within this call are sequential so `removed` always describes a confirmed
    /// input prefix, suitable for advancing a manifest. Concurrent calls share the same
    /// request budget as `remove_all`. Directories, if supplied, must follow their contents.
    /// No listing or deletion of unselected paths is performed.
    ///
    /// Unlike `remove_all`, errors are returned inside the report so callers can retain
    /// unconfirmed paths and account for a failed request's possible partial deletion.
    pub async fn remove_files<I>(&self, paths: I) -> RemoveFilesReport
    where
        I: IntoIterator<Item = String>,
        I::IntoIter: Send,
    {
        let mut paths = paths.into_iter();
        let mut report = RemoveFilesReport::default();
        loop {
            let batch: Vec<_> = paths.by_ref().take(self.shared.batch_size).collect();
            if batch.is_empty() {
                break;
            }
            let slot = self.shared.acquire().await;
            let outcome = self.shared.clone().delete_batch(slot, batch, true).await;
            report.absorb(self.shared.complete(outcome));
            if report.error.is_some() {
                break;
            }
        }
        report
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
    /// One delete request's worth of paths. The slot travels with the outcome: it is
    /// released, or retired, only once the policy has seen the result, and settled by its
    /// `Drop` if the call is cancelled.
    async fn delete_batch(
        self: Arc<Self>,
        slot: Slot,
        paths: Vec<String>,
        counted: bool,
    ) -> BatchOutcome {
        let n = if counted { paths.len() } else { 0 };
        let mut attempted = 0;
        let before = self.retry_stats.retried();
        let result = async {
            let mut deleter = self.operator.deleter().await?;
            attempted = paths.len();
            deleter.delete_iter(paths).await?;
            deleter.close().await?;
            Ok(n)
        }
        .await;
        let retried = self.retry_stats.retried().saturating_sub(before) as usize;
        BatchOutcome {
            result,
            attempted,
            retried,
            slot,
        }
    }

    /// Settle a request once, regardless of which public entry point issued it.
    fn complete(&self, outcome: BatchOutcome) -> RemoveFilesReport {
        let BatchOutcome {
            result,
            attempted,
            retried,
            slot,
        } = outcome;
        self.observe(
            ConcurrencyFeedback {
                failed: result.is_err(),
                rate_limited: result
                    .as_ref()
                    .is_err_and(|e| e.kind() == ErrorKind::RateLimited),
                retried,
            },
            slot,
        );
        let (removed, error) = match result {
            Ok(n) => (n, None),
            Err(e) => (0, Some(e)),
        };
        if removed > 0 {
            let mut progress = self.progress.lock().unwrap();
            progress.removed += removed;
            if let Some(report) = &progress.report {
                report(progress.removed);
            }
        }
        RemoveFilesReport {
            removed,
            attempted,
            batches: 1,
            retried,
            error,
        }
    }

    /// Feed a batch outcome to the policy and move the permit count with the bound, then
    /// give the batch's own slot back.
    ///
    /// Growing first cancels permits still owed from an earlier decrease and only adds what
    /// is left over. Shrinking books the whole decrease as deficit before the slot is given
    /// back, so this slot, which is in hand and cannot reach a waiter in between, is the
    /// first one retired; permits sitting free in the semaphore are retired next, right
    /// away; only what other batches still hold remains owed, and they retire it as they
    /// end. All of it happens under the deficit lock together with the policy update.
    fn observe(&self, feedback: ConcurrencyFeedback, mut slot: Slot) {
        let mut deficit = self.deficit.lock().unwrap();
        let (before, after) = self.concurrency.observe_change(feedback, Instant::now());
        if after > before {
            let growth = after - before;
            let cancelled = growth.min(*deficit);
            *deficit -= cancelled;
            if growth > cancelled {
                self.slots.add_permits(growth - cancelled);
            }
        } else if after < before {
            *deficit += before - after;
            log::warn!(
                "Storage delete pressure: lowering request concurrency from {} to {}",
                before,
                after
            );
        }
        if let Some(permit) = slot.permit.take() {
            Self::give_back(&mut deficit, permit);
        }
        self.retire_free_permits(&mut deficit);
    }

    /// Retire the permit if the bound owes one, release it otherwise.
    fn give_back(deficit: &mut MutexGuard<'_, usize>, permit: OwnedSemaphorePermit) {
        if **deficit > 0 {
            **deficit -= 1;
            permit.forget();
        } else {
            drop(permit);
        }
    }

    /// Retire free permits against the deficit.
    fn retire_free_permits(&self, deficit: &mut MutexGuard<'_, usize>) {
        while **deficit > 0 {
            let Ok(permit) = self.slots.clone().try_acquire_owned() else {
                return;
            };
            **deficit -= 1;
            permit.forget();
        }
    }

    /// Wait for a slot under the current bound.
    async fn acquire(self: &Arc<Self>) -> Slot {
        let permit = self
            .slots
            .clone()
            .acquire_owned()
            .await
            .expect("the slot semaphore is never closed");
        Slot {
            permit: Some(permit),
            shared: self.clone(),
        }
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
        let slot = loop {
            if self.first_error.is_some() {
                return;
            }
            tokio::select! {
                biased;
                slot = self.shared.acquire() => break slot,
                settled = self.batches.next() => match settled {
                    Some(outcome) => self.settle(outcome),
                    // Nothing of ours in flight: wait for another call to release a slot.
                    None => break self.shared.acquire().await,
                },
            }
        };
        if self.first_error.is_some() {
            // Dropping the slot gives it back, retiring it first if the bound owes one.
            return;
        }
        self.batches.push(Box::pin(
            self.shared.clone().delete_batch(slot, paths, counted),
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
    fn settle(&mut self, outcome: BatchOutcome) {
        let report = self.shared.complete(outcome);
        self.report.removed += report.removed;
        self.report.batches += report.batches;
        self.report.retried += report.retried;
        if self.first_error.is_none() {
            self.first_error = report.error;
        }
    }
}
