// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// `VACUUM TEMPORARY FILES`: remove expired spill files from the tenant's spill prefix.
//
// Layout:
// - [`SpillDeleter`]: delegates selected-path deletion, retry observation, request concurrency
//   and progress to the shared [`AdaptiveDeleter`]; adds spill-specific outcome and error logs.
// - [`RunBudget`]: `LIMIT` accounting.
// - [`Pacing`]: decides after each delete batch whether a task issues the next one.
// - Directory strategies (`delete_by_meta`, `delete_by_list`, `delete_by_probe`) are plain
//   functions over a [`SpillDeleter`]: they return a [`DeleteOutcome`] and keep no shared state.
// - [`vacuum_by_duration`] schedules directory tasks and is the single place where outcomes are
//   settled into the budget and the logs. Request-level feedback is handled by the deleter.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use databend_common_catalog::table_context::AbortChecker;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_storage::AdaptiveConcurrencyConfig;
use databend_common_storage::AdaptiveDeleter;
use databend_common_storage::DataOperator;
use databend_common_storage::RetryStats;
use databend_enterprise_vacuum_handler::vacuum_handler::VacuumReport;
use databend_enterprise_vacuum_handler::vacuum_handler::VacuumTempOptions;
use futures_util::FutureExt;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesUnordered;
use log::info;
use log::warn;
use opendal::Buffer;
use opendal::ErrorKind;
use opendal::Operator;
use opendal::Scheme;

// Default retention duration for temporary files: 3 days.
const DEFAULT_RETAIN_DURATION: Duration = Duration::from_secs(60 * 60 * 24 * 3);
const SPILL_META_SUFFIX: &str = ".list";
/// Paths materialized per delete batch, from a manifest or a listing; also the S3 multi-delete
/// maximum. Bounds a task's memory independently of `LIMIT`.
const DELETE_BATCH_FILES: usize = 1000;
/// Bound concurrent manifest reads and listings, independently of the deleter's adaptive
/// request budget. Only the shared deleter adjusts delete concurrency.
const MAX_DIRECTORY_TASKS: usize = 32;

// ---------------------------------------------------------------------------------------------
// Delete I/O
// ---------------------------------------------------------------------------------------------

/// Result of one delete batch.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct DeleteOutcome {
    /// Files known to be removed: those of the requests that went through, a prefix of the
    /// batch, all of it unless `failed`.
    removed: usize,
    /// What `LIMIT` is charged: `removed`, plus the paths of a failed request, since a
    /// multi-delete reports per object and may have removed some of them before the error,
    /// whatever its kind. A run thus never removes more than it says, even while the store is
    /// failing requests half-way.
    charged: usize,
    /// A request was rejected, for whatever reason; the paths from `removed` on are left for
    /// the next run.
    failed: bool,
    /// The batch itself was rejected as rate limited.
    rate_limited: bool,
    /// Retries of the vacuum's own operator while the batch was in flight, of any error kind:
    /// S3 reports throttling as `RateLimited`, but OSS, COS, OBS and Azure answer `503`s that
    /// OpenDAL only classifies as temporary. The retry layer hides both, so a successful batch
    /// may still have been throttled; concurrent batches of the same run can each see the
    /// same retry.
    retried: usize,
}

impl DeleteOutcome {
    /// Merge the outcome of a further batch issued by the same task.
    fn absorb(&mut self, other: DeleteOutcome) {
        self.removed += other.removed;
        self.charged += other.charged;
        self.failed |= other.failed;
        self.rate_limited |= other.rate_limited;
        self.retried += other.retried;
    }
}

/// Spill-specific adapter: the shared deleter owns all delete I/O and request feedback.
struct SpillDeleter {
    operator: Operator,
    deleter: AdaptiveDeleter,
}

impl SpillDeleter {
    fn new(operator: Operator, retry_stats: Arc<RetryStats>) -> Self {
        Self::with_report(operator, retry_stats, None)
    }

    fn with_report(
        operator: Operator,
        retry_stats: Arc<RetryStats>,
        report: Option<VacuumReport>,
    ) -> Self {
        let deleter = AdaptiveDeleter::new(
            operator.clone(),
            retry_stats,
            AdaptiveConcurrencyConfig::default(),
        )
        .expect("default adaptive delete bounds must be valid");
        let deleter = match report {
            Some(report) => deleter.with_progress(report),
            None => deleter,
        };
        Self { operator, deleter }
    }

    fn operator(&self) -> &Operator {
        &self.operator
    }

    async fn delete(&self, paths: Vec<String>, from: &str) -> DeleteOutcome {
        let n = paths.len();
        let report = self.deleter.remove_files(paths).await;
        if let Some(e) = &report.error {
            warn!(
                "Vacuum temporary files: failed to delete {} of {} files from '{}', will retry in the next run: {}",
                n - report.removed,
                n,
                from,
                e
            );
        }
        DeleteOutcome {
            removed: report.removed,
            charged: report.attempted,
            failed: report.error.is_some(),
            rate_limited: report
                .error
                .as_ref()
                .is_some_and(|e| e.kind() == ErrorKind::RateLimited),
            retried: report.retried,
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Budget
// ---------------------------------------------------------------------------------------------

/// `LIMIT` accounting for one run. Owned by the scheduler; leaf tasks never touch it.
struct RunBudget {
    limit: usize,
    removed: usize,
    charged: usize,
}

impl RunBudget {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            removed: 0,
            charged: 0,
        }
    }

    /// Files known to be removed: what progress and logs report.
    fn removed(&self) -> usize {
        self.removed
    }

    /// The share of `limit` used, `removed` included: what callers continuing under the same
    /// `LIMIT` must subtract.
    fn charged(&self) -> usize {
        self.charged
    }

    fn remaining(&self) -> usize {
        self.limit.saturating_sub(self.charged)
    }

    fn exhausted(&self) -> bool {
        self.charged >= self.limit
    }

    fn record(&mut self, outcome: &DeleteOutcome) {
        self.removed += outcome.removed;
        self.charged += outcome.charged;
    }
}

// ---------------------------------------------------------------------------------------------
// Pacing
// ---------------------------------------------------------------------------------------------

/// Decides, after each delete batch, whether a task issues the next one. Shared by the manifest
/// and the listing loops so both stop for the same reasons.
#[derive(Clone)]
enum Pacing {
    /// Under the `VACUUM TEMPORARY FILES` scheduler: a batch the store pushed back on ends the
    /// task. [`AdaptiveDeleter`] has already observed the request feedback; an aborted command
    /// also stops the task. The remainder is picked up next run.
    Scheduled(AbortChecker),
    /// The query hook has no later pass for its query, which may already be aborted. It
    /// keeps draining successful batches even after absorbed retries; the shared deleter
    /// still observes every request and adjusts its bound.
    Drain,
}

impl Pacing {
    /// `false` once the task must stop issuing batches: the last one failed, or, when
    /// scheduled, the store pushed back or the command is being aborted.
    fn continues_after(&self, outcome: &DeleteOutcome) -> bool {
        if outcome.failed {
            return false;
        }
        match self {
            Pacing::Scheduled(abort_checker) => {
                !outcome.rate_limited
                    && outcome.retried == 0
                    && abort_checker.try_check_aborting().is_ok()
            }
            Pacing::Drain => true,
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Expiry
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct Expiry {
    now_ms: i64,
    retain_ms: i64,
}

impl Expiry {
    fn new(retain: Duration) -> Self {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        Self {
            now_ms,
            retain_ms: retain.as_millis() as i64,
        }
    }

    fn expired(&self, last_modified_ms: i64) -> bool {
        self.now_ms - last_modified_ms >= self.retain_ms
    }
}

// ---------------------------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------------------------

/// Returns the share of `limit` used: files known to be removed plus those of failed delete
/// requests that may have been removed before the error. Callers continuing under the same
/// `LIMIT` subtract it; the running count of files known to be removed goes to the report.
#[async_backtrace::framed]
pub async fn do_vacuum_temporary_files(
    abort_checker: AbortChecker,
    temporary_dir: String,
    options: &VacuumTempOptions,
    limit: usize,
) -> Result<usize> {
    if limit == 0 {
        return Ok(0);
    }

    match options {
        VacuumTempOptions::QueryHook(nodes, query_id) => {
            vacuum_query_hook(
                abort_checker,
                &temporary_dir,
                nodes.as_slice(),
                query_id,
                limit,
            )
            .await
        }
        VacuumTempOptions::VacuumCommand(duration, report) => {
            vacuum_by_duration(
                abort_checker,
                &temporary_dir,
                limit,
                duration.unwrap_or(DEFAULT_RETAIN_DURATION),
                report.clone(),
            )
            .await
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------------------------------

/// What one directory task hands back to the scheduler.
struct DirOutcome {
    dir_path: String,
    /// `None`: the directory was skipped, no delete was attempted.
    delete: Option<DeleteOutcome>,
}

/// Bookkeeping of one `VACUUM TEMPORARY FILES` run. Owned by the scheduler; the only place
/// where directory outcomes are settled.
struct Run {
    budget: RunBudget,
    deleter: AdaptiveDeleter,
    gc_metas: HashSet<String>,
    started: Instant,
    last_progress_log: Instant,
}

impl Run {
    const PROGRESS_LOG_INTERVAL: Duration = Duration::from_secs(10);

    fn new(limit: usize, deleter: AdaptiveDeleter) -> Self {
        let now = Instant::now();
        Self {
            budget: RunBudget::new(limit),
            deleter,
            gc_metas: HashSet::new(),
            started: now,
            last_progress_log: now,
        }
    }

    fn settle(&mut self, outcome: DirOutcome) {
        let Some(delete) = outcome.delete else {
            return;
        };
        self.budget.record(&delete);
        let now = Instant::now();
        if now.duration_since(self.last_progress_log) >= Self::PROGRESS_LOG_INTERVAL {
            self.last_progress_log = now;
            info!(
                "Vacuum temporary files progress: removed {} files, delete request concurrency {}, elapsed {} sec (last batch: {} from '{}')",
                self.budget.removed(),
                self.deleter.concurrency(),
                self.started.elapsed().as_secs(),
                delete.removed,
                outcome.dir_path,
            );
        }
    }

    /// Returns the budget used, see [`RunBudget::charged`].
    fn finish(self) -> usize {
        info!(
            "Vacuum temporary files finished: removed {} files ({} charged to the limit), final delete request concurrency {}, elapsed {} sec",
            self.budget.removed(),
            self.budget.charged(),
            self.deleter.concurrency(),
            self.started.elapsed().as_secs()
        );
        self.budget.charged()
    }
}

/// Vacuum temporary spill files by retention duration.
///
/// Background:
/// - On object stores like S3, a directory entry may not expose a reliable `last_modified`.
/// - Treating such directories as expired is unsafe because an active query's spill directory can
///   be deleted while the query is still running.
///
/// Decision order for directories:
/// 1. Use directory metadata `last_modified` if available.
/// 2. Otherwise, use the sibling `.list` meta file's `last_modified`.
/// 3. Otherwise, probe the directory by listing a bounded number of files and using their
///    `last_modified` as the directory's age.
///
/// Concurrency:
/// - The top-level listing is consumed sequentially; each spill directory is vacuumed as an
///   independent task, bounded by [`MAX_DIRECTORY_TASKS`] to cap manifest/listing memory.
///   Delete requests share the [`AdaptiveDeleter`]'s adaptive budget across every task.
/// - `limit` is a soft upper bound: no new directory is started once it is reached, but tasks
///   already in flight finish, so the final count may exceed it by the contents of up to
///   the configured maximum number of directories.
/// - A directory's sibling `.list` meta file is claimed when the directory task is scheduled, so
///   the top-level listing never vacuums the same meta file concurrently, whatever order the
///   store lists the two entries in.
/// - Cancellation is checked on every top-level entry and every settled task, and tasks stop
///   issuing batches themselves once the query is aborted, so a no-`LIMIT` run does not keep
///   deleting after the client gave up.
async fn vacuum_by_duration(
    abort_checker: AbortChecker,
    temporary_dir: &str,
    limit: usize,
    retain: Duration,
    report: VacuumReport,
) -> Result<usize> {
    // A dedicated operator, so the retries sampled around each delete are those of this
    // command's own requests and not of everything else the node sends to storage.
    let retry_stats = Arc::new(RetryStats::default());
    let operator = DataOperator::instance().observed_spill_operator(retry_stats.clone())?;
    let deleter = Arc::new(SpillDeleter::with_report(
        operator,
        retry_stats,
        Some(report),
    ));
    let expiry = Expiry::new(retain);
    let pacing = Pacing::Scheduled(abort_checker.clone());
    let mut run = Run::new(limit, deleter.deleter.clone());

    let temporary_dir: Arc<str> = format!("{}/", temporary_dir.trim_end_matches('/')).into();
    let mut ds = deleter.operator().lister_with(&temporary_dir).await?;

    let mut temp_files = Vec::new();
    // Meta-backed and directory tasks share one bounded pool; on S3 `x.list` is listed before
    // `x/`, so the meta task is the common path and must not run inline.
    let mut in_flight: FuturesUnordered<BoxFuture<'static, Result<DirOutcome>>> =
        FuturesUnordered::new();

    loop {
        if run.budget.exhausted() {
            break;
        }

        while in_flight.len() >= MAX_DIRECTORY_TASKS {
            if let Some(res) = in_flight.next().await {
                run.settle(res?);
                abort_checker.try_check_aborting()?;
            }
        }

        let de = match ds.try_next().await {
            Ok(Some(de)) => de,
            Ok(None) => break,
            Err(e) if e.kind() == ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        abort_checker.try_check_aborting()?;
        if de.path() == temporary_dir.as_ref() {
            continue;
        }

        let name = de.name();
        if de.metadata().is_file() {
            let Some(last_modified) =
                resolve_file_last_modified(deleter.operator(), de.path(), de.metadata()).await
            else {
                continue;
            };
            if !expiry.expired(last_modified) {
                continue;
            }

            if name.ends_with(SPILL_META_SUFFIX) {
                if !run.gc_metas.insert(name.to_owned()) {
                    continue;
                }
                in_flight.push(
                    vacuum_meta_entry(
                        deleter.clone(),
                        temporary_dir.clone(),
                        de.path().to_owned(),
                        run.budget.remaining(),
                        pacing.clone(),
                    )
                    .boxed(),
                );
            } else {
                temp_files.push(de.path().to_owned());
                if temp_files.len() >= run.budget.remaining() {
                    break;
                }
            }
        } else {
            // Claim the sibling meta file. If it was already handled this run (deleted, or its
            // batch was throttled and left for the next run), the directory is done too.
            let meta_name = format!("{}{}", name.trim_end_matches('/'), SPILL_META_SUFFIX);
            if !run.gc_metas.insert(meta_name) {
                continue;
            }
            let (dir_path, metadata) = de.into_parts();
            in_flight.push(
                vacuum_dir_entry(
                    deleter.clone(),
                    temporary_dir.clone(),
                    dir_path,
                    metadata,
                    expiry,
                    run.budget.remaining(),
                    pacing.clone(),
                )
                .boxed(),
            );
        }
    }

    // Draining may outlive the listing by a lot (up to `MAX_DIRECTORY_TASKS` manifests), so keep
    // honouring cancellation; returning drops the in-flight tasks.
    while let Some(res) = in_flight.next().await {
        run.settle(res?);
        abort_checker.try_check_aborting()?;
    }
    abort_checker.try_check_aborting()?;

    if !temp_files.is_empty() && temp_files.len() <= run.budget.remaining() {
        let delete = deleter.delete(temp_files, &temporary_dir).await;
        run.settle(DirOutcome {
            dir_path: temporary_dir.to_string(),
            delete: Some(delete),
        });
    }

    Ok(run.finish())
}

/// Vacuum the files recorded in an expired spill meta file. Runs in the same bounded pool as
/// directory tasks.
///
/// Every outcome leaves the pair in a state the next run, with the same `LIMIT`, can advance:
/// a manifest is deleted only together with its tail, is rewritten without the names already
/// deleted, and the listing fallback gives a single remaining slot to a directory entry.
async fn vacuum_meta_entry(
    deleter: Arc<SpillDeleter>,
    temporary_dir: Arc<str>,
    meta_path: String,
    max: usize,
    pacing: Pacing,
) -> Result<DirOutcome> {
    if max == 0 {
        return Ok(DirOutcome {
            dir_path: meta_path,
            delete: None,
        });
    }
    let mut delete = DeleteOutcome::default();
    let mut remaining = max;
    match delete_by_meta(&deleter, &temporary_dir, &meta_path, max, &pacing).await? {
        MetaVacuum::Partial(outcome) => {
            return Ok(DirOutcome {
                dir_path: meta_path,
                delete: Some(outcome),
            });
        }
        MetaVacuum::Exhausted(outcome) => {
            // The manifest ended without naming itself and its directory: it was truncated
            // before `unload_spill_meta` appended them. Whatever it listed is gone; finish
            // the directory it describes below.
            delete.absorb(outcome);
            remaining = remaining.saturating_sub(outcome.charged);
        }
        MetaVacuum::Missing | MetaVacuum::Nothing => {}
    }
    // The claim on the manifest keeps the directory task away, so vacuum the directory here and
    // drop the manifest with it.
    let dir_path = format!("{}/", meta_path.trim_end_matches(SPILL_META_SUFFIX));
    finish_directory_and_meta(
        &deleter,
        &dir_path,
        &meta_path,
        remaining,
        &pacing,
        &mut delete,
    )
    .await?;
    Ok(DirOutcome {
        dir_path: meta_path,
        delete: Some(delete),
    })
}

/// List-delete what is left under `dir_path`, then drop the manifest that failed to describe
/// it. Under throttling the manifest is kept so the next run finishes the directory; the task
/// ends here either way and the scheduler sees the signal. One unit of `max` is reserved for
/// the manifest so the task never removes more than `max` objects; with room for one object
/// only, a directory entry goes if there is one (the manifest keeps the claim), else the
/// manifest, so even `LIMIT 1` makes progress on the pair.
async fn finish_directory_and_meta(
    deleter: &SpillDeleter,
    dir_path: &str,
    meta_path: &str,
    max: usize,
    pacing: &Pacing,
    delete: &mut DeleteOutcome,
) -> Result<()> {
    let Some(list_budget) = max.checked_sub(1) else {
        return Ok(());
    };
    if list_budget > 0 {
        let by_list = delete_by_list(deleter, dir_path, list_budget, pacing).await?;
        delete.absorb(by_list.outcome);
        // Anything short of an emptied directory (a failed or throttled batch, the budget, an
        // abort) leaves files behind: keep the manifest so the directory is claimed and
        // finished next run.
        if !by_list.finished {
            return Ok(());
        }
    } else if let Some(path) = first_dir_entry(deleter, dir_path).await? {
        delete.absorb(deleter.delete(vec![path], dir_path).await);
        return Ok(());
    }
    if delete.retried == 0 && !delete.rate_limited {
        delete.absorb(deleter.delete(vec![meta_path.to_owned()], meta_path).await);
    }
    Ok(())
}

/// How far a manifest-driven vacuum got.
enum MetaVacuum {
    /// There is no manifest.
    Missing,
    /// The manifest names nothing under the spill prefix, or nothing it names fits `max`.
    Nothing,
    /// The task stops here: lines remain (budget or failure) or the store was throttling.
    /// Whatever is left is written back and picked up next run. When the manifest listed
    /// itself it is gone by now.
    Partial(DeleteOutcome),
    /// Every line was processed, with nothing making the task stop early, but the manifest
    /// never named itself, so neither it nor its directory marker were removed.
    Exhausted(DeleteOutcome),
}

impl MetaVacuum {
    fn outcome(&self) -> DeleteOutcome {
        match self {
            MetaVacuum::Missing | MetaVacuum::Nothing => DeleteOutcome::default(),
            MetaVacuum::Partial(o) | MetaVacuum::Exhausted(o) => *o,
        }
    }
}

/// Vacuum one spill directory. Runs concurrently; returns its outcome instead of touching shared
/// state. `max` is the budget left when the task was started.
async fn vacuum_dir_entry(
    deleter: Arc<SpillDeleter>,
    temporary_dir: Arc<str>,
    dir_path: String,
    metadata: opendal::Metadata,
    expiry: Expiry,
    max: usize,
    pacing: Pacing,
) -> Result<DirOutcome> {
    let meta_path = format!("{}{}", dir_path.trim_end_matches('/'), SPILL_META_SUFFIX);
    let outcome = |delete| DirOutcome {
        dir_path: dir_path.clone(),
        delete,
    };
    if max == 0 {
        return Ok(outcome(None));
    }

    let Some(last_modified) =
        resolve_dir_last_modified(deleter.operator(), &metadata, &dir_path, &meta_path).await
    else {
        let delete = delete_by_probe(&deleter, &dir_path, expiry, max, &pacing).await?;
        return Ok(outcome(delete));
    };
    if !expiry.expired(last_modified) {
        return Ok(outcome(None));
    }
    // A partial meta batch is final for this run, whether it succeeded or was throttled. Only a
    // directory without a usable meta file, or one whose truncated meta never named the
    // directory, falls back to listing. The sibling manifest was claimed with this task, so
    // when it exists it is finished here too: the top-level listing will skip it.
    let by_meta = delete_by_meta(&deleter, &temporary_dir, &meta_path, max, &pacing).await?;
    let delete = match by_meta {
        MetaVacuum::Partial(by_meta) => Some(by_meta),
        MetaVacuum::Missing => Some(
            delete_by_list(&deleter, &dir_path, max, &pacing)
                .await?
                .outcome,
        ),
        MetaVacuum::Nothing | MetaVacuum::Exhausted(_) => {
            let mut by_meta = by_meta.outcome();
            let remaining = max.saturating_sub(by_meta.charged);
            finish_directory_and_meta(
                &deleter,
                &dir_path,
                &meta_path,
                remaining,
                &pacing,
                &mut by_meta,
            )
            .await?;
            Some(by_meta)
        }
    };
    Ok(outcome(delete))
}

// ---------------------------------------------------------------------------------------------
// Directory strategies
// ---------------------------------------------------------------------------------------------

/// Delete the files recorded in a spill meta file. `None` when there is no meta file or it
/// lists nothing to delete, i.e. no batch was attempted.
async fn delete_by_meta(
    deleter: &SpillDeleter,
    temporary_dir: &str,
    meta_path: &str,
    max: usize,
    pacing: &Pacing,
) -> Result<MetaVacuum> {
    let meta = match deleter.operator().read(meta_path).await {
        Ok(meta) => meta,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(MetaVacuum::Missing),
        Err(e) => return Err(e.into()),
    };
    delete_by_meta_buffer(deleter, temporary_dir, meta_path, meta, max, pacing).await
}

/// Delete up to `max` files recorded in `meta` (one full path per line, must live under
/// `temporary_dir`). Lines beyond `max` are written back so the next run continues from there.
/// If the batch failed the meta file is left untouched, so the same files are retried instead of
/// becoming orphans.
async fn delete_by_meta_buffer(
    deleter: &SpillDeleter,
    temporary_dir: &str,
    meta_path: &str,
    meta: Buffer,
    max: usize,
    pacing: &Pacing,
) -> Result<MetaVacuum> {
    // The manifest is walked in place: at most `DELETE_BATCH_FILES` paths are materialized per
    // batch, whatever `max` is, so a large manifest never becomes a second full copy. What was
    // not deleted is written back as a slice of the buffer already held.
    let bytes = meta.to_bytes();
    let text = std::str::from_utf8(&bytes).map_err(|e| {
        ErrorCode::StorageOther(format!("spill meta {meta_path} is not utf-8: {e}"))
    })?;

    let mut total = DeleteOutcome::default();
    let mut attempted = false;
    let mut stopped_early = false;
    let mut manifest_deleted = false;
    let mut consumed = 0;
    let mut budget = max;
    while budget > 0 {
        let mut batch = Vec::new();
        // Where each path's line ends, so a batch that failed part-way advances exactly past
        // the paths that were removed.
        let mut line_ends = Vec::new();
        let mut batch_end = consumed;
        let mut meta_index = None;
        let cap = budget.min(DELETE_BATCH_FILES);
        for line in text[consumed..].split_inclusive('\n') {
            if batch.len() >= cap {
                break;
            }
            let path = line.trim_end_matches(['\n', '\r']);
            if path == meta_path {
                // The manifest goes only together with everything it still names after itself
                // (normally just the directory marker). Deleting it any earlier would have
                // the remainder written back into a manifest that was just removed, which no
                // later run could ever finish within the same budget.
                let after = text[batch_end + line.len()..]
                    .lines()
                    .filter(|l| l.trim_end_matches('\r').starts_with(temporary_dir))
                    .count();
                if batch.len() + 1 + after > cap {
                    break;
                }
                meta_index = Some(batch.len());
            }
            batch_end += line.len();
            if path.starts_with(temporary_dir) {
                batch.push(path.to_owned());
                line_ends.push(batch_end);
            }
        }
        if batch.is_empty() {
            break;
        }
        attempted = true;
        budget -= batch.len();
        let outcome = deleter.delete(batch, meta_path).await;
        total.absorb(outcome);
        manifest_deleted |= meta_index.is_some_and(|i| i < outcome.removed);
        if outcome.failed {
            // Keep what was not removed in the manifest for the next run.
            if outcome.removed > 0 {
                consumed = line_ends[outcome.removed - 1];
            }
            stopped_early = true;
            break;
        }
        consumed = batch_end;
        if !pacing.continues_after(&outcome) {
            // The store pushed back or the query is being aborted: the rest stays in the
            // manifest and the task ends here.
            stopped_early = true;
            break;
        }
    }

    if !attempted {
        return Ok(MetaVacuum::Nothing);
    }
    let exhausted = text[consumed..]
        .lines()
        .all(|l| !l.trim_end_matches('\r').starts_with(temporary_dir));
    // What was deleted leaves the manifest on disk too, even when that leaves it empty: should
    // the directory not be finished in this task (budget, throttling, abort), the next run
    // must go straight to the listing instead of re-deleting, and re-counting, the same names.
    if total.removed > 0 && !manifest_deleted {
        deleter
            .operator()
            .write(meta_path, bytes.slice(consumed..))
            .await?;
    }
    // A task that stopped early ends here either way; a truncated manifest is finished next
    // run rather than followed by a listing while the store is pushing back.
    if exhausted && !manifest_deleted && !stopped_early {
        return Ok(MetaVacuum::Exhausted(total));
    }
    Ok(MetaVacuum::Partial(total))
}

/// Collect paths from a recursive listing of `dir_path` until `max` are buffered or the listing
/// ends, skipping the directory entry itself. When the listing was exhausted the directory
/// marker is appended if there is capacity left and `true` is returned, so a directory with
/// more files than `max` is never fully buffered and is only removed once it is empty.
async fn collect_dir_paths(
    lister: &mut opendal::Lister,
    dir_path: &str,
    paths: &mut Vec<String>,
    max: usize,
) -> Result<bool> {
    while paths.len() < max {
        match lister.try_next().await {
            Ok(Some(entry)) if entry.path() == dir_path => continue,
            Ok(Some(entry)) => paths.push(entry.path().to_string()),
            Ok(None) => {
                paths.push(dir_path.to_owned());
                return Ok(true);
            }
            Err(e) if e.kind() == ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(false)
}

/// The first path a recursive listing of `dir_path` yields, the directory entry itself aside;
/// `None` for an empty directory.
async fn first_dir_entry(deleter: &SpillDeleter, dir_path: &str) -> Result<Option<String>> {
    let mut lister = deleter
        .operator()
        .lister_with(dir_path)
        .recursive(true)
        .await?;
    loop {
        match lister.try_next().await {
            Ok(Some(entry)) if entry.path() == dir_path => continue,
            Ok(Some(entry)) => return Ok(Some(entry.path().to_string())),
            Ok(None) => return Ok(None),
            Err(e) if e.kind() == ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        }
    }
}

/// Result of a listing-driven vacuum of one directory.
struct ListVacuum {
    outcome: DeleteOutcome,
    /// The listing was drained and every batch, the directory marker included, succeeded.
    finished: bool,
}

/// Delete what `lister` yields under `dir_path` in batches of at most `DELETE_BATCH_FILES`,
/// `max` paths in total, so a large directory is never fully buffered whatever `LIMIT` is.
/// `paths` may carry entries already pulled from the lister. Stops when the listing ends or
/// `pacing` says so.
async fn delete_listed(
    deleter: &SpillDeleter,
    lister: &mut opendal::Lister,
    dir_path: &str,
    mut paths: Vec<String>,
    max: usize,
    pacing: &Pacing,
) -> Result<ListVacuum> {
    let mut total = DeleteOutcome::default();
    let mut budget = max;
    let mut finished = false;
    while budget > 0 {
        let exhausted =
            collect_dir_paths(lister, dir_path, &mut paths, budget.min(DELETE_BATCH_FILES)).await?;
        if paths.is_empty() {
            break;
        }
        budget -= paths.len();
        let outcome = deleter.delete(std::mem::take(&mut paths), dir_path).await;
        total.absorb(outcome);
        if exhausted {
            // The marker was in this batch: the directory is gone iff the batch went through.
            finished = !outcome.failed;
            break;
        }
        if !pacing.continues_after(&outcome) {
            break;
        }
    }
    Ok(ListVacuum {
        outcome: total,
        finished,
    })
}

/// Delete everything under `dir_path` found by a recursive listing, then the directory itself.
/// `max` must be positive; at least one batch is always issued, if only for the directory
/// marker of an empty directory.
async fn delete_by_list(
    deleter: &SpillDeleter,
    dir_path: &str,
    max: usize,
    pacing: &Pacing,
) -> Result<ListVacuum> {
    let mut lister = deleter
        .operator()
        .lister_with(dir_path)
        .recursive(true)
        .await?;
    delete_listed(deleter, &mut lister, dir_path, Vec::new(), max, pacing).await
}

/// For a directory without a usable `last_modified`: probe the first file and use its
/// `last_modified` as the directory's age. If expired, reuse the same lister to finish deletion
/// and avoid an extra probe list. `None` when the directory is empty, fresh or of unknown age.
async fn delete_by_probe(
    deleter: &SpillDeleter,
    dir_path: &str,
    expiry: Expiry,
    max: usize,
    pacing: &Pacing,
) -> Result<Option<DeleteOutcome>> {
    let operator = deleter.operator();
    let mut lister = match operator.lister_with(dir_path).recursive(true).await {
        Ok(lister) => lister,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };

    let first = loop {
        match lister.try_next().await {
            Ok(Some(entry)) if entry.path() == dir_path => continue,
            Ok(Some(entry)) => break entry,
            Ok(None) => return Ok(None),
            Err(e) if e.kind() == ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        }
    };
    let Some(last_modified) =
        resolve_file_last_modified(operator, first.path(), first.metadata()).await
    else {
        // Unknown age: be conservative and keep the directory.
        return Ok(None);
    };
    if !expiry.expired(last_modified) {
        return Ok(None);
    }

    let paths = vec![first.path().to_string()];
    let by_list = delete_listed(deleter, &mut lister, dir_path, paths, max, pacing).await?;
    Ok(Some(by_list.outcome))
}

// ---------------------------------------------------------------------------------------------
// Query hook: vacuum the spill of one finished (or killed) query
// ---------------------------------------------------------------------------------------------

async fn vacuum_query_hook(
    _abort_checker: AbortChecker,
    temporary_dir: &str,
    nodes: &[usize],
    query_id: &str,
    limit: usize,
) -> Result<usize> {
    // The hook drains regardless of absorbed retries, but its requests still use the same
    // observed, adaptive deletion path as the scheduled command.
    let retry_stats = Arc::new(RetryStats::default());
    let operator = DataOperator::instance().observed_spill_operator(retry_stats.clone())?;
    let deleter = SpillDeleter::new(operator, retry_stats);
    let mut budget = RunBudget::new(limit);

    let metas_f = nodes
        .iter()
        .map(|i| {
            let operator = deleter.operator().clone();
            let meta_path = format!("{}/{}_{}{}", temporary_dir, query_id, i, SPILL_META_SUFFIX);
            async move {
                let buffer = operator.read(&meta_path).await?;
                std::result::Result::<(String, Buffer), opendal::Error>::Ok((meta_path, buffer))
            }
        })
        .collect::<Vec<_>>();
    let metas = futures_util::future::join_all(metas_f)
        .await
        .into_iter()
        .filter_map(|x| x.ok());

    for (meta_path, buffer) in metas {
        let outcome = delete_by_meta_buffer(
            &deleter,
            temporary_dir,
            &meta_path,
            buffer,
            budget.remaining(),
            &Pacing::Drain,
        )
        .await?;
        budget.record(&outcome.outcome());
    }

    Ok(budget.charged())
}

// ---------------------------------------------------------------------------------------------
// last_modified resolution
// ---------------------------------------------------------------------------------------------

/// Read `last_modified` for a single object path via `stat`.
async fn stat_last_modified(operator: &Operator, path: &str) -> Option<i64> {
    operator.stat(path).await.ok().and_then(|meta| {
        meta.last_modified()
            .map(|v| v.into_inner().as_millisecond())
    })
}

/// Resolve `last_modified` for a file entry: prefer listing metadata, fall back to `stat`.
async fn resolve_file_last_modified(
    operator: &Operator,
    path: &str,
    metadata: &opendal::Metadata,
) -> Option<i64> {
    if let Some(v) = metadata.last_modified() {
        return Some(v.into_inner().as_millisecond());
    }
    stat_last_modified(operator, path).await
}

/// Resolve `last_modified` for a directory: the entry metadata, then the directory itself where
/// directories are real, then the sibling `.list` meta file. `None` means the age is unknown and
/// the directory must be probed.
///
/// `stat(dir)` is only issued on backends with native directories, where it returns the mtime
/// that moves every time a file is added. On object stores opendal answers it with a
/// `list(limit = 1)` that never carries `last_modified`, so it would only cost one LIST.
async fn resolve_dir_last_modified(
    operator: &Operator,
    metadata: &opendal::Metadata,
    dir_path: &str,
    meta_path: &str,
) -> Option<i64> {
    if let Some(v) = metadata.last_modified() {
        return Some(v.into_inner().as_millisecond());
    }
    if has_native_directories(operator)
        && let Some(v) = stat_last_modified(operator, dir_path).await
    {
        return Some(v);
    }
    stat_last_modified(operator, meta_path).await
}

/// Backends whose directories are real objects with their own mtime. Object stores simulate
/// `stat(dir)` with a `list(limit = 1)` that never carries `last_modified`.
fn has_native_directories(operator: &Operator) -> bool {
    let scheme = operator.info().scheme();
    [Scheme::Fs, Scheme::Hdfs, Scheme::Webhdfs]
        .iter()
        .any(|s| s.into_static() == scheme)
}
