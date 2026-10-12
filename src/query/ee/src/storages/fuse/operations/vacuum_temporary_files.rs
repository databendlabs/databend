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
// - [`SpillDeleter`]: the only piece that talks to the object store for deletes and knows how
//   throttling shows up (`RateLimited` errors, hidden retries).
// - [`AdaptiveConcurrency`]: pure policy deciding how many spill directories may be vacuumed at
//   once, driven by the delete outcomes.
// - [`RunBudget`]: `LIMIT` accounting and progress reporting.
// - Directory strategies (`delete_by_meta`, `delete_by_list`, `delete_by_probe`) are plain
//   functions over a [`SpillDeleter`]: they return a [`DeleteOutcome`] and keep no shared state.
// - [`vacuum_by_duration`] schedules directory tasks and is the single place where outcomes are
//   settled into the budget, the concurrency policy and the logs.

use std::collections::HashSet;
use std::io::BufRead;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use databend_common_catalog::table_context::AbortChecker;
use databend_common_exception::Result;
use databend_common_storage::DataOperator;
use databend_common_storage::opendal_rate_limited_retries;
use databend_enterprise_vacuum_handler::vacuum_handler::VacuumReport;
use databend_enterprise_vacuum_handler::vacuum_handler::VacuumTempOptions;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use futures_util::stream::FuturesUnordered;
use log::info;
use log::warn;
use opendal::Buffer;
use opendal::ErrorKind;
use opendal::Operator;
use parking_lot::Mutex;

// Default retention duration for temporary files: 3 days.
const DEFAULT_RETAIN_DURATION: Duration = Duration::from_secs(60 * 60 * 24 * 3);
const SPILL_META_SUFFIX: &str = ".list";

// ---------------------------------------------------------------------------------------------
// Delete I/O
// ---------------------------------------------------------------------------------------------

/// Result of one delete batch.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct DeleteOutcome {
    /// Files actually removed. A batch that fails counts as `0` and is left for the next run.
    removed: usize,
    /// The batch itself was rejected as rate limited.
    rate_limited: bool,
    /// `RateLimited` retries recorded process-wide while the batch was in flight. The retry
    /// layer hides `SlowDown`, so a successful batch may still have been throttled.
    retried: usize,
}

/// Deletes spill files and observes how the object store responds.
struct SpillDeleter {
    operator: Operator,
}

impl SpillDeleter {
    fn new(operator: Operator) -> Self {
        Self { operator }
    }

    fn operator(&self) -> &Operator {
        &self.operator
    }

    async fn delete(&self, paths: Vec<String>, from: &str) -> DeleteOutcome {
        let n = paths.len();
        let retries_before = opendal_rate_limited_retries();
        let result = self.operator.delete_iter(paths).await;
        let retried = opendal_rate_limited_retries().saturating_sub(retries_before) as usize;
        match result {
            Ok(()) => DeleteOutcome {
                removed: n,
                rate_limited: false,
                retried,
            },
            Err(e) => {
                warn!(
                    "Vacuum temporary files: failed to delete {} files from '{}', will retry in the next run: {}",
                    n, from, e
                );
                DeleteOutcome {
                    removed: 0,
                    rate_limited: e.kind() == ErrorKind::RateLimited,
                    retried,
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Concurrency policy
// ---------------------------------------------------------------------------------------------

/// Additive-increase / multiplicative-decrease bound on concurrently vacuumed spill directories.
///
/// Object stores throttle deletes per key prefix (S3: 3,500 keys/s), so the right concurrency
/// depends on the store, not the node: 32 already triggers `SlowDown` storms, 1 is latency-bound.
/// Every `RAMP_UP_BATCHES` clear batches raise the bound by one; a throttled batch halves it, at
/// most once per `DECREASE_COOLDOWN` so one throttling episode (visible to every overlapping
/// batch for the 1s/2s/4s retry back-offs) is counted once.
struct AdaptiveConcurrency(Mutex<ConcurrencyState>);

struct ConcurrencyState {
    current: usize,
    clear_streak: usize,
    last_decrease: Option<Instant>,
}

impl AdaptiveConcurrency {
    const INIT: usize = 8;
    const MIN: usize = 1;
    const MAX: usize = 32;
    const RAMP_UP_BATCHES: usize = 16;
    const DECREASE_COOLDOWN: Duration = Duration::from_secs(10);

    fn new() -> Self {
        Self(Mutex::new(ConcurrencyState {
            current: Self::INIT,
            clear_streak: 0,
            last_decrease: None,
        }))
    }

    fn current(&self) -> usize {
        self.0.lock().current
    }

    /// Sporadic retries are noise the retry layer already absorbs and must not stall the ramp-up;
    /// only a burst of at least half the in-flight batches, and never fewer than a full `INIT`
    /// round, counts as throttling.
    fn is_throttled(&self, outcome: &DeleteOutcome) -> bool {
        outcome.rate_limited || outcome.retried >= self.current().div_ceil(2).max(Self::INIT)
    }

    fn observe(&self, outcome: &DeleteOutcome, now: Instant) {
        if self.is_throttled(outcome) {
            self.back_off(now);
        } else {
            self.ramp_up();
        }
    }

    fn ramp_up(&self) {
        let mut s = self.0.lock();
        s.clear_streak += 1;
        if s.clear_streak < Self::RAMP_UP_BATCHES {
            return;
        }
        s.clear_streak = 0;
        if s.current < Self::MAX {
            s.current += 1;
        }
    }

    fn back_off(&self, now: Instant) {
        let mut s = self.0.lock();
        s.clear_streak = 0;
        let cooled = s
            .last_decrease
            .is_none_or(|t| now.saturating_duration_since(t) >= Self::DECREASE_COOLDOWN);
        if !cooled || s.current <= Self::MIN {
            return;
        }
        s.last_decrease = Some(now);
        s.current = (s.current / 2).max(Self::MIN);
        warn!(
            "Vacuum temporary files: object store is rate limiting deletes, lowering directory concurrency to {}",
            s.current
        );
    }
}

// ---------------------------------------------------------------------------------------------
// Budget
// ---------------------------------------------------------------------------------------------

/// `LIMIT` accounting for one run. Owned by the scheduler; leaf tasks never touch it.
struct RunBudget {
    limit: usize,
    removed: usize,
    report: Option<VacuumReport>,
}

impl RunBudget {
    fn new(limit: usize, report: Option<VacuumReport>) -> Self {
        Self {
            limit,
            removed: 0,
            report,
        }
    }

    fn removed(&self) -> usize {
        self.removed
    }

    fn remaining(&self) -> usize {
        self.limit.saturating_sub(self.removed)
    }

    fn exhausted(&self) -> bool {
        self.removed >= self.limit
    }

    fn record(&mut self, removed: usize) {
        self.removed += removed;
        if let Some(report) = &self.report {
            report(self.removed);
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
    /// Meta file consumed by this task; the scheduler skips it when the listing reaches it.
    gc_meta: Option<String>,
}

/// Bookkeeping of one `VACUUM TEMPORARY FILES` run. Owned by the scheduler; the only place
/// where directory outcomes are settled.
struct Run {
    budget: RunBudget,
    concurrency: AdaptiveConcurrency,
    gc_metas: HashSet<String>,
    started: Instant,
    last_progress_log: Instant,
}

impl Run {
    const PROGRESS_LOG_INTERVAL: Duration = Duration::from_secs(10);

    fn new(limit: usize, report: VacuumReport) -> Self {
        let now = Instant::now();
        Self {
            budget: RunBudget::new(limit, Some(report)),
            concurrency: AdaptiveConcurrency::new(),
            gc_metas: HashSet::new(),
            started: now,
            last_progress_log: now,
        }
    }

    fn settle(&mut self, outcome: DirOutcome) {
        if let Some(meta) = outcome.gc_meta {
            self.gc_metas.insert(meta);
        }
        let Some(delete) = outcome.delete else {
            return;
        };
        self.budget.record(delete.removed);
        let now = Instant::now();
        self.concurrency.observe(&delete, now);
        if now.duration_since(self.last_progress_log) >= Self::PROGRESS_LOG_INTERVAL {
            self.last_progress_log = now;
            info!(
                "Vacuum temporary files progress: removed {} files, directory concurrency {}, elapsed {} sec (last batch: {} from '{}')",
                self.budget.removed(),
                self.concurrency.current(),
                self.started.elapsed().as_secs(),
                delete.removed,
                outcome.dir_path,
            );
        }
    }

    fn finish(self) -> usize {
        info!(
            "Vacuum temporary files finished: removed {} files, final directory concurrency {}, elapsed {} sec",
            self.budget.removed(),
            self.concurrency.current(),
            self.started.elapsed().as_secs()
        );
        self.budget.removed()
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
///   independent task, bounded by [`AdaptiveConcurrency`].
/// - `limit` is a soft upper bound: no new directory is started once it is reached, but tasks
///   already in flight finish, so the final count may exceed it by up to
///   `AdaptiveConcurrency::MAX` directories.
async fn vacuum_by_duration(
    abort_checker: AbortChecker,
    temporary_dir: &str,
    limit: usize,
    retain: Duration,
    report: VacuumReport,
) -> Result<usize> {
    let deleter = Arc::new(SpillDeleter::new(DataOperator::instance().spill_operator()));
    let expiry = Expiry::new(retain);
    let mut run = Run::new(limit, report);

    let temporary_dir: Arc<str> = format!("{}/", temporary_dir.trim_end_matches('/')).into();
    let mut ds = deleter.operator().lister_with(&temporary_dir).await?;

    let mut temp_files = Vec::new();
    let mut in_flight = FuturesUnordered::new();

    loop {
        if run.budget.exhausted() {
            break;
        }

        while in_flight.len() >= run.concurrency.current() {
            if let Some(res) = in_flight.next().await {
                run.settle(res?);
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
                if run.gc_metas.contains(name) {
                    continue;
                }
                let delete =
                    delete_by_meta(&deleter, &temporary_dir, de.path(), run.budget.remaining())
                        .await?;
                run.settle(DirOutcome {
                    dir_path: de.path().to_owned(),
                    delete: Some(delete),
                    gc_meta: Some(name.to_owned()),
                });
            } else {
                temp_files.push(de.path().to_owned());
                if temp_files.len() >= run.budget.remaining() {
                    break;
                }
            }
        } else {
            let (dir_path, metadata) = de.into_parts();
            in_flight.push(vacuum_dir_entry(
                deleter.clone(),
                temporary_dir.clone(),
                dir_path,
                metadata,
                expiry,
                run.budget.remaining(),
            ));
        }
    }

    while let Some(res) = in_flight.next().await {
        run.settle(res?);
    }

    if !temp_files.is_empty() && temp_files.len() <= run.budget.remaining() {
        let delete = deleter.delete(temp_files, &temporary_dir).await;
        run.settle(DirOutcome {
            dir_path: temporary_dir.to_string(),
            delete: Some(delete),
            gc_meta: None,
        });
    }

    Ok(run.finish())
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
) -> Result<DirOutcome> {
    let dir = dir_path.trim_end_matches('/');
    let meta_path = format!("{}{}", dir, SPILL_META_SUFFIX);
    // Name as the top-level listing reports the sibling meta file (no trailing '/').
    let meta_name = format!(
        "{}{}",
        dir.rsplit('/').next().unwrap_or(dir),
        SPILL_META_SUFFIX
    );
    let skipped = |dir_path| DirOutcome {
        dir_path,
        delete: None,
        gc_meta: None,
    };
    if max == 0 {
        return Ok(skipped(dir_path));
    }

    match resolve_dir_last_modified(deleter.operator(), &metadata, &meta_path).await {
        Some(last_modified) => {
            if !expiry.expired(last_modified) {
                return Ok(skipped(dir_path));
            }
            let by_meta = delete_by_meta(&deleter, &temporary_dir, &meta_path, max).await?;
            if by_meta.removed > 0 {
                return Ok(DirOutcome {
                    dir_path,
                    delete: Some(by_meta),
                    gc_meta: Some(meta_name),
                });
            }
            let delete = delete_by_list(&deleter, &dir_path, max).await?;
            Ok(DirOutcome {
                dir_path,
                delete: Some(delete),
                gc_meta: None,
            })
        }
        None => {
            let delete = delete_by_probe(&deleter, &dir_path, expiry, max).await?;
            let gc_meta = (delete.removed > 0).then_some(meta_name);
            Ok(DirOutcome {
                dir_path,
                delete: Some(delete),
                gc_meta,
            })
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Directory strategies
// ---------------------------------------------------------------------------------------------

/// Delete the files recorded in a spill meta file. A missing meta file yields an empty outcome.
async fn delete_by_meta(
    deleter: &SpillDeleter,
    temporary_dir: &str,
    meta_path: &str,
    max: usize,
) -> Result<DeleteOutcome> {
    let meta = match deleter.operator().read(meta_path).await {
        Ok(meta) => meta,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(DeleteOutcome::default()),
        Err(e) => return Err(e.into()),
    };
    delete_by_meta_buffer(deleter, temporary_dir, meta_path, meta, max).await
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
) -> Result<DeleteOutcome> {
    let meta = meta.to_bytes();
    let files: Vec<String> = meta.lines().map(|x| Ok(x?)).collect::<Result<Vec<_>>>()?;
    let (to_be_removed, remain) = files.split_at(max.min(files.len()));

    let to_be_removed: Vec<String> = to_be_removed
        .iter()
        .filter(|f| f.starts_with(temporary_dir))
        .cloned()
        .collect();
    let attempted = to_be_removed.len();
    let outcome = deleter.delete(to_be_removed, meta_path).await;

    if attempted > 0 && outcome.removed == 0 {
        return Ok(outcome);
    }
    if !remain.is_empty() {
        deleter
            .operator()
            .write(meta_path, remain.join("\n"))
            .await?;
    }
    Ok(outcome)
}

/// Delete everything under `dir_path` found by a recursive listing, then the directory itself.
async fn delete_by_list(
    deleter: &SpillDeleter,
    dir_path: &str,
    max: usize,
) -> Result<DeleteOutcome> {
    let mut lister = deleter
        .operator()
        .lister_with(dir_path)
        .recursive(true)
        .await?;
    let mut paths = vec![];
    while let Some(entry) = lister.try_next().await? {
        if entry.path() == dir_path {
            continue;
        }
        paths.push(entry.path().to_string());
    }
    paths.push(dir_path.to_owned());
    paths.truncate(max);
    Ok(deleter.delete(paths, dir_path).await)
}

/// For a directory without a usable `last_modified`: probe the first file and use its
/// `last_modified` as the directory's age. If expired, reuse the same lister to finish deletion
/// and avoid an extra probe list.
async fn delete_by_probe(
    deleter: &SpillDeleter,
    dir_path: &str,
    expiry: Expiry,
    max: usize,
) -> Result<DeleteOutcome> {
    let operator = deleter.operator();
    let mut lister = match operator.lister_with(dir_path).recursive(true).await {
        Ok(lister) => lister,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(DeleteOutcome::default()),
        Err(e) => return Err(e.into()),
    };
    let mut paths = Vec::new();

    let mut first_entry_last_modified = None;
    loop {
        let entry = match lister.try_next().await {
            Ok(Some(entry)) => entry,
            Ok(None) => break,
            Err(e) if e.kind() == ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        if entry.path() == dir_path {
            continue;
        }
        paths.push(entry.path().to_string());
        first_entry_last_modified =
            resolve_file_last_modified(operator, entry.path(), entry.metadata()).await;
        break;
    }

    let Some(first_entry_last_modified) = first_entry_last_modified else {
        return Ok(DeleteOutcome::default());
    };
    if !expiry.expired(first_entry_last_modified) {
        return Ok(DeleteOutcome::default());
    }

    loop {
        let entry = match lister.try_next().await {
            Ok(Some(entry)) => entry,
            Ok(None) => break,
            Err(e) if e.kind() == ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        if entry.path() == dir_path {
            continue;
        }
        paths.push(entry.path().to_string());
    }
    paths.push(dir_path.to_owned());
    paths.truncate(max);
    Ok(deleter.delete(paths, dir_path).await)
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
    let deleter = SpillDeleter::new(DataOperator::instance().spill_operator());
    let mut budget = RunBudget::new(limit, None);

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
        )
        .await?;
        budget.record(outcome.removed);
    }

    Ok(budget.removed())
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

/// Resolve `last_modified` for a directory: prefer the entry metadata, fall back to the sibling
/// `.list` meta file. `None` means the age is unknown and the directory must be probed.
///
/// The directory path itself is deliberately not `stat`ed: on stores without native directories
/// (S3) opendal answers `stat("dir/")` with a `list(limit = 1)` that carries no `last_modified`,
/// so it can never resolve a timestamp and only costs one extra LIST per directory.
async fn resolve_dir_last_modified(
    operator: &Operator,
    metadata: &opendal::Metadata,
    meta_path: &str,
) -> Option<i64> {
    if let Some(v) = metadata.last_modified() {
        return Some(v.into_inner().as_millisecond());
    }
    stat_last_modified(operator, meta_path).await
}
