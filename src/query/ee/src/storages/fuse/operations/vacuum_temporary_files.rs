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

use std::collections::HashSet;
use std::io::BufRead;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_catalog::table_context::AbortChecker;
use databend_common_exception::Result;
use databend_common_storage::DataOperator;
use databend_common_storage::opendal_rate_limited_retries;
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
/// Throttling observed by one delete batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ThrottleSignal {
    Clear,
    /// Some requests were retried but the batch went through.
    Light,
    /// Rejected as rate limited, or most in-flight requests were retried.
    Heavy,
}

/// Additive-increase / multiplicative-decrease bound on concurrently vacuumed spill directories.
///
/// Object stores throttle deletes per key prefix (S3: 3,500 keys/s), so the right concurrency
/// depends on the store, not the node: 32 already triggers `SlowDown` storms, 1 is latency-bound.
/// `Clear` ramps up by one every `RAMP_UP_BATCHES`; `Light` holds; `Heavy` halves, at most once
/// per `DECREASE_COOLDOWN` so one throttling episode (visible to every overlapping batch for the
/// 1s/2s/4s retry back-offs) is counted once. Takes the clock as a parameter for testability.
struct DirConcurrency {
    current: AtomicUsize,
    ok_streak: AtomicUsize,
    /// `ms since epoch + 1` of the last decrease; `0` = never.
    last_decrease_ms: AtomicU64,
    epoch: Instant,
}

impl DirConcurrency {
    const INIT: usize = 8;
    const MIN: usize = 1;
    const MAX: usize = 32;
    const RAMP_UP_BATCHES: usize = 16;
    const DECREASE_COOLDOWN: Duration = Duration::from_secs(10);

    fn new(epoch: Instant) -> Self {
        Self {
            current: AtomicUsize::new(Self::INIT),
            ok_streak: AtomicUsize::new(0),
            last_decrease_ms: AtomicU64::new(0),
            epoch,
        }
    }

    fn current(&self) -> usize {
        self.current.load(Ordering::Relaxed)
    }

    fn observe(&self, signal: ThrottleSignal, now: Instant) {
        match signal {
            ThrottleSignal::Clear => self.ramp_up(),
            ThrottleSignal::Light => self.ok_streak.store(0, Ordering::Relaxed),
            ThrottleSignal::Heavy => {
                self.ok_streak.store(0, Ordering::Relaxed);
                self.back_off(now);
            }
        }
    }

    fn ramp_up(&self) {
        // Modular streak: concurrent batches trigger exactly one increase per full streak.
        let prev = self
            .ok_streak
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |s| {
                Some((s + 1) % Self::RAMP_UP_BATCHES)
            })
            .expect("closure always returns Some");
        if prev + 1 != Self::RAMP_UP_BATCHES {
            return;
        }
        let raised = self
            .current
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                (c < Self::MAX).then_some(c + 1)
            });
        if let Ok(before) = raised {
            info!(
                "Vacuum temporary files: raising directory concurrency {} -> {}",
                before,
                before + 1
            );
        }
    }

    fn back_off(&self, now: Instant) {
        let now_ms = now.saturating_duration_since(self.epoch).as_millis() as u64 + 1;
        let last = self.last_decrease_ms.load(Ordering::Relaxed);
        if last != 0 && now_ms.saturating_sub(last) < Self::DECREASE_COOLDOWN.as_millis() as u64 {
            return;
        }
        // CAS on the timestamp picks a single winner among concurrent reporters.
        if self
            .last_decrease_ms
            .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let lowered = self
            .current
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                (c > Self::MIN).then_some((c / 2).max(Self::MIN))
            });
        if let Ok(before) = lowered {
            warn!(
                "Vacuum temporary files: deletes are rate limited, lowering directory concurrency {} -> {}",
                before,
                (before / 2).max(Self::MIN)
            );
        }
    }
}

/// Shared progress of one vacuum run.
struct VacuumProgress {
    removed: AtomicUsize,
    dirs: DirConcurrency,
    /// Reported as one row per removed file, so `system.processes` shows the running command.
    sink: Option<Arc<Progress>>,
}

impl VacuumProgress {
    fn new(sink: Option<Arc<Progress>>) -> Self {
        Self {
            removed: AtomicUsize::new(0),
            dirs: DirConcurrency::new(Instant::now()),
            sink,
        }
    }

    fn removed(&self) -> usize {
        self.removed.load(Ordering::Relaxed)
    }

    fn add_removed(&self, n: usize) -> usize {
        if let Some(sink) = &self.sink {
            sink.incr(&ProgressValues { rows: n, bytes: 0 });
        }
        self.removed.fetch_add(n, Ordering::Relaxed) + n
    }

    fn concurrency(&self) -> usize {
        self.dirs.current()
    }
}

/// `retried`: `RateLimited` retries recorded process-wide while the batch was in flight (the
/// retry layer hides `SlowDown`, so a successful batch may still have been throttled).
fn classify_delete(
    result: &opendal::Result<()>,
    retried: usize,
    in_flight: usize,
) -> ThrottleSignal {
    let rate_limited = matches!(result, Err(e) if e.kind() == ErrorKind::RateLimited);
    if rate_limited || retried >= in_flight.div_ceil(2) {
        ThrottleSignal::Heavy
    } else if retried > 0 {
        ThrottleSignal::Light
    } else {
        ThrottleSignal::Clear
    }
}

/// Delete `paths`, feed the concurrency controller, and return how many files were removed.
/// A batch that still fails after retries counts as `0` and is left for the next run.
async fn delete_counted(
    operator: &Operator,
    paths: Vec<String>,
    from: &str,
    progress: &VacuumProgress,
) -> usize {
    let n = paths.len();
    let in_flight = progress.concurrency();
    let retries_before = opendal_rate_limited_retries();
    let result = operator.delete_iter(paths).await;
    let retried = opendal_rate_limited_retries().saturating_sub(retries_before) as usize;
    progress
        .dirs
        .observe(classify_delete(&result, retried, in_flight), Instant::now());
    match result {
        Ok(()) => n,
        Err(e) => {
            warn!(
                "Vacuum temporary files: failed to delete {} files from '{}', will retry in the next run: {}",
                n, from, e
            );
            0
        }
    }
}

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
        VacuumTempOptions::VacuumCommand(duration, progress) => {
            vacuum_by_duration(
                abort_checker,
                &temporary_dir,
                limit,
                duration,
                progress.clone(),
            )
            .await
        }
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
/// - Prefer the directory's own `last_modified` when available.
/// - Otherwise, use the paired spill meta file that ends with `.list`, because it is a better
///   signal for spill activity on remote storage.
/// - If the meta file is also unavailable, probe only the first file in the directory and use its
///   `last_modified` as a conservative fallback.
///
/// Cost control:
/// - `list` on remote storage is expensive, so the extra probe is only used in the final fallback.
/// - If probing decides the directory is expired, the same lister is reused to finish deletion so
///   we don't pay for an additional probe list.
///
/// Each spill directory is vacuumed as an independent task, bounded by [`DirConcurrency`].
/// `limit` is soft: in-flight directories finish, so the total may exceed it by up to
/// `DirConcurrency::MAX` directories.
async fn vacuum_by_duration(
    abort_checker: AbortChecker,
    temporary_dir: &str,
    limit: usize,
    duration: &Option<Duration>,
    sink: Arc<Progress>,
) -> Result<usize> {
    let operator = DataOperator::instance().spill_operator();
    let start_time = Instant::now();

    let expire_time = duration.unwrap_or(DEFAULT_RETAIN_DURATION).as_millis() as i64;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let progress = Arc::new(VacuumProgress::new(Some(sink)));
    let gc_metas: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
    let temporary_dir = format!("{}/", temporary_dir.trim_end_matches('/'));
    let mut ds = operator.lister_with(&temporary_dir).await?;

    let mut temp_files = Vec::new();
    let mut in_flight = FuturesUnordered::new();

    loop {
        if progress.removed() >= limit {
            break;
        }

        // Bound in-flight directory tasks by the adaptive limit.
        while in_flight.len() >= progress.concurrency() {
            if let Some(res) = in_flight.next().await {
                res?;
            }
        }

        let de = match ds.try_next().await {
            Ok(Some(de)) => de,
            Ok(None) => break,
            Err(e) if e.kind() == ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        abort_checker.try_check_aborting()?;
        if de.path() == temporary_dir {
            continue;
        }

        let remaining = limit.saturating_sub(progress.removed());
        let name = de.name();
        if de.metadata().is_file() {
            let Some(last_modified) =
                resolve_file_last_modified(&operator, de.path(), de.metadata()).await
            else {
                continue;
            };

            if timestamp - last_modified < expire_time {
                continue;
            }

            if name.ends_with(SPILL_META_SUFFIX) {
                if gc_metas.lock().contains(name) {
                    continue;
                }
                vacuum_by_meta_with_operator(
                    &operator,
                    &temporary_dir,
                    de.path(),
                    remaining,
                    &progress,
                )
                .await?;
                gc_metas.lock().insert(name.to_owned());
            } else {
                temp_files.push(de.path().to_owned());
                if temp_files.len() >= remaining {
                    break;
                }
            }
        } else {
            in_flight.push(vacuum_dir_entry(
                operator.clone(),
                temporary_dir.clone(),
                de.path().to_owned(),
                name.to_owned(),
                de.metadata().clone(),
                timestamp,
                expire_time,
                limit,
                progress.clone(),
                gc_metas.clone(),
            ));
        }
    }

    // Drain the directories that are still in flight.
    while let Some(res) = in_flight.next().await {
        res?;
    }

    let remaining = limit.saturating_sub(progress.removed());
    if !temp_files.is_empty() && temp_files.len() <= remaining {
        let removed = delete_counted(&operator, temp_files, &temporary_dir, &progress).await;
        progress.add_removed(removed);
    }

    let removed_total = progress.removed();
    // Log for the final total progress
    info!(
        "vacuum command finished, total cleaned {} files, final directory concurrency: {}, total elapsed: {} seconds",
        removed_total,
        progress.concurrency(),
        start_time.elapsed().as_secs()
    );
    Ok(removed_total)
}

/// Vacuum one spill directory found under the temporary dir.
///
/// This is the per-directory unit of work that runs concurrently in `vacuum_by_duration`.
#[allow(clippy::too_many_arguments)]
async fn vacuum_dir_entry(
    operator: Operator,
    temporary_dir: String,
    dir_path: String,
    name: String,
    metadata: opendal::Metadata,
    timestamp: i64,
    expire_time: i64,
    limit: usize,
    progress: Arc<VacuumProgress>,
    gc_metas: Arc<Mutex<HashSet<String>>>,
) -> Result<()> {
    let removed_so_far = progress.removed();
    if removed_so_far >= limit {
        return Ok(());
    }
    let remaining = limit - removed_so_far;

    let meta_path = format!("{}{}", dir_path.trim_end_matches('/'), SPILL_META_SUFFIX);
    let meta_name = format!("{}{}", name, SPILL_META_SUFFIX);

    match resolve_dir_last_modified(&operator, &metadata, &meta_path).await {
        Some(last_modified) => {
            if timestamp - last_modified < expire_time {
                return Ok(());
            }

            let removed = vacuum_by_meta_with_operator(
                &operator,
                &temporary_dir,
                &meta_path,
                remaining,
                &progress,
            )
            .await?;
            if removed > 0 {
                gc_metas.lock().insert(meta_name);
            } else {
                vacuum_by_list_dir_with_operator(&operator, &dir_path, remaining, &progress)
                    .await?;
            }
        }
        None => {
            let removed = vacuum_dir_with_probe(
                &operator,
                &dir_path,
                timestamp,
                expire_time,
                remaining,
                &progress,
            )
            .await?;
            if removed > 0 {
                gc_metas.lock().insert(meta_name);
            }
        }
    }

    Ok(())
}

/// Read `last_modified` for a single object path via `stat`.
async fn stat_last_modified(operator: &Operator, path: &str) -> Option<i64> {
    operator.stat(path).await.ok().and_then(|meta| {
        meta.last_modified()
            .map(|v| v.into_inner().as_millisecond())
    })
}

/// Resolve a file's `last_modified`, preferring list metadata and falling back to `stat`.
async fn resolve_file_last_modified(
    operator: &Operator,
    path: &str,
    metadata: &opendal::Metadata,
) -> Option<i64> {
    if let Some(last_modified) = metadata.last_modified() {
        return Some(last_modified.into_inner().as_millisecond());
    }

    if let Some(last_modified) = stat_last_modified(operator, path).await {
        return Some(last_modified);
    }

    None
}

/// Resolve a directory's `last_modified` from the listing metadata first, then from its `.list`
/// file.
///
/// The directory path itself is deliberately not `stat`ed. For object stores without a native
/// `create_dir` capability (e.g. S3), opendal's `CompleteLayer` answers `stat("dir/")` with a
/// `list(limit = 1)` and returns a `DIR` metadata that carries no `last_modified`, so the call
/// can never resolve a timestamp and only costs one extra LIST request per directory. Backends
/// that do have directory mtimes (e.g. local fs) fall through to the probe path below, which
/// reads the first file's `last_modified` and yields the same decision.
async fn resolve_dir_last_modified(
    operator: &Operator,
    metadata: &opendal::Metadata,
    meta_path: &str,
) -> Option<i64> {
    if let Some(last_modified) = metadata.last_modified() {
        return Some(last_modified.into_inner().as_millisecond());
    }

    stat_last_modified(operator, meta_path).await
}

/// Probe only the first file in a directory to decide whether the whole directory can be vacuumed.
/// If it is expired, reuse the same lister to finish deletion and avoid an extra probe list.
async fn vacuum_dir_with_probe(
    operator: &Operator,
    dir_path: &str,
    timestamp: i64,
    expire_time: i64,
    limit: usize,
    progress: &VacuumProgress,
) -> Result<usize> {
    let start_time = Instant::now();
    let mut lister = match operator.lister_with(dir_path).recursive(true).await {
        Ok(lister) => lister,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e.into()),
    };
    let mut paths = Vec::new();

    // Probe a file to check last_modified
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
        first_entry_last_modified = if let Some(last_modified) = entry.metadata().last_modified() {
            Some(last_modified.into_inner().as_millisecond())
        } else {
            stat_last_modified(operator, entry.path()).await
        };
        break;
    }

    let Some(first_entry_last_modified) = first_entry_last_modified else {
        return Ok(0);
    };

    if timestamp - first_entry_last_modified < expire_time {
        return Ok(0);
    }

    // delete by lister
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

    paths.truncate(limit);
    let cur_removed = delete_counted(operator, paths, dir_path, progress).await;

    let total = progress.add_removed(cur_removed);
    info!(
        "Vacuum temporary files progress(by probed dir): Total removed: {}, Current batch: {} (from '{}'), Concurrency: {}, Time: {} sec",
        total,
        cur_removed,
        dir_path,
        progress.concurrency(),
        start_time.elapsed().as_secs(),
    );

    Ok(cur_removed)
}

// Vacuum temporary files by query hook
// If query was killed, we still need to clean up the temporary files
async fn vacuum_query_hook(
    _abort_checker: AbortChecker,
    temporary_dir: &str,
    nodes: &[usize],
    query_id: &str,
    mut limit: usize,
) -> Result<usize> {
    let progress = VacuumProgress::new(None);
    let metas_f = nodes
        .iter()
        .map(|i| async move {
            let operator = DataOperator::instance().spill_operator();
            let meta_file_path =
                format!("{}/{}_{}{}", temporary_dir, query_id, i, SPILL_META_SUFFIX);
            let buffer = operator.read(&meta_file_path).await?;
            std::result::Result::<(String, Buffer), opendal::Error>::Ok((meta_file_path, buffer))
        })
        .collect::<Vec<_>>();

    let metas = futures_util::future::join_all(metas_f)
        .await
        .into_iter()
        .filter_map(|x| x.is_ok().then(|| x.unwrap()));

    for (meta_file_path, buffer) in metas {
        let operator = DataOperator::instance().spill_operator();
        let removed = vacuum_by_meta_buffer_with_operator(
            &operator,
            &meta_file_path,
            temporary_dir,
            buffer,
            limit,
            &progress,
        )
        .await?;
        limit = limit.saturating_sub(removed);
    }

    Ok(progress.removed())
}

/// Delete temporary files recorded in a spill meta buffer with the provided operator.
async fn vacuum_by_meta_buffer_with_operator(
    operator: &Operator,
    meta_file_path: &str,
    temporary_dir: &str,
    meta: Buffer,
    limit: usize,
    progress: &VacuumProgress,
) -> Result<usize> {
    let start_time = Instant::now();
    let meta = meta.to_bytes();
    let files: Vec<String> = meta.lines().map(|x| Ok(x?)).collect::<Result<Vec<_>>>()?;

    let (to_be_removed, remain) = files.split_at(limit.min(files.len()));
    let remain = remain.to_vec();

    let to_be_removed: Vec<String> = to_be_removed
        .iter()
        .filter(|f| f.starts_with(temporary_dir))
        .cloned()
        .collect();
    let attempted = to_be_removed.len();
    let cur_removed = delete_counted(operator, to_be_removed, meta_file_path, progress).await;

    // Update the unfinished meta file. If the batch failed, keep the meta file untouched so the
    // next run retries the same files instead of leaving them as orphans.
    if attempted > 0 && cur_removed == 0 {
        return Ok(0);
    }
    if !remain.is_empty() {
        let remain = remain.join("\n");
        operator.write(meta_file_path, remain).await?;
    }

    let total = progress.add_removed(cur_removed);
    // Log for the current batch
    info!(
        "Vacuum temporary files progress(by meta file): Total removed: {}, Current batch: {} (from '{}'), Dir: '{}', Concurrency: {}, Time: {} sec",
        total,
        cur_removed,
        meta_file_path,
        temporary_dir,
        progress.concurrency(),
        start_time.elapsed().as_secs(),
    );

    Ok(cur_removed)
}

/// Delete temporary files through a spill meta file with the provided operator.
async fn vacuum_by_meta_with_operator(
    operator: &Operator,
    temporary_dir: &str,
    meta_file_path: &str,
    limit: usize,
    progress: &VacuumProgress,
) -> Result<usize> {
    let meta: Buffer;
    let r = operator.read(meta_file_path).await;
    match r {
        Ok(r) => meta = r,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e.into()),
    }
    vacuum_by_meta_buffer_with_operator(
        operator,
        meta_file_path,
        temporary_dir,
        meta,
        limit,
        progress,
    )
    .await
}

/// Delete all files under a temporary directory with the provided operator.
async fn vacuum_by_list_dir_with_operator(
    operator: &Operator,
    dir_path: &str,
    limit: usize,
    progress: &VacuumProgress,
) -> Result<usize> {
    let start_time = Instant::now();
    let mut r = operator.lister_with(dir_path).recursive(true).await?;
    let mut batches = vec![];

    while let Some(entry) = r.try_next().await? {
        // Let's remove it at last
        if entry.path() == dir_path {
            continue;
        }
        let path = entry.path().to_string();
        batches.push(path);
    }
    batches.push(dir_path.to_owned());

    batches.truncate(limit);
    let cur_removed = delete_counted(operator, batches, dir_path, progress).await;

    let total = progress.add_removed(cur_removed);
    // Log progress for the current batch
    info!(
        "Vacuum temporary files progress(by list dir): Total removed: {}, Current batch: {} (from '{}'), Concurrency: {}, Time: {} sec",
        total,
        cur_removed,
        dir_path,
        progress.concurrency(),
        start_time.elapsed().as_secs(),
    );

    Ok(cur_removed)
}
