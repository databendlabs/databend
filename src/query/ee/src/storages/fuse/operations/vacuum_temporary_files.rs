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
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use databend_common_catalog::table_context::AbortChecker;
use databend_common_exception::Result;
use databend_common_storage::DataOperator;
use databend_enterprise_vacuum_handler::vacuum_handler::VacuumTempOptions;
use futures_util::TryStreamExt;
use log::info;
use opendal::Buffer;
use opendal::ErrorKind;
use opendal::Operator;

// Default retention duration for temporary files: 3 days.
const DEFAULT_RETAIN_DURATION: Duration = Duration::from_secs(60 * 60 * 24 * 3);
const SPILL_META_SUFFIX: &str = ".list";

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
        VacuumTempOptions::VacuumCommand(duration) => {
            vacuum_by_duration(abort_checker, &temporary_dir, limit, duration).await
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
async fn vacuum_by_duration(
    abort_checker: AbortChecker,
    temporary_dir: &str,
    mut limit: usize,
    duration: &Option<Duration>,
) -> Result<usize> {
    let operator = DataOperator::instance().spill_operator();
    let start_time = Instant::now();

    let expire_time = duration.unwrap_or(DEFAULT_RETAIN_DURATION).as_millis() as i64;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let mut removed_total = 0;
    let temporary_dir = format!("{}/", temporary_dir.trim_end_matches('/'));
    let mut ds = operator.lister_with(&temporary_dir).await?;

    let mut temp_files = Vec::new();
    let mut gc_metas = HashSet::new();

    loop {
        let de = ds.try_next().await;
        match de {
            Ok(Some(de)) => {
                abort_checker.try_check_aborting()?;
                if de.path() == temporary_dir {
                    continue;
                }
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
                        if gc_metas.contains(name) {
                            continue;
                        }
                        let removed = vacuum_by_meta_with_operator(
                            &operator,
                            &temporary_dir,
                            de.path(),
                            limit,
                            &mut removed_total,
                        )
                        .await?;
                        limit = limit.saturating_sub(removed);
                        gc_metas.insert(name.to_owned());
                    } else {
                        temp_files.push(de.path().to_owned());
                        if temp_files.len() >= limit {
                            break;
                        }
                    }
                } else {
                    let meta_path =
                        format!("{}{}", de.path().trim_end_matches('/'), SPILL_META_SUFFIX);
                    if let Some(last_modified) =
                        resolve_dir_last_modified(&operator, de.path(), de.metadata(), &meta_path)
                            .await
                    {
                        if timestamp - last_modified < expire_time {
                            continue;
                        }

                        let removed = vacuum_by_meta_with_operator(
                            &operator,
                            &temporary_dir,
                            &meta_path,
                            limit,
                            &mut removed_total,
                        )
                        .await?;
                        if removed > 0 {
                            let meta_name = format!("{}{}", name, SPILL_META_SUFFIX);
                            if gc_metas.contains(&meta_name) {
                                continue;
                            }

                            limit = limit.saturating_sub(removed);
                            gc_metas.insert(meta_name);
                        } else {
                            let removed = vacuum_by_list_dir_with_operator(
                                &operator,
                                de.path(),
                                limit,
                                &mut removed_total,
                            )
                            .await?;
                            limit = limit.saturating_sub(removed);
                        }
                    } else {
                        let removed = vacuum_dir_with_probe(
                            &operator,
                            de.path(),
                            timestamp,
                            expire_time,
                            limit,
                            &mut removed_total,
                        )
                        .await?;
                        limit = limit.saturating_sub(removed);
                        if removed > 0 {
                            gc_metas.insert(format!("{}{}", name, SPILL_META_SUFFIX));
                        }
                    }
                }
                if limit == 0 {
                    break;
                }
            }
            Ok(None) => break,
            Err(e) if e.kind() == ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        }
    }

    if temp_files.len() <= limit {
        removed_total += temp_files.len();
        let _ = operator.delete_iter(temp_files).await;
    }

    // Log for the final total progress
    info!(
        "vacuum command finished, total cleaned {} files, total elapsed: {} seconds",
        removed_total,
        start_time.elapsed().as_secs()
    );
    Ok(removed_total)
}

/// Read `last_modified` for a single object path via `stat`.
async fn stat_last_modified(operator: &Operator, path: &str) -> Option<i64> {
    operator
        .stat(path)
        .await
        .ok()
        .and_then(|meta| meta.last_modified().map(|v| v.timestamp_millis()))
}

/// Resolve a file's `last_modified`, preferring list metadata and falling back to `stat`.
async fn resolve_file_last_modified(
    operator: &Operator,
    path: &str,
    metadata: &opendal::Metadata,
) -> Option<i64> {
    if let Some(last_modified) = metadata.last_modified() {
        return Some(last_modified.timestamp_millis());
    }

    if let Some(last_modified) = stat_last_modified(operator, path).await {
        return Some(last_modified);
    }

    None
}

/// Resolve a directory's `last_modified` from the dir itself first, then from its `.list` file.
async fn resolve_dir_last_modified(
    operator: &Operator,
    dir_path: &str,
    metadata: &opendal::Metadata,
    meta_path: &str,
) -> Option<i64> {
    if let Some(last_modified) = metadata.last_modified() {
        return Some(last_modified.timestamp_millis());
    }

    if let Some(last_modified) = stat_last_modified(operator, dir_path).await {
        return Some(last_modified);
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
    removed_total: &mut usize,
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
            Some(last_modified.timestamp_millis())
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

    let cur_removed = paths.len().min(limit);
    let _ = operator.delete_iter(paths.into_iter().take(limit)).await;

    *removed_total += cur_removed;
    info!(
        "Vacuum temporary files progress(by probed dir): Total removed: {}, Current batch: {} (from '{}'), Time: {} sec",
        *removed_total,
        cur_removed,
        dir_path,
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
    let mut removed_total = 0;
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
            &mut removed_total,
        )
        .await?;
        limit = limit.saturating_sub(removed);
    }

    Ok(removed_total)
}

/// Delete temporary files recorded in a spill meta buffer with the provided operator.
async fn vacuum_by_meta_buffer_with_operator(
    operator: &Operator,
    meta_file_path: &str,
    temporary_dir: &str,
    meta: Buffer,
    limit: usize,
    removed_total: &mut usize,
) -> Result<usize> {
    let start_time = Instant::now();
    let meta = meta.to_bytes();
    let files: Vec<String> = meta.lines().map(|x| Ok(x?)).collect::<Result<Vec<_>>>()?;

    let (to_be_removed, remain) = files.split_at(limit.min(files.len()));
    let remain = remain.to_vec();

    let cur_removed = to_be_removed.len();
    let _ = operator
        .delete_iter(
            files
                .into_iter()
                .filter(|f| f.starts_with(temporary_dir))
                .take(limit),
        )
        .await;

    // update unfinished meta file
    if !remain.is_empty() {
        let remain = remain.join("\n");
        operator.write(meta_file_path, remain).await?;
    }

    *removed_total += cur_removed;
    // Log for the current batch
    info!(
        "Vacuum temporary files progress(by meta file): Total removed: {}, Current batch: {} (from '{}'), Dir: '{}', Time: {} sec",
        *removed_total,
        cur_removed,
        meta_file_path,
        temporary_dir,
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
    removed_total: &mut usize,
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
        removed_total,
    )
    .await
}

/// Delete all files under a temporary directory with the provided operator.
async fn vacuum_by_list_dir_with_operator(
    operator: &Operator,
    dir_path: &str,
    limit: usize,
    removed_total: &mut usize,
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

    let cur_removed = batches.len().min(limit);
    let _ = operator.delete_iter(batches.into_iter().take(limit)).await;

    *removed_total += cur_removed;
    // Log progress for the current batch
    info!(
        "Vacuum temporary files progress(by list dir): Total removed: {}, Current batch: {} (from '{}'), Time: {} sec",
        *removed_total,
        cur_removed,
        dir_path,
        start_time.elapsed().as_secs(),
    );

    Ok(cur_removed)
}
