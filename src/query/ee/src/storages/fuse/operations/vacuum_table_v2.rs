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

// Logs from this module will show up as "[FUSE-VACUUM2] ...".
databend_common_tracing::register_module_tag!("[FUSE-VACUUM2]");

use std::collections::HashSet;
use std::sync::Arc;

use chrono::DateTime;
use chrono::TimeDelta;
use chrono::Utc;
use databend_common_base::base::uuid::Uuid;
use databend_common_base::base::uuid::Version;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::LeastVisibleTime;
use databend_common_meta_app::schema::ListIndexesByIdReq;
use databend_common_meta_app::schema::SnapshotRefType;
use databend_common_meta_app::schema::least_visible_time_ident::LeastVisibleTimeIdent;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::RetentionPolicy;
use databend_common_storages_fuse::io::SegmentsIO;
use databend_common_storages_fuse::io::SnapshotsIO;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::operations::ASSUMPTION_MAX_TXN_DURATION;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_io::Files;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::VACUUM2_OBJECT_KEY_PREFIX;
use log::info;
use opendal::Entry;
use opendal::ErrorKind;

#[async_backtrace::framed]
pub async fn do_vacuum2(
    table: &dyn Table,
    ctx: Arc<dyn TableContext>,
    respect_flash_back: bool,
) -> Result<Vec<String>> {
    let table_info = table.get_table_info();
    {
        if ctx.txn_mgr().lock().is_active() {
            info!(
                "Transaction is active, skipping vacuum, target table {}",
                table_info.desc
            );
            return Ok(vec![]);
        }
    }

    let fuse_table = FuseTable::try_from_table(table)?;

    let Some(latest_snapshot) = fuse_table.read_table_snapshot().await? else {
        info!("Table {} has no snapshot, stopping vacuum", table_info.desc);
        return Ok(vec![]);
    };

    // Process snapshot refs (branches and tags) before main vacuum
    let ref_info = process_snapshot_refs(fuse_table, &ctx).await?;

    let start = std::time::Instant::now();
    let retention_policy = fuse_table.get_data_retention_policy(ctx.as_ref())?;

    // By default, do not vacuum all the historical snapshots.
    let mut is_vacuum_all = false;
    let mut respect_flash_back_with_lvt = None;

    let snapshots_before_lvt = match retention_policy {
        RetentionPolicy::ByTimePeriod(delta_duration) => {
            info!("Using ByTimePeriod policy {:?}", delta_duration);
            let retention_period = if fuse_table.is_transient() {
                // For transient table, keep no history data
                TimeDelta::zero()
            } else {
                delta_duration
            };

            // A zero retention period indicates that we should vacuum all the historical snapshots
            is_vacuum_all = retention_period.is_zero();

            let Some(lvt) =
                set_lvt(fuse_table, latest_snapshot, ctx.as_ref(), retention_period).await?
            else {
                return Ok(vec![]);
            };

            if respect_flash_back {
                respect_flash_back_with_lvt = Some(lvt);
            }

            ctx.set_status_info(&format!(
                "Set LVT for table {}, elapsed: {:?}, LVT: {:?}",
                table_info.desc,
                start.elapsed(),
                lvt
            ));

            let snapshots_before_lvt =
                collect_gc_candidates_by_retention_period(fuse_table, lvt, is_vacuum_all).await?;
            snapshots_before_lvt
        }
        RetentionPolicy::ByNumOfSnapshotsToKeep(num_snapshots_to_keep) => {
            info!(
                "Using ByNumOfSnapshotsToKeep policy {:?}",
                num_snapshots_to_keep
            );
            // List the snapshot order by timestamp asc, till the current snapshot(inclusively).
            let need_one_more = true;
            let mut snapshots = fuse_table
                .list_files_until_prefix(
                    fuse_table
                        .meta_location_generator()
                        .snapshot_location_prefix(),
                    // Safe to unwrap here: we have checked that `fuse_table` has a snapshot
                    fuse_table.snapshot_loc().unwrap().as_str(),
                    need_one_more,
                    None,
                )
                .await?;

            let len = snapshots.len();
            if len <= num_snapshots_to_keep {
                // Only the current snapshot is there, done
                return Ok(vec![]);
            }
            if num_snapshots_to_keep == 1 {
                // Expecting only one snapshot left, which means that we can use the current snapshot
                // as gc root, this flag will be propagated to the select_gc_root func later.
                is_vacuum_all = true;
            }

            // When selecting the GC root later, the last snapshot in `snapshots` (after truncation)
            // is the candidate, but its commit status is uncertain, so its previous snapshot is used
            // as the GC root instead (except in the is_vacuum_all case).

            // Therefore, during snapshot truncation, we keep 2 extra snapshots;
            // see `select_gc_root` for details.
            let num_candidates = len - num_snapshots_to_keep + 2;
            snapshots.truncate(num_candidates);
            snapshots
        }
    };

    let elapsed = start.elapsed();
    ctx.set_status_info(&format!(
        "Listed snapshots for table {}, elapsed: {:?}, snapshots_dir: {:?}, snapshots: {:?}",
        table_info.desc,
        elapsed,
        fuse_table
            .meta_location_generator()
            .snapshot_location_prefix(),
        slice_summary(&snapshots_before_lvt)
    ));

    let Some((gc_root, snapshots_to_gc, mut gc_root_meta_ts)) = select_gc_root(
        &ctx,
        fuse_table,
        &snapshots_before_lvt,
        is_vacuum_all,
        respect_flash_back_with_lvt,
    )
    .await?
    else {
        return Ok(vec![]);
    };

    // Use the oldest gc_root_meta_ts between main branch and refs
    if let Some(ref_ts) = ref_info.gc_root_meta_ts {
        gc_root_meta_ts = gc_root_meta_ts.min(ref_ts);
    }
    let gc_root_timestamp = ref_info
        .gc_root_timestamp
        .into_iter()
        .chain(gc_root.timestamp)
        .min()
        .unwrap();
    ctx.set_status_info(&format!(
        "Selected gc_root for table {}, elapsed: {:?}, gc_root: {:?}, snapshots_to_gc: {:?}",
        table_info.desc,
        start.elapsed(),
        gc_root,
        slice_summary(&snapshots_to_gc)
    ));

    let start = std::time::Instant::now();
    let gc_root_segments: HashSet<_> = gc_root
        .segments
        .iter()
        .chain(ref_info.ref_gc_roots.iter().flat_map(|r| r.segments.iter()))
        .map(|(path, _)| path.clone())
        .collect();
    let segments_before_gc_root = fuse_table
        .list_files_until_timestamp(
            fuse_table
                .meta_location_generator()
                .segment_location_prefix(),
            gc_root_timestamp,
            false,
            Some(gc_root_meta_ts),
        )
        .await?
        .into_iter()
        .map(|v| v.path().to_owned())
        .collect::<Vec<_>>();

    ctx.set_status_info(&format!(
        "Listed segments before gc_root for table {}, elapsed: {:?}, segment_dir: {:?}, gc_root_timestamp: {:?}, segments: {:?}",
        table_info.desc,
        start.elapsed(),
        fuse_table.meta_location_generator().segment_location_prefix(),
        gc_root_timestamp,
        slice_summary(&segments_before_gc_root)
    ));

    let start = std::time::Instant::now();
    let segments_to_gc: Vec<String> = segments_before_gc_root
        .into_iter()
        .filter(|s| !gc_root_segments.contains(s))
        .collect();
    let stats_to_gc = segments_to_gc
        .iter()
        .map(|v| TableMetaLocationGenerator::gen_segment_stats_location_from_segment_location(v))
        .collect::<Vec<_>>();
    ctx.set_status_info(&format!(
        "Filtered segments_to_gc for table {}, elapsed: {:?}, segments_to_gc: {:?}, stats_to_gc: {:?}",
        table_info.desc,
        start.elapsed(),
        slice_summary(&segments_to_gc),
        slice_summary(&stats_to_gc)
    ));

    let start = std::time::Instant::now();
    let segments_io =
        SegmentsIO::create(ctx.clone(), fuse_table.get_operator(), fuse_table.schema());

    // Collect blocks from main gc_root
    let segments = segments_io
        .read_segments::<Arc<CompactSegmentInfo>>(&gc_root.segments, false)
        .await?;
    let mut gc_root_blocks = HashSet::new();
    for segment in segments {
        gc_root_blocks.extend(segment?.block_metas()?.iter().map(|b| b.location.0.clone()));
    }

    // Collect blocks from ref gc_roots
    for ref_gc_root in ref_info.ref_gc_roots {
        let ref_segments = segments_io
            .read_segments::<Arc<CompactSegmentInfo>>(&ref_gc_root.segments, false)
            .await?;
        for segment in ref_segments {
            gc_root_blocks.extend(segment?.block_metas()?.iter().map(|b| b.location.0.clone()));
        }
    }

    ctx.set_status_info(&format!(
        "Read segments for table {}, elapsed: {:?}, total protected blocks: {}",
        table_info.desc,
        start.elapsed(),
        gc_root_blocks.len()
    ));

    let start = std::time::Instant::now();
    let blocks_before_gc_root = fuse_table
        .list_files_until_timestamp(
            fuse_table.meta_location_generator().block_location_prefix(),
            gc_root_timestamp,
            false,
            Some(gc_root_meta_ts),
        )
        .await?
        .into_iter()
        .map(|v| v.path().to_owned())
        .collect::<Vec<_>>();

    ctx.set_status_info(&format!(
        "Listed blocks before gc_root for table {}, elapsed: {:?}, block_dir: {:?}, gc_root_timestamp: {:?}, blocks: {:?}",
        table_info.desc,
        start.elapsed(),
        fuse_table.meta_location_generator().block_location_prefix(),
        gc_root_timestamp,
        slice_summary(&blocks_before_gc_root)
    ));

    let start = std::time::Instant::now();
    let blocks_to_gc: Vec<String> = blocks_before_gc_root
        .into_iter()
        .filter(|b| !gc_root_blocks.contains(b))
        .collect();
    ctx.set_status_info(&format!(
        "Filtered blocks_to_gc for table {}, elapsed: {:?}, blocks_to_gc: {:?}",
        table_info.desc,
        start.elapsed(),
        slice_summary(&blocks_to_gc)
    ));

    let start = std::time::Instant::now();
    let catalog = ctx.get_default_catalog()?;
    let table_agg_index_ids = catalog
        .list_index_ids_by_table_id(ListIndexesByIdReq::new(
            ctx.get_tenant(),
            fuse_table.get_id(),
        ))
        .await?;
    let inverted_indexes = &table_info.meta.indexes;
    let mut indexes_to_gc = Vec::with_capacity(
        blocks_to_gc.len() * (table_agg_index_ids.len() + inverted_indexes.len() + 1),
    );
    for loc in &blocks_to_gc {
        for index_id in &table_agg_index_ids {
            indexes_to_gc.push(
                TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                    loc, *index_id,
                ),
            );
        }
        for idx in inverted_indexes.values() {
            indexes_to_gc.push(
                TableMetaLocationGenerator::gen_inverted_index_location_from_block_location(
                    loc,
                    idx.name.as_str(),
                    idx.version.as_str(),
                ),
            );
        }
        indexes_to_gc
            .push(TableMetaLocationGenerator::gen_bloom_index_location_from_block_location(loc));
    }

    ctx.set_status_info(&format!(
        "Collected indexes_to_gc for table {}, elapsed: {:?}, indexes_to_gc: {:?}",
        table_info.desc,
        start.elapsed(),
        slice_summary(&indexes_to_gc)
    ));

    let start = std::time::Instant::now();
    let subject_files_to_gc: Vec<_> = segments_to_gc
        .into_iter()
        .chain(blocks_to_gc.into_iter())
        .chain(stats_to_gc.into_iter())
        .collect();
    let op = Files::create(ctx.clone(), fuse_table.get_operator());

    // order is important
    // indexes should be removed before blocks, because index locations to gc are generated from block locations
    // subject_files should be removed before snapshots, because gc of subject_files depend on gc root
    op.remove_file_in_batch(&indexes_to_gc).await?;
    op.remove_file_in_batch(&subject_files_to_gc).await?;

    // Evict snapshot caches from the local node.
    //
    // Note:
    // - Cached snapshots may also exist on other nodes in a multi-node cluster. If these remote
    //   caches are not synchronized, it could lead to incorrect results in operations like
    //   `fuse_snapshot(...)`. However, this does not compromise the safety of the table data.
    // - TODO: To ensure correctness in such cases, the table's Least Visible Timestamp (LVT),
    //   stored in the meta-server, should be utilized to determine snapshot visibility and
    //   resolve potential issues.
    if let Some(snapshot_cache) = CacheManager::instance().get_table_snapshot_cache() {
        for path in snapshots_to_gc.iter() {
            snapshot_cache.evict(path);
        }
    }
    let op = Files::create(ctx.clone(), fuse_table.get_operator());
    op.remove_file_in_batch(&snapshots_to_gc).await?;

    let files_to_gc: Vec<_> = subject_files_to_gc
        .into_iter()
        .chain(snapshots_to_gc.into_iter())
        .chain(indexes_to_gc.into_iter())
        .chain(ref_info.files_to_gc.into_iter())
        .collect();
    ctx.set_status_info(&format!(
        "Removed files for table {}, elapsed: {:?}, files_to_gc: {:?}",
        table_info.desc,
        start.elapsed(),
        slice_summary(&files_to_gc)
    ));
    Ok(files_to_gc)
}

async fn collect_gc_candidates_by_retention_period(
    fuse_table: &FuseTable,
    lvt: DateTime<Utc>,
    is_vacuum_all: bool,
) -> Result<Vec<Entry>> {
    let snapshots_before_lvt = if is_vacuum_all {
        fuse_table
            .list_files_until_prefix(
                fuse_table
                    .meta_location_generator()
                    .snapshot_location_prefix(),
                fuse_table.snapshot_loc().unwrap().as_str(),
                true,
                None,
            )
            .await?
    } else {
        fuse_table
            .list_files_until_timestamp(
                fuse_table
                    .meta_location_generator()
                    .snapshot_location_prefix(),
                lvt,
                true,
                None,
            )
            .await?
    };

    Ok(snapshots_before_lvt)
}

/// Try set lvt as min(latest_snapshot.timestamp, now - retention_time).
///
/// Return `None` means we stop vacuuming, but don't want to report error to user.
async fn set_lvt(
    fuse_table: &FuseTable,
    latest_snapshot: Arc<TableSnapshot>,
    ctx: &dyn TableContext,
    retention_period: TimeDelta,
) -> Result<Option<DateTime<Utc>>> {
    if !is_uuid_v7(&latest_snapshot.snapshot_id) {
        info!(
            "Latest snapshot is not v7, stopping vacuum: {:?}",
            latest_snapshot.snapshot_id
        );
        return Ok(None);
    }
    let cat = ctx.get_default_catalog()?;
    // safe to unwrap, as we have checked the version is v4
    let latest_ts = latest_snapshot.timestamp.unwrap();
    let lvt_point_candidate = std::cmp::min(Utc::now() - retention_period, latest_ts);

    let lvt_point = cat
        .set_table_lvt(
            &LeastVisibleTimeIdent::new(ctx.get_tenant(), fuse_table.get_id()),
            &LeastVisibleTime::new(lvt_point_candidate),
        )
        .await?
        .time;
    Ok(Some(lvt_point))
}

fn is_uuid_v7(uuid: &Uuid) -> bool {
    let version = uuid.get_version();
    version.is_some_and(|v| matches!(v, Version::SortRand))
}

async fn select_gc_root(
    ctx: &Arc<dyn TableContext>,
    fuse_table: &FuseTable,
    snapshots_before_lvt: &[Entry],
    is_vacuum_all: bool,
    respect_flash_back: Option<DateTime<Utc>>,
) -> Result<Option<(Arc<TableSnapshot>, Vec<String>, DateTime<Utc>)>> {
    let op = fuse_table.get_operator();
    let gc_root_path = if is_vacuum_all {
        // safe to unwrap, or we should have stopped vacuuming in set_lvt()
        fuse_table.snapshot_loc().unwrap()
    } else if let Some(lvt) = respect_flash_back {
        let latest_location = fuse_table.snapshot_loc().unwrap();
        let gc_root = fuse_table
            .find_location(ctx, latest_location, |snapshot| {
                snapshot.timestamp.is_some_and(|ts| ts <= lvt)
            })
            .await
            .ok();
        let Some(gc_root) = gc_root else {
            info!("no gc_root found, stop vacuuming");
            return Ok(None);
        };
        gc_root
    } else {
        if snapshots_before_lvt.is_empty() {
            info!("no snapshots before lvt, stop vacuuming");
            return Ok(None);
        }
        let (anchor, _) = SnapshotsIO::read_snapshot(
            snapshots_before_lvt.last().unwrap().path().to_owned(),
            op.clone(),
            false,
        )
        .await?;
        let Some((gc_root_id, gc_root_ver)) = anchor.prev_snapshot_id else {
            info!("anchor has no prev_snapshot_id, stop vacuuming");
            return Ok(None);
        };
        let gc_root_path = fuse_table
            .meta_location_generator()
            .snapshot_location_from_uuid(&gc_root_id, gc_root_ver)?;
        if !is_uuid_v7(&gc_root_id) {
            info!("gc_root {} is not v7", gc_root_path);
            return Ok(None);
        }
        gc_root_path
    };

    let dal = fuse_table.get_operator_ref();
    let gc_root = SnapshotsIO::read_snapshot(gc_root_path.clone(), op.clone(), false).await;

    let gc_root_meta_ts = match dal.stat(&gc_root_path).await {
        Ok(v) => v
            .last_modified()
            .ok_or_else(|| {
                ErrorCode::StorageOther(format!(
                    "Failed to get `last_modified` metadata of the gc root object '{}'",
                    gc_root_path
                ))
            })
            .map(|v| DateTime::from_timestamp_nanos(v.into_inner().as_nanosecond() as i64))?,
        Err(e) => {
            return if e.kind() == ErrorKind::NotFound {
                // Concurrent vacuum, ignore it
                Ok(None)
            } else {
                Err(e.into())
            };
        }
    };

    match gc_root {
        Ok((gc_root, _)) => {
            info!("gc_root found: {:?}", gc_root);
            let mut gc_candidates = Vec::with_capacity(snapshots_before_lvt.len());

            for snapshot in snapshots_before_lvt.iter() {
                let path = snapshot.path();
                let last_part = path.rsplit('/').next().unwrap();
                if last_part.starts_with(VACUUM2_OBJECT_KEY_PREFIX) {
                    gc_candidates.push(path.to_owned());
                } else {
                    // This snapshot is created by a node of the previous version which does not
                    // support vacuum2, we rely on the `ASSUMPTION_MAX_TXN_DURATION` to identify if
                    // it is available to be vacuumed.
                    let last_modified = match snapshot.metadata().last_modified() {
                        None => dal.stat(path).await?.last_modified().ok_or_else(|| {
                            ErrorCode::StorageOther(format!(
                                "Failed to get `last_modified` metadata of the snapshot object '{}'",
                                gc_root_path
                            ))
                        })?,
                        Some(v) => v
                    };

                    let last_modified = DateTime::from_timestamp_nanos(
                        last_modified.into_inner().as_nanosecond() as i64,
                    );

                    if last_modified + ASSUMPTION_MAX_TXN_DURATION < gc_root_meta_ts {
                        gc_candidates.push(path.to_owned());
                    }
                }
            }

            let gc_root_idx = gc_candidates.binary_search(&gc_root_path).map_err(|_| {
                ErrorCode::Internal(format!(
                    "gc root path {} should be one of the candidates, candidates: {:?}",
                    gc_root_path, gc_candidates
                ))
            })?;
            let snapshots_to_gc = gc_candidates[..gc_root_idx].to_vec();

            Ok(Some((gc_root, snapshots_to_gc, gc_root_meta_ts)))
        }
        Err(e) => {
            info!("read gc_root {} failed: {:?}", gc_root_path, e);
            Ok(None)
        }
    }
}

fn slice_summary<T: std::fmt::Debug>(s: &[T]) -> String {
    if s.len() > 10 {
        let first_five = &s[..5];
        let last_five = &s[s.len() - 5..];
        format!(
            "First five: {:?}, Last five: {:?},Len: {}",
            first_five,
            last_five,
            s.len()
        )
    } else {
        format!("{:?}", s)
    }
}

/// Result of vacuum processing for table refs (branches and tags)
struct RefVacuumInfo {
    /// GC root snapshots from refs (to protect their segments and blocks)
    ref_gc_roots: Vec<Arc<TableSnapshot>>,
    /// The oldest gc_root_meta_ts among all refs
    gc_root_meta_ts: Option<DateTime<Utc>>,
    /// The oldest gc_root_ts among all refs
    gc_root_timestamp: Option<DateTime<Utc>>,
    /// The files(include snapshot and expired refs' dir) to be cleaned up
    files_to_gc: Vec<String>,
}

/// Process snapshot refs (branches and tags) for vacuum
#[async_backtrace::framed]
async fn process_snapshot_refs(
    fuse_table: &FuseTable,
    ctx: &Arc<dyn TableContext>,
) -> Result<RefVacuumInfo> {
    let start = std::time::Instant::now();
    let op = fuse_table.get_operator();
    // Refs that expired and should be cleaned up
    let mut expired_refs = HashSet::new();
    // Ref snapshot paths to be cleaned up
    let mut ref_snapshots_to_gc = Vec::new();
    let mut ref_gc_roots = Vec::new();
    let mut gc_root_meta_ts: Option<DateTime<Utc>> = None;
    let mut gc_root_timestamp: Option<DateTime<Utc>> = None;
    let mut files_to_gc = Vec::new();

    let now = Utc::now();
    let (retention_time, num_snapshots_to_keep) =
        fuse_table.get_refs_retention_policy(ctx.as_ref(), now)?;
    let table_info = fuse_table.get_table_info();
    // Process active refs
    for (ref_name, snapshot_ref) in table_info.meta.refs.iter() {
        if snapshot_ref.expire_at.is_some_and(|v| v < now) {
            expired_refs.insert(ref_name);
            continue;
        }

        match snapshot_ref.typ {
            SnapshotRefType::Tag => {
                // Tag: read head snapshot as gc root to protect its segments and blocks
                let (tag_snapshot, _) =
                    SnapshotsIO::read_snapshot(snapshot_ref.loc.clone(), op.clone(), false).await?;
                ref_gc_roots.push(tag_snapshot);
            }
            SnapshotRefType::Branch => {
                let branch_id = snapshot_ref.id;
                let snapshots_before_retention = fuse_table
                    .list_branch_snapshots_with_fallback(
                        branch_id,
                        &snapshot_ref.loc,
                        retention_time,
                        num_snapshots_to_keep,
                    )
                    .await?;

                let (gc_root_location, gc_root_snap) = match process_branch_gc_root(
                    fuse_table,
                    branch_id,
                    &snapshot_ref.loc,
                    &snapshots_before_retention,
                    &mut ref_snapshots_to_gc,
                )
                .await?
                {
                    Some((location, sn)) => (location, sn),
                    None => {
                        // No gc_root found, use snapshot history to find earliest
                        let earliest_snap = fuse_table
                            .find_earliest_snapshot_via_history(ref_name, snapshot_ref)
                            .await?;
                        (snapshot_ref.loc.clone(), earliest_snap)
                    }
                };

                if snapshot_ref.loc != gc_root_location {
                    // Only collect snapshot timestamps when the GC root is NOT the head location.
                    // The head location serves as the current root and does not participate in
                    // the minimum timestamp calculation.
                    let meta = fuse_table
                        .get_operator_ref()
                        .stat(&gc_root_location)
                        .await?;

                    let meta_ts = meta
                        .last_modified()
                        .ok_or_else(|| {
                            ErrorCode::StorageOther(format!(
                                "Failed to get `last_modified` metadata of snapshot '{}'",
                                gc_root_location
                            ))
                        })
                        .map(|v| {
                            DateTime::from_timestamp_nanos(v.into_inner().as_nanosecond() as i64)
                        })?;

                    // Update minimum metadata timestamp
                    gc_root_meta_ts = Some(gc_root_meta_ts.map_or(meta_ts, |cur| cur.min(meta_ts)));

                    // Update minimum snapshot timestamp (keep original unwrap logic)
                    let gc_root_ts = gc_root_snap.timestamp.unwrap();
                    gc_root_timestamp =
                        Some(gc_root_timestamp.map_or(gc_root_ts, |cur| cur.min(gc_root_ts)));
                }
                ref_gc_roots.push(gc_root_snap);
            }
        }
    }

    if !expired_refs.is_empty() {
        let start_update = std::time::Instant::now();
        files_to_gc = fuse_table
            .update_table_refs_meta(ctx, &expired_refs)
            .await?;
        ctx.set_status_info(&format!(
            "Updated table meta for table {}, elapsed: {:?}",
            table_info.desc,
            start_update.elapsed()
        ));
    }

    // Step 3: Purge ref snapshots
    if !ref_snapshots_to_gc.is_empty() {
        let file_op = Files::create(ctx.clone(), op.clone());
        file_op.remove_file_in_batch(&ref_snapshots_to_gc).await?;
    }

    let expired_vec = expired_refs.into_iter().collect::<Vec<_>>();
    ctx.set_status_info(&format!(
        "Processed snapshot refs for table {}, elapsed: {:?}, expire_refs: {}, ref_snapshots_to_gc: {}, ref_gc_root_meta_ts: {:?}",
        table_info.desc,
        start.elapsed(),
        slice_summary(&expired_vec),
        slice_summary(&ref_snapshots_to_gc),
        gc_root_meta_ts
    ));

    files_to_gc.extend(ref_snapshots_to_gc.into_iter());
    Ok(RefVacuumInfo {
        ref_gc_roots,
        gc_root_meta_ts,
        gc_root_timestamp,
        files_to_gc,
    })
}

/// Process branch gc_root: find gc_root, collect snapshots to GC, and update collections
#[async_backtrace::framed]
async fn process_branch_gc_root(
    fuse_table: &FuseTable,
    branch_id: u64,
    head: &str,
    snapshots_before_retention: &[Entry],
    ref_snapshots_to_gc: &mut Vec<String>,
) -> Result<Option<(String, Arc<TableSnapshot>)>> {
    if snapshots_before_retention.is_empty() {
        return Ok(None);
    }

    let op = fuse_table.get_operator();
    // Read the last snapshot (oldest one)
    let last_snapshot_path = snapshots_before_retention.last().unwrap().path();
    let (last_snapshot, _) =
        SnapshotsIO::read_snapshot(last_snapshot_path.to_string(), op.clone(), false).await?;
    // If last_snapshot_path is head, use head as gc_root and clean up all snapshots before retention
    if last_snapshot_path == head {
        // All snapshots before retention can be cleaned up, except the last one (head itself as gc_root)
        let len = snapshots_before_retention.len();
        for snapshot in snapshots_before_retention.iter().take(len - 1) {
            ref_snapshots_to_gc.push(snapshot.path().to_owned());
        }
        return Ok(Some((head.to_string(), last_snapshot)));
    }

    // Get its prev_snapshot_id as gc_root
    let Some((gc_root_id, gc_root_ver)) = last_snapshot.prev_snapshot_id else {
        return Ok(None);
    };
    let gc_root_path = fuse_table
        .meta_location_generator()
        .ref_snapshot_location_from_uuid(branch_id, &gc_root_id, gc_root_ver)?;

    // Try to read gc_root snapshot
    match SnapshotsIO::read_snapshot(gc_root_path.clone(), op, false).await {
        Ok((gc_root_snap, _)) => {
            // Collect snapshots_to_gc
            let mut gc_candidates = Vec::with_capacity(snapshots_before_retention.len());
            for snapshot in snapshots_before_retention.iter() {
                gc_candidates.push(snapshot.path().to_owned());
            }

            // Find gc_root position in candidates
            let gc_root_idx = gc_candidates.binary_search(&gc_root_path).ok();
            let snapshots_to_gc = if let Some(idx) = gc_root_idx {
                gc_candidates[..idx].to_vec()
            } else {
                return Ok(None);
            };
            ref_snapshots_to_gc.extend(snapshots_to_gc);
            Ok(Some((gc_root_path, gc_root_snap)))
        }
        Err(_) => Ok(None),
    }
}
