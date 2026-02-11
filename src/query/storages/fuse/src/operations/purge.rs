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

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use chrono::Duration;
use chrono::Utc;
use databend_common_catalog::table::NavigationPoint;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::DropTableBranchReq;
use databend_common_meta_app::schema::DropTableTagReq;
use databend_common_meta_app::schema::LeastVisibleTime;
use databend_common_meta_app::schema::ListHistoryTableBranchesReq;
use databend_common_meta_app::schema::ListIndexesByIdReq;
use databend_common_meta_app::schema::ListTableTagsReq;
use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::least_visible_time_ident::LeastVisibleTimeIdent;
use databend_meta_types::MatchSeq;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CachedObject;
use databend_storages_common_index::BloomIndexMeta;
use databend_storages_common_index::InvertedIndexMeta;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::TableSnapshotStatistics;
use log::info;
use log::warn;

use super::RefVacuumInfo;
use crate::FuseTable;
use crate::index::InvertedIndexFile;
use crate::io::InvertedIndexReader;
use crate::io::SnapshotLiteExtended;
use crate::io::SnapshotsIO;
use crate::io::TableMetaLocationGenerator;

struct PreparedPurgeRoot {
    root_snapshot: Arc<TableSnapshot>,
    snapshot_files_to_gc: Vec<String>,
    ref_vacuum_info: RefVacuumInfo,
}

struct BranchPurgeRoot {
    gc_root: Arc<TableSnapshot>,
    snapshots_to_gc: Vec<String>,
}

#[derive(Debug)]
struct PurgeCounter {
    start: Instant,
    blocks: usize,
    agg_indexes: usize,
    inverted_indexes: usize,
    blooms: usize,
    hlls: usize,
    segments: usize,
    table_statistics: usize,
    snapshots: usize,
}

impl PurgeCounter {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            blocks: 0,
            agg_indexes: 0,
            inverted_indexes: 0,
            blooms: 0,
            hlls: 0,
            segments: 0,
            table_statistics: 0,
            snapshots: 0,
        }
    }
}

impl FuseTable {
    /// Run purge with explicit ref handling so branch/tag cleanup stays aligned with the gc root.
    pub async fn do_ref_aware_purge(
        &self,
        ctx: &Arc<dyn TableContext>,
        instant: Option<NavigationPoint>,
        num_snapshot_limit: Option<usize>,
        dry_run: bool,
    ) -> Result<Option<Vec<String>>> {
        let prepared = self
            .prepare_purge_root_and_refs(ctx, instant, dry_run)
            .await?;
        let Some(prepared) = prepared else {
            return if dry_run { Ok(Some(vec![])) } else { Ok(None) };
        };

        self.execute_root_purge(ctx, prepared, num_snapshot_limit, dry_run)
            .await
    }

    /// Resolve the effective purge root, freeze visibility, then collect ref protections.
    async fn prepare_purge_root_and_refs(
        &self,
        ctx: &Arc<dyn TableContext>,
        instant: Option<NavigationPoint>,
        dry_run: bool,
    ) -> Result<Option<PreparedPurgeRoot>> {
        // Step 1: navigate to the purge root. The returned table is rooted at the gc snapshot,
        // so snapshot_loc() is the effective purge root.
        let (root_table, mut snapshot_files_to_gc) = self.navigate_for_purge(ctx, instant).await?;
        root_table.snapshot_loc().ok_or_else(|| {
            ErrorCode::Internal(format!(
                "table {} has snapshot metadata but no snapshot location",
                root_table.get_table_info().desc
            ))
        })?;
        let root_snapshot = root_table.read_table_snapshot().await?.ok_or_else(|| {
            ErrorCode::Internal(format!(
                "table {} has root snapshot location but no snapshot metadata",
                root_table.get_table_info().desc
            ))
        })?;
        // Step 2: freeze visibility at the selected gc root before scanning refs.
        let catalog = ctx.get_default_catalog()?;
        if !dry_run {
            let time = root_snapshot.timestamp.ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "root snapshot timestamp missing for table {}",
                    self.get_table_info().desc
                ))
            })?;
            catalog
                .set_table_lvt(
                    &LeastVisibleTimeIdent::new(ctx.get_tenant(), self.get_id()),
                    &LeastVisibleTime::new(time),
                )
                .await?;
        }

        // Step 3: process tags and branches after the LVT boundary is fixed.
        let mut ref_vacuum_info = self
            .process_tags_for_purge(ctx, &mut snapshot_files_to_gc, dry_run)
            .await?;

        let branch_ref_info = self.process_branches_for_purge(ctx, dry_run).await?;
        ref_vacuum_info.merge(branch_ref_info);

        Ok(Some(PreparedPurgeRoot {
            root_snapshot,
            snapshot_files_to_gc,
            ref_vacuum_info,
        }))
    }

    /// Protect base segments referenced by tags and remove tagged snapshots from gc candidates.
    async fn process_tags_for_purge(
        &self,
        ctx: &Arc<dyn TableContext>,
        snapshot_files_to_gc: &mut Vec<String>,
        dry_run: bool,
    ) -> Result<RefVacuumInfo> {
        let mut ref_info = RefVacuumInfo::default();
        let now = Utc::now();
        let table_info = self.get_table_info();
        let catalog = ctx.get_catalog(table_info.catalog()).await?;
        let tags = catalog
            .list_table_tags(ListTableTagsReq {
                table_id: self.get_id(),
                include_expired: true,
            })
            .await?;

        for (tag_name, seq_tag) in tags {
            if seq_tag
                .data
                .expire_at
                .is_some_and(|expire_at| expire_at <= now)
            {
                if !dry_run {
                    let req = DropTableTagReq {
                        table_id: self.get_id(),
                        tag_name,
                        seq: MatchSeq::Exact(seq_tag.seq),
                    };
                    if let Err(e) = catalog.drop_table_tag(req).await {
                        warn!(
                            "drop expired tag failed, ignored, table: {}, err: {}",
                            table_info.desc, e
                        );
                    }
                }
                continue;
            }

            let tag_snapshot_loc = seq_tag.data.snapshot_loc;
            let Some(tag_snapshot_idx) = snapshot_files_to_gc
                .iter()
                .position(|loc| loc == &tag_snapshot_loc)
            else {
                continue;
            };

            let Some(tag_snapshot) =
                SnapshotsIO::read_snapshot_for_vacuum(self.get_operator(), &tag_snapshot_loc)
                    .await?
            else {
                continue;
            };

            snapshot_files_to_gc.swap_remove(tag_snapshot_idx);
            ref_info
                .protected_segments
                .extend(tag_snapshot.segments.iter().cloned());
        }

        Ok(ref_info)
    }

    /// Process branch metadata cleanup and collect base refs still reachable from branch gc roots.
    async fn process_branches_for_purge(
        &self,
        ctx: &Arc<dyn TableContext>,
        dry_run: bool,
    ) -> Result<RefVacuumInfo> {
        let now = Utc::now();
        let table_info = self.get_table_info();
        let tenant = ctx.get_tenant();
        let catalog = ctx.get_catalog(table_info.catalog()).await?;
        let retention_boundary =
            now - Duration::days(ctx.get_settings().get_data_retention_time_in_days()? as i64);
        let s3_storage_class = ctx.get_settings().get_s3_storage_class()?;
        let base_segment_prefix = self.meta_location_generator().segment_location_prefix();
        let base_block_prefix = self.meta_location_generator().block_location_prefix();
        let mut ref_info = RefVacuumInfo::default();

        let branch_candidates = catalog
            .list_history_table_branches(ListHistoryTableBranchesReq {
                table_id: self.get_id(),
                retention_boundary: Some(retention_boundary),
            })
            .await?;

        for branch in branch_candidates {
            let branch_name = branch.branch_name.clone();
            let branch_id = branch.branch_id.table_id;

            if branch.expire_at.is_some_and(|expire_at| expire_at <= now) && !dry_run {
                let req = DropTableBranchReq {
                    tenant: tenant.clone(),
                    table_id: self.get_id(),
                    branch_name: branch_name.clone(),
                    branch_id,
                    catalog_name: Some(table_info.catalog().to_string()),
                };
                if let Err(e) = catalog.drop_table_branch(req).await {
                    warn!(
                        "drop expired branch failed, ignored, table: {}, branch: {}, err: {}",
                        table_info.desc, branch_name, e
                    );
                }
            }

            let branch_table = self.branch_table_from_meta(branch, &s3_storage_class)?;
            let branch_ref_info = branch_table
                .purge_branch_snapshots_and_collect_base_refs(
                    ctx,
                    base_segment_prefix,
                    base_block_prefix,
                    dry_run,
                )
                .await?;
            ref_info.merge(branch_ref_info);
        }

        Ok(ref_info)
    }

    /// Purge branch-owned snapshots first, then return the base refs still visible from the branch root.
    async fn purge_branch_snapshots_and_collect_base_refs(
        &self,
        ctx: &Arc<dyn TableContext>,
        base_segment_prefix: &str,
        base_block_prefix: &str,
        dry_run: bool,
    ) -> Result<RefVacuumInfo> {
        // Step 4.1: pick the branch gc root and the branch snapshots to clean.
        let Some(branch_root) = self.prepare_branch_purge_root(ctx).await? else {
            return Ok(RefVacuumInfo::default());
        };

        // Step 4.2: purge only branch snapshots. Branch data files remain owned by base purge.
        self.cleanup_snapshot_files(ctx, &branch_root.snapshots_to_gc, dry_run)
            .await?;

        // Step 4.3: return only base refs reachable from the chosen branch gc root.
        self.collect_base_refs_from_ref_snapshot_with_files(
            ctx,
            branch_root.gc_root.as_ref(),
            base_segment_prefix,
            base_block_prefix,
            branch_root.snapshots_to_gc,
        )
        .await
    }

    /// Run the final purge walk against the selected root snapshot plus all protected ref data.
    async fn execute_root_purge(
        &self,
        ctx: &Arc<dyn TableContext>,
        prepared: PreparedPurgeRoot,
        num_snapshot_limit: Option<usize>,
        dry_run: bool,
    ) -> Result<Option<Vec<String>>> {
        let PreparedPurgeRoot {
            root_snapshot,
            snapshot_files_to_gc,
            mut ref_vacuum_info,
        } = prepared;
        let mut counter = PurgeCounter::new();

        // Step 5: build the root reference view used by the final purge walk.
        let (referenced_locations, root_snapshot_lite) = self
            .build_root_snapshot_view(ctx, root_snapshot.as_ref(), &ref_vacuum_info)
            .await?;

        let snapshots_io = SnapshotsIO::create(ctx.clone(), self.get_operator());
        let location_gen = self.meta_location_generator();
        let purged_snapshot_limit = num_snapshot_limit.unwrap_or(snapshot_files_to_gc.len());
        let mut read_snapshot_count = 0;
        let mut remain_snapshots = Vec::<SnapshotLiteExtended>::new();
        let mut dry_run_purge_files = std::mem::take(&mut ref_vacuum_info.files_to_gc);
        let mut purged_snapshot_count = 0;
        let catalog = ctx.get_catalog(self.get_table_info().catalog()).await?;
        let table_agg_index_ids = catalog
            .list_index_ids_by_table_id(ListIndexesByIdReq::new(ctx.get_tenant(), self.get_id()))
            .await?;
        let inverted_indexes = &self.get_table_info().meta.indexes;
        let chunk_size = ctx.get_settings().get_max_threads()? as usize * 4;

        // Step 6: walk snapshot candidates before the chosen root and purge unprotected data.
        for chunk in snapshot_files_to_gc.chunks(chunk_size).rev() {
            if let Err(err) = ctx.check_aborting() {
                log::error!(
                    "gc: aborted query, because the server is shutting down or the query was killed. table: {}, ident {}",
                    self.table_info.desc,
                    self.table_info.ident,
                );
                return Err(err.with_context("failed to read snapshot"));
            }

            let results = snapshots_io
                .read_snapshot_lite_extends(chunk, root_snapshot_lite.clone(), false)
                .await?;
            let mut snapshots = results.into_iter().flatten().collect::<Vec<_>>();
            if snapshots.is_empty() {
                break;
            }

            snapshots.extend(std::mem::take(&mut remain_snapshots));
            snapshots.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

            let base_snapshot = snapshots.pop().unwrap();
            let base_segments = base_snapshot.segments.clone();
            let base_timestamp = base_snapshot.timestamp;
            let base_ts_location = base_snapshot.table_statistics_location.clone();
            remain_snapshots.push(base_snapshot);

            let mut snapshots_to_be_purged = HashSet::new();
            let mut segments_to_be_purged = HashSet::new();
            let mut ts_to_be_purged = HashSet::new();

            for snapshot in snapshots {
                if snapshot.timestamp.is_some() && snapshot.timestamp >= base_timestamp {
                    remain_snapshots.push(snapshot);
                    continue;
                }

                if let Ok(loc) = location_gen
                    .gen_snapshot_location(&snapshot.snapshot_id, snapshot.format_version)
                {
                    if purged_snapshot_count >= purged_snapshot_limit {
                        break;
                    }
                    snapshots_to_be_purged.insert(loc);
                    purged_snapshot_count += 1;
                }

                segments_to_be_purged.extend(
                    snapshot
                        .segments
                        .difference(&base_segments)
                        .cloned()
                        .collect::<HashSet<_>>(),
                );

                if snapshot.table_statistics_location.is_some()
                    && snapshot.table_statistics_location != base_ts_location
                {
                    ts_to_be_purged.insert(snapshot.table_statistics_location.unwrap());
                }
            }

            read_snapshot_count += chunk.len();
            ctx.set_status_info(&format!(
                "gc: read snapshot files:{}/{}, cost:{:?}",
                read_snapshot_count,
                snapshot_files_to_gc.len(),
                counter.start.elapsed()
            ));

            if snapshots_to_be_purged.is_empty() {
                continue;
            }

            if dry_run {
                self.dry_run_root_purge(
                    ctx,
                    &mut dry_run_purge_files,
                    &referenced_locations,
                    segments_to_be_purged,
                    ts_to_be_purged,
                    snapshots_to_be_purged,
                    &table_agg_index_ids,
                )
                .await?;

                if num_snapshot_limit.is_some_and(|limit| purged_snapshot_count >= limit) {
                    return Ok(Some(dry_run_purge_files));
                }
            } else {
                self.partial_root_purge(
                    ctx,
                    &mut counter,
                    &referenced_locations,
                    segments_to_be_purged,
                    ts_to_be_purged,
                    snapshots_to_be_purged,
                    &table_agg_index_ids,
                    inverted_indexes,
                )
                .await?;

                if purged_snapshot_count >= purged_snapshot_limit {
                    info!("purge counter {:?}", counter);
                    return Ok(None);
                }
            }
        }

        if !remain_snapshots.is_empty() {
            let mut snapshots_to_be_purged = HashSet::new();
            let mut segments_to_be_purged = HashSet::new();
            let mut ts_to_be_purged = HashSet::new();
            for snapshot in remain_snapshots {
                if let Ok(loc) = location_gen
                    .gen_snapshot_location(&snapshot.snapshot_id, snapshot.format_version)
                {
                    if purged_snapshot_count >= purged_snapshot_limit {
                        break;
                    }
                    snapshots_to_be_purged.insert(loc);
                    purged_snapshot_count += 1;
                }

                segments_to_be_purged.extend(snapshot.segments);
                if let Some(location) = snapshot.table_statistics_location {
                    ts_to_be_purged.insert(location);
                }
            }

            if dry_run {
                self.dry_run_root_purge(
                    ctx,
                    &mut dry_run_purge_files,
                    &referenced_locations,
                    segments_to_be_purged,
                    ts_to_be_purged,
                    snapshots_to_be_purged,
                    &table_agg_index_ids,
                )
                .await?;
                if num_snapshot_limit.is_some_and(|limit| purged_snapshot_count >= limit) {
                    return Ok(Some(dry_run_purge_files));
                }
            } else {
                self.partial_root_purge(
                    ctx,
                    &mut counter,
                    &referenced_locations,
                    segments_to_be_purged,
                    ts_to_be_purged,
                    snapshots_to_be_purged,
                    &table_agg_index_ids,
                    inverted_indexes,
                )
                .await?;
            }
        }

        info!("purge counter {:?}", counter);
        if dry_run {
            return Ok(Some(dry_run_purge_files));
        }

        Ok(None)
    }

    #[allow(clippy::too_many_arguments)]
    async fn dry_run_root_purge(
        &self,
        ctx: &Arc<dyn TableContext>,
        purge_files: &mut Vec<String>,
        locations_referenced_by_root: &super::gc::LocationTuple,
        segments_to_be_purged: HashSet<Location>,
        ts_to_be_purged: HashSet<String>,
        snapshots_to_be_purged: HashSet<String>,
        table_agg_index_ids: &[u64],
    ) -> Result<()> {
        let chunk_size = ctx.get_settings().get_max_threads()? as usize * 4;
        let segment_locations = Vec::from_iter(segments_to_be_purged);
        for chunk in segment_locations.chunks(chunk_size) {
            let chunk_refs = chunk.iter().collect::<Vec<_>>();
            let locations = self
                .get_block_locations(ctx.clone(), &chunk_refs, false, true)
                .await?;

            for loc in &locations.block_location {
                if locations_referenced_by_root.block_location.contains(loc) {
                    continue;
                }
                purge_files.push(loc.to_string());
                for index_id in table_agg_index_ids {
                    purge_files.push(
                        TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                            loc, *index_id,
                        ),
                    );
                }
            }

            for loc in &locations.bloom_location {
                if locations_referenced_by_root.bloom_location.contains(loc) {
                    continue;
                }
                purge_files.push(loc.to_string());
            }

            purge_files.extend(chunk.iter().map(|loc| loc.0.clone()));
        }

        purge_files.extend(ts_to_be_purged);
        purge_files.extend(snapshots_to_be_purged);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn partial_root_purge(
        &self,
        ctx: &Arc<dyn TableContext>,
        counter: &mut PurgeCounter,
        locations_referenced_by_root: &super::gc::LocationTuple,
        segments_to_be_purged: HashSet<Location>,
        ts_to_be_purged: HashSet<String>,
        snapshots_to_be_purged: HashSet<String>,
        table_agg_index_ids: &[u64],
        inverted_indexes: &BTreeMap<String, TableIndex>,
    ) -> Result<()> {
        let chunk_size = ctx.get_settings().get_max_threads()? as usize * 4;
        let segment_locations = Vec::from_iter(segments_to_be_purged);
        let mut count = 0;

        for chunk in segment_locations.chunks(chunk_size) {
            let chunk_refs = chunk.iter().collect::<Vec<_>>();
            let locations = self
                .get_block_locations(ctx.clone(), &chunk_refs, false, true)
                .await?;

            let mut blocks_to_be_purged = HashSet::new();
            let mut agg_indexes_to_be_purged = HashSet::new();
            let mut inverted_indexes_to_be_purged = HashSet::new();
            for loc in &locations.block_location {
                if locations_referenced_by_root.block_location.contains(loc) {
                    continue;
                }
                blocks_to_be_purged.insert(loc.to_string());
                for index_id in table_agg_index_ids {
                    agg_indexes_to_be_purged.insert(
                        TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                            loc, *index_id,
                        ),
                    );
                }
                for idx in inverted_indexes.values() {
                    inverted_indexes_to_be_purged.insert(
                        TableMetaLocationGenerator::gen_inverted_index_location_from_block_location(
                            loc,
                            idx.name.as_str(),
                            idx.version.as_str(),
                        ),
                    );
                }
            }

            let mut blooms_to_be_purged = HashSet::new();
            for loc in &locations.bloom_location {
                if locations_referenced_by_root.bloom_location.contains(loc) {
                    continue;
                }
                blooms_to_be_purged.insert(loc.to_string());
            }

            let mut stats_to_be_purged = HashSet::new();
            for loc in &locations.hll_location {
                if locations_referenced_by_root.hll_location.contains(loc) {
                    continue;
                }
                stats_to_be_purged.insert(loc.to_string());
            }

            let segment_locations_to_be_purged = chunk
                .iter()
                .map(|loc| loc.0.clone())
                .collect::<HashSet<_>>();

            count += chunk.len();
            ctx.set_status_info(&format!(
                "gc: read purged segment files:{}/{}, cost:{:?}",
                count,
                segment_locations.len(),
                counter.start.elapsed()
            ));

            self.purge_root_block_segments(
                ctx,
                counter,
                blocks_to_be_purged,
                agg_indexes_to_be_purged,
                inverted_indexes_to_be_purged,
                blooms_to_be_purged,
                stats_to_be_purged,
                segment_locations_to_be_purged,
            )
            .await?;
        }

        self.purge_root_ts_snapshots(ctx, counter, ts_to_be_purged, snapshots_to_be_purged)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn purge_root_block_segments(
        &self,
        ctx: &Arc<dyn TableContext>,
        counter: &mut PurgeCounter,
        blocks_to_be_purged: HashSet<String>,
        agg_indexes_to_be_purged: HashSet<String>,
        inverted_indexes_to_be_purged: HashSet<String>,
        blooms_to_be_purged: HashSet<String>,
        stats_to_be_purged: HashSet<String>,
        segments_to_be_purged: HashSet<String>,
    ) -> Result<()> {
        let blocks_count = blocks_to_be_purged.len();
        if blocks_count > 0 {
            counter.blocks += blocks_count;
            self.try_purge_location_files(ctx.clone(), blocks_to_be_purged)
                .await?;
        }

        let agg_index_count = agg_indexes_to_be_purged.len();
        if agg_index_count > 0 {
            counter.agg_indexes += agg_index_count;
            self.try_purge_location_files(ctx.clone(), agg_indexes_to_be_purged)
                .await?;
        }

        let inverted_index_count = inverted_indexes_to_be_purged.len();
        if inverted_index_count > 0 {
            counter.inverted_indexes += inverted_index_count;
            if let Some(inverted_index_cache) = InvertedIndexFile::cache() {
                for index_path in &inverted_indexes_to_be_purged {
                    InvertedIndexReader::cache_key_of_index_columns(index_path)
                        .iter()
                        .for_each(|cache_key| {
                            inverted_index_cache.evict(cache_key);
                        });
                }
            }

            self.try_purge_location_files_and_cache::<InvertedIndexMeta, _>(
                ctx.clone(),
                inverted_indexes_to_be_purged,
            )
            .await?;
        }

        let bloom_count = blooms_to_be_purged.len();
        if bloom_count > 0 {
            counter.blooms += bloom_count;
            self.try_purge_location_files_and_cache::<BloomIndexMeta, _>(
                ctx.clone(),
                blooms_to_be_purged,
            )
            .await?;
        }

        let stats_count = stats_to_be_purged.len();
        if stats_count > 0 {
            counter.hlls += stats_count;
            self.try_purge_location_files(ctx.clone(), stats_to_be_purged)
                .await?;
        }

        let segments_count = segments_to_be_purged.len();
        if segments_count > 0 {
            counter.segments += segments_count;
            self.try_purge_location_files_and_cache::<
                databend_storages_common_table_meta::meta::SegmentInfo,
                _,
            >(ctx.clone(), segments_to_be_purged)
            .await?;
        }

        Ok(())
    }

    async fn purge_root_ts_snapshots(
        &self,
        ctx: &Arc<dyn TableContext>,
        counter: &mut PurgeCounter,
        ts_to_be_purged: HashSet<String>,
        snapshots_to_be_purged: HashSet<String>,
    ) -> Result<()> {
        let ts_count = ts_to_be_purged.len();
        if ts_count > 0 {
            counter.table_statistics += ts_count;
            self.try_purge_location_files_and_cache::<TableSnapshotStatistics, _>(
                ctx.clone(),
                ts_to_be_purged,
            )
            .await?;
        }

        let snapshots_count = snapshots_to_be_purged.len();
        if snapshots_count > 0 {
            counter.snapshots += snapshots_count;
            self.try_purge_location_files_and_cache::<TableSnapshot, _>(
                ctx.clone(),
                snapshots_to_be_purged,
            )
            .await?;
        }

        ctx.set_status_info(&format!(
            "gc: block files purged:{}, bloom files purged:{}, segment stats files purged:{}, segment files purged:{}, table statistic files purged:{}, snapshots purged:{}, take:{:?}",
            counter.blocks,
            counter.blooms,
            counter.hlls,
            counter.segments,
            counter.table_statistics,
            counter.snapshots,
            counter.start.elapsed()
        ));
        Ok(())
    }

    /// Merge base root refs with branch/tag protections into one snapshot-lite view for final gc.
    async fn build_root_snapshot_view(
        &self,
        ctx: &Arc<dyn TableContext>,
        root_snapshot: &TableSnapshot,
        ref_vacuum_info: &RefVacuumInfo,
    ) -> Result<(super::gc::LocationTuple, Arc<SnapshotLiteExtended>)> {
        let mut segments = HashSet::with_capacity(
            ref_vacuum_info.protected_segments.len() + root_snapshot.segments.len(),
        );
        segments.extend(ref_vacuum_info.protected_segments.iter().cloned());
        segments.extend(root_snapshot.segments.iter().cloned());

        let segment_refs = segments.iter().collect::<Vec<_>>();
        let mut referenced_locations = self
            .get_block_locations(ctx.clone(), &segment_refs, true, false)
            .await?;
        referenced_locations
            .block_location
            .extend(ref_vacuum_info.protected_blocks.iter().cloned());
        referenced_locations
            .bloom_location
            .extend(ref_vacuum_info.protected_blocks.iter().map(|loc| {
                TableMetaLocationGenerator::gen_bloom_index_location_from_block_location(loc)
            }));

        let root_snapshot_lite = Arc::new(SnapshotLiteExtended {
            format_version: root_snapshot.format_version,
            snapshot_id: root_snapshot.snapshot_id,
            timestamp: root_snapshot.timestamp,
            segments,
            table_statistics_location: root_snapshot.table_statistics_location(),
        });
        Ok((referenced_locations, root_snapshot_lite))
    }

    /// Resolve the branch-side gc root used by purge before branch snapshot cleanup starts.
    async fn prepare_branch_purge_root(
        &self,
        ctx: &Arc<dyn TableContext>,
    ) -> Result<Option<BranchPurgeRoot>> {
        let Some(latest_snapshot) = self.read_table_snapshot().await? else {
            return Ok(None);
        };

        let selection = self
            .prepare_snapshot_gc_selection(ctx, latest_snapshot.clone(), false)
            .await?;
        let (gc_root, snapshots_to_gc) = if let Some(selection) = selection {
            (selection.gc_root, selection.snapshots_to_gc)
        } else {
            (latest_snapshot, Vec::new())
        };

        Ok(Some(BranchPurgeRoot {
            gc_root,
            snapshots_to_gc,
        }))
    }
}
