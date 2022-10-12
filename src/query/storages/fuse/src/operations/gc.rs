//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::collections::HashSet;
use std::sync::Arc;

use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::meta::Location;
use common_fuse_meta::meta::SnapshotId;
use opendal::Operator;
use tracing::info;
use tracing::warn;

use crate::fuse_segment::read_segments;
use crate::fuse_snapshot::read_snapshots_by_root_file;
use crate::FuseTable;

impl FuseTable {
    pub async fn do_gc(&self, ctx: &Arc<dyn TableContext>, keep_last_snapshot: bool) -> Result<()> {
        let r = self.read_table_snapshot(ctx.clone()).await;
        let snapshot_opt = match r {
            Err(e) if e.code() == ErrorCode::storage_not_found_code() => {
                // concurrent gc: someone else has already collected this snapshot, ignore it
                warn!(
                    "concurrent gc: snapshot {:?} already collected. table: {}, ident {}",
                    self.snapshot_loc(),
                    self.table_info.desc,
                    self.table_info.ident,
                );
                return Ok(());
            }
            Err(e) => return Err(e),
            Ok(v) => v,
        };

        let chunk_size = ctx.get_settings().get_max_storage_io_requests()? as usize;

        // 1. Root snapshot.
        let mut segments_referenced_by_root = HashSet::new();
        let mut blocks_referenced_by_root = HashSet::new();
        let root_snapshot_id = if let Some(root_snapshot) = snapshot_opt {
            let segments = root_snapshot.segments.clone();
            blocks_referenced_by_root = self.get_block_locations(ctx.clone(), &segments).await?;

            segments_referenced_by_root = HashSet::from_iter(segments);
            root_snapshot.snapshot_id
        } else {
            SnapshotId::new_v4()
        };

        // 2. Get all snapshot(including root snapshot).
        let mut all_snapshot_lites = vec![];
        let mut all_segment_locations = HashSet::new();
        if let Some(root_snapshot_location) = self.snapshot_loc() {
            (all_snapshot_lites, all_segment_locations) = read_snapshots_by_root_file(
                ctx.clone(),
                root_snapshot_location,
                self.snapshot_format_version(),
                &self.operator,
                true,
            )
            .await?;
        }

        // 3. Find.
        let mut snapshots_to_be_deleted = HashSet::new();
        let mut segments_to_be_deleted = HashSet::new();

        // 3.1 Find all the snapshots need to be deleted.
        {
            for snapshot in &all_snapshot_lites {
                // Skip the root snapshot if the keep_last_snapshot is true.
                if keep_last_snapshot && snapshot.snapshot_id == root_snapshot_id {
                    continue;
                }
                snapshots_to_be_deleted.insert((snapshot.snapshot_id, snapshot.format_version));
            }
        }

        // 3.2 Find all the segments need to be deleted.
        {
            for segment in &all_segment_locations {
                // Skip the root snapshot segments if the keep_last_snapshot is true.
                if keep_last_snapshot && segments_referenced_by_root.contains(segment) {
                    continue;
                }
                segments_to_be_deleted.insert(segment.clone());
            }
        }

        // 4. Purge segments&blocks by chunk size(max_storage_io_requests).
        {
            let segments_to_be_delete_vec = Vec::from_iter(segments_to_be_deleted);
            for (idx, chunk) in segments_to_be_delete_vec.chunks(chunk_size).enumerate() {
                info!(
                    "[Chunk: {}] Start purge blocks, chunk size:{}",
                    idx, chunk_size
                );
                self.try_purge_blocks(
                    ctx.clone(),
                    chunk,
                    &blocks_referenced_by_root,
                    keep_last_snapshot,
                )
                .await?;
                info!("[Chunk: {}] Finish purge blocks", idx);
            }
        }

        // 5. Purge snapshots by chunk size(max_storage_io_requests).
        {
            let snapshots_to_be_delete_vec = Vec::from_iter(snapshots_to_be_deleted);
            for (idx, chunk) in snapshots_to_be_delete_vec.chunks(chunk_size).enumerate() {
                info!(
                    "[Chunk: {}] Start purge snapshot, chunk size:{}",
                    idx, chunk_size
                );
                // self.try_purge_snapshots(ctx.clone(), chunk).await?;
                info!("[Chunk: {}] Finish purge snapshots", idx);
            }
        }

        Ok(())
    }

    async fn try_purge_blocks(
        &self,
        ctx: Arc<dyn TableContext>,
        segments_need_to_delete: &[Location],
        blocks_referenced_by_root: &HashSet<String>,
        keep_last_snapshot: bool,
    ) -> Result<()> {
        let segments = read_segments(self.operator.clone(), ctx, segments_need_to_delete).await?;

        let mut blocks_need_to_delete = HashSet::new();
        let mut blooms_need_to_delete = HashSet::new();
        for segment in segments {
            let segment = segment?;

            // TODO(bohu): clean the LRUCache: https://github.com/datafuselabs/databend/issues/8157
            for block_meta in &segment.blocks {
                let loc = block_meta.location.0.as_str();
                // Skip root block if keep_last_snapshot is true.
                if keep_last_snapshot && blocks_referenced_by_root.contains(loc) {
                    continue;
                }
                blocks_need_to_delete.insert(loc.to_string());

                // Bloom index file.
                if let Some(bloom_index_location) = &block_meta.bloom_filter_index_location {
                    blooms_need_to_delete.insert(bloom_index_location.0.to_string());
                }
            }
        }

        // TODO(bohu): delete file in parallel
        for _location in blocks_need_to_delete {}
        for _location in blooms_need_to_delete {}
        for _location in segments_need_to_delete {}

        Ok(())
    }

    async fn get_block_locations(
        &self,
        ctx: Arc<dyn TableContext>,
        segment_locations: &[Location],
    ) -> Result<HashSet<String>> {
        let mut result = HashSet::new();

        let segments = read_segments(self.operator.clone(), ctx, segment_locations).await?;
        for (idx, segment) in segments.iter().enumerate() {
            let segment = segment.clone();
            let segment_info = match segment {
                Err(e) if e.code() == ErrorCode::storage_not_found_code() => {
                    let location = &segment_locations[idx];
                    // concurrent gc: someone else has already collected this segment, ignore it
                    warn!(
                        "concurrent gc: segment of location {} already collected. table: {}, ident {}",
                        location.0, self.table_info.desc, self.table_info.ident,
                    );
                    continue;
                }
                Err(e) => return Err(e),
                Ok(v) => v,
            };
            for block_meta in &segment_info.blocks {
                result.insert(block_meta.location.0.clone());
            }
        }

        Ok(result)
    }

    async fn remove_file_by_location(
        &self,
        data_accessor: &Operator,
        block_location: impl AsRef<str>,
    ) -> Result<()> {
        match self
            .do_remove_file_by_location(data_accessor, block_location.as_ref())
            .await
        {
            Err(e) if e.code() == ErrorCode::storage_not_found_code() => {
                warn!(
                    "concurrent gc: block of location {} already collected. table: {}, ident {}",
                    block_location.as_ref(),
                    self.table_info.desc,
                    self.table_info.ident,
                );
                Ok(())
            }
            Err(e) => Err(e),
            Ok(_) => Ok(()),
        }
    }

    // make type checker happy
    #[inline]
    async fn do_remove_file_by_location(
        &self,
        data_accessor: &Operator,
        location: &str,
    ) -> Result<()> {
        Ok(data_accessor.object(location.as_ref()).delete().await?)
    }
}
