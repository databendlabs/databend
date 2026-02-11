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

use std::collections::HashSet;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_io::Files;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::TableSnapshot;

use crate::FuseTable;
use crate::operations::RefVacuumInfo;

impl FuseTable {
    /// Read the object-store last-modified time for a snapshot object.
    pub async fn snapshot_last_modified(&self, location: &str) -> Result<DateTime<Utc>> {
        self.get_operator_ref()
            .stat(location)
            .await?
            .last_modified()
            .ok_or_else(|| {
                ErrorCode::StorageOther(format!(
                    "Failed to get `last_modified` metadata of snapshot '{}'",
                    location
                ))
            })
    }

    /// List owner files that are eligible for gc under an optional `(root_ts, root_meta_ts)` cutoff.
    pub async fn list_files_for_gc(
        &self,
        prefix: &str,
        cutoff: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> Result<Vec<String>> {
        match cutoff {
            Some((root_timestamp, root_snapshot_meta_ts)) => {
                let files = self
                    .list_files_until_timestamp(
                        prefix,
                        root_timestamp,
                        false,
                        Some(root_snapshot_meta_ts),
                    )
                    .await?
                    .into_iter()
                    .map(|entry| entry.path().to_string())
                    .collect();
                Ok(files)
            }
            None => self.list_files(prefix.to_string(), |_, _| true).await,
        }
    }

    /// Split a mixed snapshot view into branch-owned segments and base-owned segments.
    pub(super) fn split_base_and_branch_segments(
        &self,
        snapshot: &TableSnapshot,
        base_segment_prefix: &str,
    ) -> (Vec<Location>, HashSet<Location>) {
        let branch_segment_prefix = self.meta_location_generator().segment_location_prefix();
        snapshot.segments.iter().cloned().fold(
            (Vec::new(), HashSet::new()),
            |(mut branch_segments, mut base_segments), segment| {
                if segment.0.starts_with(branch_segment_prefix) {
                    branch_segments.push(segment);
                } else if segment.0.starts_with(base_segment_prefix) {
                    base_segments.insert(segment);
                }
                (branch_segments, base_segments)
            },
        )
    }

    /// Read branch segments once and classify their referenced blocks by storage prefix.
    pub(super) async fn collect_branch_and_base_blocks_from_segments(
        &self,
        ctx: &Arc<dyn TableContext>,
        branch_segments: &[Location],
        branch_block_prefix: Option<&str>,
        base_block_prefix: &str,
    ) -> Result<(HashSet<String>, HashSet<String>)> {
        if branch_segments.is_empty() {
            return Ok((HashSet::new(), HashSet::new()));
        }

        let segment_refs = branch_segments.iter().collect::<Vec<_>>();
        let block_locations = self
            .get_block_locations(ctx.clone(), &segment_refs, false, false)
            .await?
            .block_location;

        let mut branch_blocks = HashSet::new();
        let mut base_blocks = HashSet::new();
        for block in block_locations {
            if branch_block_prefix.is_some_and(|prefix| block.starts_with(prefix)) {
                branch_blocks.insert(block);
            } else if block.starts_with(base_block_prefix) {
                base_blocks.insert(block);
            }
        }

        Ok((branch_blocks, base_blocks))
    }

    /// Collect base-table refs reachable from a branch/tag snapshot and carry files already chosen for gc.
    pub(super) async fn collect_base_refs_from_ref_snapshot_with_files(
        &self,
        ctx: &Arc<dyn TableContext>,
        snapshot: &TableSnapshot,
        base_segment_prefix: &str,
        base_block_prefix: &str,
        files_to_gc: Vec<String>,
    ) -> Result<RefVacuumInfo> {
        let (branch_segments, protected_segments) =
            self.split_base_and_branch_segments(snapshot, base_segment_prefix);
        let (_, protected_blocks) = self
            .collect_branch_and_base_blocks_from_segments(
                ctx,
                &branch_segments,
                None,
                base_block_prefix,
            )
            .await?;

        Ok(RefVacuumInfo {
            protected_segments,
            protected_snapshots: HashSet::new(),
            protected_blocks,
            files_to_gc,
        })
    }

    /// Remove snapshot objects and evict the corresponding snapshot cache entries first.
    pub async fn cleanup_snapshot_files(
        &self,
        ctx: &Arc<dyn TableContext>,
        snapshot_files: &[String],
        dry_run: bool,
    ) -> Result<()> {
        if dry_run || snapshot_files.is_empty() {
            return Ok(());
        }

        if let Some(snapshot_cache) = CacheManager::instance().get_table_snapshot_cache() {
            for path in snapshot_files {
                snapshot_cache.evict(path);
            }
        }

        Files::create(ctx.clone(), self.get_operator())
            .remove_file_in_batch(snapshot_files)
            .await
    }
}
