// Copyright 2023 Datafuse Labs.
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
//

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use chrono::Duration;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_storage::StorageMetrics;
use futures_util::StreamExt;
use itertools::Itertools;
use opendal::Operator;
use storages_common_table_meta::meta::SnapshotId;
use storages_common_table_meta::meta::TableSnapshot;

use crate::io::Files;
use crate::io::SnapshotsIO;
use crate::operations::gc::mini_meta::LocationDigest;
use crate::operations::gc::mini_meta::MiniMeta;
use crate::operations::gc::mini_meta::MiniSnapshot;

pub struct SnapshotCollector<'a> {
    operator: Operator,
    chunk_size: usize,
    root_snapshot: &'a TableSnapshot,
    ctx: &'a Arc<dyn TableContext>,
    data_metrics: &'a StorageMetrics,
}

impl SnapshotCollector<'_> {
    // scan the snapshot path and rm the orphan snapshot files, return a set of segments that
    // should be kept, which will be used in the next stage of orphan data GC.
    async fn collect_snapshots(
        &self,
        snapshot_path: String,
    ) -> Result<(HashSet<LocationDigest>, HashSet<SnapshotId>)> {
        // list all the files, open the snapshot files and chain them from the root snapshot,
        // those not chained, and have timestamps out of retention interval, should be removed.
        // segments that referenced by the chained snapshots should be kept.
        //
        // to balance the memory usage and performance of execution, stream is used while listing and
        // filtering, snapshots files are read and deserialized in batch, and converted to the minimal
        // representation `MiniSnapshot` "in-place". also the segments should be kept is represented by a set of
        // u128, the sip 2-4 128 bit hash of segment location.

        let chunk_size = self.chunk_size;

        // get the stream of snapshot metas
        let snapshot_file_metas =
            Files::list_files::<MiniMeta>(self.operator.clone(), snapshot_path);

        // chunk the stream by setting
        let mut meta_chunks = snapshot_file_metas.chunks(chunk_size).boxed();
        let format = 0; // TODO

        // process the chunks
        let snapshots_io = SnapshotsIO::create(self.ctx.clone(), self.operator.clone(), format);
        let root = &self.root_snapshot;
        let mut segment_should_keep = HashSet::new();
        let mut orphan_snapshots = HashSet::new();
        let mut mini_snapshots = Vec::new();
        let base_timestamp = root.timestamp;

        let retention_interval = Duration::hours(ctx.get_settings().get_retention_period()? as i64);
        let retention_point = base_timestamp.map(|s| s - retention_interval);

        let start = Instant::now();
        while let Some(file_paths) = meta_chunks.next().await {
            // read chunk of snapshot file path
            let (file_paths, size_sum) = file_paths.into_iter().try_fold(
                (Vec::new(), 0),
                |(mut file_paths, mut sum), entry| {
                    let entry = entry?;
                    sum += entry.converted.size;
                    file_paths.push(entry.path);
                    Ok::<_, ErrorCode>((file_paths, sum))
                },
            )?;

            if lite.timestamp < retention_point {}
            // read them in as MiniSnapshot
            let mut snapshots = snapshots_io
                .read_snapshots_stream_new::<MiniSnapshot>(&file_paths)
                .await?
                .into_iter()
                .collect::<Result<Vec<_>>>()?;
            mini_snapshots.append(&mut snapshots);

            // report status
            let status = format!(
                "gc orphans: scanned snapshot files:{}, size:{} bytes, elapsed:{} sec",
                file_paths.len(),
                size_sum,
                start.elapsed().as_secs(),
            );
            self.data_metrics.set_status(&status);
        }
        mini_snapshots.shrink_to_fit();

        let (chained, orphan) = Self::chain_snapshots(mini_snapshots, &root);
        for mini_snapshot in chained.into_iter() {
            segment_should_keep.extend(mini_snapshot.segment_digests);
        }
        for mini_snapshot in orphan.into_iter() {
            orphan_snapshots.insert(mini_snapshot.id);
        }
        // TODO for orphan gc, we can remove orphan snapshot first
        Ok((segment_should_keep, orphan_snapshots))
    }

    pub fn chain_snapshots(
        snapshot_lites: Vec<MiniSnapshot>,
        root_snapshot: &TableSnapshot,
    ) -> (Vec<MiniSnapshot>, Vec<MiniSnapshot>) {
        let mut snapshot_map = HashMap::new();
        let mut chained_snapshot_lites = vec![];
        for snapshot_lite in snapshot_lites.into_iter() {
            snapshot_map.insert(snapshot_lite.snapshot_id(), snapshot_lite);
        }
        // let root_snapshot_lite = MiniSnapshot::from(root_snapshot);
        let mut prev_snapshot_id_tuple = root_snapshot_lite.prev_snapshot_id();
        chained_snapshot_lites.push(root_snapshot_lite);
        while let Some((prev_snapshot_id, _)) = prev_snapshot_id_tuple {
            let prev_snapshot_lite = snapshot_map.remove(&prev_snapshot_id);
            match prev_snapshot_lite {
                None => {
                    break;
                }
                Some(prev_snapshot) => {
                    prev_snapshot_id_tuple = prev_snapshot.prev_snapshot_id();
                    chained_snapshot_lites.push(prev_snapshot);
                }
            }
        }
        // remove root from orphan list
        snapshot_map.remove(&root_snapshot.snapshot_id);
        (chained_snapshot_lites, snapshot_map.into_values().collect())
    }
}
