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

use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::Hash;
use std::hash::Hasher;
use std::path::PathBuf;
use std::sync::Arc;

use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use futures::Stream;
use futures::StreamExt;
use futures_util::TryStreamExt;
use opendal::ObjectMetadata;
use opendal::ObjectMode;
use siphasher::sip128;
use siphasher::sip128::Hasher128;
use storages_common_cache::LoadParams;
use storages_common_table_meta::meta::FormatVersion;
use storages_common_table_meta::meta::SnapshotId;
use storages_common_table_meta::meta::TableSnapshot;
use tracing::warn;
use uuid::Timestamp;

use crate::io::MetaReaders;
use crate::io::SnapshotsIO;
use crate::FuseTable;
use crate::FUSE_TBL_BLOCK_INDEX_PREFIX;
use crate::FUSE_TBL_BLOCK_PREFIX;
use crate::FUSE_TBL_SEGMENT_PREFIX;
use crate::FUSE_TBL_SNAPSHOT_PREFIX;

// implementation of statement
// `optimize table T gc [dry-run]`
impl FuseTable {
    pub async fn do_purge_orphans(&self, ctx: &Arc<dyn TableContext>) -> Result<()> {
        let root = self.meta_location_generator.prefix();
        let table_prefix = format!("{}/{}", root, Self::parse_storage_prefix(&self.table_info)?);
        let snapshot_path = format!("{}/{}", table_prefix, FUSE_TBL_SNAPSHOT_PREFIX);
        let segment_path = format!("{}/{}", table_prefix, FUSE_TBL_SEGMENT_PREFIX);
        let block_path = format!("{}/{}", table_prefix, FUSE_TBL_BLOCK_PREFIX);
        let block_index_pref = format!("{}/{}", table_prefix, FUSE_TBL_BLOCK_INDEX_PREFIX);

        // collect orphan snapshot
        let (segments_should_be_kept, _orphan_snapshot) =
            self.process_snapshot(ctx, snapshot_path).await?;

        // collect orphan segments
        let blocks_should_be_kept = self
            .process_segments(segment_path, segments_should_be_kept)
            .await?;

        // collect orphan blocks by uuid
        self.process_objects_by_uuid(block_path, &blocks_should_be_kept)
            .await?;

        // collect orphan block indexes by uuid
        self.process_objects_by_uuid(block_index_pref, &blocks_should_be_kept)
            .await
    }

    async fn process_snapshot(
        &self,
        ctx: &Arc<dyn TableContext>,
        snapshot_path: String,
    ) -> Result<(HashSet<LocationDigest>, HashSet<SnapshotId>)> {
        let snapshot_path_stream = self.list_files_as_stream(snapshot_path);
        let chunk_size = ctx.get_settings().get_max_storage_io_requests()? as usize;
        let mut snapshot_path_chunk = snapshot_path_stream.chunks(chunk_size).boxed();
        let snapshots_io = SnapshotsIO::create(
            ctx.clone(),
            self.operator.clone(),
            self.snapshot_format_version().await?,
        );
        let root = self.read_table_snapshot().await?.unwrap(); // TODO
        let mut segment_should_keep = HashSet::new();
        let mut orphan_snapshots = HashSet::new();
        while let Some(snapshot_paths) = snapshot_path_chunk.next().await {
            // TODO refactor read_snapshots,
            // stop the collection on the first error
            // pass a mapper, convert snapshot to lit ASAP
            let snapshots = snapshot_paths
                .into_iter()
                .map(|v| v.map(|v| v.path))
                .collect::<Result<Vec<_>>>()?;
            let snapshots = snapshots_io
                .read_snapshots_stream::<MiniSnapshot>(&snapshots)
                .await?
                .into_iter()
                .collect::<Result<Vec<_>>>()?;
            // TODO filter by retention
            let (chained, orphan) = Self::chain_snapshots(snapshots, root.clone());
            for mini_snapshot in chained.into_iter() {
                segment_should_keep.extend(mini_snapshot.segment_digests);
            }
            for mini_snapshot in orphan.into_iter() {
                orphan_snapshots.insert(mini_snapshot.id);
            }
        }
        // TODO for orphan gc, we can remove orphan snapshot first
        Ok((segment_should_keep, orphan_snapshots))
    }

    async fn process_segments(
        &self,
        segment_path: String,
        segment_should_keep: HashSet<LocationDigest>,
    ) -> Result<HashSet<LocationDigest>> {
        // TODO
        let format_version = 0;

        let schema = self.table_info.schema();
        let reader = MetaReaders::segment_info_reader(self.operator.clone(), schema);
        let segment_files_beyond_retention = self.list_files_as_stream(segment_path);
        segment_files_beyond_retention
            .try_fold(HashSet::new(), |mut block_should_be_kept, meta| async {
                if !segment_should_keep.contains(&meta.path.as_str().into()) {
                    // no, just delete it
                    // orphan_segments.insert(meta.path.clone());
                    let load_params = LoadParams {
                        location: meta.path,
                        len_hint: None,
                        ver: format_version,
                    };
                    let segment = reader.read(&load_params).await?;
                    for block_meta in segment.blocks.iter() {
                        // here the uuid of block location used, later it will be used
                        // to remove both blocks and block indexes
                        let uuid_string = extract_uuid_from(block_meta.location.0.as_str())?;
                        block_should_be_kept.insert(uuid_string.into());
                    }
                }
                Ok::<_, ErrorCode>(block_should_be_kept)
            })
            .await
    }

    async fn process_objects_by_uuid(
        &self,
        block_path: String,
        referenced_blocks: &HashSet<LocationDigest>,
    ) -> Result<()> {
        let chunk_size = 1000; //TODO
        let block_files_beyond_retention = self.list_files_as_stream(block_path);
        let mut chunks = block_files_beyond_retention.chunks(chunk_size).boxed();
        while let Some(chunk) = chunks.next().await {
            let mini_metas = chunk.into_iter().collect::<Result<Vec<_>>>()?;
            let paths = mini_metas
                .into_iter()
                .filter_map(|meta| {
                    let filter = || {
                        let path = extract_uuid_from(&meta.path)?;
                        let location_digest = LocationDigest::from(path.as_str());
                        if !referenced_blocks.contains(&location_digest) {
                            Ok::<_, ErrorCode>(Some(path))
                        } else {
                            Ok(None)
                        }
                    };
                    filter().transpose()
                })
                .collect::<Result<Vec<_>>>()?;
            self.delete_all(paths).await?
        }
        Ok(())
    }

    fn list_files_as_stream(&self, path: String) -> impl Stream<Item = Result<MiniMeta>> {
        let operator = self.operator.clone();
        futures::stream::once(async move { operator.object(&path).list().await })
        .map_ok(|entry_stream| {
                        let a = entry_stream.try_filter_map(|entry| async move {
                            if let ObjectMode::FILE = entry.mode().await? {
                                let meta = entry.metadata().await?;
                                if let Some(ts) = meta.last_modified() {
                                    if beyond_retention(ts) {
                                        return Ok(Some(MiniMeta::try_from(meta)));
                                    }
                                } else {
                                    warn!(
                                        "file without last modification timestamp, will not be collected {}",
                                        entry.path(),
                                    );
                                }
                            }
                            return Ok(None)
                        });
                        a.map_err(|_| ErrorCode::StorageOther(""))
        }).try_flatten().map(|v| Ok(v??))
    }

    // TODO duplication of Snapshot::chain_snapshots
    pub fn chain_snapshots(
        snapshot_lites: Vec<MiniSnapshot>,
        root_snapshot: Arc<TableSnapshot>,
    ) -> (Vec<MiniSnapshot>, Vec<MiniSnapshot>) {
        let mut snapshot_map = HashMap::new();
        let mut chained_snapshot_lites = vec![];
        for snapshot_lite in snapshot_lites.into_iter() {
            snapshot_map.insert(snapshot_lite.snapshot_id(), snapshot_lite);
        }
        let root_snapshot_lite = MiniSnapshot::from(root_snapshot.as_ref());
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

    async fn delete_all(&self, _paths: impl IntoIterator<Item = String>) -> Result<()> {
        todo!()
    }
}

fn beyond_retention<T>(_ts: T) -> bool {
    todo!()
}

fn extract_uuid_from(path: &str) -> Result<String> {
    let path_buffer = PathBuf::from(path);
    // TODO error handling
    let file_name = path_buffer
        .file_stem()
        .ok_or_else(|| ErrorCode::StorageOther(format!("malformed file name {}", path)))?
        .to_str()
        .expect("should not happen, since path passed in is valid utf-8");

    Ok(if let Some(pos) = file_name.find("_v") {
        file_name[..pos].to_owned()
    } else {
        file_name.to_owned()
    })
}

struct MiniMeta {
    path: String,
    _size: usize,
    _create_time: Timestamp,
}

impl TryFrom<ObjectMetadata> for MiniMeta {
    type Error = ErrorCode;

    fn try_from(_value: ObjectMetadata) -> std::result::Result<Self, Self::Error> {
        todo!()
    }
}

#[derive(Eq, Hash, PartialEq)]
struct LocationDigest {
    digest: u128,
}

impl<T: AsRef<str>> From<T> for LocationDigest {
    fn from(v: T) -> Self {
        let mut sip = sip128::SipHasher24::new();
        sip.write(v.as_ref().as_bytes());
        let digest = sip.finish128().into();
        LocationDigest { digest }
    }
}

pub struct MiniSnapshot {
    id: SnapshotId,
    _version: FormatVersion,
    _prev_id: Option<SnapshotId>,
    segment_digests: HashSet<LocationDigest>,
}

impl MiniSnapshot {
    #[inline]
    fn snapshot_id(&self) -> SnapshotId {
        todo!()
    }
    #[inline]
    fn prev_snapshot_id(&self) -> Option<(SnapshotId, u64)> {
        todo!()
    }
}

impl From<Arc<TableSnapshot>> for MiniSnapshot {
    fn from(_value: Arc<TableSnapshot>) -> Self {
        todo!()
    }
}

impl From<&TableSnapshot> for MiniSnapshot {
    fn from(_value: &TableSnapshot) -> Self {
        todo!()
    }
}
