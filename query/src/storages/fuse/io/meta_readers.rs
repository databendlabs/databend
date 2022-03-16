// Copyright 2021 Datafuse Labs.
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

use std::sync::Arc;

use common_arrow::arrow::io::parquet::read::read_metadata_async;
use common_arrow::arrow::io::parquet::read::schema::FileMetaData as ParquetFileMetaData;
use common_exception::ErrorCode;
use common_exception::Result;
use common_tracing::tracing::debug_span;
use common_tracing::tracing::Instrument;
use futures::io::BufReader;
use opendal::error::Kind as DalErrorKind;
use opendal::Reader;

use crate::sessions::QueryContext;
use crate::storages::fuse::cache::MemoryCache;
use crate::storages::fuse::cache::TenantLabel;
use crate::storages::fuse::io::versioned_reader::VersionedLoader;
use crate::storages::fuse::io::CachedReader;
use crate::storages::fuse::io::HasTenantLabel;
use crate::storages::fuse::io::Loader;
use crate::storages::fuse::io::TableMetaLocationGenerator;
use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::meta::SegmentInfoVersions;
use crate::storages::fuse::meta::SnapshotVersions;
use crate::storages::fuse::meta::TableSnapshot;

/// Provider of [BufReader]
///
/// Mainly used as a auxiliary facility in the implementation of [Loader], such that the acquirement
/// of an [BufReader] can be deferred or avoided (e.g. if hits cache).
#[async_trait::async_trait]
pub trait BufReaderProvider {
    async fn buf_reader(&self, path: &str, len: Option<u64>) -> Result<BufReader<Reader>>;
}

/// A Newtype of [FileMetaData].
///
/// To avoid implementation (of trait [Loader]) conflicts
pub struct FileMetaData(ParquetFileMetaData);

impl FileMetaData {
    pub fn inner(&self) -> &ParquetFileMetaData {
        &self.0
    }
}

pub type SegmentInfoCache = MemoryCache<SegmentInfo>;
pub type TableSnapshotCache = MemoryCache<TableSnapshot>;
pub type BlockMetaCache = MemoryCache<FileMetaData>;

pub type SegmentInfoReader<'a> = CachedReader<SegmentInfo, &'a QueryContext>;
pub type TableSnapshotReader<'a> = CachedReader<TableSnapshot, &'a QueryContext>;

/// A sugar type of BlockMeta reader
pub type BlockMetaReader = CachedReader<FileMetaData, Arc<QueryContext>>;

/// Aux struct, factory of common readers
pub struct MetaReaders;

impl MetaReaders {
    pub fn segment_info_reader(ctx: &QueryContext) -> SegmentInfoReader {
        SegmentInfoReader::new(
            ctx.get_storage_cache_manager().get_table_segment_cache(),
            ctx,
            "SEGMENT_INFO_CACHE".to_owned(),
        )
    }

    pub fn table_snapshot_reader(ctx: &QueryContext) -> TableSnapshotReader {
        TableSnapshotReader::new(
            ctx.get_storage_cache_manager().get_table_snapshot_cache(),
            ctx,
            "SNAPSHOT_CACHE".to_owned(),
        )
    }

    pub fn block_meta_reader(ctx: Arc<QueryContext>) -> BlockMetaReader {
        BlockMetaReader::new(
            ctx.get_storage_cache_manager().get_block_meta_cache(),
            ctx,
            "BLOCK_META_CACHE".to_owned(),
        )
    }
}

impl<'a> TableSnapshotReader<'a> {
    pub async fn read_snapshot_history(
        &self,
        latest_snapshot_location: Option<&String>,
        format_version: u64,
        meta_locs: TableMetaLocationGenerator,
    ) -> Result<Vec<Arc<TableSnapshot>>> {
        let mut snapshots = vec![];
        let mut current_snapshot_location = latest_snapshot_location.cloned();
        let current_format_version = format_version;
        while let Some(loc) = current_snapshot_location {
            let r = self.read(loc, None, current_format_version).await;
            let snapshot = match r {
                Ok(s) => s,
                Err(e) if e.code() == ErrorCode::dal_path_not_found_code() => {
                    break;
                }
                Err(e) => return Err(e),
            };
            let prev = snapshot.prev_snapshot_id;
            snapshots.push(snapshot);
            // TODO buggy, set the current ver
            current_snapshot_location =
                prev.map(|(id, _)| meta_locs.snapshot_location_from_uuid(&id));
        }
        Ok(snapshots)
    }
}

#[async_trait::async_trait]
impl<T> Loader<TableSnapshot> for T
where T: BufReaderProvider + Sync
{
    async fn load(
        &self,
        key: &str,
        length_hint: Option<u64>,
        version: u64,
    ) -> Result<TableSnapshot> {
        let version = SnapshotVersions::try_from(version)?;
        let reader = self.buf_reader(key, length_hint).await?;
        version.vload(reader).await
    }
}

#[async_trait::async_trait]
impl<T> Loader<SegmentInfo> for T
where T: BufReaderProvider + Sync
{
    async fn load(&self, key: &str, length_hint: Option<u64>, version: u64) -> Result<SegmentInfo> {
        let version = SegmentInfoVersions::try_from(version)?;
        let reader = self.buf_reader(key, length_hint).await?;
        version.vload(reader).await
    }
}

#[async_trait::async_trait]
impl<T> Loader<FileMetaData> for T
where T: BufReaderProvider + Sync
{
    async fn load(
        &self,
        key: &str,
        length_hint: Option<u64>,
        _version: u64,
    ) -> Result<FileMetaData> {
        let mut reader = self.buf_reader(key, length_hint).await?;
        let meta = read_metadata_async(&mut reader)
            .instrument(debug_span!("parquet_source_read_meta"))
            .await
            .map_err(|e| ErrorCode::ParquetError(e.to_string()))?;
        Ok(FileMetaData(meta))
    }
}

#[async_trait::async_trait]
impl BufReaderProvider for &QueryContext {
    async fn buf_reader(&self, path: &str, len: Option<u64>) -> Result<BufReader<Reader>> {
        let operator = self.get_storage_operator().await?;
        let object = operator.object(path);

        let len = match len {
            Some(l) => l,
            None => {
                let meta = object.metadata().await.map_err(|e| match e.kind() {
                    DalErrorKind::ObjectNotExist => ErrorCode::DalPathNotFound(e.to_string()),
                    _ => ErrorCode::DalTransportError(e.to_string()),
                })?;

                meta.content_length()
            }
        };

        let reader = object.limited_reader(len);
        let read_buffer_size = self.get_settings().get_storage_read_buffer_size()?;
        Ok(BufReader::with_capacity(read_buffer_size as usize, reader))
    }
}

#[async_trait::async_trait]
impl BufReaderProvider for Arc<QueryContext> {
    async fn buf_reader(&self, path: &str, len: Option<u64>) -> Result<BufReader<Reader>> {
        self.as_ref().buf_reader(path, len).await
    }
}

impl HasTenantLabel for &QueryContext {
    fn tenant_label(&self) -> TenantLabel {
        ctx_tenant_label(self)
    }
}

impl HasTenantLabel for Arc<QueryContext> {
    fn tenant_label(&self) -> TenantLabel {
        ctx_tenant_label(self)
    }
}

fn ctx_tenant_label(ctx: &QueryContext) -> TenantLabel {
    let mgr = ctx.get_storage_cache_manager();
    TenantLabel {
        tenant_id: mgr.get_tenant_id().to_owned(),
        cluster_id: mgr.get_cluster_id().to_owned(),
    }
}
