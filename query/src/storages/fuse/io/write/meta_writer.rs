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

use std::pin::Pin;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use futures::io::BufReader;
use futures::stream;
use opendal::BytesReader;

use super::cached_reader::CachedReader;
use super::cached_reader::HasTenantLabel;
use super::cached_reader::Loader;
use super::versioned_reader::VersionedReader;
use crate::sessions::QueryContext;
use crate::storages::fuse::cache::TenantLabel;
use crate::storages::fuse::io::TableMetaLocationGenerator;
use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::meta::SegmentInfoVersion;
use crate::storages::fuse::meta::SnapshotVersion;
use crate::storages::fuse::meta::TableSnapshot;

/// Provider of [BufReader]
///
/// Mainly used as a auxiliary facility in the implementation of [Loader], such that the acquirement
/// of an [BufReader] can be deferred or avoided (e.g. if hits cache).
#[async_trait::async_trait]
pub trait BufReaderProvider {
    async fn buf_reader(&self, path: &str, len: Option<u64>) -> Result<BufReader<BytesReader>>;
}

pub type SegmentInfoReader<'a> = CachedReader<SegmentInfo, &'a QueryContext>;
pub type TableSnapshotReader<'a> = CachedReader<TableSnapshot, &'a QueryContext>;

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
}

impl<'a> TableSnapshotReader<'a> {
    pub async fn read_snapshot_history(
        &self,
        latest_snapshot_location: Option<impl AsRef<str>>,
        format_version: u64,
        location_gen: TableMetaLocationGenerator,
    ) -> Result<Vec<Arc<TableSnapshot>>> {
        let mut snapshots = vec![];
        if let Some(loc) = latest_snapshot_location {
            let mut ver = format_version;
            let mut loc = loc.as_ref().to_string();
            loop {
                let snapshot = match self.read(loc, None, ver).await {
                    Ok(s) => s,
                    Err(e) => {
                        if e.code() == ErrorCode::storage_not_found_code() {
                            break;
                        } else {
                            return Err(e);
                        }
                    }
                };
                if let Some((id, v)) = snapshot.prev_snapshot_id {
                    ver = v;
                    loc = location_gen.snapshot_location_from_uuid(&id, v)?;
                    snapshots.push(snapshot);
                } else {
                    snapshots.push(snapshot);
                    break;
                }
            }
        }

        Ok(snapshots)
    }

    pub fn snapshot_history(
        &'a self,
        location: String,
        format_version: u64,
        location_gen: TableMetaLocationGenerator,
    ) -> Pin<Box<dyn futures::stream::Stream<Item = Result<Arc<TableSnapshot>>> + 'a + Send>> {
        let stream = stream::try_unfold(
            (self, location_gen, Some((location, format_version))),
            |(reader, gen, next)| async move {
                if let Some((loc, ver)) = next {
                    let snapshot = match reader.read(loc, None, ver).await {
                        Ok(s) => Ok(Some(s)),
                        Err(e) => {
                            if e.code() == ErrorCode::storage_not_found_code() {
                                Ok(None)
                            } else {
                                Err(e)
                            }
                        }
                    };
                    match snapshot {
                        Ok(Some(snapshot)) => {
                            if let Some((id, v)) = snapshot.prev_snapshot_id {
                                let new_ver = v;
                                let new_loc = gen.snapshot_location_from_uuid(&id, v)?;
                                Ok(Some((snapshot, (reader, gen, Some((new_loc, new_ver))))))
                            } else {
                                Ok(Some((snapshot, (reader, gen, None))))
                            }
                        }
                        Ok(None) => Ok(None),
                        Err(e) => Err(e),
                    }
                } else {
                    Ok(None)
                }
            },
        );
        Box::pin(stream)
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
        let version = SnapshotVersion::try_from(version)?;
        let reader = self.buf_reader(key, length_hint).await?;
        version.read(reader).await
    }
}

#[async_trait::async_trait]
impl<T> Loader<SegmentInfo> for T
where T: BufReaderProvider + Sync
{
    async fn load(&self, key: &str, length_hint: Option<u64>, version: u64) -> Result<SegmentInfo> {
        let version = SegmentInfoVersion::try_from(version)?;
        let reader = self.buf_reader(key, length_hint).await?;
        version.read(reader).await
    }
}

#[async_trait::async_trait]
impl BufReaderProvider for &QueryContext {
    async fn buf_reader(&self, path: &str, len: Option<u64>) -> Result<BufReader<BytesReader>> {
        let operator = self.get_storage_operator()?;
        let object = operator.object(path);

        let len = match len {
            Some(l) => l,
            None => {
                let meta = object.metadata().await?;

                meta.content_length()
            }
        };

        let reader = object.range_reader(..len).await?;
        let read_buffer_size = self.get_settings().get_storage_read_buffer_size()?;
        Ok(BufReader::with_capacity(
            read_buffer_size as usize,
            Box::new(reader),
        ))
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
