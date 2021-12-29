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
use common_arrow::arrow::io::parquet::read::schema::FileMetaData;
use common_cache::storage::StorageCache;
use common_dal::DataAccessor;
use common_exception::ErrorCode;
use common_exception::Result;
use serde::de::DeserializeOwned;

use crate::storages::fuse::io::snapshot_location;
use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::meta::TableSnapshot;

async fn read_obj<T: DeserializeOwned>(
    da: &dyn DataAccessor,
    loc: impl AsRef<str>,
    cache: Arc<Option<Box<dyn StorageCache>>>,
) -> Result<T> {
    let loc = loc.as_ref();
    let bytes = if let Some(cache) = &*cache {
        cache.get(loc, da).await?
    } else {
        da.read(loc).await?
    };
    let r = serde_json::from_slice::<T>(&bytes)?;
    Ok(r)
}

pub struct SnapshotReader {}

impl SnapshotReader {
    pub async fn read(
        da: &dyn DataAccessor,
        loc: impl AsRef<str>,
        cache: Arc<Option<Box<dyn StorageCache>>>,
    ) -> Result<TableSnapshot> {
        let snapshot: TableSnapshot = read_obj(da, loc, cache).await?;
        Ok(snapshot)
    }

    pub async fn read_snapshot_history(
        data_accessor: &dyn DataAccessor,
        latest_snapshot_location: Option<&String>,
        cache: Arc<Option<Box<dyn StorageCache>>>,
    ) -> Result<Vec<TableSnapshot>> {
        let mut snapshots = vec![];
        let mut current_snapshot_location = latest_snapshot_location.cloned();
        while let Some(loc) = current_snapshot_location {
            let r = Self::read(data_accessor, loc, cache.clone()).await;

            let snapshot = match r {
                Ok(s) => s,
                Err(e) if e.code() == ErrorCode::dal_path_not_found_code() => {
                    // snapshot has been truncated
                    break;
                }
                Err(e) => return Err(e),
            };
            let prev = snapshot.prev_snapshot_id;
            snapshots.push(snapshot);
            current_snapshot_location = prev.map(|id| snapshot_location(&id));
        }
        Ok(snapshots)
    }
}

pub struct SegmentReader {}

impl SegmentReader {
    pub async fn read(
        da: &dyn DataAccessor,
        loc: impl AsRef<str>,
        cache: Arc<Option<Box<dyn StorageCache>>>,
    ) -> Result<SegmentInfo> {
        let segment_info: SegmentInfo = read_obj(da, loc, cache).await?;
        Ok(segment_info)
    }
}

pub struct ParquetMetaReader {}

impl ParquetMetaReader {
    pub async fn read(
        da: &dyn DataAccessor,
        loc: impl AsRef<str>,
        _cache: Arc<Option<Box<dyn StorageCache>>>,
    ) -> Result<FileMetaData> {
        let mut reader = da.get_input_stream(loc.as_ref(), None)?;
        read_metadata_async(&mut reader)
            .await
            .map_err(|e| ErrorCode::ParquetError(e.to_string()))
    }
}
