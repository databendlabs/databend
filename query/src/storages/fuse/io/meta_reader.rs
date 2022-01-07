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

//use std::sync::Arc;
//
//use common_dal::DataAccessor;
//use common_exception::ErrorCode;
//use common_exception::Result;
//use serde::de::DeserializeOwned;
//
//use crate::sessions::QueryContext;
//use crate::storages::cache::StorageCache;
//use crate::storages::fuse::io::snapshot_location;
//use crate::storages::fuse::meta::SegmentInfo;
//use crate::storages::fuse::meta::TableSnapshot;

//async fn read_obj<T: DeserializeOwned>(
//    da: &dyn DataAccessor,
//    loc: impl AsRef<str>,
//    cache: Arc<Option<Box<dyn StorageCache>>>,
//) -> Result<T> {
//    let loc = loc.as_ref();
//    let bytes = if let Some(cache) = &*cache {
//        cache.get(loc, da).await?
//    } else {
//        da.read(loc).await?
//    };
//    let r = serde_json::from_slice::<T>(&bytes)?;
//    Ok(r)
//}

//pub struct SegmentReader {}
//
//impl SegmentReader {
//    pub async fn read(
//        da: &dyn DataAccessor,
//        loc: impl AsRef<str>,
//        cache: Arc<Option<Box<dyn StorageCache>>>,
//    ) -> Result<SegmentInfo> {
//        let segment_info: SegmentInfo = read_obj(da, loc, cache).await?;
//        Ok(segment_info)
//    }
//}
