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

use common_exception::Result;
use opendal::Operator;
use serde::Serialize;
use storages_common_cache::CacheAccessor;
use storages_common_cache_manager::CachedObject;

#[async_trait::async_trait]
pub trait MetaWriter<T> {
    /// If meta has a `to_bytes` function, such as `SegmentInfo` and `TableSnapshot`
    /// We should not use `write_meta`. Instead, use `write_meta_data`
    async fn write_meta(&self, data_accessor: &Operator, location: &str) -> Result<()>;

    async fn write_meta_data(
        &self,
        data_accessor: &Operator,
        location: &str,
        bs: Vec<u8>,
    ) -> Result<()>;
}

#[async_trait::async_trait]
impl<T> MetaWriter<T> for T
where T: Serialize + Sync + Send
{
    #[async_backtrace::framed]
    async fn write_meta(&self, data_accessor: &Operator, location: &str) -> Result<()> {
        write_to_storage(data_accessor, location, &self).await?;
        Ok(())
    }

    #[async_backtrace::framed]
    async fn write_meta_data(
        &self,
        data_accessor: &Operator,
        location: &str,
        bs: Vec<u8>,
    ) -> Result<()> {
        data_accessor.write(location, bs).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
pub trait CachedMetaWriter<T> {
    /// If meta has a `to_bytes` function, such as `SegmentInfo` and `TableSnapshot`
    /// We should not use `write_meta_through_cache`. Instead, use `write_meta_data_through_cache`
    async fn write_meta_through_cache(self, data_accessor: &Operator, location: &str)
    -> Result<()>;

    async fn write_meta_data_through_cache(
        self,
        data_accessor: &Operator,
        location: &str,
        bs: Vec<u8>,
    ) -> Result<()>;
}

#[async_trait::async_trait]
impl<T, C> CachedMetaWriter<T> for T
where
    T: CachedObject<T, Cache = C> + Send + Sync,
    T: Serialize,
    C: CacheAccessor<String, T>,
{
    #[async_backtrace::framed]
    async fn write_meta_through_cache(
        self,
        data_accessor: &Operator,
        location: &str,
    ) -> Result<()> {
        write_to_storage(data_accessor, location, &self).await?;
        if let Some(cache) = T::cache() {
            cache.put(location.to_owned(), Arc::new(self))
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn write_meta_data_through_cache(
        self,
        data_accessor: &Operator,
        location: &str,
        bs: Vec<u8>,
    ) -> Result<()> {
        data_accessor.write(location, bs).await?;
        if let Some(cache) = T::cache() {
            cache.put(location.to_owned(), Arc::new(self))
        }
        Ok(())
    }
}

async fn write_to_storage<T>(data_accessor: &Operator, location: &str, meta: &T) -> Result<()>
where T: Serialize {
    let bs = serde_json::to_vec(&meta)?;
    data_accessor.write(location, bs).await?;

    Ok(())
}

// TODO batch write
// async fn batch_write_to_storage<T>(
//    data_accessor: &Operator,
//    location: &str,
//    meta: &[&T],
//) -> Result<()>
// where
//    T: Serialize,
//{
//    todo!()
//}
