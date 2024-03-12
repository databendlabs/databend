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

use std::future::Future;
use std::sync::Arc;

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::TrySpawn;
use databend_common_base::GLOBAL_TASK;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_storages_common_cache::CacheKey;
use databend_storages_common_cache::InMemoryCacheReader;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_cache::Loader;
use databend_storages_common_cache_manager::CachedObject;
use databend_storages_common_cache_manager::InvertedIndexFilterMeter;
use databend_storages_common_index::InvertedIndexDirectory;
use databend_storages_common_table_meta::meta::IndexInfo;
use databend_storages_common_table_meta::meta::IndexSegmentInfo;
use databend_storages_common_table_meta::meta::Location;
use opendal::Operator;

use crate::io::MetaReaders;

type CachedReader = InMemoryCacheReader<
    InvertedIndexDirectory,
    InvertedIndexFilterLoader,
    InvertedIndexFilterMeter,
>;

/// Loads inverted index info data
/// read data from cache, or populate cache items if possible
#[minitrace::trace]
pub async fn load_inverted_index_info(
    dal: Operator,
    index_info_loc: Option<&Location>,
) -> Result<Option<Arc<IndexInfo>>> {
    match index_info_loc {
        Some((index_info_loc, ver)) => {
            let reader = MetaReaders::inverted_index_info_reader(dal);
            let params = LoadParams {
                location: index_info_loc.clone(),
                len_hint: None,
                ver: *ver,
                put_cache: true,
            };
            Ok(Some(reader.read(&params).await?))
        }
        None => Ok(None),
    }
}

/// Loads data of invereted index and create InvertedIndexDirectory
#[minitrace::trace]
pub(crate) async fn load_inverted_index_filter(
    dal: Operator,
    index_loc: String,
    index_segments: Vec<IndexSegmentInfo>,
) -> Result<Arc<InvertedIndexDirectory>> {
    let storage_runtime = GlobalIORuntime::instance();
    let filter = {
        let reader = InvertedIndexFilter::new(index_loc, dal, index_segments);
        async move { reader.read().await }
    }
    .execute_in_runtime(&storage_runtime)
    .await??;
    Ok(filter)
}

#[async_trait::async_trait]
trait InRuntime
where Self: Future
{
    async fn execute_in_runtime(self, runtime: &Runtime) -> Result<Self::Output>;
}

#[async_trait::async_trait]
impl<T> InRuntime for T
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    #[async_backtrace::framed]
    async fn execute_in_runtime(self, runtime: &Runtime) -> Result<T::Output> {
        runtime
            .try_spawn(GLOBAL_TASK, self)?
            .await
            .map_err(|e| ErrorCode::TokioError(format!("runtime join error. {}", e)))
    }
}

pub struct InvertedIndexFilter {
    cached_reader: CachedReader,
    param: LoadParams,
}

impl InvertedIndexFilter {
    pub fn new(
        index_loc: String,
        operator: Operator,
        index_segments: Vec<IndexSegmentInfo>,
    ) -> Self {
        let loader = InvertedIndexFilterLoader {
            operator,
            index_segments,
        };

        let cached_reader = CachedReader::new(InvertedIndexDirectory::cache(), loader);

        let param = LoadParams {
            location: index_loc,
            len_hint: None,
            ver: 0,
            put_cache: true,
        };

        InvertedIndexFilter {
            cached_reader,
            param,
        }
    }

    #[async_backtrace::framed]
    pub async fn read(&self) -> Result<Arc<InvertedIndexDirectory>> {
        self.cached_reader.read(&self.param).await
    }
}

/// Loader read inverted index data and create InvertedIndexDirectory
pub struct InvertedIndexFilterLoader {
    pub operator: Operator,
    pub index_segments: Vec<IndexSegmentInfo>,
}

#[async_trait::async_trait]
impl Loader<InvertedIndexDirectory> for InvertedIndexFilterLoader {
    #[async_backtrace::framed]
    async fn load(&self, params: &LoadParams) -> Result<InvertedIndexDirectory> {
        let bytes = self.operator.read_with(&params.location).await?;

        let directory = InvertedIndexDirectory::try_create(bytes, self.index_segments.clone())?;
        Ok(directory)
    }

    fn cache_key(&self, params: &LoadParams) -> CacheKey {
        params.location.clone()
    }
}
