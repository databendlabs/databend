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
use opendal::Operator;

type CachedReader = InMemoryCacheReader<
    InvertedIndexDirectory,
    InvertedIndexFilterLoader,
    InvertedIndexFilterMeter,
>;

/// Loads data of invereted index filter
#[minitrace::trace]
pub(crate) async fn load_inverted_index_filter(
    dal: Operator,
    index_loc: String,
) -> Result<Arc<InvertedIndexDirectory>> {
    let storage_runtime = GlobalIORuntime::instance();
    let filter = {
        let reader = InvertedIndexFilter::new(index_loc, dal);
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
    pub fn new(index_loc: String, operator: Operator) -> Self {
        let loader = InvertedIndexFilterLoader { operator };

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
}

#[async_trait::async_trait]
impl Loader<InvertedIndexDirectory> for InvertedIndexFilterLoader {
    #[async_backtrace::framed]
    async fn load(&self, params: &LoadParams) -> Result<InvertedIndexDirectory> {
        let bytes = self.operator.read_with(&params.location).await?;

        let directory = InvertedIndexDirectory::try_create(bytes)?;
        Ok(directory)
    }

    fn cache_key(&self, params: &LoadParams) -> CacheKey {
        params.location.clone()
    }
}
