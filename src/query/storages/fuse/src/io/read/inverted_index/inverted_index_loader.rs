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
use std::time::Instant;

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::TrySpawn;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_read_milliseconds;
use databend_storages_common_cache::CacheKey;
use databend_storages_common_cache::InMemoryCacheReader;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_cache::Loader;
use databend_storages_common_cache_manager::CachedObject;
use databend_storages_common_cache_manager::InvertedIndexFileMeter;
use databend_storages_common_index::InvertedIndexDirectory;
use databend_storages_common_index::InvertedIndexMeta;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use futures_util::future::try_join_all;
use opendal::Operator;

use crate::index::InvertedIndexFile;
use crate::io::MetaReaders;

type CachedReader =
    InMemoryCacheReader<InvertedIndexFile, InvertedIndexFileLoader, InvertedIndexFileMeter>;

const INDEX_COLUMN_NAMES: [&str; 8] = [
    "fast",
    "store",
    "fieldnorm",
    "pos",
    "idx",
    "term",
    "meta.json",
    ".managed.json",
];

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
            .try_spawn(self)?
            .await
            .map_err(|e| ErrorCode::TokioError(format!("runtime join error. {}", e)))
    }
}

/// Loads inverted index meta data
/// read data from cache, or populate cache items if possible
#[minitrace::trace]
async fn load_inverted_index_meta(dal: Operator, path: &str) -> Result<Arc<InvertedIndexMeta>> {
    let path_owned = path.to_owned();
    async move {
        let reader = MetaReaders::inverted_index_meta_reader(dal);
        let version = 0;

        let load_params = LoadParams {
            location: path_owned,
            len_hint: None,
            ver: version,
            put_cache: true,
        };

        reader.read(&load_params).await
    }
    .execute_in_runtime(&GlobalIORuntime::instance())
    .await?
}

/// Loads bytes of each inverted index files
/// read data from cache, or populate cache items if possible
#[minitrace::trace]
async fn load_inverted_index_file<'a>(
    index_path: &'a str,
    name: &'a str,
    col_meta: &'a SingleColumnMeta,
    need_position: bool,
    dal: &'a Operator,
) -> Result<Arc<InvertedIndexFile>> {
    // Because the position file is relatively large, reading it will take more time.
    // And position data is only used when the query has phrase terms.
    // If the query has no phrase terms, we can ignore it and use empty position data instead.
    if name == "pos" && !need_position {
        let file = InvertedIndexFile::try_create(name.to_owned(), vec![])?;
        return Ok(Arc::new(file));
    }

    let storage_runtime = GlobalIORuntime::instance();
    let file = {
        let inverted_index_file_reader = InvertedIndexFileReader::new(
            index_path.to_owned(),
            name.to_owned(),
            col_meta,
            dal.clone(),
        );
        async move { inverted_index_file_reader.read().await }
    }
    .execute_in_runtime(&storage_runtime)
    .await??;
    Ok(file)
}

/// load inverted index directory
#[minitrace::trace]
pub(crate) async fn load_inverted_index_directory<'a>(
    dal: Operator,
    need_position: bool,
    field_nums: usize,
    index_path: &'a str,
) -> Result<InvertedIndexDirectory> {
    let start = Instant::now();
    // load inverted index meta, contains the offsets of each files.
    let inverted_index_meta = load_inverted_index_meta(dal.clone(), index_path).await?;

    // load inverted index files, usually including following eight files:
    // 1. fast file
    // 2. store file
    // 3. fieldnorm file
    // 4. position file
    // 5. idx file
    // 6. term file
    // 7. meta.json file
    // 8. .managed.json file
    let futs = inverted_index_meta
        .columns
        .iter()
        .map(|(name, column_meta)| {
            load_inverted_index_file(index_path, name, column_meta, need_position, &dal)
        })
        .collect::<Vec<_>>();

    let files: Vec<_> = try_join_all(futs).await?.into_iter().collect();
    // use those files to create inverted index directory
    let directory = InvertedIndexDirectory::try_create(field_nums, files)?;

    // Perf.
    {
        metrics_inc_block_inverted_index_read_milliseconds(start.elapsed().as_millis() as u64);
    }

    Ok(directory)
}

/// Read the inverted index file data.
pub struct InvertedIndexFileReader {
    cached_reader: CachedReader,
    param: LoadParams,
}

impl InvertedIndexFileReader {
    pub fn new(
        index_path: String,
        name: String,
        column_meta: &SingleColumnMeta,
        operator: Operator,
    ) -> Self {
        let cache_key = Self::cache_key_of_column(&index_path, &name);

        let loader = InvertedIndexFileLoader {
            offset: column_meta.offset,
            len: column_meta.len,
            name,
            cache_key,
            operator,
        };

        let cached_reader = CachedReader::new(InvertedIndexFile::cache(), loader);

        let param = LoadParams {
            location: index_path,
            len_hint: None,
            ver: 0,
            put_cache: true,
        };

        InvertedIndexFileReader {
            cached_reader,
            param,
        }
    }

    #[async_backtrace::framed]
    pub async fn read(&self) -> Result<Arc<InvertedIndexFile>> {
        self.cached_reader.read(&self.param).await
    }

    fn cache_key_of_column(index_path: &str, index_column_name: &str) -> String {
        format!("{index_path}-{index_column_name}")
    }

    pub(crate) fn cache_key_of_index_columns(index_path: &str) -> Vec<String> {
        INDEX_COLUMN_NAMES
            .iter()
            .map(|column_name| Self::cache_key_of_column(index_path, column_name))
            .collect()
    }
}

/// Loader that fetch range of the target object with customized cache key
pub struct InvertedIndexFileLoader {
    pub offset: u64,
    pub len: u64,
    pub name: String,
    pub cache_key: String,
    pub operator: Operator,
}

#[async_trait::async_trait]
impl Loader<InvertedIndexFile> for InvertedIndexFileLoader {
    #[async_backtrace::framed]
    async fn load(&self, params: &LoadParams) -> Result<InvertedIndexFile> {
        let bytes = self
            .operator
            .read_with(&params.location)
            .range(self.offset..self.offset + self.len)
            .await?;

        InvertedIndexFile::try_create(self.name.clone(), bytes.to_vec())
    }

    fn cache_key(&self, _params: &LoadParams) -> CacheKey {
        self.cache_key.clone()
    }
}
