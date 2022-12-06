//  Copyright 2022 Datafuse Labs.
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

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::future::BoxFuture;
use moka::sync::Cache;
use opendal::layers::CachePolicy;
use opendal::raw::Accessor;
use opendal::raw::BytesReader;
use opendal::raw::RpRead;
use opendal::ErrorKind;
use opendal::OpRead;
use opendal::OpWrite;
use opendal::Result;

/// VisitStatistics is used to track visit statistics.
///
/// # Two-level Caching
///
/// Databend Query will have a two-level cache.
///
/// - The first cache layer is a fixed-size in-memory cache
/// - The second cache layer should be a storage service which slower than memory but quicker than object storage services like local fs or redis.
///
/// We will cache recent reading content into in-memory cache, and spill the
/// **hot** data into second cache layer.
///
/// # Visit Statistics
///
/// We will record visit statistics at first cache layer. If the path has been
/// accessed in recent, we will spill it to second cache layer too.
///
/// # Notes
///
/// The cache logic could be changed at anytime, PLEASE DON'T depend on it's behavior.
#[derive(Debug, Clone)]
pub struct VisitStatistics {
    cache: Arc<Cache<String, ()>>,
}

impl VisitStatistics {
    /// Create a new visit statistics with given capacity.
    pub fn new(capacity: u64) -> Self {
        VisitStatistics {
            cache: Arc::new(
                Cache::builder()
                    .max_capacity(capacity)
                    // Time to live (TTL): 30 minutes
                    //
                    // TODO: make this a user setting.
                    .time_to_live(Duration::from_secs(30 * 60))
                    // Time to idle (TTI):  5 minutes
                    //
                    // TODO: make this a user setting.
                    .time_to_idle(Duration::from_secs(5 * 60))
                    .build(),
            ),
        }
    }

    /// Is given path has been visited?
    fn is_visited(&self, path: &str) -> bool {
        self.cache.get(path).is_some()
    }

    /// Visit this path.
    fn visit(&self, path: String) {
        self.cache.insert(path, ())
    }
}

#[derive(Debug)]
pub struct MemoryCachePolicy {
    visit: VisitStatistics,
}

impl MemoryCachePolicy {
    pub fn new(visit: VisitStatistics) -> Self {
        MemoryCachePolicy { visit }
    }
}

#[async_trait]
impl CachePolicy for MemoryCachePolicy {
    fn on_read(
        &self,
        inner: Arc<dyn Accessor>,
        cache: Arc<dyn Accessor>,
        path: &str,
        args: OpRead,
    ) -> BoxFuture<'static, Result<(RpRead, BytesReader)>> {
        let path = path.to_string();

        Box::pin(range_caching(inner, cache, self.visit.clone(), path, args))
    }
}

/// TODO: implement more complex cache logic.
///
/// For example:
///
/// - Implement a top n heap, and only cache files exist in heap.
/// - Only cache data file, and ignore snapshot files.
#[derive(Debug)]
pub struct FuseCachePolicy {
    visit: VisitStatistics,
}

impl FuseCachePolicy {
    pub fn new(visit: VisitStatistics) -> Self {
        FuseCachePolicy { visit }
    }
}

#[async_trait]
impl CachePolicy for FuseCachePolicy {
    fn on_read(
        &self,
        inner: Arc<dyn Accessor>,
        cache: Arc<dyn Accessor>,
        path: &str,
        args: OpRead,
    ) -> BoxFuture<'static, Result<(RpRead, BytesReader)>> {
        let path = path.to_string();
        let cache_path = range_based_cache_path(&path, &args);

        if self.visit.is_visited(&cache_path) {
            Box::pin(range_caching(inner, cache, self.visit.clone(), path, args))
        } else {
            Box::pin(async move { inner.read(&path, args).await })
        }
    }
}

fn range_based_cache_path(path: &str, args: &OpRead) -> String {
    format!("{path}.cache-{}", args.range().to_string())
}

/// Cache file based on it's rane.
async fn range_caching(
    inner: Arc<dyn Accessor>,
    cache: Arc<dyn Accessor>,
    visit: VisitStatistics,
    path: String,
    args: OpRead,
) -> Result<(RpRead, BytesReader)> {
    let cache_path = range_based_cache_path(&path, &args);

    let v = match cache.read(&cache_path, OpRead::default()).await {
        Ok(v) => Ok(v),
        Err(err) if err.kind() == ErrorKind::ObjectNotFound => {
            let (rp, r) = inner.read(&path, args.clone()).await?;
            let size = rp.clone().into_metadata().content_length();

            // Ignore errors returned by cache services.
            let _ = cache.write(&cache_path, OpWrite::new(size), r).await;

            match cache.read(&cache_path, OpRead::default()).await {
                Ok(v) => Ok(v),
                Err(_) => inner.read(&path, args).await,
            }
        }
        Err(_) => inner.read(&path, args).await,
    };

    // Mark cache path has been visited.
    visit.visit(cache_path);

    v
}
