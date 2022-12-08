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

//! Range caching is the simplest cache policy.
//!
//! By range caching we will cache the content by given path and range. Content fron (path, range) will be saved into `{path}-cache-{range}`.
//!
//! - For every request, we will try to load from cache first.
//! - If cache missed, we will load data from inner storage.
//! - If the path has been requests over threshold, we will try to fill it in the cache.

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use common_base::base::tokio::sync::Semaphore;
use common_base::base::GlobalIORuntime;
use common_base::base::TrySpawn;
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
#[derive(Clone)]
pub struct VisitStatistics {
    cache: Cache<String, usize>,
}

impl Debug for VisitStatistics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VisitStatistics")
            .field("count", &self.cache.entry_count())
            .finish()
    }
}

impl VisitStatistics {
    /// Create a new visit statistics with given capacity.
    pub fn new(capacity: u64) -> Self {
        VisitStatistics {
            cache: Cache::builder()
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
        }
    }

    /// Visit this path, returns the previous value
    ///
    /// This is not an atomic operation so it's value is inaccurate.
    /// But it's OK, in the worse case, we will miss some path that need to
    ///  cache.
    fn visit(&self, path: String) -> usize {
        let v = self.cache.get(&path).unwrap_or_default();
        self.cache.insert(path, v + 1);
        v
    }
}

/// RangeCachePolicy will try to read and store cache based on range.
///
/// We will count recent `records`, if they have been visited at least `threshold`
/// times. We will cache it in the cache layer.
#[derive(Debug, Clone)]
pub struct RangeCachePolicy {
    visit: VisitStatistics,
    threshold: usize,

    enable_async: bool,
    concurrency: Arc<Semaphore>,
}

impl RangeCachePolicy {
    /// Create a new range cache policy.
    pub fn new(records: u64, threshold: usize) -> Self {
        RangeCachePolicy {
            visit: VisitStatistics::new(records),
            threshold,
            enable_async: false,
            concurrency: Arc::new(Semaphore::new(16)),
        }
    }

    /// Enable async caching.
    pub fn enable_async(mut self) -> Self {
        self.enable_async = true;
        self
    }

    fn cache_path(&self, path: &str, args: &OpRead) -> String {
        format!("{path}.cache-{}", args.range())
    }
}

#[async_trait]
impl CachePolicy for RangeCachePolicy {
    fn on_read(
        &self,
        inner: Arc<dyn Accessor>,
        cache: Arc<dyn Accessor>,
        path: &str,
        args: OpRead,
    ) -> BoxFuture<'static, Result<(RpRead, BytesReader)>> {
        let path = path.to_string();
        let cache_path = self.cache_path(&path, &args);

        let enable_async = self.enable_async;
        let threshold = self.threshold;
        // Record a visit to cache_path.
        let count = self.visit.visit(cache_path.clone());
        let concurrency = self.concurrency.clone();

        Box::pin(async move {
            match cache.read(&cache_path, OpRead::default()).await {
                Ok(v) => return Ok(v),
                Err(err) => {
                    // If error's kind is not object not found, we should return
                    // inner read directly. the cache services chould be down.
                    if err.kind() != ErrorKind::ObjectNotFound {
                        return inner.read(&path, args).await;
                    }
                    // The path is not warm enough, we just go back to inner.
                    if count < threshold {
                        return inner.read(&path, args).await;
                    }
                }
            };

            // permit will be dropped after cache filled.
            let _permit = match concurrency.try_acquire() {
                Ok(permit) => permit,
                Err(_) => return inner.read(&path, args).await,
            };

            // Start filling cache.
            let (rp, r) = inner.read(&path, args.clone()).await?;
            let size = rp.clone().into_metadata().content_length();

            if enable_async {
                GlobalIORuntime::instance().spawn(async move {
                    // Ignore errors returned by cache services.
                    let _ = cache.write(&cache_path, OpWrite::new(size), r).await;
                });

                inner.read(&path, args).await
            } else {
                let _ = cache
                    .write(&cache_path, OpWrite::new(size), Box::new(r))
                    .await;

                match cache.read(&cache_path, OpRead::default()).await {
                    Ok(r) => Ok(r),
                    Err(_) => inner.read(&path, args).await,
                }
            }
        })
    }
}
