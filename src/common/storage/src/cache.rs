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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
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
    cache: Arc<Cache<String, Arc<AtomicUsize>>>,
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

    /// Visit this path.
    ///
    /// Returns the previous value
    fn visit(&self, path: String) -> usize {
        let v = self.cache.get_with(path, || Arc::new(AtomicUsize::new(0)));
        v.fetch_add(1, Ordering::Relaxed)
    }
}

/// RangeCachePolicy will try to read and store cache based on range.
///
/// We will count recent `records`, if they have been visited at least `threshold`
/// times. We will cache it in the cache layer.
#[derive(Debug)]
pub struct RangeCachePolicy {
    visit: VisitStatistics,
    threshold: usize,
}

impl RangeCachePolicy {
    /// Create a new range cache policy.
    pub fn new(records: u64, threshold: usize) -> Self {
        RangeCachePolicy {
            visit: VisitStatistics::new(records),
            threshold,
        }
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

        let threshold = self.threshold;
        // Record a visit to cache_path.
        let count = self.visit.visit(cache_path.clone());

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

            // Start filling cache.
            let (rp, r) = inner.read(&path, args.clone()).await?;
            let size = rp.clone().into_metadata().content_length();

            // Ignore errors returned by cache services.
            let _ = cache.write(&cache_path, OpWrite::new(size), r).await;

            match cache.read(&cache_path, OpRead::default()).await {
                Ok(v) => Ok(v),
                // fallback to inner read no matter what error happened.
                Err(_) => inner.read(&path, args).await,
            }
        })
    }
}
