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

use std::sync::Arc;

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::LimitMemGuard;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_storages_common_cache::DiskCachedStorage;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::SegmentStatistics;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::TableSnapshotStatistics;
use databend_storages_common_table_meta::meta::Versioned;
use opendal::Operator;

use crate::io::FuseCaches;

#[async_trait::async_trait]
pub trait MetaWriter<T> {
    async fn write_meta(&self, data_accessor: &Operator, location: &str) -> Result<()>;
}

#[async_trait::async_trait]
impl<T> MetaWriter<T> for T
where T: Marshal + Sync + Send
{
    #[async_backtrace::framed]
    async fn write_meta(&self, data_accessor: &Operator, location: &str) -> Result<()> {
        data_accessor.write(location, self.marshal()?).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
pub trait CachedMetaWriter<T> {
    async fn write_meta_through_cache(
        &self,
        data_accessor: &Operator,
        location: &str,
    ) -> Result<()>;
}

#[async_trait::async_trait]
impl CachedMetaWriter<SegmentInfo> for SegmentInfo {
    #[async_backtrace::framed]
    async fn write_meta_through_cache(
        &self,
        data_accessor: &Operator,
        location: &str,
    ) -> Result<()> {
        let bytes = self.marshal()?;
        let decoded: Option<CompactSegmentInfo> = match FuseCaches::instance().segments.is_enabled()
        {
            true => Some(self.try_into()?),
            false => None,
        };
        // Segments are rewritten in place by ANALYZE: after the write, the memory cache takes
        // the new value and the disk cache drops the old bytes. The write and both updates run
        // to their end on the IO runtime when the caller is cancelled, even with the write in
        // flight: a rewrite that went through leaves no stale entry. Nor are the updates held to
        // the query's memory limit, which the task keeps.
        let dal = data_accessor.clone();
        let location = location.to_string();
        GlobalIORuntime::instance()
            .spawn(async move {
                dal.write(&location, bytes).await?;
                {
                    let _unlimited = LimitMemGuard::enter_unlimited();
                    let segments = &FuseCaches::instance().segments;
                    match decoded {
                        Some(decoded) => segments.put(&location, Arc::new(decoded)),
                        // The cache may have been turned on since, and filled by a reader with
                        // the segment as it was.
                        None => segments.evict(&location),
                    }
                }
                DiskCachedStorage::new(dal).evict_objects(&[location]).await
            })
            .await
            .map_err(|e| ErrorCode::Internal(format!("segment rewrite task failed: {e}")))?
    }
}

trait Marshal {
    fn marshal(&self) -> Result<Vec<u8>>;
}

impl Marshal for SegmentInfo {
    fn marshal(&self) -> Result<Vec<u8>> {
        // make sure the table meta we write down to object store always has the current version
        // can we expressed as type constraint?
        assert_eq!(self.format_version, SegmentInfo::VERSION);
        self.to_bytes()
    }
}

impl Marshal for TableSnapshot {
    fn marshal(&self) -> Result<Vec<u8>> {
        // make sure the table meta we write down to object store always has the current version
        // can not by expressed as type constraint yet.
        assert_eq!(self.format_version, TableSnapshot::VERSION);
        self.to_bytes()
    }
}

impl Marshal for TableSnapshotStatistics {
    fn marshal(&self) -> Result<Vec<u8>> {
        // make sure the table meta we write down to object store always has the current version
        // can we expressed as type constraint?
        assert_eq!(self.format_version, TableSnapshotStatistics::VERSION);
        let bytes = serde_json::to_vec(self)?;
        Ok(bytes)
    }
}

impl Marshal for SegmentStatistics {
    fn marshal(&self) -> Result<Vec<u8>> {
        assert_eq!(self.format_version, SegmentStatistics::VERSION);
        self.to_bytes()
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use databend_common_base::runtime::catch_unwind;
    use databend_common_expression::TableSchema;
    use databend_storages_common_table_meta::meta::Statistics;
    use databend_storages_common_table_meta::meta::TableMetaTimestamps;

    use super::*;

    /// Holds every write until a permit is added.
    #[derive(Clone)]
    struct StallWrites(Arc<tokio::sync::Semaphore>);

    impl<A: opendal::raw::Access> opendal::raw::Layer<A> for StallWrites {
        type LayeredAccess = StalledAccessor<A>;

        fn layer(&self, inner: A) -> Self::LayeredAccess {
            StalledAccessor {
                inner,
                release: self.0.clone(),
            }
        }
    }

    struct StalledAccessor<A> {
        inner: A,
        release: Arc<tokio::sync::Semaphore>,
    }

    impl<A> std::fmt::Debug for StalledAccessor<A> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("StalledAccessor").finish()
        }
    }

    impl<A: opendal::raw::Access> opendal::raw::LayeredAccess for StalledAccessor<A> {
        type Inner = A;
        type Reader = A::Reader;
        type Writer = A::Writer;
        type Lister = A::Lister;
        type Deleter = A::Deleter;

        fn inner(&self) -> &Self::Inner {
            &self.inner
        }

        async fn read(
            &self,
            path: &str,
            args: opendal::raw::OpRead,
        ) -> opendal::Result<(opendal::raw::RpRead, Self::Reader)> {
            self.inner.read(path, args).await
        }

        async fn write(
            &self,
            path: &str,
            args: opendal::raw::OpWrite,
        ) -> opendal::Result<(opendal::raw::RpWrite, Self::Writer)> {
            let _permit = self.release.acquire().await.unwrap();
            self.inner.write(path, args).await
        }

        async fn list(
            &self,
            path: &str,
            args: opendal::raw::OpList,
        ) -> opendal::Result<(opendal::raw::RpList, Self::Lister)> {
            self.inner.list(path, args).await
        }

        async fn delete(&self) -> opendal::Result<(opendal::raw::RpDelete, Self::Deleter)> {
            self.inner.delete().await
        }
    }

    /// A caller cancelled with the in-place write in flight: the write and the cache updates
    /// still run to their end.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_segment_rewrite_survives_cancellation_in_flight() -> Result<()> {
        crate::test_utils::init_test_globals()?;
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let op = Operator::new(opendal::services::Memory::default())?
            .layer(StallWrites(release.clone()))
            .finish();
        let location = "segment_rewrite_cancelled/segment";
        let segment = SegmentInfo::new(vec![], Statistics::default());

        let rewrite = segment.write_meta_through_cache(&op, location);
        let wait = std::time::Duration::from_millis(100);
        assert!(tokio::time::timeout(wait, rewrite).await.is_err());
        release.add_permits(1);

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !op.exists(location).await? {
            assert!(
                std::time::Instant::now() < deadline,
                "the cancelled rewrite was not written"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        Ok(())
    }

    /// A rewrite that prepared no replacement, the cache being off, still drops the old
    /// segment a reader cached once the cache was turned back on.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_segment_rewrite_evicts_what_it_did_not_replace() -> Result<()> {
        use databend_common_config::CacheConfig;
        use databend_storages_common_cache::CacheKind;
        use databend_storages_common_cache::CacheManager;
        use databend_storages_common_cache::kinds::segment;

        use crate::io::SegmentInfoReader;

        crate::test_utils::init_test_globals()?;
        let manager = CacheManager::instance();
        let name = CacheKind::of::<segment::Info>().name();
        let segments = &FuseCaches::instance().segments;
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let op = Operator::new(opendal::services::Memory::default())?
            .layer(StallWrites(release.clone()))
            .finish();
        let location = "segment_rewrite_not_replaced/segment";

        // Off when the rewrite starts: it prepares no replacement.
        manager.set_cache_capacity(name, 0)?;
        assert!(!segments.is_enabled());
        let rewritten = SegmentInfo::new(vec![], Statistics::default());
        let rewrite = rewritten.write_meta_through_cache(&op, location);
        let wait = std::time::Duration::from_millis(100);
        assert!(tokio::time::timeout(wait, rewrite).await.is_err());

        // Back on, a reader caches the old segment before the write commits.
        let capacity = CacheConfig::default().table_meta_segment_bytes;
        manager.set_cache_capacity(name, capacity)?;
        let old = SegmentInfo::new(vec![], Statistics {
            row_count: 7,
            ..Default::default()
        });
        segments.put(location, Arc::new((&old).try_into()?));
        release.add_permits(1);

        let reader = SegmentInfoReader::new(op.clone(), Arc::new(TableSchema::empty()));
        let key = (location.to_string(), SegmentInfo::VERSION);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if let Ok(segment) = segments.get(&reader, key.clone()).await
                && segment.summary.row_count == 0
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the old segment stayed cached"
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        Ok(())
    }

    #[test]
    fn test_segment_format_version_validation() {
        // old versions are not allowed (runtime panics)
        for v in 0..SegmentInfo::VERSION {
            let r = catch_unwind(|| {
                let mut segment = SegmentInfo::new(vec![], Statistics::default());
                segment.format_version = v;
                let _ = segment.marshal();
            });
            assert!(r.is_err())
        }

        // current version allowed
        let segment = SegmentInfo::new(vec![], Statistics::default());
        segment.marshal().unwrap();
    }

    #[test]
    fn test_snapshot_format_version_validation() {
        // old versions are not allowed (runtime panics)
        for v in 0..TableSnapshot::VERSION {
            let r = catch_unwind(|| {
                let mut snapshot = TableSnapshot::try_new(
                    None,
                    None,
                    TableSchema::default(),
                    Statistics::default(),
                    vec![],
                    None,
                    None,
                    // Dummy timestamps for test
                    TableMetaTimestamps::new(None, Duration::hours(1)),
                )
                .unwrap();
                snapshot.format_version = v;
                let _ = snapshot.marshal();
            });
            assert!(r.is_err())
        }

        // current version allowed
        let snapshot = TableSnapshot::try_new(
            None,
            None,
            TableSchema::default(),
            Statistics::default(),
            vec![],
            None,
            None,
            // Dummy timestamps for test
            TableMetaTimestamps::new(None, Duration::hours(1)),
        )
        .unwrap();
        snapshot.marshal().unwrap();
    }
}
