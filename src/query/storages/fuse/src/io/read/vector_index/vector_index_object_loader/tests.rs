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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use databend_common_exception::ErrorCode;
use databend_common_expression::Column;
use databend_common_expression::types::F32;
use databend_common_expression::types::VectorColumn;
use databend_common_storage::StorageMetrics;
use databend_common_storage::StorageMetricsLayer;
use futures::future::join_all;
use opendal::services::Memory;
use tokio::sync::oneshot;

use super::*;

fn index_columns() -> Result<Vec<Column>> {
    let values = (0..64)
        .map(|i| F32::from((i as f32 * 0.17).sin()))
        .collect::<Vec<_>>();
    let column = Column::Vector(VectorColumn::Float32((values.into(), 8)));
    let (_, entries) = HNSWIndex::build(4, 8, 0, column, DistanceType::L2)?;
    Ok(entries.iter().map(|entry| entry.to_column()).collect())
}

fn open(columns: Vec<Column>) -> Result<HNSWIndex> {
    HNSWIndex::open(DistanceType::L2, 8, 8, columns)
}

#[tokio::test]
async fn test_warm_queries_reuse_opened_index() -> Result<()> {
    let cache = VectorIndexCache::new(1024 * 1024);
    let key = Uuid::new_v4().to_string();
    let columns = index_columns()?;
    let opens = AtomicUsize::new(0);
    let baseline = open(columns.clone())?;
    let first = load_cached_index(Some(cache.clone()), key.clone(), || {
        opens.fetch_add(1, Ordering::Relaxed);
        let columns = columns.clone();
        async move { open(columns) }
    })
    .await?;

    for i in 0..100 {
        let index = load_cached_index(Some(cache.clone()), key.clone(), || {
            opens.fetch_add(1, Ordering::Relaxed);
            let columns = columns.clone();
            async move { open(columns) }
        })
        .await?;
        assert!(Arc::ptr_eq(&first, &index));
        let query = vec![i as f32 * 0.01; 8];
        assert_eq!(
            index.index().search(3, &query)?,
            baseline.search(3, &query)?
        );
        assert_eq!(
            index.index().generate_scores(8, &query)?,
            baseline.generate_scores(8, &query)?
        );
    }
    assert_eq!(opens.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test]
async fn test_oversized_queries_reload_without_retaining_or_changing_results() -> Result<()> {
    let cache = VectorIndexCache::new(1);
    let key = Uuid::new_v4().to_string();
    let columns = index_columns()?;
    let opens = AtomicUsize::new(0);
    let baseline = open(columns.clone())?;
    for value in [0.0, 0.5] {
        let index = load_cached_index(Some(cache.clone()), key.clone(), || {
            opens.fetch_add(1, Ordering::Relaxed);
            let columns = columns.clone();
            async move { open(columns) }
        })
        .await?;
        let query = vec![value; 8];
        assert_eq!(
            index.index().search(3, &query)?,
            baseline.search(3, &query)?
        );
        assert_eq!(
            index.index().generate_scores(8, &query)?,
            baseline.generate_scores(8, &query)?
        );
        assert!(cache.is_empty());
        assert_eq!(cache.bytes_size(), 0);
    }
    assert_eq!(opens.load(Ordering::Relaxed), 2);
    Ok(())
}

#[tokio::test]
async fn test_concurrent_loads_share_results_even_without_admission() -> Result<()> {
    let columns = index_columns()?;
    // Exercise ordinary admission, oversized bypass, and a disabled cache separately.
    for cache in [
        Some(VectorIndexCache::new(1024 * 1024)),
        Some(VectorIndexCache::new(1)),
        None,
    ] {
        let key = Uuid::new_v4().to_string();
        let opens = AtomicUsize::new(0);
        let results = join_all((0..8).map(|_| {
            load_cached_index(cache.clone(), key.clone(), || {
                opens.fetch_add(1, Ordering::Relaxed);
                let columns = columns.clone();
                async move {
                    tokio::task::yield_now().await;
                    open(columns)
                }
            })
        }))
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        assert_eq!(opens.load(Ordering::Relaxed), 1);
        assert!(results.iter().all(|index| Arc::ptr_eq(index, &results[0])));
    }
    Ok(())
}

#[tokio::test]
async fn test_waiters_share_result_after_eviction() -> Result<()> {
    let cache = VectorIndexCache::new(1024 * 1024);
    let key = Uuid::new_v4().to_string();
    let columns = index_columns()?;
    let (release, released) = oneshot::channel();
    let mut first = Box::pin(load_cached_index(
        Some(cache.clone()),
        key.clone(),
        || async move {
            released.await.unwrap();
            open(columns)
        },
    ));
    assert!(first.as_mut().now_or_never().is_none());
    let mut second = Box::pin(load_cached_index(
        Some(cache.clone()),
        key.clone(),
        || async { panic!("the waiter must not reload an evicted result") },
    ));
    assert!(second.as_mut().now_or_never().is_none());
    release.send(()).unwrap();
    let first = first.await?;
    assert!(cache.evict(&key));
    let second = second.await?;
    assert!(Arc::ptr_eq(&first, &second));
    assert!(!INDEX_LOADS.lock().contains_key(&key));
    Ok(())
}

#[tokio::test]
async fn test_disabled_cache_does_not_retain_completed_loads() -> Result<()> {
    let key = Uuid::new_v4().to_string();
    let columns = index_columns()?;
    let first_columns = columns.clone();
    let first = load_cached_index(None, key.clone(), || async move { open(first_columns) }).await?;
    assert!(!INDEX_LOADS.lock().contains_key(&key));
    let second = load_cached_index(None, key.clone(), || async move { open(columns) }).await?;
    assert!(!Arc::ptr_eq(&first, &second));
    assert!(!INDEX_LOADS.lock().contains_key(&key));
    Ok(())
}

#[tokio::test]
async fn test_cancelling_first_waiter_does_not_cancel_other_waiters() -> Result<()> {
    let key = Uuid::new_v4().to_string();
    let columns = index_columns()?;
    let (release, released) = oneshot::channel();
    let opens = AtomicUsize::new(0);
    let mut first = Box::pin(load_cached_index(None, key.clone(), || {
        opens.fetch_add(1, Ordering::Relaxed);
        async move {
            released.await.unwrap();
            open(columns)
        }
    }));
    assert!(first.as_mut().now_or_never().is_none());

    let mut second = Box::pin(load_cached_index(None, key.clone(), || async {
        panic!("another waiter must reuse the in-flight load")
    }));
    assert!(second.as_mut().now_or_never().is_none());
    drop(first);
    release.send(()).unwrap();
    assert_eq!(
        second.await?.index().generate_scores(8, &[0.0; 8])?.len(),
        8
    );
    assert_eq!(opens.load(Ordering::Relaxed), 1);
    assert!(!INDEX_LOADS.lock().contains_key(&key));
    Ok(())
}

#[tokio::test]
async fn test_cancelling_all_waiters_cleans_up_and_allows_retry() -> Result<()> {
    let key = Uuid::new_v4().to_string();
    let columns = index_columns()?;
    let (release, released) = oneshot::channel::<()>();
    let source_columns = columns.clone();
    let mut load = Box::pin(load_cached_index(None, key.clone(), || async move {
        released.await.unwrap();
        open(source_columns)
    }));
    assert!(load.as_mut().now_or_never().is_none());
    drop(load);
    assert!(release.is_closed());
    assert!(!INDEX_LOADS.lock().contains_key(&key));
    load_cached_index(None, key, || async move { open(columns) }).await?;
    Ok(())
}

#[tokio::test]
async fn test_shared_failure_can_be_retried() -> Result<()> {
    let key = Uuid::new_v4().to_string();
    let loads = AtomicUsize::new(0);
    let failures = join_all((0..8).map(|_| {
        load_cached_index(None, key.clone(), || {
            loads.fetch_add(1, Ordering::Relaxed);
            async {
                tokio::task::yield_now().await;
                Err(ErrorCode::StorageOther("test load failure"))
            }
        })
    }))
    .await;
    assert_eq!(loads.load(Ordering::Relaxed), 1);
    for failure in failures {
        assert_eq!(failure.err().unwrap().message(), "test load failure");
    }
    assert!(!INDEX_LOADS.lock().contains_key(&key));

    let columns = index_columns()?;
    load_cached_index(None, key, || async move { open(columns) }).await?;
    Ok(())
}

#[tokio::test]
async fn test_different_keys_do_not_serialize_loading() -> Result<()> {
    let columns = index_columns()?;
    let (release, released) = oneshot::channel();
    let source_columns = columns.clone();
    let mut blocked = Box::pin(load_cached_index(
        None,
        Uuid::new_v4().to_string(),
        || async move {
            released.await.unwrap();
            open(source_columns)
        },
    ));
    assert!(blocked.as_mut().now_or_never().is_none());
    load_cached_index(
        None,
        Uuid::new_v4().to_string(),
        || async move { open(columns) },
    )
    .await?;
    release.send(()).unwrap();
    blocked.await?;
    Ok(())
}

#[test]
fn test_cache_key_includes_storage_and_index_identity() -> Result<()> {
    let operator = Operator::new(Memory::default())?.finish();
    let settings = ReadSettings {
        max_gap_size: 1024,
        max_range_size: 1024 * 1024,
        parquet_fast_read_bytes: 0,
    };
    let loader = VectorIndexLoader::new(operator.clone(), settings, DistanceType::L2, 8, vec![
        "0-l2_graph_links".to_string(),
    ]);
    let original = loader.cache_key(8, "index_v1.parquet")?;
    let same_operator = VectorIndexLoader::new(
        operator.clone(),
        settings,
        DistanceType::L2,
        8,
        loader.columns.clone(),
    );
    assert_eq!(original, same_operator.cache_key(8, "index_v1.parquet")?);
    let another_table_instance = VectorIndexLoader::new(
        operator.layer(StorageMetricsLayer::new(
            Arc::new(StorageMetrics::default()),
        )),
        settings,
        DistanceType::L2,
        8,
        loader.columns.clone(),
    );
    assert_eq!(
        original,
        another_table_instance.cache_key(8, "index_v1.parquet")?
    );
    assert_ne!(original, loader.cache_key(9, "index_v1.parquet")?);
    assert_ne!(original, loader.cache_key(8, "refreshed_v1.parquet")?);
    assert_ne!(original, loader.cache_key(8, "index_v2.parquet")?);
    let mut different = loader.clone();
    different.columns = vec!["1-l2_graph_links".to_string()];
    assert_ne!(original, different.cache_key(8, "index_v1.parquet")?);
    different = loader.clone();
    different.distance_type = DistanceType::L1;
    assert_ne!(original, different.cache_key(8, "index_v1.parquet")?);
    different = loader.clone();
    different.dim = 16;
    assert_ne!(original, different.cache_key(8, "index_v1.parquet")?);

    let other_storage = VectorIndexLoader::new(
        Operator::new(Memory::default())?.finish(),
        settings,
        DistanceType::L2,
        8,
        loader.columns.clone(),
    );
    assert_ne!(original, other_storage.cache_key(8, "index_v1.parquet")?);
    Ok(())
}
