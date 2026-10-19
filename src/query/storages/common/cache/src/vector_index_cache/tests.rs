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

use databend_common_expression::Column;
use databend_common_expression::types::F32;
use databend_common_expression::types::VectorColumn;
use databend_storages_common_index::DistanceType;

use super::*;

const COUNT: usize = 8;

fn index_columns(dim: usize) -> Vec<Column> {
    let values = (0..COUNT * dim)
        .map(|i| F32::from((i as f32 * 0.17).sin()))
        .collect::<Vec<_>>();
    let column = Column::Vector(VectorColumn::Float32((values.into(), dim)));
    let (_, entries) = HNSWIndex::build(4, 8, 0, column, DistanceType::L2).unwrap();
    entries.iter().map(|entry| entry.to_column()).collect()
}

fn index(columns: &[Column], dim: usize) -> CachedVectorIndex {
    CachedVectorIndex::new(HNSWIndex::open(DistanceType::L2, dim, COUNT, columns.to_vec()).unwrap())
}

#[test]
fn test_evicted_index_stays_charged_until_last_reader() {
    let columns = index_columns(8);
    let value = index(&columns, 8);
    let size = value.memory_size as u64;
    let cache = VectorIndexCache::new(size as usize);
    let reader = cache.insert("index".to_string(), value);
    assert_eq!(cache.bytes_size(), size);
    assert!(cache.evict("index"));
    assert!(cache.is_empty());
    assert_eq!(cache.bytes_size(), size);

    let another_reader = reader.clone();
    drop(reader);
    assert_eq!(cache.bytes_size(), size);
    drop(another_reader);
    assert_eq!(cache.bytes_size(), 0);
}

#[test]
fn test_oversized_index_does_not_evict_hot_index() {
    let columns = index_columns(8);
    let value = index(&columns, 8);
    let size = value.memory_size;
    let cache = VectorIndexCache::new(size);
    drop(cache.insert("hot".to_string(), value));

    let large_columns = index_columns(256);
    let large = index(&large_columns, 256);
    assert!(large.memory_size > size);
    let reader = cache.insert("large".to_string(), large);
    let baseline = HNSWIndex::open(DistanceType::L2, 256, COUNT, large_columns).unwrap();
    let query = vec![0.0; 256];
    assert_eq!(
        reader.index().search(3, &query).unwrap(),
        baseline.search(3, &query).unwrap()
    );
    assert_eq!(
        reader
            .index()
            .generate_scores(COUNT as u32, &query)
            .unwrap(),
        baseline.generate_scores(COUNT as u32, &query).unwrap()
    );
    assert!(!cache.contains_key("large"));
    assert!(cache.contains_key("hot"));
    assert_eq!(cache.bytes_size(), size as u64);
    drop(reader);
    assert_eq!(cache.bytes_size(), size as u64);
}

#[test]
fn test_pinned_budget_bypasses_admission_then_allows_lru_eviction() {
    let columns = index_columns(8);
    let value = index(&columns, 8);
    let size = value.memory_size;
    let cache = VectorIndexCache::new(size);
    let pinned = cache.insert("first".to_string(), value);

    let bypassed = cache.insert("second".to_string(), index(&columns, 8));
    assert!(cache.contains_key("first"));
    assert!(!cache.contains_key("second"));
    assert_eq!(cache.bytes_size(), size as u64);
    drop(bypassed);
    drop(pinned);

    let second = cache.insert("second".to_string(), index(&columns, 8));
    assert!(!cache.contains_key("first"));
    assert!(cache.contains_key("second"));
    assert_eq!(cache.bytes_size(), size as u64);
    cache.clear();
    assert!(cache.is_empty());
    assert_eq!(cache.bytes_size(), size as u64);
    drop(second);
    assert_eq!(cache.bytes_size(), 0);
}

#[test]
fn test_shrinking_and_disabling_preserve_live_charges() {
    let columns = index_columns(8);
    let first = index(&columns, 8);
    let size = first.memory_size;
    let cache = VectorIndexCache::new(size * 2);
    let first = cache.insert("first".to_string(), first);
    let second = cache.insert("second".to_string(), index(&columns, 8));
    assert_eq!(cache.bytes_size(), (size * 2) as u64);

    cache.set_bytes_capacity(size);
    assert_eq!(cache.len(), 1);
    assert_eq!(cache.bytes_size(), (size * 2) as u64);
    drop(first);
    assert_eq!(cache.bytes_size(), size as u64);

    cache.set_bytes_capacity(0);
    assert!(cache.is_empty());
    let bypassed = cache.insert("disabled".to_string(), index(&columns, 8));
    assert!(cache.is_empty());
    assert_eq!(cache.bytes_size(), size as u64);
    drop(second);
    drop(bypassed);
    assert_eq!(cache.bytes_size(), 0);
}

#[test]
fn test_lru_order_and_duplicate_insertion() {
    let columns = index_columns(8);
    let first = index(&columns, 8);
    let cache = VectorIndexCache::new(first.memory_size * 2);
    drop(cache.insert("first".to_string(), first));
    drop(cache.insert("second".to_string(), index(&columns, 8)));
    let first = cache.get("first").unwrap();
    let duplicate = cache.insert("first".to_string(), index(&columns, 8));
    assert!(Arc::ptr_eq(&first, &duplicate));
    drop(first);
    drop(duplicate);
    drop(cache.insert("third".to_string(), index(&columns, 8)));
    assert!(cache.contains_key("first"));
    assert!(!cache.contains_key("second"));
    assert!(cache.contains_key("third"));
}

#[test]
fn test_shared_index_search_and_counter_wraparound() {
    let columns = index_columns(8);
    let baseline = HNSWIndex::open(DistanceType::L2, 8, COUNT, columns.clone()).unwrap();
    let cache = VectorIndexCache::new(1024 * 1024);
    let shared = cache.insert("shared".to_string(), index(&columns, 8));
    let size = cache.bytes_size();
    std::thread::scope(|scope| {
        for thread in 0..4 {
            let shared = &shared;
            let baseline = &baseline;
            scope.spawn(move || {
                let query = vec![thread as f32 * 0.1; 8];
                let expected = baseline.search(3, &query).unwrap();
                let expected_scores = baseline.generate_scores(COUNT as u32, &query).unwrap();
                // More than u8::MAX searches exercise reset of the reusable visited counters.
                for _ in 0..300 {
                    assert_eq!(shared.index().search(3, &query).unwrap(), expected);
                    assert_eq!(
                        shared
                            .index()
                            .generate_scores(COUNT as u32, &query)
                            .unwrap(),
                        expected_scores
                    );
                }
            });
        }
    });
    assert_eq!(cache.bytes_size(), size);
    assert_eq!(shared.index().memory_size(), baseline.memory_size());
}
