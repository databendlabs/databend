// Copyright 2026 Datafuse Labs
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

//! Hand-written `mem_bytes` against the bytes actually allocated while building the value.

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::System;
use std::cell::Cell;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_storage::MetaHLL;
use databend_storages_common_cache::CachedItem;
use databend_storages_common_index::BloomIndexMeta;
use databend_storages_common_index::IndexMeta;
use databend_storages_common_index::VectorIndexMeta;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ColumnCountMinSketch;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::Compression;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::TableSnapshotStatistics;

pub struct CountingAlloc;

thread_local! {
    static ALLOCATED: Cell<isize> = const { Cell::new(0) };
}

fn track(delta: isize) {
    let _ = ALLOCATED.try_with(|a| a.set(a.get() + delta));
}

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        track(layout.size() as isize);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        track(-(layout.size() as isize));
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        track(layout.size() as isize);
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        track(new_size as isize - layout.size() as isize);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

/// Returns the value with its true footprint: retained heap bytes plus its inline size.
pub(crate) fn measure<T>(build: impl FnOnce() -> T) -> (T, usize) {
    let before = ALLOCATED.with(Cell::get);
    let value = build();
    let heap = ALLOCATED.with(Cell::get) - before;
    (value, heap as usize + size_of::<T>())
}

fn assert_accurate<T: CachedItem>(name: &str, build: impl FnOnce() -> T) {
    let (value, truth) = measure(build);
    let ratio = value.mem_bytes() as f64 / truth as f64;
    assert!(
        (0.75..=1.1).contains(&ratio),
        "{name}: mem_bytes {} vs allocated {truth} (ratio {ratio:.2})",
        value.mem_bytes()
    );
}

pub(crate) fn schema(columns: u32) -> TableSchema {
    TableSchema::new(
        (0..columns)
            .map(|i| {
                TableField::new(
                    &format!("column_{i}"),
                    TableDataType::Nullable(Box::new(TableDataType::String)),
                )
            })
            .collect(),
    )
}

fn col_stats(columns: u32) -> HashMap<u32, ColumnStatistics> {
    (0..columns)
        .map(|i| {
            let min = Scalar::String(format!("min_value_of_column_{i}"));
            let max = Scalar::String(format!("max_value_of_column_{i}"));
            (i, ColumnStatistics::new(min, max, 0, 1024, Some(100)))
        })
        .collect()
}

pub(crate) fn summary(columns: u32) -> Statistics {
    Statistics {
        col_stats: col_stats(columns),
        ..Default::default()
    }
}

pub(crate) fn block_meta(columns: u32, i: u32) -> BlockMeta {
    let col_metas = (0..columns)
        .map(|c| {
            let meta = SingleColumnMeta {
                offset: c as u64 * 1024,
                len: 1024,
                num_values: 1000,
            };
            (c, ColumnMeta::Parquet(meta))
        })
        .collect();
    let location = |kind: &str| {
        (
            format!("1/2/_{kind}/h0193b2c0cf2d7f6aa0d8f9f2a1e{i:05}_v2.parquet"),
            4,
        )
    };
    BlockMeta::new(
        1000,
        1 << 20,
        1 << 18,
        col_stats(columns),
        col_metas,
        None,
        location("b"),
        Some(location("i")),
        4096,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Compression::Lz4Raw,
        None,
    )
}

pub(crate) fn block_metas(columns: u32, blocks: u32) -> Vec<Arc<BlockMeta>> {
    (0..blocks)
        .map(|i| Arc::new(block_meta(columns, i)))
        .collect()
}

pub(crate) fn index_meta(columns: u32) -> IndexMeta {
    IndexMeta {
        columns: (0..columns)
            .map(|i| {
                (format!("column_{i}"), SingleColumnMeta {
                    offset: 0,
                    len: 64,
                    num_values: 1,
                })
            })
            .collect(),
        metadata: BTreeMap::from([("version".to_string(), "1".to_string())]),
    }
}

#[test]
fn test_table_snapshot() {
    for columns in [1, 500] {
        assert_accurate("table_snapshot", || {
            let segments = (0..1000)
                .map(|i| {
                    (
                        format!("1/2/_sg/h0193b2c0cf2d7f6aa0d8f9f2a1e{i:05}_v4.mpk"),
                        4,
                    )
                })
                .collect();
            TableSnapshot::try_new(
                None,
                None,
                schema(columns),
                summary(columns),
                segments,
                None,
                None,
                TableMetaTimestamps::new(None, chrono::Duration::hours(1)),
            )
            .unwrap()
        });
    }
}

#[test]
fn test_table_snapshot_stats() {
    for columns in [1, 500] {
        assert_accurate("table_snapshot_stats", || {
            let hll = (0..columns).map(|i| (i, MetaHLL::new())).collect();
            let cms = (0..columns)
                .map(|i| (i, ColumnCountMinSketch::new(64, 4)))
                .collect();
            TableSnapshotStatistics::new(
                hll,
                HashMap::new(),
                cms,
                HashMap::new(),
                Default::default(),
                0,
            )
        });
    }
}

#[test]
fn test_segment_block_metas() {
    for (columns, blocks) in [(1, 1000), (500, 20)] {
        assert_accurate("segment_block_metas", || block_metas(columns, blocks));
    }
}

#[test]
fn test_compact_segment_info() {
    for (columns, blocks) in [(1, 1000), (500, 20)] {
        let segment = SegmentInfo::new(block_metas(columns, blocks), summary(columns));
        assert_accurate("segment_info", || {
            CompactSegmentInfo::try_from(&segment).unwrap()
        });
    }
}

#[test]
fn test_index_metas() {
    for columns in [1, 500] {
        assert_accurate("bloom_index_meta", || BloomIndexMeta {
            columns: index_meta(columns).columns,
        });
        assert_accurate("spatial_index_meta", || index_meta(columns));
        assert_accurate("vector_index_meta", || {
            VectorIndexMeta::Legacy(index_meta(columns))
        });
    }
}
