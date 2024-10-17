//  Copyright 2023 Datafuse Labs.
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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use chrono::Utc;
use databend_common_base::base::tokio;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::statistics::gen_columns_statistics;
use databend_common_storages_fuse::statistics::STATS_STRING_PREFIX_LEN;
use databend_common_storages_fuse::FuseStorageFormat;
use databend_query::test_kits::*;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheValue;
use databend_storages_common_cache::InMemoryLruCache;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::ColumnarSegmentInfo;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::Compression;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::Versioned;
use opendal::Operator;
use parquet::format::FileMetaData;
use rand::Rng;
use sysinfo::get_current_pid;
use sysinfo::System;
use uuid::Uuid;

// NOTE:
//
// usage of memory is observed at *process* level, please do not combine them into
// one test function.
//
// by default, these cases are ignored (in CI).
//
// please run the following two cases individually (in different process)

// #[tokio::test(flavor = "multi_thread")]
// #[ignore]
// async fn test_index_meta_cache_size_file_meta_data() -> databend_common_exception::Result<()> {
//     let thrift_file_meta = setup().await?;

//     let cache_number = 300_000;

//     let meta: FileMetaData = FileMetaData::try_from_thrift(thrift_file_meta)?;

//     let sys = System::new_all();
//     let pid = get_current_pid().unwrap();
//     let process = sys.process(pid).unwrap();
//     let base_memory_usage = process.memory();
//     let scenario = "FileMetaData";

//     eprintln!(
//         "scenario {}, pid {}, base memory {}",
//         scenario, pid, base_memory_usage
//     );

//     let cache = InMemoryCacheBuilder::new_item_cache::<FileMetaData>(cache_number as u64);

//     populate_cache(&cache, meta, cache_number);
//     show_memory_usage(scenario, base_memory_usage, cache_number);

//     drop(cache);

//     Ok(())
// }

// #[tokio::test(flavor = "multi_thread")]
// #[ignore]
// async fn test_index_meta_cache_size_bloom_meta() -> databend_common_exception::Result<()> {
//     let thrift_file_meta = setup().await?;

//     let cache_number = 300_000;

//     let bloom_index_meta = BloomIndexMeta::try_from(thrift_file_meta)?;

//     let sys = System::new_all();
//     let pid = get_current_pid().unwrap();
//     let process = sys.process(pid).unwrap();
//     let base_memory_usage = process.memory();

//     let scenario = "BloomIndexMeta(mini)";
//     eprintln!(
//         "scenario {}, pid {}, base memory {}",
//         scenario, pid, base_memory_usage
//     );

//     let cache = InMemoryCacheBuilder::new_item_cache::<BloomIndexMeta>(cache_number as u64);
//     populate_cache(&cache, bloom_index_meta, cache_number);
//     show_memory_usage("BloomIndexMeta(Mini)", base_memory_usage, cache_number);

//     drop(cache);

//     Ok(())
// }

// cargo test --test it storages::fuse::bloom_index_meta_size::test_random_location_memory_size --no-fail-fast -- --ignored --exact -Z unstable-options --show-output
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_random_location_memory_size() -> databend_common_exception::Result<()> {
    // generate random location of Type Location
    let location_gen = TableMetaLocationGenerator::with_prefix("/root".to_string());

    let num_segments = 5_000_000;
    let sys = System::new_all();
    let pid = get_current_pid().unwrap();
    let process = sys.process(pid).unwrap();
    let base_memory_usage = process.memory();
    let scenario = format!("{} segment locations", num_segments);

    eprintln!(
        "scenario {}, pid {}, base memory {}",
        scenario, pid, base_memory_usage
    );

    let mut locations: HashSet<Location, _> = HashSet::new();
    for _ in 0..num_segments {
        let segment_path = location_gen.gen_segment_info_location();
        let segment_location = (segment_path, SegmentInfo::VERSION);
        locations.insert(segment_location);
    }

    show_memory_usage(&scenario, base_memory_usage, num_segments);

    drop(locations);

    Ok(())
}

// cargo test --test it storages::fuse::bloom_index_meta_size::test_segment_info_size --no-fail-fast -- --ignored --exact -Z unstable-options --show-output
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_segment_info_size() -> databend_common_exception::Result<()> {
    let cache_number = 3000;
    let num_block_per_seg = 1000;

    let (segment_info, _) = build_test_segment_info(num_block_per_seg)?;

    let sys = System::new_all();
    let pid = get_current_pid().unwrap();
    let process = sys.process(pid).unwrap();
    let base_memory_usage = process.memory();
    let scenario = format!(
        "{} SegmentInfo, {} block per seg ",
        cache_number, num_block_per_seg
    );

    eprintln!(
        "scenario {}, pid {}, base memory {}",
        scenario, pid, base_memory_usage
    );

    let cache = InMemoryLruCache::with_items_capacity(String::from(""), cache_number);
    for _ in 0..cache_number {
        let uuid = Uuid::new_v4();
        let block_metas = segment_info
            .blocks
            .iter()
            .map(|b: &Arc<BlockMeta>| Arc::new(b.as_ref().clone()))
            .collect::<Vec<_>>();
        let statistics = segment_info.summary.clone();
        let segment_info = SegmentInfo::new(block_metas, statistics);
        cache.insert(format!("{}", uuid.simple()), segment_info);
    }
    show_memory_usage("SegmentInfoCache", base_memory_usage, cache_number);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_columnar_segment_info_size() -> databend_common_exception::Result<()> {
    let cache_number = 3000;
    let num_block_per_seg = 1000;

    let (segment_info, table_schema) = build_test_segment_info(num_block_per_seg)?;

    let sys = System::new_all();
    let pid = get_current_pid().unwrap();
    let process = sys.process(pid).unwrap();
    let base_memory_usage = process.memory();
    let scenario = format!(
        "{} SegmentInfo, {} block per seg ",
        cache_number, num_block_per_seg
    );

    eprintln!(
        "scenario {}, pid {}, base memory {}",
        scenario, pid, base_memory_usage
    );

    let cache = InMemoryLruCache::with_items_capacity(String::from(""), cache_number);
    {
        for _ in 0..cache_number {
            let uuid = Uuid::new_v4();
            cache.insert(
                format!("{}", uuid.simple()),
                ColumnarSegmentInfo::try_from_segment_info_and_schema(
                    segment_info.clone(),
                    &table_schema,
                )?,
            );
        }
    }
    show_memory_usage("ColumnarSegmentInfoCache", base_memory_usage, cache_number);

    Ok(())
}

// cargo test --test it storages::fuse::bloom_index_meta_size::test_segment_raw_bytes_size --no-fail-fast -- --ignored --exact -Z unstable-options --show-output
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_segment_raw_bytes_size() -> databend_common_exception::Result<()> {
    let cache_number = 3000;
    let num_block_per_seg = 1000;

    let (segment_info, _) = build_test_segment_info(num_block_per_seg)?;
    let segment_info_bytes = CompactSegmentInfo::try_from(segment_info)?;

    let sys = System::new_all();
    let pid = get_current_pid().unwrap();
    let process = sys.process(pid).unwrap();
    let base_memory_usage = process.memory();
    let scenario = format!(
        "{} SegmentInfo(raw bytes, Vec<u8>), {} block per seg ",
        cache_number, num_block_per_seg
    );

    eprintln!(
        "scenario {}, pid {}, base memory {}",
        scenario, pid, base_memory_usage
    );

    let cache = InMemoryLruCache::with_items_capacity(String::from(""), cache_number);

    for _ in 0..cache_number {
        let uuid = Uuid::new_v4();
        cache.insert(format!("{}", uuid.simple()), segment_info_bytes.clone());
    }

    show_memory_usage(
        "SegmentInfoCache(raw bytes Vec<u8>)",
        base_memory_usage,
        cache_number,
    );

    Ok(())
}

// cargo test --test it storages::fuse::bloom_index_meta_size::test_segment_raw_repr_bytes_size --no-fail-fast -- --ignored --exact -Z unstable-options --show-output
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_segment_raw_repr_bytes_size() -> databend_common_exception::Result<()> {
    let cache_number = 3000;
    let num_block_per_seg = 1000;

    let (segment_info, _) = build_test_segment_info(num_block_per_seg)?;
    let segment_raw = CompactSegmentInfo::try_from(&segment_info)?;

    let sys = System::new_all();
    let pid = get_current_pid().unwrap();
    let process = sys.process(pid).unwrap();
    let base_memory_usage = process.memory();

    let scenario = format!(
        "{} SegmentInfo(compact repr), {} block per seg ",
        cache_number, num_block_per_seg
    );

    eprintln!(
        "scenario {}, pid {}, base memory {}",
        scenario, pid, base_memory_usage
    );

    let cache = InMemoryLruCache::with_items_capacity(String::from(""), cache_number);

    for _ in 0..cache_number {
        let uuid = Uuid::new_v4();
        cache.insert(format!("{}", uuid.simple()), segment_raw.clone());
    }
    show_memory_usage(
        "SegmentInfoCache (compact repr)",
        base_memory_usage,
        cache_number,
    );

    Ok(())
}

fn build_test_segment_info(
    num_blocks_per_seg: usize,
) -> databend_common_exception::Result<(SegmentInfo, TableSchema)> {
    let mut rng = rand::thread_rng();
    let num_string_columns = 20;
    let num_number_columns = 5;
    let location_gen = TableMetaLocationGenerator::with_prefix("/root/12345/67890".to_owned());
    let mut block_metas = Vec::with_capacity(num_blocks_per_seg);
    for _ in 0..num_blocks_per_seg {
        let (block_location, block_uuid) = location_gen.gen_block_location();
        let mut col_stats = HashMap::new();
        let mut col_metas = HashMap::new();
        for id in 0..num_string_columns + num_number_columns {
            col_metas.insert(
                id as ColumnId,
                ColumnMeta::Parquet(SingleColumnMeta {
                    offset: rng.gen_range(0..150 * 1024 * 1024),
                    len: rng.gen_range(10 * 1024..10 * 1024 * 1024),
                    num_values: rng.gen_range(100_000..1_000_000),
                }),
            );
        }
        for id in 0..num_string_columns {
            col_stats.insert(
                id as ColumnId,
                ColumnStatistics::new(
                    Scalar::String(
                        (0..STATS_STRING_PREFIX_LEN)
                            .map(|_| rng.gen_range(b'a'..=b'z') as char)
                            .collect(),
                    ),
                    Scalar::String(
                        (0..STATS_STRING_PREFIX_LEN)
                            .map(|_| rng.gen_range(b'a'..=b'z') as char)
                            .collect(),
                    ),
                    rng.gen_range(100_000..1_000_000),
                    rng.gen_range(100_000..1_000_000),
                    Some(rng.gen_range(10_000..100_000)),
                ),
            );
        }
        for id in num_string_columns..num_string_columns + num_number_columns {
            col_stats.insert(
                id as ColumnId,
                ColumnStatistics::new(
                    Scalar::Number(NumberScalar::Int32(rng.gen_range(-100_000..100_000))),
                    Scalar::Number(NumberScalar::Int32(rng.gen_range(-100_000..100_000))),
                    rng.gen_range(100_000..1_000_000),
                    rng.gen_range(100_000..1_000_000),
                    Some(rng.gen_range(10_000..100_000)),
                ),
            );
        }
        assert_eq!(col_metas.len(), num_string_columns + num_number_columns);
        assert_eq!(col_stats.len(), num_string_columns + num_number_columns);
        let block_meta = BlockMeta {
            row_count: rng.gen_range(100_000..1_000_000),
            block_size: rng.gen_range(50 * 1024 * 1024..150 * 1024 * 1024),
            file_size: rng.gen_range(10 * 1024 * 1024..50 * 1024 * 1024),
            col_stats,
            col_metas,
            cluster_stats: None,
            location: block_location,
            bloom_filter_index_location: Some(location_gen.block_bloom_index_location(&block_uuid)),
            bloom_filter_index_size: rng.gen_range(1024 * 1024..5 * 1024 * 1024),
            inverted_index_size: None,
            compression: Compression::Lz4,
            create_on: Some(Utc::now()),
        };
        block_metas.push(Arc::new(block_meta));
    }

    let mut fields = vec![];
    for id in 0..num_string_columns {
        fields.push(TableField::new(
            &format!("col_{}", id),
            TableDataType::String,
        ));
    }
    for id in num_string_columns..num_string_columns + num_number_columns {
        fields.push(TableField::new(
            &format!("col_{}", id),
            TableDataType::Number(NumberDataType::Int32),
        ));
    }
    let table_schema = TableSchema::new(fields);

    let statistics = Statistics::default();

    Ok((SegmentInfo::new(block_metas, statistics), table_schema))
}

#[allow(dead_code)]
fn populate_cache<T>(cache: &InMemoryLruCache<T>, item: T, num_cache: usize)
where T: Clone + Into<CacheValue<T>> {
    for _ in 0..num_cache {
        let uuid = Uuid::new_v4();
        cache.insert(format!("{}", uuid.simple()), item.clone());
    }
}

#[allow(dead_code)]
async fn setup() -> databend_common_exception::Result<FileMetaData> {
    let fields = (0..23)
        .map(|_| TableField::new("id", TableDataType::Number(NumberDataType::Int32)))
        .collect::<Vec<_>>();

    let schema = TableSchemaRefExt::create(fields);

    let mut columns = vec![];
    for _ in 0..schema.fields().len() {
        // values do not matter
        let column = Int32Type::from_data(vec![1]);
        columns.push(column)
    }

    let block = DataBlock::new_from_columns(columns);
    let operator = Operator::new(opendal::services::Memory::default())?.finish();
    let loc_generator = TableMetaLocationGenerator::with_prefix("/".to_owned());
    let col_stats = gen_columns_statistics(&block, None, &schema)?;
    let block_writer = BlockWriter::new(&operator, &loc_generator);
    let (_block_meta, thrift_file_meta) = block_writer
        .write(FuseStorageFormat::Parquet, &schema, block, col_stats, None)
        .await?;

    Ok(thrift_file_meta.unwrap())
}

fn show_memory_usage(case: &str, base_memory_usage: u64, num_cache_items: usize) {
    let sys = System::new_all();
    let pid = get_current_pid().unwrap();
    let process = sys.process(pid).unwrap();
    {
        let memory_after = process.memory();
        let delta = memory_after - base_memory_usage;
        let delta_gb = (delta as f64) / 1024.0 / 1024.0 / 1024.0;
        eprintln!(
            "
            cache item type : {},
            number of cached items {},
            mem usage(B):{:+},
            mem usage(GB){:+}
            ",
            case, num_cache_items, delta, delta_gb
        );
    }
}
