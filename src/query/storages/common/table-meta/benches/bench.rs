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

#[macro_use]
extern crate criterion;

use std::collections::HashMap;
use std::sync::Arc;

use common_expression::types::NumberScalar;
use common_expression::ColumnId;
use common_expression::Scalar;
use common_storages_fuse::io::TableMetaLocationGenerator;
use common_storages_fuse::statistics::STATS_STRING_PREFIX_LEN;
use criterion::black_box;
use criterion::Criterion;
use storages_common_table_meta::meta::testing::MetaEncoding;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::ColumnMeta;
use storages_common_table_meta::meta::ColumnStatistics;
use storages_common_table_meta::meta::Compression;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::SingleColumnMeta;
use storages_common_table_meta::meta::Statistics;

fn bench_encode(c: &mut Criterion) {
    let mut grp = c.benchmark_group("encoding");

    let segment_info = build_test_segment_info(1000).unwrap();

    grp.bench_function("bincode-encode-summary", |b| {
        b.iter(|| {
            let _ = bincode::serialize(black_box(&segment_info.summary)).unwrap();
        })
    });

    #[cfg(with_pot)]
    grp.bench_function("pot-encode-summary", |b| {
        b.iter(|| {
            let _ = pot::to_vec(black_box(&segment_info.summary)).unwrap();
        })
    });

    grp.bench_function("msg-pack-encode-summary", |b| {
        b.iter(|| {
            let _ = rmp_serde::to_vec_named(black_box(&segment_info.summary)).unwrap();
        })
    });

    grp.bench_function("bincode-encode-block-metas", |b| {
        b.iter(|| {
            let _ = bincode::serialize(black_box(&segment_info.blocks)).unwrap();
        })
    });

    #[cfg(with_pot)]
    grp.bench_function("pot-encode-block-metas", |b| {
        b.iter(|| {
            let _ = pot::to_vec(black_box(&segment_info.blocks)).unwrap();
        })
    });

    grp.bench_function("msg-pack-encode-block-metas", |b| {
        b.iter(|| {
            let _ = rmp_serde::to_vec_named(black_box(&segment_info.blocks)).unwrap();
        })
    });

    #[cfg(comment_out)]
    grp.bench_function("bincode-segment-serialization", |b| {
        b.iter(|| {
            let _ = segment_info
                .to_bytes_with_encoding(MetaEncoding::Bincode)
                .unwrap();
        })
    });

    #[cfg(with_pot)]
    grp.bench_function("pot-segment-serialization", |b| {
        b.iter(|| {
            let _ = segment_info
                .to_bytes_with_encoding(MetaEncoding::Pot)
                .unwrap();
        })
    });
}

fn bench_decode(c: &mut Criterion) {
    let mut grp = c.benchmark_group("decoding");

    let segment_info = build_test_segment_info(1000).unwrap();

    let bincode_summary_bytes = bincode::serialize(black_box(&segment_info.summary)).unwrap();
    let bincode_block_meta_bytes = bincode::serialize(black_box(&segment_info.blocks)).unwrap();

    #[cfg(with_pot)]
    let pot_summary_bytes = pot::to_vec(black_box(&segment_info.summary)).unwrap();
    #[cfg(with_pot)]
    let pot_blocks_meta_bytes = pot::to_vec(black_box(&segment_info.blocks)).unwrap();

    let msgpack_summary_bytes = rmp_serde::to_vec_named(black_box(&segment_info.summary)).unwrap();
    let msgpack_blocks_meta_bytes =
        rmp_serde::to_vec_named(black_box(&segment_info.blocks)).unwrap();

    grp.bench_function("bincode-decode-summary", |b| {
        b.iter(|| {
            let _: Statistics = bincode::deserialize(black_box(&bincode_summary_bytes)).unwrap();
        })
    });

    #[cfg(with_pot)]
    grp.bench_function("pot-decode-summary", |b| {
        b.iter(|| {
            let _: Statistics = pot::from_slice(black_box(&pot_summary_bytes)).unwrap();
        })
    });

    grp.bench_function("msg-pack-summary", |b| {
        b.iter(|| {
            let _: Statistics = rmp_serde::from_slice(black_box(&msgpack_summary_bytes)).unwrap();
        })
    });

    grp.bench_function("bincode-decode-block-metas", |b| {
        b.iter(|| {
            let _: Vec<Arc<BlockMeta>> =
                bincode::deserialize(black_box(&bincode_block_meta_bytes)).unwrap();
        })
    });

    #[cfg(with_pot)]
    grp.bench_function("pot-decode-block-metas", |b| {
        b.iter(|| {
            let _: Vec<Arc<BlockMeta>> =
                pot::from_slice(black_box(&pot_blocks_meta_bytes)).unwrap();
        })
    });

    grp.bench_function("msg-pack-decode-block-metas", |b| {
        b.iter(|| {
            let _: Vec<Arc<BlockMeta>> =
                rmp_serde::from_slice(black_box(&msgpack_blocks_meta_bytes)).unwrap();
        })
    });

    #[cfg(with_pot)]
    let segment_pot_bytes = segment_info
        .to_bytes_with_encoding(MetaEncoding::Pot)
        .unwrap();
    let segment_bincode_bytes = segment_info
        .to_bytes_with_encoding(MetaEncoding::Bincode)
        .unwrap();
    let segment_msgpack_bytes = segment_info
        .to_bytes_with_encoding(MetaEncoding::MessagePack)
        .unwrap();

    grp.bench_function("bincode-segment-deserialization", |b| {
        b.iter(|| {
            let _ = SegmentInfo::from_slice(&segment_bincode_bytes).unwrap();
        })
    });

    #[cfg(with_pot)]
    grp.bench_function("pot-segment-deserialization", |b| {
        b.iter(|| {
            let _ = SegmentInfo::from_slice(&segment_pot_bytes).unwrap();
        })
    });

    grp.bench_function("msg-pack-segment-deserialization", |b| {
        b.iter(|| {
            let _ = SegmentInfo::from_slice(&segment_msgpack_bytes).unwrap();
        })
    });

    println!("----------------------------------");
    println!("segment_bincode_bytes: {}", segment_bincode_bytes.len());

    #[cfg(with_pot)]
    println!("segment_pot_bytes: {}", segment_pot_bytes.len());
    println!("segment_msgpack_bytes: {}", segment_msgpack_bytes.len());

    #[cfg(with_pot)]
    println!(
        "segment_pot_bytes / segment_bincode_bytes: {}",
        segment_pot_bytes.len() as f64 / segment_bincode_bytes.len() as f64
    );

    println!(
        "segment_msgpack_bytes / segment_bincode_bytes: {}",
        segment_msgpack_bytes.len() as f64 / segment_bincode_bytes.len() as f64
    );
}

fn build_test_segment_info(num_blocks_per_seg: usize) -> common_exception::Result<SegmentInfo> {
    let col_meta = ColumnMeta::Parquet(SingleColumnMeta {
        offset: 0,
        len: 0,
        num_values: 0,
    });

    let col_stat = ColumnStatistics {
        min: Scalar::String(String::from_utf8(vec![b'a'; STATS_STRING_PREFIX_LEN])?.into_bytes()),
        max: Scalar::String(String::from_utf8(vec![b'a'; STATS_STRING_PREFIX_LEN])?.into_bytes()),
        null_count: 0,
        in_memory_size: 0,
        distinct_of_values: None,
    };

    let number_col_stat = ColumnStatistics {
        min: Scalar::Number(NumberScalar::Int32(0)),
        max: Scalar::Number(NumberScalar::Int32(0)),
        null_count: 0,
        in_memory_size: 0,
        distinct_of_values: None,
    };

    // 20 string columns, 5 number columns
    let num_string_columns = 20;
    let num_number_columns = 5;
    let col_metas = (0..num_string_columns + num_number_columns)
        .map(|id| (id as ColumnId, col_meta.clone()))
        .collect::<HashMap<_, _>>();

    assert_eq!(num_number_columns + num_string_columns, col_metas.len());

    let mut col_stats = (0..num_string_columns)
        .map(|id| (id as ColumnId, col_stat.clone()))
        .collect::<HashMap<_, _>>();
    for idx in num_string_columns..num_string_columns + num_number_columns {
        col_stats.insert(idx as ColumnId, number_col_stat.clone());
    }
    assert_eq!(num_number_columns + num_string_columns, col_stats.len());

    let location_gen = TableMetaLocationGenerator::with_prefix("/root/12345/67890".to_owned());

    let (block_location, block_uuid) = location_gen.gen_block_location();
    let block_meta = BlockMeta {
        row_count: 0,
        block_size: 0,
        file_size: 0,
        col_stats: col_stats.clone(),
        col_metas,
        cluster_stats: None,
        location: block_location,
        bloom_filter_index_location: Some(location_gen.block_bloom_index_location(&block_uuid)),
        bloom_filter_index_size: 0,
        compression: Compression::Lz4,
    };

    let block_metas = (0..num_blocks_per_seg)
        .map(|_| Arc::new(block_meta.clone()))
        .collect::<Vec<_>>();

    let statistics = Statistics {
        row_count: 0,
        block_count: 0,
        perfect_block_count: 0,
        uncompressed_byte_size: 0,
        compressed_byte_size: 0,
        index_size: 0,
        col_stats: col_stats.clone(),
    };

    Ok(SegmentInfo::new(block_metas, statistics))
}

criterion_group!(benches, bench_encode, bench_decode);
criterion_main!(benches);
