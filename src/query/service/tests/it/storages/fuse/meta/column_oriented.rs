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

use databend_common_exception::Result;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_storages_fuse::io::read::read_column_oriented_segment;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::statistics::gen_columns_statistics;
use databend_common_storages_fuse::statistics::reduce_block_metas;
use databend_common_storages_fuse::FuseStorageFormat;
use databend_query::test_kits::BlockWriter;
use databend_query::test_kits::TestFixture;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_table_meta::meta::column_oriented_segment::*;
use databend_storages_common_table_meta::meta::decode;
use databend_storages_common_table_meta::meta::testing::MetaEncoding;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::Compression;
use opendal::Operator;

async fn generate_column_oriented_segment(
) -> Result<(ColumnOrientedSegment, Vec<BlockMeta>, TableSchemaRef)> {
    let field_1 = TableField::new("u64", TableDataType::Number(NumberDataType::UInt64));
    let field_2 = TableField::new(
        "nullable_u64",
        TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
    );
    let field_3 = TableField::new("binary", TableDataType::Binary);
    let field_4 = TableField::new("tuple", TableDataType::Tuple {
        fields_name: vec!["u64".to_string(), "u64".to_string()],
        fields_type: vec![
            TableDataType::Number(NumberDataType::UInt64),
            TableDataType::Number(NumberDataType::UInt64),
        ],
    });
    let table_schema = Arc::new(TableSchema::new(vec![field_1, field_2, field_3, field_4]));

    let mut data_blocks = Vec::new();
    for i in 0..100 {
        data_blocks.push(DataBlock::new_from_columns(vec![
            UInt64Type::from_data(vec![i as u64, i as u64 * 2]),
            UInt64Type::from_opt_data(vec![None, None]),
            BinaryType::from_data(vec![vec![i as u8], vec![i as u8 + 1]]),
            Column::Tuple(vec![
                UInt64Type::from_data(vec![i as u64, i as u64 * 3]),
                UInt64Type::from_data(vec![i as u64, i as u64 * 4]),
            ]),
        ]));
    }

    let cluster_stats = Some(ClusterStatistics::new(
        0,
        vec![Scalar::from(1i64)],
        vec![Scalar::from(3i64)],
        1,
        None,
    ));

    let mut block_metas = Vec::new();

    let operator = Operator::new(opendal::services::Memory::default())
        .unwrap()
        .finish();
    let loc_generator = TableMetaLocationGenerator::new("/".to_owned());
    for block in data_blocks {
        let col_stats = gen_columns_statistics(&block, None, &table_schema).unwrap();
        let block_writer = BlockWriter::new(&operator, &loc_generator, Default::default(), true);
        let (block_meta, _index_meta) = block_writer
            .write(
                FuseStorageFormat::Parquet,
                &table_schema,
                block,
                col_stats,
                cluster_stats.clone(),
            )
            .await?;
        block_metas.push(block_meta);
    }

    let column_oriented_segment = {
        let mut segment_builder = ColumnOrientedSegmentBuilder::new(table_schema.clone(), 100);
        for block_meta in block_metas.iter() {
            segment_builder.add_block(block_meta.clone()).unwrap();
        }
        segment_builder.build(Default::default(), Some(0)).unwrap()
    };

    assert_eq!(
        column_oriented_segment.block_metas.num_rows(),
        block_metas.len()
    );

    Ok((column_oriented_segment, block_metas, table_schema))
}

fn check_column_stats_and_meta(
    block_metas: &[BlockMeta],
    column_oriented_segment: &ColumnOrientedSegment,
    projection: &[u32],
) {
    for (i, block_meta) in block_metas.iter().enumerate() {
        for (col_id, col_stat) in block_meta.col_stats.iter() {
            if !projection.contains(col_id) {
                continue;
            }
            let stat = column_oriented_segment.stat_col(*col_id).unwrap();
            let stat = stat.as_tuple().unwrap();
            let min = stat[0].index(i).unwrap();
            let max = stat[1].index(i).unwrap();
            let null_count = stat[2].index(i).unwrap();
            let null_count = null_count.as_number().unwrap().as_u_int64().unwrap();
            let in_memory_size = stat[3].index(i).unwrap();
            let in_memory_size = in_memory_size.as_number().unwrap().as_u_int64().unwrap();
            let distinct_of_values = stat[4].index(i).unwrap();
            let distinct_of_values = distinct_of_values
                .as_number()
                .unwrap()
                .as_u_int64()
                .unwrap();
            assert_eq!(min, col_stat.min.as_ref());
            assert_eq!(max, col_stat.max.as_ref());
            assert_eq!(null_count, &col_stat.null_count);
            assert_eq!(in_memory_size, &col_stat.in_memory_size);
            assert_eq!(distinct_of_values, &col_stat.distinct_of_values.unwrap());
        }
    }

    // check column meta
    for (i, block_meta) in block_metas.iter().enumerate() {
        for (col_id, col_meta) in block_meta.col_metas.iter() {
            if !projection.contains(col_id) {
                continue;
            }
            let col_meta = col_meta.as_parquet().unwrap();
            let meta = column_oriented_segment.meta_col(*col_id).unwrap();
            let meta = meta.as_tuple().unwrap();
            let offset = meta[0].index(i).unwrap();
            let offset = offset.as_number().unwrap().as_u_int64().unwrap();
            let len = meta[1].index(i).unwrap();
            let len = len.as_number().unwrap().as_u_int64().unwrap();
            let num_values = meta[2].index(i).unwrap();
            let num_values = num_values.as_number().unwrap().as_u_int64().unwrap();
            assert_eq!(offset, &col_meta.offset);
            assert_eq!(len, &col_meta.len);
            assert_eq!(num_values, &col_meta.num_values);
        }
    }
}

fn check_block_level_meta(
    block_metas: &[BlockMeta],
    column_oriented_segment: &ColumnOrientedSegment,
) {
    // check row count
    let row_count = column_oriented_segment.col_by_name(&[ROW_COUNT]).unwrap();
    for (row_count, block_meta) in row_count.iter().zip(block_metas.iter()) {
        let row_count = row_count.as_number().unwrap().as_u_int64().unwrap();
        assert_eq!(row_count, &block_meta.row_count);
    }

    // check block size
    let block_size = column_oriented_segment.col_by_name(&[BLOCK_SIZE]).unwrap();
    for (block_size, block_meta) in block_size.iter().zip(block_metas.iter()) {
        let block_size = block_size.as_number().unwrap().as_u_int64().unwrap();
        assert_eq!(block_size, &block_meta.block_size);
    }

    // check file size
    let file_size = column_oriented_segment.col_by_name(&[FILE_SIZE]).unwrap();
    for (file_size, block_meta) in file_size.iter().zip(block_metas.iter()) {
        let file_size = file_size.as_number().unwrap().as_u_int64().unwrap();
        assert_eq!(file_size, &block_meta.file_size);
    }

    // check cluster stats
    let cluster_stats = column_oriented_segment
        .col_by_name(&[CLUSTER_STATS])
        .unwrap();
    for (cluster_stats, block_meta) in cluster_stats.iter().zip(block_metas.iter()) {
        let cluster_stats = cluster_stats.as_binary().unwrap();
        let cluster_stats: ClusterStatistics =
            decode(&MetaEncoding::MessagePack, cluster_stats).unwrap();
        assert_eq!(&cluster_stats, block_meta.cluster_stats.as_ref().unwrap());
    }

    // check location
    let location_path = column_oriented_segment
        .col_by_name(&[LOCATION, LOCATION_PATH])
        .unwrap();
    for (location_path, block_meta) in location_path.iter().zip(block_metas.iter()) {
        assert_eq!(location_path.as_string().unwrap(), &block_meta.location.0);
    }

    let location_format_version = column_oriented_segment
        .col_by_name(&[LOCATION, LOCATION_FORMAT_VERSION])
        .unwrap();
    for (location_format_version, block_meta) in
        location_format_version.iter().zip(block_metas.iter())
    {
        assert_eq!(
            location_format_version
                .as_number()
                .unwrap()
                .as_u_int64()
                .unwrap(),
            &block_meta.location.1
        );
    }

    // check bloom filter index location
    let bloom_filter_index_location = column_oriented_segment
        .col_by_name(&[BLOOM_FILTER_INDEX_LOCATION])
        .unwrap();
    for (bloom_filter_index_location, block_meta) in
        bloom_filter_index_location.iter().zip(block_metas.iter())
    {
        let bloom_filter_index_location = bloom_filter_index_location.as_tuple();
        if let Some(bloom_filter_index_location) = bloom_filter_index_location {
            assert_eq!(
                bloom_filter_index_location[0].as_string().unwrap(),
                &block_meta.bloom_filter_index_location.as_ref().unwrap().0
            );
            assert_eq!(
                bloom_filter_index_location[1]
                    .as_number()
                    .unwrap()
                    .as_u_int64()
                    .unwrap(),
                &block_meta.bloom_filter_index_location.as_ref().unwrap().1
            );
        } else {
            assert!(block_meta.bloom_filter_index_location.is_none());
        }
    }

    // check bloom filter index size
    let bloom_filter_index_size = column_oriented_segment
        .col_by_name(&[BLOOM_FILTER_INDEX_SIZE])
        .unwrap();
    for (bloom_filter_index_size, block_meta) in
        bloom_filter_index_size.iter().zip(block_metas.iter())
    {
        let bloom_filter_index_size = bloom_filter_index_size
            .as_number()
            .unwrap()
            .as_u_int64()
            .unwrap();
        assert_eq!(bloom_filter_index_size, &block_meta.bloom_filter_index_size);
    }

    // check inverted index size
    let inverted_index_size = column_oriented_segment
        .col_by_name(&[INVERTED_INDEX_SIZE])
        .unwrap();
    for (inverted_index_size, block_meta) in inverted_index_size.iter().zip(block_metas.iter()) {
        let is_null = inverted_index_size.is_null();
        assert_eq!(is_null, block_meta.inverted_index_size.is_none());
        assert!(is_null);
    }

    // check compression
    let compression = column_oriented_segment.col_by_name(&[COMPRESSION]).unwrap();
    for (compression, block_meta) in compression.iter().zip(block_metas.iter()) {
        let compression = compression.as_number().unwrap().as_u_int8().unwrap();
        assert_eq!(Compression::from_u8(*compression), block_meta.compression);
    }

    // check create_on
    let create_on = column_oriented_segment.col_by_name(&[CREATE_ON]).unwrap();
    for (create_on, block_meta) in create_on.iter().zip(block_metas.iter()) {
        let create_on = create_on.as_number().unwrap().as_int64().unwrap();
        assert_eq!(create_on, &block_meta.create_on.unwrap().timestamp());
    }
}

fn check_summary(block_metas: &[BlockMeta], column_oriented_segment: &ColumnOrientedSegment) {
    let summary = reduce_block_metas(block_metas, Default::default(), Some(0));
    assert_eq!(summary.row_count, column_oriented_segment.summary.row_count);
    assert_eq!(
        summary.block_count,
        column_oriented_segment.summary.block_count
    );
    assert_eq!(
        summary.perfect_block_count,
        column_oriented_segment.summary.perfect_block_count
    );
    assert_eq!(
        summary.uncompressed_byte_size,
        column_oriented_segment.summary.uncompressed_byte_size
    );
    assert_eq!(
        summary.compressed_byte_size,
        column_oriented_segment.summary.compressed_byte_size
    );
    assert_eq!(
        summary.index_size,
        column_oriented_segment.summary.index_size
    );
    assert_eq!(summary.col_stats, column_oriented_segment.summary.col_stats);
    assert_eq!(
        summary.cluster_stats,
        column_oriented_segment.summary.cluster_stats
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_segment_builder() -> Result<()> {
    let (column_oriented_segment, block_metas, _) = generate_column_oriented_segment().await?;

    check_block_level_meta(&block_metas, &column_oriented_segment);
    check_column_stats_and_meta(&block_metas, &column_oriented_segment, &[]);
    check_summary(&block_metas, &column_oriented_segment);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_segment_cache() -> Result<()> {
    let _fixture = TestFixture::setup_with_segment_cache_bytes(1024 * 1024 * 10).await?;
    let operator = Operator::new(opendal::services::Memory::default())
        .unwrap()
        .finish();
    let loc_generator = TableMetaLocationGenerator::new("/".to_owned());
    let location = loc_generator.gen_segment_info_location(Default::default(), true);
    let (column_oriented_segment, block_metas, table_schema) =
        generate_column_oriented_segment().await?;
    operator
        .write(&location, column_oriented_segment.serialize().unwrap())
        .await?;
    let cache = CacheManager::instance()
        .get_column_oriented_segment_info_cache()
        .unwrap();
    assert!(cache.get(&location).is_none());

    // 1. only read and cache block level meta
    let _column_oriented_segment = read_column_oriented_segment(
        operator.clone(),
        &location,
        &block_level_field_names(),
        true,
    )
    .await?;
    let cached = cache.get(&location).unwrap();
    assert_eq!(cached.segment_schema.fields.len(), 10);
    assert_eq!(cached.segment_schema, segment_schema(&TableSchema::empty()));
    check_summary(&block_metas, &cached);
    check_block_level_meta(&block_metas, &cached);

    // 2. read and cache meta and stats of column 1
    let col_id = 1;
    let mut projection = block_level_field_names();
    projection.insert(meta_name(col_id));
    projection.insert(stat_name(col_id));
    let _column_oriented_segment =
        read_column_oriented_segment(operator.clone(), &location, &projection, true).await?;
    let cached = cache.get(&location).unwrap();
    assert_eq!(cached.segment_schema.fields.len(), 12);

    let column_1 = table_schema.field_of_column_id(col_id).unwrap();
    let stat_1 = column_oriented_segment
        .segment_schema
        .field_with_name(&stat_name(col_id))?;
    assert_eq!(col_stats_type(&column_1.data_type), stat_1.data_type);
    let meta_1 = column_oriented_segment
        .segment_schema
        .field_with_name(&meta_name(col_id))?;
    assert_eq!(col_meta_type(), meta_1.data_type);
    check_summary(&block_metas, &cached);
    check_block_level_meta(&block_metas, &cached);
    check_column_stats_and_meta(&block_metas, &cached, &[col_id]);

    // 3. read and cache meta and stats of column 2
    let col_id = 2;
    let mut projection = block_level_field_names();
    projection.insert(meta_name(col_id));
    projection.insert(stat_name(col_id));
    let _column_oriented_segment =
        read_column_oriented_segment(operator.clone(), &location, &projection, true).await?;
    let cached = cache.get(&location).unwrap();
    // column 2 does not have stats
    assert_eq!(cached.segment_schema.fields.len(), 13);
    check_summary(&block_metas, &cached);
    check_block_level_meta(&block_metas, &cached);
    check_column_stats_and_meta(&block_metas, &cached, &[1, 2]);

    // 4. read column 1 again, should hit cache
    let col_id = 1;
    let mut projection = block_level_field_names();
    projection.insert(meta_name(col_id));
    projection.insert(stat_name(col_id));
    let column_oriented_segment =
        read_column_oriented_segment(operator.clone(), &location, &projection, true).await?;
    let cached = cache.get(&location).unwrap();
    // column 2 does not have stats
    assert_eq!(cached.segment_schema.fields.len(), 13);
    check_summary(&block_metas, &cached);
    check_block_level_meta(&block_metas, &cached);
    check_column_stats_and_meta(&block_metas, &cached, &[1, 2]);
    assert_eq!(column_oriented_segment.summary, cached.summary);
    assert_eq!(
        column_oriented_segment.segment_schema,
        cached.segment_schema
    );
    Ok(())
}
