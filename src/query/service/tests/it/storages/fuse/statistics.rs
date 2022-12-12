//  Copyright 2021 Datafuse Labs.
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

use common_base::base::tokio;
use common_datablocks::BlockCompactThresholds;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_functions::aggregates::eval_aggr;
use common_sql::evaluator::ChunkOperator;
use common_sql::evaluator::Evaluator;
use common_sql::executor::add;
use common_sql::executor::col;
use common_sql::executor::lit;
use common_storages_fuse::statistics::reducers::reduce_block_metas;
use common_storages_fuse::statistics::Trim;
use common_storages_fuse::statistics::STATS_REPLACEMENT_CHAR;
use common_storages_fuse::statistics::STATS_STRING_PREFIX_LEN;
use common_storages_table_meta::meta::BlockMeta;
use common_storages_table_meta::meta::ClusterStatistics;
use common_storages_table_meta::meta::ColumnStatistics;
use common_storages_table_meta::meta::Statistics;
use databend_query::storages::fuse::io::BlockWriter;
use databend_query::storages::fuse::io::TableMetaLocationGenerator;
use databend_query::storages::fuse::statistics::gen_columns_statistics;
use databend_query::storages::fuse::statistics::reducers;
use databend_query::storages::fuse::statistics::BlockStatistics;
use databend_query::storages::fuse::statistics::ClusterStatsGenerator;
use databend_query::storages::fuse::statistics::StatisticsAccumulator;
use opendal::Operator;
use rand::Rng;

use crate::storages::fuse::table_test_fixture::TestFixture;

#[test]
fn test_ft_stats_block_stats() -> common_exception::Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", i32::to_data_type()),
        DataField::new("b", Vu8::to_data_type()),
    ]);
    let block = DataBlock::create(schema, vec![
        Series::from_data(vec![1, 2, 3]),
        Series::from_data(vec!["aa", "aa", "bb"]),
    ]);
    let r = gen_columns_statistics(&block, None)?;
    assert_eq!(2, r.len());
    let col_stats = r.get(&0).unwrap();
    assert_eq!(col_stats.min, DataValue::Int64(1));
    assert_eq!(col_stats.max, DataValue::Int64(3));
    assert_eq!(col_stats.distinct_of_values, Some(3));
    let col_stats = r.get(&1).unwrap();
    assert_eq!(col_stats.min, DataValue::String(b"aa".to_vec()));
    assert_eq!(col_stats.max, DataValue::String(b"bb".to_vec()));
    assert_eq!(col_stats.distinct_of_values, Some(2));
    Ok(())
}

#[test]
fn test_ft_stats_block_stats_with_column_distinct_count() -> common_exception::Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", i32::to_data_type()),
        DataField::new("b", Vu8::to_data_type()),
    ]);
    let block = DataBlock::create(schema, vec![
        Series::from_data(vec![1, 2, 3]),
        Series::from_data(vec!["aa", "aa", "bb"]),
    ]);
    let mut column_distinct_count = HashMap::new();
    column_distinct_count.insert(0, 3);
    column_distinct_count.insert(1, 2);
    let r = gen_columns_statistics(&block, Some(column_distinct_count))?;
    assert_eq!(2, r.len());
    let col_stats = r.get(&0).unwrap();
    assert_eq!(col_stats.min, DataValue::Int64(1));
    assert_eq!(col_stats.max, DataValue::Int64(3));
    assert_eq!(col_stats.distinct_of_values, Some(3));
    let col_stats = r.get(&1).unwrap();
    assert_eq!(col_stats.min, DataValue::String(b"aa".to_vec()));
    assert_eq!(col_stats.max, DataValue::String(b"bb".to_vec()));
    assert_eq!(col_stats.distinct_of_values, Some(2));
    Ok(())
}

#[test]
fn test_ft_tuple_stats_block_stats() -> common_exception::Result<()> {
    let inner_names = vec!["a".to_string(), "b".to_string()];
    let inner_data_types = vec![i32::to_data_type(), i32::to_data_type()];
    let tuple_data_type = StructType::new_impl(Some(inner_names), inner_data_types);
    let schema = DataSchemaRefExt::create(vec![DataField::new("t", tuple_data_type.clone())]);
    let inner_columns = vec![
        Series::from_data(vec![1, 2, 3]),
        Series::from_data(vec![4, 5, 6]),
    ];
    let column = StructColumn::from_data(inner_columns, tuple_data_type).arc();

    let block = DataBlock::create(schema, vec![column]);
    let r = gen_columns_statistics(&block, None)?;
    assert_eq!(2, r.len());
    let col0_stats = r.get(&0).unwrap();
    assert_eq!(col0_stats.min, DataValue::Int64(1));
    assert_eq!(col0_stats.max, DataValue::Int64(3));
    let col1_stats = r.get(&1).unwrap();
    assert_eq!(col1_stats.min, DataValue::Int64(4));
    assert_eq!(col1_stats.max, DataValue::Int64(6));
    Ok(())
}

#[test]
fn test_ft_stats_col_stats_reduce() -> common_exception::Result<()> {
    let num_of_blocks = 10;
    let rows_per_block = 3;
    let val_start_with = 1;

    let blocks = TestFixture::gen_sample_blocks_ex(num_of_blocks, rows_per_block, val_start_with);
    let col_stats = blocks
        .iter()
        .map(|b| gen_columns_statistics(&b.clone().unwrap(), None))
        .collect::<common_exception::Result<Vec<_>>>()?;
    let r = reducers::reduce_block_statistics(&col_stats, None);
    assert!(r.is_ok());
    let r = r.unwrap();
    assert_eq!(3, r.len());
    let col0_stats = r.get(&0).unwrap();
    assert_eq!(col0_stats.min, DataValue::Int64(val_start_with as i64));
    assert_eq!(col0_stats.max, DataValue::Int64(num_of_blocks as i64));
    let col1_stats = r.get(&1).unwrap();
    assert_eq!(
        col1_stats.min,
        DataValue::Int64((val_start_with * 2) as i64)
    );
    assert_eq!(col1_stats.max, DataValue::Int64((num_of_blocks * 2) as i64));
    let col2_stats = r.get(&2).unwrap();
    assert_eq!(
        col2_stats.min,
        DataValue::Int64((val_start_with * 3) as i64)
    );
    assert_eq!(col2_stats.max, DataValue::Int64((num_of_blocks * 3) as i64));

    Ok(())
}

#[test]
fn test_reduce_block_statistics_in_memory_size() -> common_exception::Result<()> {
    let iter = |mut idx| {
        std::iter::from_fn(move || {
            idx += 1;
            Some((idx, ColumnStatistics {
                min: DataValue::Null,
                max: DataValue::Null,
                null_count: 1,
                in_memory_size: 1,
                distinct_of_values: Some(1),
            }))
        })
    };

    let num_of_cols = 100;
    // combine two statistics
    let col_stats_left = HashMap::from_iter(iter(0).take(num_of_cols));
    let col_stats_right = HashMap::from_iter(iter(0).take(num_of_cols));
    let r = reducers::reduce_block_statistics(&[col_stats_left, col_stats_right], None)?;
    assert_eq!(num_of_cols, r.len());
    // there should be 100 columns in the result
    for idx in 1..=100 {
        let col_stats = r.get(&idx);
        assert!(col_stats.is_some());
        let col_stats = col_stats.unwrap();
        // for each column, the reduced value of in_memory_size should be 1 + 1
        assert_eq!(col_stats.in_memory_size, 2);
        // for each column, the reduced value of null_count should be 1 + 1
        assert_eq!(col_stats.null_count, 2);
    }
    Ok(())
}

#[tokio::test]
async fn test_accumulator() -> common_exception::Result<()> {
    let blocks = TestFixture::gen_sample_blocks(10, 1);
    let mut stats_acc = StatisticsAccumulator::default();

    let operator = Operator::new(opendal::services::memory::Builder::default().build()?);
    let loc_generator = TableMetaLocationGenerator::with_prefix("/".to_owned());

    for item in blocks {
        let block = item?;
        let col_stats = gen_columns_statistics(&block, None)?;
        let block_statistics =
            BlockStatistics::from(&block, "does_not_matter".to_owned(), None, None)?;
        let block_writer = BlockWriter::new(&operator, &loc_generator);
        let block_meta = block_writer.write(block, col_stats, None).await?;
        stats_acc.add_with_block_meta(block_meta, block_statistics)?;
    }

    assert_eq!(10, stats_acc.blocks_statistics.len());
    assert!(stats_acc.index_size > 0);
    Ok(())
}

#[test]
fn test_ft_stats_cluster_stats() -> common_exception::Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", i32::to_data_type()),
        DataField::new("b", Vu8::to_data_type()),
    ]);
    let blocks = DataBlock::create(schema, vec![
        Series::from_data(vec![1i32, 2, 3]),
        Series::from_data(vec!["123456", "234567", "345678"]),
    ]);

    let block_compactor = BlockCompactThresholds::new(1_000_000, 800_000, 100 * 1024 * 1024);
    let stats_gen = ClusterStatsGenerator::new(0, vec![0], vec![], 0, block_compactor, vec![]);
    let (stats, _) = stats_gen.gen_stats_for_append(&blocks)?;
    assert!(stats.is_some());
    let stats = stats.unwrap();
    assert_eq!(vec![DataValue::Int64(1)], stats.min);
    assert_eq!(vec![DataValue::Int64(3)], stats.max);

    let stats_gen = ClusterStatsGenerator::new(1, vec![1], vec![], 0, block_compactor, vec![]);
    let (stats, _) = stats_gen.gen_stats_for_append(&blocks)?;
    assert!(stats.is_some());
    let stats = stats.unwrap();
    assert_eq!(vec![DataValue::String(b"12345".to_vec())], stats.min);
    assert_eq!(vec![DataValue::String(b"34567".to_vec())], stats.max);

    Ok(())
}

#[tokio::test]
async fn test_ft_cluster_stats_with_stats() -> common_exception::Result<()> {
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", i32::to_data_type())]);
    let blocks = DataBlock::create(schema.clone(), vec![Series::from_data(vec![1i32, 2, 3])]);
    let origin = Some(ClusterStatistics {
        cluster_key_id: 0,
        min: vec![DataValue::Int64(1)],
        max: vec![DataValue::Int64(5)],
        level: 0,
    });

    let block_compactor = BlockCompactThresholds::new(1_000_000, 800_000, 100 * 1024 * 1024);
    let stats_gen = ClusterStatsGenerator::new(0, vec![0], vec![], 0, block_compactor, vec![]);
    let stats = stats_gen.gen_with_origin_stats(&blocks, origin.clone())?;
    assert!(stats.is_some());
    let stats = stats.unwrap();
    assert_eq!(vec![DataValue::Int64(1)], stats.min);
    assert_eq!(vec![DataValue::Int64(3)], stats.max);

    // add expression executor.
    let expr = add(col("a", i32::to_data_type()), lit(1));
    let field = DataField::new("(a + 1)", i64::to_data_type());
    let operators = vec![ChunkOperator::Map {
        eval: Evaluator::eval_expression(&expr, &schema)?,
        name: field.name().to_string(),
    }];

    let stats_gen = ClusterStatsGenerator::new(0, vec![1], vec![], 0, block_compactor, operators);
    let stats = stats_gen.gen_with_origin_stats(&blocks, origin.clone())?;
    assert!(stats.is_some());
    let stats = stats.unwrap();
    assert_eq!(vec![DataValue::Int64(2)], stats.min);
    assert_eq!(vec![DataValue::Int64(4)], stats.max);

    // different cluster_key_id.
    let stats_gen = ClusterStatsGenerator::new(1, vec![0], vec![], 0, block_compactor, vec![]);
    let stats = stats_gen.gen_with_origin_stats(&blocks, origin)?;
    assert!(stats.is_none());

    Ok(())
}

#[test]
fn test_ft_stats_block_stats_string_columns_trimming() -> common_exception::Result<()> {
    let suite = || -> common_exception::Result<()> {
        // prepare random strings
        // 100 string, length ranges from 0 to 100 (chars)
        let mut rand_strings: Vec<String> = vec![];
        for _ in 0..100 {
            let mut rnd = rand::thread_rng();
            let rand_string: String = rand::thread_rng()
                .sample_iter::<char, _>(rand::distributions::Standard)
                .take(rnd.gen_range(0..1000))
                .collect();

            rand_strings.push(rand_string);
        }

        let min_expr = rand_strings.iter().min().unwrap();
        let max_expr = rand_strings.iter().max().unwrap();

        let data_value_min = DataValue::String(min_expr.to_owned().into_bytes());
        let data_value_max = DataValue::String(max_expr.to_owned().into_bytes());

        let trimmed_min = data_value_min.clone().trim_min();
        let trimmed_max = data_value_max.clone().trim_max();

        let meaningless_to_collect_max = is_degenerated_case(max_expr.as_str());

        if meaningless_to_collect_max {
            assert!(trimmed_max.is_none());
        } else {
            assert!(trimmed_max.is_some());
            let trimmed = trimmed_max.unwrap().as_string()?;
            assert!(char_len(&trimmed) <= STATS_STRING_PREFIX_LEN);
            assert!(DataValue::String(trimmed) >= data_value_max)
        }

        {
            assert!(trimmed_min.is_some());
            let trimmed = trimmed_min.unwrap().as_string()?;
            assert!(char_len(&trimmed) <= STATS_STRING_PREFIX_LEN);
            assert!(DataValue::String(trimmed) <= data_value_min);
        }
        Ok(())
    };

    // let runs = 0..1000;  // use this setting at home
    let runs = 0..100;
    for _ in runs {
        suite()?
    }
    Ok(())
}

#[test]
fn test_ft_stats_block_stats_string_columns_trimming_using_eval() -> common_exception::Result<()> {
    let data_type = StringType::new_impl();
    let data_filed = DataField::new("a", data_type.clone());
    let schema = DataSchemaRefExt::create(vec![data_filed]);

    // verifies (randomly) the following assumptions:
    //
    // https://github.com/datafuselabs/databend/issues/7829
    // > ...
    // > in a way that preserves the property of min/max statistics:
    // > the trimmed max should be larger than the non-trimmed one, and the trimmed min
    // > should be lesser than the non-trimmed one.

    let suite = || -> common_exception::Result<()> {
        // prepare random strings
        // 100 string, length ranges from 0 to 100 (chars)
        let mut rand_strings: Vec<String> = vec![];
        for _ in 0..100 {
            let mut rnd = rand::thread_rng();
            let rand_string: String = rand::thread_rng()
                .sample_iter::<char, _>(rand::distributions::Standard)
                .take(rnd.gen_range(0..1000))
                .collect();

            rand_strings.push(rand_string);
        }

        // build test data block, which has only on column, of String type
        let data_col = Series::from_data(
            rand_strings
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<&str>>(),
        );
        let block = DataBlock::create(schema.clone(), vec![data_col.clone()]);

        // calculate UNTRIMMED min max values of the test column
        // by using eval_aggr (to be consistent with the column_statistic mod)
        let data_field = DataField::new("", data_type.clone());
        let rows = data_col.len();
        let column_field = ColumnWithField::new(data_col, data_field);
        let min_col = eval_aggr("min", vec![], &[column_field.clone()], rows)?;
        let max_col = eval_aggr("max", vec![], &[column_field], rows)?;

        let min_expr = min_col.get(0);
        let max_expr = max_col.get(0);

        // generate the statistics of column
        let stats_of_columns = gen_columns_statistics(&block, None).unwrap();

        // check if the max value (untrimmed) is in degenerated condition:
        // - the length of string value is larger or equal than STRING_PREFIX_LEN
        // - AND the string has a prefix of length STRING_PREFIX_LEN, for all the char C in prefix,
        //   C > REPLACEMENT_CHAR; which means we can not replace any of them.
        let string_max_expr = String::from_utf8(max_expr.as_string()?).unwrap();
        let meaningless_to_collect_max = is_degenerated_case(string_max_expr.as_str());

        if meaningless_to_collect_max {
            // no stats will be collected
            assert!(stats_of_columns.get(&0).is_none())
        } else {
            // Finally:
            // check that, trimmed "col_stats.max" always large than or equal to the untrimmed "max_expr"
            let col_stats = stats_of_columns.get(&0).unwrap();
            assert!(
                col_stats.max >= max_expr,
                "left [{}]\nright [{}]",
                col_stats.max,
                max_expr
            );
            // check that, trimmed "col_stats.min" always less than or equal to the untrimmed "mn_expr"
            assert!(
                col_stats.min <= min_expr,
                "left [{}]\nright [{}]",
                col_stats.min,
                min_expr
            );
        }
        Ok(())
    };

    // let runs = 0..1000;  // use this at home

    let runs = 0..100;
    for _ in runs {
        suite()?
    }
    Ok(())
}

fn is_degenerated_case(value: &str) -> bool {
    // check if the value (untrimmed) is in degenerated condition:
    // - the length of string value is larger or equal than STRING_PREFIX_LEN
    // - AND the string has a prefix of length STRING_PREFIX_LEN, for all the char C in prefix,
    //   C > REPLACEMENT_CHAR; which means we can not replace any of them.
    let larger_than_prefix_len = value.chars().count() > STATS_STRING_PREFIX_LEN;
    let prefixed_with_irreplaceable_chars = value
        .chars()
        .take(STATS_STRING_PREFIX_LEN)
        .all(|c| c >= STATS_REPLACEMENT_CHAR);

    larger_than_prefix_len && prefixed_with_irreplaceable_chars
}

fn char_len(value: &[u8]) -> usize {
    String::from_utf8(value.to_vec())
        .unwrap()
        .as_str()
        .chars()
        .count()
}

#[test]
fn test_reduce_block_meta() -> common_exception::Result<()> {
    // case 1: empty input should return the default statistics
    let block_metas: Vec<BlockMeta> = vec![];
    let reduced = reduce_block_metas(&block_metas, BlockCompactThresholds::default())?;
    assert_eq!(Statistics::default(), reduced);

    // case 2: accumulated variants of size index should be as expected
    // reduction of ColumnStatistics, ClusterStatistics already covered by other cases
    let mut rng = rand::thread_rng();
    let size = 100_u64;
    let location = ("".to_owned(), 0);
    let mut blocks = vec![];
    let mut acc_row_count = 0;
    let mut acc_block_size = 0;
    let mut acc_file_size = 0;
    let mut acc_bloom_filter_index_size = 0;
    for _ in 0..size {
        let row_count = rng.gen::<u64>() / size;
        let block_size = rng.gen::<u64>() / size;
        let file_size = rng.gen::<u64>() / size;
        let bloom_filter_index_size = rng.gen::<u64>() / size;
        acc_row_count += row_count;
        acc_block_size += block_size;
        acc_file_size += file_size;
        acc_bloom_filter_index_size += bloom_filter_index_size;
        let block_meta = BlockMeta::new(
            row_count,
            block_size,
            file_size,
            HashMap::new(),
            HashMap::new(),
            None,
            location.clone(),
            None,
            bloom_filter_index_size,
        );
        blocks.push(block_meta);
    }

    let stats = reduce_block_metas(&blocks, BlockCompactThresholds::default())?;

    assert_eq!(acc_row_count, stats.row_count);
    assert_eq!(acc_block_size, stats.uncompressed_byte_size);
    assert_eq!(acc_file_size, stats.compressed_byte_size);
    assert_eq!(acc_bloom_filter_index_size, stats.index_size);

    Ok(())
}
