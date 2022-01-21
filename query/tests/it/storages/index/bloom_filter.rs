// Copyright 2021 Datafuse Labs.
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

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_planners::*;
use databend_query::storages::index::BloomFilter;
use databend_query::storages::index::BloomFilterExprEvalResult;
use databend_query::storages::index::BloomFilterIndexer;
use pretty_assertions::assert_eq;

fn create_seeds() -> [u64; 4] {
    let seed0: u64 = rand::random();
    let seed1: u64 = rand::random();
    let seed2: u64 = rand::random();
    let seed3: u64 = rand::random();
    [seed0, seed1, seed2, seed3]
}

#[test]
fn test_num_bits_hashes() -> Result<()> {
    // use this website for verification: https://hur.st/bloomfilter/
    let bloom = BloomFilter::with_rate(100000, 0.01, create_seeds());
    assert_eq!(bloom.num_bits(), 958506);
    assert_eq!(bloom.num_hashes(), 7);

    let bloom = BloomFilter::with_rate(4000, 0.02, create_seeds());
    assert_eq!(bloom.num_bits(), 32570);
    assert_eq!(bloom.num_hashes(), 6);
    Ok(())
}

#[test]
fn test_bloom_add_find_string() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![DataField::new("name", DataType::String, true)]);
    let block = DataBlock::create_by_array(schema, vec![Series::new(vec![
        "Alice", "Bob", "Batman", "Superman",
    ])]);

    let col = block.column(0);

    let mut bloom = BloomFilter::with_rate(4, 0.000001, create_seeds());

    bloom.add(col)?;
    assert!(bloom.find(DataValue::String(Some(b"Alice".to_vec())))?);
    assert!(bloom.find(DataValue::String(Some(b"Bob".to_vec())))?);
    assert!(bloom.find(DataValue::String(Some(b"Batman".to_vec())))?);
    assert!(bloom.find(DataValue::String(Some(b"Superman".to_vec())))?);

    // this case no false positive
    assert!(!bloom.find(DataValue::String(Some(b"alice1".to_vec())))?);
    assert!(!bloom.find(DataValue::String(Some(b"alice2".to_vec())))?);
    assert!(!bloom.find(DataValue::String(Some(b"alice3".to_vec())))?);

    Ok(())
}

#[test]
fn test_bloom_interval() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![DataField::new(
        "Interval",
        DataType::Interval(IntervalUnit::DayTime),
        true,
    )]);

    let block = DataBlock::create_by_array(schema, vec![Series::new([
        None,
        None,
        None,
        None,
        Some(1234_i64),
        Some(-4321_i64),
    ])]);

    let col = block.column(0);

    let mut bloom = BloomFilter::with_rate(6, 0.000001, create_seeds());

    // this case false positive not exist
    bloom.add(col)?;
    assert!(bloom.find(DataValue::Int64(Some(1234_i64)))?);
    assert!(bloom.find(DataValue::Int64(Some(-4321_i64)))?);
    Ok(())
}

#[test]
fn test_bloom_f64_serialization() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![DataField::new("Float64", DataType::Float64, true)]);

    let block = DataBlock::create_by_array(schema, vec![Series::new([
        None,
        None,
        None,
        None,
        Some(1234.1234_f64),
        Some(-4321.4321_f64),
        Some(88.88_f64),
    ])]);

    let col = block.column(0);

    let mut bloom = BloomFilter::with_rate(10, 0.000001, create_seeds());
    bloom.add(col)?;

    let buf = bloom.to_vec()?;
    let bloom = BloomFilter::from_vec(buf.as_slice())?;

    assert!(bloom.find(DataValue::Float64(Some(1234.1234_f64)))?);
    assert!(bloom.find(DataValue::Float64(Some(-4321.4321_f64)))?);
    assert!(bloom.find(DataValue::Float64(Some(88.88_f64)))?);

    // a random number not exist
    assert!(!bloom.find(DataValue::Float64(Some(88.88001_f64)))?);
    Ok(())
}

// create test data, all numerics are odd number, even numbers are reserved for testing.
fn create_blocks() -> Vec<DataBlock> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("ColumnUInt8", DataType::UInt8, true),
        DataField::new("ColumnUInt16", DataType::UInt16, true),
        DataField::new("ColumnUInt32", DataType::UInt32, true),
        DataField::new("ColumnUInt64", DataType::UInt64, true),
        DataField::new("ColumnInt8", DataType::Int8, true),
        DataField::new("ColumnInt16", DataType::Int16, true),
        DataField::new("ColumnInt32", DataType::Int32, true),
        DataField::new("ColumnInt64", DataType::Int64, true),
        DataField::new("ColumnFloat32", DataType::Float32, true),
        DataField::new("ColumnFloat64", DataType::Float64, true),
        DataField::new("ColumnDate16", DataType::Date16, true),
        DataField::new("ColumnDate32", DataType::Date32, true),
        DataField::new("ColumnDateTime32", DataType::DateTime32(None), true),
        DataField::new("ColumnDateTime64", DataType::DateTime64(3, None), true),
        DataField::new(
            "ColumnIntervalDayTime",
            DataType::Interval(IntervalUnit::DayTime),
            true,
        ),
        DataField::new("ColumnString", DataType::String, true),
    ]);

    let block1 = DataBlock::create_by_array(schema.clone(), vec![
        Series::new(vec![1_u8, 3, 5, 7]),
        Series::new(vec![1_u16, 3, 5, 7]),
        Series::new(vec![1_u32, 3, 5, 7]),
        Series::new(vec![1_u64, 3, 5, 7]),
        Series::new(vec![-1_i8, -3, -5, -7]),
        Series::new(vec![-1_i16, -3, -5, -7]),
        Series::new(vec![-1_i32, -3, -5, -7]),
        Series::new(vec![-1_i64, -3, -5, -7]),
        Series::new(vec![1.0_f32, 3.0, 5.0, 7.0]),
        Series::new(vec![1.0_f64, 3.0, 5.0, 7.0]),
        Series::new(vec![1_u16, 3, 5, 7]),
        Series::new(vec![1_u32, 3, 5, 7]),
        Series::new(vec![1_u32, 3, 5, 7]),
        Series::new(vec![1_u64, 3, 5, 7]),
        Series::new(vec![1_i64, 3, 5, 7]),
        Series::new(vec!["Alice", "Bob", "Batman", "Superman"]),
    ]);

    let block2 = DataBlock::create_by_array(schema, vec![
        Series::new(vec![9_u8, 11, 13, 15]),
        Series::new(vec![9_u16, 11, 13, 15]),
        Series::new(vec![9_u32, 11, 13, 15]),
        Series::new(vec![9_u64, 11, 13, 15]),
        Series::new(vec![-9_i8, -11, -13, -15]),
        Series::new(vec![-9_i16, -11, -13, -15]),
        Series::new(vec![-9_i32, -11, -13, -15]),
        Series::new(vec![-9_i64, -11, -13, -15]),
        Series::new(vec![9.0_f32, 11.0, 13.0, 15.0]),
        Series::new(vec![9.0_f64, 11.0, 13.0, 17.0]),
        Series::new(vec![9_u16, 11, 13, 15]),
        Series::new(vec![9_u32, 11, 13, 15]),
        Series::new(vec![9_u32, 11, 13, 15]),
        Series::new(vec![9_u64, 11, 13, 15]),
        Series::new(vec![9_i64, 11, 13, 15]),
        Series::new(vec!["Iron man", "Thor", "Professor X", "Wolverine"]),
    ]);

    vec![block1, block2]
}

#[test]
fn test_bloom_indexer_single_column_prune() -> Result<()> {
    struct Test {
        name: &'static str,
        expr: Expression,
        expected_eval_result: BloomFilterExprEvalResult,
    }

    let tests: Vec<Test> = vec![
        Test {
            name: "ColumnUInt8 = 32",
            expr: col("ColumnUInt8").eq(lit(32u8)),
            expected_eval_result: BloomFilterExprEvalResult::False,
        },
        Test {
            name: "ColumnUInt16 = 30",
            expr: col("ColumnUInt16").eq(lit(30u16)),
            expected_eval_result: BloomFilterExprEvalResult::False,
        },
        Test {
            name: "ColumnUInt32 = 12134",
            expr: col("ColumnUInt32").eq(lit(12134u32)),
            expected_eval_result: BloomFilterExprEvalResult::False,
        },
        Test {
            name: "ColumnUInt32 = 21323722",
            expr: col("ColumnUInt64").eq(lit(21323722_u64)),
            expected_eval_result: BloomFilterExprEvalResult::False,
        },
        Test {
            name: "ColumnString = 'batman'",
            expr: col("ColumnString").eq(lit("batman".as_bytes())),
            expected_eval_result: BloomFilterExprEvalResult::False,
        },
    ];

    let data_blocks = create_blocks();
    let indexer = BloomFilterIndexer::from_data(data_blocks.as_ref())?;

    for test in tests {
        let res = indexer.eval(&test.expr)?;
        assert_eq!(res, test.expected_eval_result, "{}", test.name);
    }
    Ok(())
}

#[test]
fn test_bloom_indexer_logical_and_prune() -> Result<()> {
    struct Test {
        name: &'static str,
        expr: Expression,
        expected_eval_result: BloomFilterExprEvalResult,
    }

    let tests: Vec<Test> = vec![
        Test {
            // Both values exists in the data block
            name: "ColumnFloat64 = 11 and ColumnString = 'Batman'",
            expr: col("ColumnFloat64")
                .eq(lit(11_f64))
                .and(col("ColumnString").eq(lit("Batman".as_bytes()))),
            expected_eval_result: BloomFilterExprEvalResult::Unknown,
        },
        Test {
            // The left is false and right is unknown so expected result should be false
            name: "ColumnFloat64 = 2 and ColumnString = 'Batman'",
            expr: col("ColumnFloat64")
                .eq(lit(2_f64))
                .and(col("ColumnString").eq(lit("Batman".as_bytes()))),
            expected_eval_result: BloomFilterExprEvalResult::False,
        },
        Test {
            // The left is false and right is NotApplicable so expected result should be false
            name: "ColumnFloat64 = 2 and ColumnString = NULL",
            expr: col("ColumnFloat64")
                .eq(lit(2_f64))
                .and(col("ColumnString").eq(lit_null())),
            expected_eval_result: BloomFilterExprEvalResult::False,
        },
    ];

    let data_blocks = create_blocks();
    let indexer = BloomFilterIndexer::from_data(data_blocks.as_ref())?;

    for test in tests {
        let res = indexer.eval(&test.expr)?;
        assert_eq!(res, test.expected_eval_result, "{}", test.name);
    }
    Ok(())
}

#[test]
fn test_bloom_indexer_logical_or_prune() -> Result<()> {
    struct Test {
        name: &'static str,
        expr: Expression,
        expected_eval_result: BloomFilterExprEvalResult,
    }

    let tests: Vec<Test> = vec![
        Test {
            // Neither value exists in the data block, should return false;
            name: "ColumnDate32 = 2 or ColumnString = 'not-exist'",
            expr: col("ColumnDate32")
                .eq(lit(2_u32))
                .or(col("ColumnString").eq(lit("not-exist".as_bytes()))),
            expected_eval_result: BloomFilterExprEvalResult::False,
        },
        Test {
            // Both values exists in the data block, should return unknown.
            name: "ColumnInt64 = -1 or ColumnString = 'Professor X'",
            expr: col("ColumnFloat64")
                .eq(lit(-1_i64))
                .or(col("ColumnString").eq(lit("Professor X".as_bytes()))),
            expected_eval_result: BloomFilterExprEvalResult::Unknown,
        },
        Test {
            // left is false and right is unknown, should return unknown.
            name: "ColumnFloat64 = 3 or ColumnString = 'Batman'",
            expr: col("ColumnFloat64")
                .eq(lit(3_f64))
                .or(col("ColumnString").eq(lit("Batman".as_bytes()))),
            expected_eval_result: BloomFilterExprEvalResult::Unknown,
        },
        Test {
            // Bloom filter doesn't support NULL, so we expect the result to be NotApplicable
            name: "ColumnFloat64 = 2 and ColumnString = NULL",
            expr: col("ColumnFloat64")
                .eq(lit(2_f64))
                .or(col("ColumnString").eq(lit_null())),
            expected_eval_result: BloomFilterExprEvalResult::NotApplicable,
        },
    ];

    let data_blocks = create_blocks();
    let indexer = BloomFilterIndexer::from_data(data_blocks.as_ref())?;

    for test in tests {
        let res = indexer.eval(&test.expr)?;
        assert_eq!(res, test.expected_eval_result, "{}", test.name);
    }
    Ok(())
}
