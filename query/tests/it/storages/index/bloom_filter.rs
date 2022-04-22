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
use common_base::tokio;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_planners::*;
use databend_query::storages::index::BloomFilter;
use databend_query::storages::index::BloomFilterExprEvalResult;
use databend_query::storages::index::BloomFilterIndexer;
use pretty_assertions::assert_eq;

use crate::tests::create_query_context;

fn create_seed() -> u64 {
    0x16f11fe89b0d677c
}

#[test]
fn test_num_bits_hashes() -> Result<()> {
    // use this website for verification: https://hur.st/bloomfilter/
    let bloom = BloomFilter::with_rate(100000, 0.01, create_seed());
    assert_eq!(bloom.num_bits(), 958506);
    assert_eq!(bloom.num_hashes(), 7);

    let bloom = BloomFilter::with_rate(4000, 0.02, create_seed());
    assert_eq!(bloom.num_bits(), 32570);
    assert_eq!(bloom.num_hashes(), 6);
    Ok(())
}

#[tokio::test]
async fn test_bloom_add_find_string() -> Result<()> {
    let schema =
        DataSchemaRefExt::create(vec![DataField::new_nullable("name", Vu8::to_data_type())]);
    let block = DataBlock::create(schema, vec![Series::from_data(vec![
        "Alice", "Bob", "Batman", "Superman", "123",
    ])]);

    let col = block.column(0);

    let mut bloom = BloomFilter::with_rate(4, 0.000001, create_seed());

    let ctx = create_query_context().await?;
    bloom.add(col, ctx.clone())?;
    assert!(bloom.find(
        DataValue::String(b"Alice".to_vec()),
        StringType::arc(),
        ctx.clone()
    )?);
    assert!(bloom.find(
        DataValue::String(b"Bob".to_vec()),
        StringType::arc(),
        ctx.clone()
    )?);
    assert!(bloom.find(
        DataValue::String(b"Batman".to_vec()),
        StringType::arc(),
        ctx.clone()
    )?);
    assert!(bloom.find(
        DataValue::String(b"Superman".to_vec()),
        StringType::arc(),
        ctx.clone()
    )?);
    assert!(bloom.find(DataValue::UInt64(123), StringType::arc(), ctx.clone())?); // cast will happen to convert 123 to "123"

    // this case no false positive
    assert!(!bloom.find(
        DataValue::String(b"alice1".to_vec()),
        StringType::arc(),
        ctx.clone()
    )?);
    assert!(!bloom.find(
        DataValue::String(b"alice2".to_vec()),
        StringType::arc(),
        ctx.clone()
    )?);
    assert!(!bloom.find(
        DataValue::String(b"alice3".to_vec()),
        StringType::arc(),
        ctx
    )?);

    Ok(())
}

#[tokio::test]
async fn test_bloom_interval() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![DataField::new(
        "Nullable(Interval)",
        wrap_nullable(&IntervalType::arc(IntervalKind::Second)),
    )]);

    let block = DataBlock::create(schema, vec![Series::from_data([
        None,
        None,
        None,
        None,
        Some(1234_i64),
        Some(-4321_i64),
    ])]);

    let col = block.column(0);

    let mut bloom = BloomFilter::with_rate(6, 0.000001, create_seed());

    let ctx = create_query_context().await?;
    // this case false positive not exist
    bloom.add(col, ctx.clone())?;
    assert!(bloom.find(
        DataValue::Int64(1234_i64),
        IntervalType::arc(IntervalKind::Second),
        ctx.clone()
    )?);
    assert!(bloom.find(
        DataValue::Int64(-4321_i64),
        IntervalType::arc(IntervalKind::Second),
        ctx
    )?);
    Ok(())
}

#[tokio::test]
async fn test_bloom_u8() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![DataField::new(
        "UInt8",
        wrap_nullable(&UInt8Type::arc()),
    )]);

    let block = DataBlock::create(schema, vec![Series::from_data([
        None,
        None,
        None,
        None,
        Some(5_u8),
    ])]);

    let col = block.column(0);

    let mut bloom = BloomFilter::with_rate(10, 0.000001, create_seed());
    let ctx = create_query_context().await?;
    bloom.add(col, ctx.clone())?;

    let buf = bloom.to_vec()?;
    let bloom = BloomFilter::from_vec(buf.as_slice())?;

    assert!(bloom.find(DataValue::UInt64(5), UInt8Type::arc(), ctx.clone())?);

    // Although the same value, different data type will result in different hashes.
    assert!(!bloom.find(DataValue::UInt64(5), UInt16Type::arc(), ctx)?);

    Ok(())
}

#[tokio::test]
async fn test_bloom_f64_serialization() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![DataField::new(
        "Float64",
        wrap_nullable(&Float64Type::arc()),
    )]);

    let block = DataBlock::create(schema, vec![Series::from_data([
        None,
        None,
        None,
        None,
        Some(1234.1234_f64),
        Some(-4321.4321_f64),
        Some(88.88_f64),
    ])]);

    let col = block.column(0);

    let mut bloom = BloomFilter::with_rate(10, 0.000001, create_seed());
    let ctx = create_query_context().await?;
    bloom.add(col, ctx.clone())?;

    let buf = bloom.to_vec()?;
    let bloom = BloomFilter::from_vec(buf.as_slice())?;

    assert!(bloom.find(
        DataValue::Float64(1234.1234_f64),
        Float64Type::arc(),
        ctx.clone()
    )?);
    assert!(bloom.find(
        DataValue::Float64(-4321.4321_f64),
        Float64Type::arc(),
        ctx.clone()
    )?);
    assert!(bloom.find(
        DataValue::Float64(88.88_f64),
        Float64Type::arc(),
        ctx.clone()
    )?);

    // a random number not exist
    assert!(!bloom.find(DataValue::Float64(88.88001_f64), Float64Type::arc(), ctx)?);
    Ok(())
}

// A helper function to create a bloom filter, with the same bits and hashes as other.
async fn create_bloom(
    data_type: DataTypePtr,
    column: ColumnRef,
    other: &BloomFilter,
) -> Result<BloomFilter> {
    let mut bloom = other.clone_empty();
    let schema = DataSchemaRefExt::create(vec![DataField::new("num", data_type)]);
    let block = DataBlock::create(schema, vec![column]);
    let col = block.column(0);
    let ctx = create_query_context().await?;
    bloom.add(col, ctx)?;
    Ok(bloom)
}

// create test data, all numerics are odd number, even numbers are reserved for testing.
fn create_blocks() -> Vec<DataBlock> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new_nullable("ColumnUInt8", u8::to_data_type()),
        DataField::new_nullable("ColumnUInt16", u16::to_data_type()),
        DataField::new_nullable("ColumnUInt32", u32::to_data_type()),
        DataField::new_nullable("ColumnUInt64", u64::to_data_type()),
        DataField::new_nullable("ColumnInt8", i8::to_data_type()),
        DataField::new_nullable("ColumnInt16", i16::to_data_type()),
        DataField::new_nullable("ColumnInt32", i32::to_data_type()),
        DataField::new_nullable("ColumnInt64", i64::to_data_type()),
        DataField::new_nullable("ColumnFloat32", f32::to_data_type()),
        DataField::new_nullable("ColumnFloat64", f64::to_data_type()),
        DataField::new_nullable("ColumnDate16", DateType::arc()),
        DataField::new_nullable("ColumnDate32", DateType::arc()),
        DataField::new_nullable("ColumnDateTime32", DateTimeType::arc(0, None)),
        DataField::new_nullable("ColumnDateTime64", DateTimeType::arc(3, None)),
        DataField::new_nullable("ColumnIntervalDays", IntervalType::arc(IntervalKind::Day)),
        DataField::new_nullable("ColumnString", Vu8::to_data_type()),
    ]);

    let block1 = DataBlock::create(schema.clone(), vec![
        Series::from_data(vec![1_u8, 3, 5, 7]),
        Series::from_data(vec![1_u16, 3, 5, 7]),
        Series::from_data(vec![1_u32, 3, 5, 7]),
        Series::from_data(vec![1_u64, 3, 5, 7]),
        Series::from_data(vec![-1_i8, -3, -5, -7]),
        Series::from_data(vec![-1_i16, -3, -5, -7]),
        Series::from_data(vec![-1_i32, -3, -5, -7]),
        Series::from_data(vec![-1_i64, -3, -5, -7]),
        Series::from_data(vec![1.0_f32, 3.0, 5.0, 7.0]),
        Series::from_data(vec![1.0_f64, 3.0, 5.0, 7.0]),
        Series::from_data(vec![1_u16, 3, 5, 7]),
        Series::from_data(vec![1_u32, 3, 5, 7]),
        Series::from_data(vec![1_u32, 3, 5, 7]),
        Series::from_data(vec![1_i64, 3, 5, 7]),
        Series::from_data(vec![1_i64, 3, 5, 7]),
        Series::from_data(vec!["Alice", "Bob", "Batman", "Superman"]),
    ]);

    let block2 = DataBlock::create(schema, vec![
        Series::from_data(vec![9_u8, 11, 13, 15]),
        Series::from_data(vec![9_u16, 11, 13, 15]),
        Series::from_data(vec![9_u32, 11, 13, 15]),
        Series::from_data(vec![9_u64, 11, 13, 15]),
        Series::from_data(vec![-9_i8, -11, -13, -15]),
        Series::from_data(vec![-9_i16, -11, -13, -15]),
        Series::from_data(vec![-9_i32, -11, -13, -15]),
        Series::from_data(vec![-9_i64, -11, -13, -15]),
        Series::from_data(vec![9.0_f32, 11.0, 13.0, 15.0]),
        Series::from_data(vec![9.0_f64, 11.0, 13.0, 15.0]),
        Series::from_data(vec![9_u16, 11, 13, 15]),
        Series::from_data(vec![9_u32, 11, 13, 15]),
        Series::from_data(vec![9_u32, 11, 13, 15]),
        Series::from_data(vec![9_i64, 11, 13, 15]),
        Series::from_data(vec![9_i64, 11, 13, 15]),
        Series::from_data(vec!["Iron man", "Thor", "Professor X", "Wolverine"]),
    ]);

    vec![block1, block2]
}

async fn create_bloom_indexer() -> Result<BloomFilterIndexer> {
    let blocks = create_blocks();
    let ctx = create_query_context().await?;
    BloomFilterIndexer::try_create_with_seed(blocks.as_slice(), create_seed(), ctx)
}

#[tokio::test]
async fn test_bloom_uint8_existence() -> Result<()> {
    // Build the table and bloom filters, get the bloom filter for column 'ColumnUInt8'
    let indexer = create_bloom_indexer().await?;
    let bloom = indexer.try_get_bloom("ColumnUInt8")?;

    // Existence case: numbers 1, 3, 5, 7, 9, 11, 13, 15 should exist in the bloom filter.
    for num in [1_u8, 3, 5, 7, 9, 11, 13, 15] {
        let single_value_bloom =
            create_bloom(u8::to_data_type(), Series::from_data(vec![num]), &bloom).await?;

        assert!(bloom.contains(&single_value_bloom));
    }
    Ok(())
}

#[tokio::test]
async fn test_bloom_f64_existence() -> Result<()> {
    // Build the table and bloom filters, get the bloom filter for column 'ColumnUInt8'
    let indexer = create_bloom_indexer().await?;
    let bloom = indexer.try_get_bloom("ColumnFloat64")?;

    // Existence case: numbers 1, 3, 5, 7, 9, 11, 13, 15 should exist in the bloom filter.
    for num in [1.0_f64, 3.0, 5.0, 7.0, 9.0, 11.0, 13.0, 15.0] {
        let single_value_bloom =
            create_bloom(f64::to_data_type(), Series::from_data(vec![num]), &bloom).await?;

        assert!(bloom.contains(&single_value_bloom));
    }
    Ok(())
}

#[tokio::test]
async fn test_bloom_hash_collision() -> Result<()> {
    // Build the table and bloom filters, get the bloom filter for column 'ColumnUInt8'
    let indexer = create_bloom_indexer().await?;
    let bloom = indexer.try_get_bloom("ColumnUInt8")?;

    // Values [2, 4, 6, 8] doesn't exist and doesn't cause collision, so bloom.contains should return false
    for num in [2_u8, 4, 6, 8] {
        let single_value_bloom =
            create_bloom(u8::to_data_type(), Series::from_data(vec![num]), &bloom).await?;
        assert!(!bloom.contains(&single_value_bloom), "{}", num);
    }

    // When hash collision happens, although number 34 doesn't exist in data_blocks, the hash bits say yes.
    let single_value_bloom =
        create_bloom(u8::to_data_type(), Series::from_data(vec![34_u8]), &bloom).await?;
    assert!(bloom.contains(&single_value_bloom));
    Ok(())
}

#[tokio::test]
async fn test_bloom_indexer_single_column_prune() -> Result<()> {
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
            name: "ColumnUInt32 = 1213",
            expr: col("ColumnUInt32").eq(lit(1213u32)),
            expected_eval_result: BloomFilterExprEvalResult::False,
        },
        Test {
            name: "ColumnUInt64 = 21323722",
            expr: col("ColumnUInt64").eq(lit(21323722_u64)),
            expected_eval_result: BloomFilterExprEvalResult::False,
        },
        Test {
            name: "ColumnString = 'batman'",
            expr: col("ColumnString").eq(lit("batman".as_bytes())),
            expected_eval_result: BloomFilterExprEvalResult::False,
        },
    ];

    let indexer = create_bloom_indexer().await?;

    for test in tests {
        let res = indexer.eval(&test.expr)?;
        assert_eq!(res, test.expected_eval_result, "{}", test.name);
    }
    Ok(())
}

#[tokio::test]
async fn test_bloom_indexer_logical_and_prune() -> Result<()> {
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

    let indexer = create_bloom_indexer().await?;

    for test in tests {
        let res = indexer.eval(&test.expr)?;
        assert_eq!(res, test.expected_eval_result, "{}", test.name);
    }
    Ok(())
}

#[tokio::test]
async fn test_bloom_indexer_logical_or_prune() -> Result<()> {
    struct Test {
        name: &'static str,
        expr: Expression,
        expected_eval_result: BloomFilterExprEvalResult,
    }

    let tests: Vec<Test> = vec![
        Test {
            // Neither value exists in the data block, should return false;
            name: "ColumnDate32 = 1234 or ColumnString = 'nonexistence'",
            expr: col("ColumnDate32")
                .eq(lit(1234_u32))
                .or(col("ColumnString").eq(lit("nonexistence".as_bytes()))),
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

    let indexer = create_bloom_indexer().await?;

    for test in tests {
        let res = indexer.eval(&test.expr)?;
        assert_eq!(res, test.expected_eval_result, "{}", test.name);
    }
    Ok(())
}
