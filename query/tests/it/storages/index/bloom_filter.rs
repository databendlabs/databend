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
use databend_query::storages::index::BloomFilter;
use pretty_assertions::assert_eq;

#[allow(dead_code)]
fn create_block() -> DataBlock {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("UInt8", DataType::UInt8, true),
        DataField::new("UInt16", DataType::UInt16, true),
        DataField::new("UInt32", DataType::UInt32, true),
        DataField::new("UInt64", DataType::UInt64, true),
        DataField::new("Int8", DataType::Int8, true),
        DataField::new("Int16", DataType::Int16, true),
        DataField::new("Int32", DataType::Int32, true),
        DataField::new("Int64", DataType::Int64, true),
        DataField::new("Float32", DataType::Float32, true),
        DataField::new("Float64", DataType::Float64, true),
        DataField::new("Date16", DataType::Date16, true),
        DataField::new("Date32", DataType::Date32, true),
        DataField::new("DateTime32", DataType::DateTime32(None), true),
        DataField::new("DateTime64", DataType::DateTime64(3, None), true),
        DataField::new(
            "IntervalDayTime",
            DataType::Interval(IntervalUnit::DayTime),
            true,
        ),
        DataField::new("String", DataType::String, true),
    ]);

    DataBlock::create_by_array(schema, vec![
        Series::new(vec![12_u8, 46, 99, 6]),                      // UInt8
        Series::new(vec![11_u16, 6, 240, 19]),                    // UInt16
        Series::new(vec![213_u32, 1024, 966, 12133]),             // UInt32
        Series::new(vec![20003_u64, 1231024, 90066, 21323721]),   // UInt64
        Series::new(vec![-12_i8, 46, -99, 6]),                    // Int8
        Series::new(vec![-111_i16, 6, -240, 19]),                 // Int16
        Series::new(vec![-2103_i32, 100024, -90066, 62133]),      // Int32
        Series::new(vec![-20003_i64, 81231024, -93266, -413721]), // Int64
        Series::new(vec![-203.123_f32, 812.31024, -932.66, -0.413721]), // Float32
        Series::new(vec![-2003.123_f64, 812.31024, -9302.66, -0.88413721]), // Float64
        Series::new(vec![11222_u16, 60000, 24220, 119]),          // Date16
        Series::new(vec![1222_u32, 60000, 24220, 119]),           // Date32
        Series::new(vec![1222_u32, 60000, 24220, 119]),           // DateTime32
        Series::new(vec![1212222_u64, 60000, 24220, 119]),        // DateTime64
        Series::new(vec![-1212222_i64, 60000, 24220, 119]),       // IntervalDayTime
        Series::new(vec!["Alice", "Bob", "Batman", "Superman"]),  // String
    ])
}

#[test]
fn test_num_bits_hashes() -> Result<()> {
    // use this website for verification: https://hur.st/bloomfilter/
    let bloom = BloomFilter::with_rate(100000, 0.01, 123123);
    assert_eq!(bloom.num_bits(), 958506);
    assert_eq!(bloom.num_hashes(), 7);

    let bloom = BloomFilter::with_rate(4000, 0.02, 123123);
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

    let mut bloom = BloomFilter::with_rate(4, 0.000001, 123123);

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

    let mut bloom = BloomFilter::with_rate(6, 0.000001, 123123);

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

    let mut bloom = BloomFilter::with_rate(10, 0.000001, 123123);
    bloom.add(col)?;

    let buf = bloom.to_vec()?;
    let mut bloom = BloomFilter::from_vec(buf.as_slice())?;

    assert!(bloom.find(DataValue::Float64(Some(1234.1234_f64)))?);
    assert!(bloom.find(DataValue::Float64(Some(-4321.4321_f64)))?);
    assert!(bloom.find(DataValue::Float64(Some(88.88_f64)))?);

    // a random number not exist
    assert!(!bloom.find(DataValue::Float64(Some(88.88001_f64)))?);
    Ok(())
}
