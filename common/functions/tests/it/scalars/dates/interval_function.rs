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
use common_datavalues2::prelude::*;
use common_exception::Result;
use common_functions::scalars::MonthsArithmeticFunction;
use common_functions::scalars::SecondsArithmeticFunction;

#[test]
fn test_add_months() -> Result<()> {
    let dt_to_days = |dt: &str| -> i64 {
        DateTime::parse_from_rfc3339(dt).unwrap().timestamp() / (24 * 3600_i64)
    };

    let dt_to_seconds = |dt: &str| -> i64 { DateTime::parse_from_rfc3339(dt).unwrap().timestamp() };

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("date16", Date16Type::arc()),
        DataField::new("date32", Date32Type::arc()),
        DataField::new("datetime32", DateTime32Type::arc(None)),
        DataField::new("u8", u8::to_data_type()),
        DataField::new("u16", u16::to_data_type()),
        DataField::new("u32", DataType::UInt32),
        DataField::new("u64", DataType::UInt64),
        DataField::new("i8", DataType::Int8),
        DataField::new("i16", DataType::Int16),
        DataField::new("i32", DataType::Int32),
        DataField::new("i64", DataType::Int64),
    ]);

    let blocks = DataBlock::create_by_array(schema.clone(), vec![
        Series::new(vec![dt_to_days("2020-02-29T10:00:00Z") as u16]),
        Series::new(vec![dt_to_days("2020-02-29T10:00:00Z") as i32]),
        Series::new(vec![dt_to_seconds("2020-02-29T01:02:03Z") as u32]),
        Series::new(vec![12_u8]),
        Series::new(vec![12_u16]),
        Series::new(vec![12_u32]),
        Series::new(vec![12_u64]),
        Series::new(vec![-13_i8]),
        Series::new(vec![-13_i16]),
        Series::new(vec![-13_i32]),
        Series::new(vec![-13i64]),
    ]);

    let column = |col_name: &str| -> DataColumnWithField {
        DataColumnWithField::new(
            blocks.try_column_by_name(col_name).unwrap().clone(),
            schema.field_with_name(col_name).unwrap().clone(),
        )
    };

    let add_months =
        MonthsArithmeticFunction::try_create("addYear", DataValueBinaryOperator::Plus, 1)?;

    {
        let mut expects: Vec<u16> = Vec::new();
        expects.reserve(8);
        for c in ["u8", "u16", "u32", "u64", "i8", "i16", "i32", "i64"] {
            let col = add_months.eval(&[column("date16"), column(c)], 1)?;
            let raw = col.to_array()?.u16()?.inner().values().as_slice().to_vec();
            assert_eq!(raw.len(), 1);
            assert_eq!(col.data_type(), DataType::UInt16);
            expects.push(raw[0]);
        }
        assert_eq!(expects, vec![
            dt_to_days("2021-02-28T10:00:00Z") as u16,
            dt_to_days("2021-02-28T10:00:00Z") as u16,
            dt_to_days("2021-02-28T10:00:00Z") as u16,
            dt_to_days("2021-02-28T10:00:00Z") as u16,
            dt_to_days("2019-01-29T10:00:00Z") as u16,
            dt_to_days("2019-01-29T10:00:00Z") as u16,
            dt_to_days("2019-01-29T10:00:00Z") as u16,
            dt_to_days("2019-01-29T10:00:00Z") as u16,
        ]);
    }

    {
        let mut expects: Vec<i32> = Vec::new();
        expects.reserve(8);
        for c in ["u8", "u16", "u32", "u64", "i8", "i16", "i32", "i64"] {
            let col = add_months.eval(&[column("date32"), column(c)], 1)?;
            let raw = col.to_array()?.i32()?.inner().values().as_slice().to_vec();
            assert_eq!(raw.len(), 1);
            assert_eq!(col.data_type(), DataType::Int32);
            expects.push(raw[0]);
        }
        assert_eq!(expects, vec![
            dt_to_days("2021-02-28T10:00:00Z") as i32,
            dt_to_days("2021-02-28T10:00:00Z") as i32,
            dt_to_days("2021-02-28T10:00:00Z") as i32,
            dt_to_days("2021-02-28T10:00:00Z") as i32,
            dt_to_days("2019-01-29T10:00:00Z") as i32,
            dt_to_days("2019-01-29T10:00:00Z") as i32,
            dt_to_days("2019-01-29T10:00:00Z") as i32,
            dt_to_days("2019-01-29T10:00:00Z") as i32,
        ]);
    }

    {
        let mut expects: Vec<u32> = Vec::new();
        expects.reserve(8);
        for c in ["u8", "u16", "u32", "u64", "i8", "i16", "i32", "i64"] {
            let col = add_months.eval(&[column("datetime32"), column(c)], 1)?;
            let raw = col.to_array()?.u32()?.inner().values().as_slice().to_vec();
            assert_eq!(raw.len(), 1);
            assert_eq!(col.data_type(), DataType::UInt32);
            expects.push(raw[0]);
        }
        assert_eq!(expects, vec![
            dt_to_seconds("2021-02-28T01:02:03Z") as u32,
            dt_to_seconds("2021-02-28T01:02:03Z") as u32,
            dt_to_seconds("2021-02-28T01:02:03Z") as u32,
            dt_to_seconds("2021-02-28T01:02:03Z") as u32,
            dt_to_seconds("2019-01-29T01:02:03Z") as u32,
            dt_to_seconds("2019-01-29T01:02:03Z") as u32,
            dt_to_seconds("2019-01-29T01:02:03Z") as u32,
            dt_to_seconds("2019-01-29T01:02:03Z") as u32,
        ]);
    }

    Ok(())
}

#[test]
fn test_add_subtract_seconds() -> Result<()> {
    let dt_to_seconds = |dt: &str| -> i64 { DateTime::parse_from_rfc3339(dt).unwrap().timestamp() };

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("datetime32", DataType::DateTime32(None)),
        DataField::new("u8", u8::to_data_type()),
        DataField::new("u16", DataType::UInt16),
        DataField::new("u32", DataType::UInt32),
        DataField::new("u64", DataType::UInt64),
        DataField::new("i8", DataType::Int8),
        DataField::new("i16", DataType::Int16),
        DataField::new("i32", DataType::Int32),
        DataField::new("i64", DataType::Int64),
    ]);

    let blocks = DataBlock::create_by_array(schema.clone(), vec![
        Series::new(vec![dt_to_seconds("2020-02-29T23:59:59Z") as u32]),
        Series::new(vec![1_u8]),
        Series::new(vec![1_u16]),
        Series::new(vec![1_u32]),
        Series::new(vec![1_u64]),
        Series::new(vec![-1_i8]),
        Series::new(vec![-1_i16]),
        Series::new(vec![-1_i32]),
        Series::new(vec![-1_i64]),
    ]);

    let column = |col_name: &str| -> DataColumnWithField {
        DataColumnWithField::new(
            blocks.try_column_by_name(col_name).unwrap().clone(),
            schema.field_with_name(col_name).unwrap().clone(),
        )
    };

    let add_seconds =
        SecondsArithmeticFunction::try_create("addSeconds", DataValueBinaryOperator::Plus, 1)?;
    {
        let mut expects: Vec<u32> = Vec::new();
        expects.reserve(8);
        for c in ["u8", "u16", "u32", "u64", "i8", "i16", "i32", "i64"] {
            let col = add_seconds.eval(&[column(c), column("datetime32")], 1)?;
            let raw = col.to_array()?.u32()?.inner().values().as_slice().to_vec();
            assert_eq!(raw.len(), 1);
            assert_eq!(col.data_type(), DataType::UInt32);
            expects.push(raw[0]);
        }
        assert_eq!(expects, vec![
            dt_to_seconds("2020-03-01T00:00:00Z") as u32,
            dt_to_seconds("2020-03-01T00:00:00Z") as u32,
            dt_to_seconds("2020-03-01T00:00:00Z") as u32,
            dt_to_seconds("2020-03-01T00:00:00Z") as u32,
            dt_to_seconds("2020-02-29T23:59:58Z") as u32,
            dt_to_seconds("2020-02-29T23:59:58Z") as u32,
            dt_to_seconds("2020-02-29T23:59:58Z") as u32,
            dt_to_seconds("2020-02-29T23:59:58Z") as u32,
        ]);
    }
    let sub_seconds = SecondsArithmeticFunction::try_create(
        "subtractSeconds",
        DataValueBinaryOperator::Minus,
        1,
    )?;
    {
        let mut expects: Vec<u32> = Vec::new();
        expects.reserve(8);
        for c in ["u8", "u16", "u32", "u64", "i8", "i16", "i32", "i64"] {
            let col = sub_seconds.eval(&[column(c), column("datetime32")], 1)?;
            let raw = col.to_array()?.u32()?.inner().values().as_slice().to_vec();
            assert_eq!(raw.len(), 1);
            assert_eq!(col.data_type(), DataType::UInt32);
            expects.push(raw[0]);
        }
        assert_eq!(expects, vec![
            dt_to_seconds("2020-02-29T23:59:58Z") as u32,
            dt_to_seconds("2020-02-29T23:59:58Z") as u32,
            dt_to_seconds("2020-02-29T23:59:58Z") as u32,
            dt_to_seconds("2020-02-29T23:59:58Z") as u32,
            dt_to_seconds("2020-03-01T00:00:00Z") as u32,
            dt_to_seconds("2020-03-01T00:00:00Z") as u32,
            dt_to_seconds("2020-03-01T00:00:00Z") as u32,
            dt_to_seconds("2020-03-01T00:00:00Z") as u32,
        ]);
    }

    Ok(())
}
