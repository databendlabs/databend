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
use common_datavalues::chrono::DateTime;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_functions::scalars::AddMonthsFunction;
use common_functions::scalars::AddTimesFunction;

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
        DataField::new("u32", u32::to_data_type()),
        DataField::new("u64", u64::to_data_type()),
        DataField::new("i8", i8::to_data_type()),
        DataField::new("i16", i16::to_data_type()),
        DataField::new("i32", i32::to_data_type()),
        DataField::new("i64", i64::to_data_type()),
        DataField::new("f32", f32::to_data_type()),
        DataField::new("f64", f64::to_data_type()),
    ]);

    let blocks = DataBlock::create(schema.clone(), vec![
        Series::from_data(vec![dt_to_days("2020-02-29T10:00:00Z") as u16]),
        Series::from_data(vec![dt_to_days("2020-02-29T10:00:00Z") as i32]),
        Series::from_data(vec![dt_to_seconds("2020-02-29T01:02:03Z") as u32]),
        Series::from_data(vec![12_u8]),
        Series::from_data(vec![12_u16]),
        Series::from_data(vec![12_u32]),
        Series::from_data(vec![12_u64]),
        Series::from_data(vec![-13_i8]),
        Series::from_data(vec![-13_i16]),
        Series::from_data(vec![-13_i32]),
        Series::from_data(vec![-13i64]),
        Series::from_data(vec![1.2_f32]),
        Series::from_data(vec![-1.2_f64]),
    ]);

    let column = |col_name: &str| -> ColumnWithField {
        ColumnWithField::new(
            blocks.try_column_by_name(col_name).unwrap().clone(),
            schema.field_with_name(col_name).unwrap().clone(),
        )
    };

    let fields = [
        "u8", "u16", "u32", "u64", "i8", "i16", "i32", "i64", "f32", "f64",
    ];
    let args = [
        &UInt8Type::arc(),
        &UInt16Type::arc(),
        &UInt32Type::arc(),
        &UInt64Type::arc(),
        &Int8Type::arc(),
        &Int16Type::arc(),
        &Int32Type::arc(),
        &Int64Type::arc(),
        &Float32Type::arc(),
        &Float64Type::arc(),
    ];

    {
        let mut expects: Vec<u16> = Vec::new();
        expects.reserve(10);
        for (field, arg) in fields.iter().zip(args.iter()) {
            let add_months =
                AddMonthsFunction::try_create_func("addMonths", 1, &[&Date16Type::arc(), arg])?;
            let col = add_months.eval(&[column("date16"), column(field)], 1)?;
            assert_eq!(col.len(), 1);
            assert_eq!(col.data_type().data_type_id(), TypeID::UInt16);
            expects.push(col.get_u64(0)? as u16);
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
            dt_to_days("2020-03-29T10:00:00Z") as u16,
            dt_to_days("2020-01-29T10:00:00Z") as u16,
        ]);
    }

    {
        let mut expects: Vec<i32> = Vec::new();
        expects.reserve(10);
        for (field, arg) in fields.iter().zip(args.iter()) {
            let add_months =
                AddMonthsFunction::try_create_func("addMonths", 1, &[&Date32Type::arc(), arg])?;
            let col = add_months.eval(&[column("date32"), column(field)], 1)?;
            assert_eq!(col.len(), 1);
            assert_eq!(col.data_type().data_type_id(), TypeID::Int32);
            expects.push(col.get_i64(0)? as i32);
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
            dt_to_days("2020-03-29T10:00:00Z") as i32,
            dt_to_days("2020-01-29T10:00:00Z") as i32,
        ]);
    }

    {
        let mut expects: Vec<u32> = Vec::new();
        expects.reserve(10);
        for (field, arg) in fields.iter().zip(args.iter()) {
            let add_months = AddMonthsFunction::try_create_func("addMonths", 1, &[
                &DateTime32Type::arc(None),
                arg,
            ])?;
            let col = add_months.eval(&[column("datetime32"), column(field)], 1)?;
            assert_eq!(col.len(), 1);
            assert_eq!(col.data_type().data_type_id(), TypeID::UInt32);
            expects.push(col.get_u64(0)? as u32);
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
            dt_to_seconds("2020-03-29T01:02:03Z") as u32,
            dt_to_seconds("2020-01-29T01:02:03Z") as u32,
        ]);
    }

    Ok(())
}

#[test]
fn test_add_subtract_seconds() -> Result<()> {
    let dt_to_seconds = |dt: &str| -> i64 { DateTime::parse_from_rfc3339(dt).unwrap().timestamp() };

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("datetime32", DateTime32Type::arc(None)),
        DataField::new("u8", u8::to_data_type()),
        DataField::new("u16", u16::to_data_type()),
        DataField::new("u32", u32::to_data_type()),
        DataField::new("u64", u64::to_data_type()),
        DataField::new("i8", i8::to_data_type()),
        DataField::new("i16", i16::to_data_type()),
        DataField::new("i32", i32::to_data_type()),
        DataField::new("i64", i64::to_data_type()),
        DataField::new("f32", f32::to_data_type()),
        DataField::new("f64", f64::to_data_type()),
    ]);

    let blocks = DataBlock::create(schema.clone(), vec![
        Series::from_data(vec![dt_to_seconds("2020-02-29T23:59:59Z") as u32]),
        Series::from_data(vec![1_u8]),
        Series::from_data(vec![1_u16]),
        Series::from_data(vec![1_u32]),
        Series::from_data(vec![1_u64]),
        Series::from_data(vec![-1_i8]),
        Series::from_data(vec![-1_i16]),
        Series::from_data(vec![-1_i32]),
        Series::from_data(vec![-1_i64]),
        Series::from_data(vec![1.2_f32]),
        Series::from_data(vec![-1.2_f64]),
    ]);

    let column = |col_name: &str| -> ColumnWithField {
        ColumnWithField::new(
            blocks.try_column_by_name(col_name).unwrap().clone(),
            schema.field_with_name(col_name).unwrap().clone(),
        )
    };

    let fields = [
        "u8", "u16", "u32", "u64", "i8", "i16", "i32", "i64", "f32", "f64",
    ];
    let args = [
        &UInt8Type::arc(),
        &UInt16Type::arc(),
        &UInt32Type::arc(),
        &UInt64Type::arc(),
        &Int8Type::arc(),
        &Int16Type::arc(),
        &Int32Type::arc(),
        &Int64Type::arc(),
        &Float32Type::arc(),
        &Float64Type::arc(),
    ];

    {
        let mut expects: Vec<u32> = Vec::new();
        expects.reserve(10);
        for (field, arg) in fields.iter().zip(args.iter()) {
            let add_seconds = AddTimesFunction::try_create_func("addSeconds", 1, &[
                &DateTime32Type::arc(None),
                arg,
            ])?;
            let col = add_seconds.eval(&[column("datetime32"), column(field)], 1)?;
            assert_eq!(col.len(), 1);
            assert_eq!(col.data_type().data_type_id(), TypeID::UInt32);
            expects.push(col.get_u64(0)? as u32);
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
            dt_to_seconds("2020-03-01T00:00:00Z") as u32,
            dt_to_seconds("2020-02-29T23:59:58Z") as u32,
        ]);
    }

    {
        let mut expects: Vec<u32> = Vec::new();
        expects.reserve(10);
        for (field, arg) in fields.iter().zip(args.iter()) {
            let add_seconds = AddTimesFunction::try_create_func("subtractSeconds", -1, &[
                &DateTime32Type::arc(None),
                arg,
            ])?;
            let col = add_seconds.eval(&[column("datetime32"), column(field)], 1)?;
            assert_eq!(col.len(), 1);
            assert_eq!(col.data_type().data_type_id(), TypeID::UInt32);
            expects.push(col.get_u64(0)? as u32);
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
            dt_to_seconds("2020-02-29T23:59:58Z") as u32,
            dt_to_seconds("2020-03-01T00:00:00Z") as u32,
        ]);
    }

    Ok(())
}
