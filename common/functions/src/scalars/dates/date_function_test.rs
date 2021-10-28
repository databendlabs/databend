// Copyright 2020 Datafuse Labs.
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

use common_arrow::arrow::array::UInt16Array;
use common_arrow::arrow::array::UInt64Array;
use common_arrow::arrow::array::UInt8Array;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;

use crate::scalars::dates::number_function::ToDayOfMonthFunction;
use crate::scalars::dates::number_function::ToDayOfWeekFunction;
use crate::scalars::dates::number_function::ToDayOfYearFunction;
use crate::scalars::dates::number_function::ToMinuteFunction;
use crate::scalars::dates::number_function::ToMondayFunction;
use crate::scalars::dates::number_function::ToMonthFunction;
use crate::scalars::Function;
use crate::scalars::ToHourFunction;
use crate::scalars::ToSecondFunction;
use crate::scalars::ToYYYYMMDDFunction;
use crate::scalars::ToYYYYMMDDhhmmssFunction;
use crate::scalars::ToYYYYMMFunction;

#[test]
fn test_toyyyymm_function() -> Result<()> {
    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        display: &'static str,
        arg_names: Vec<&'static str>,
        func: Box<dyn Function>,
        columns: Vec<DataColumn>,
        expect: DataColumn,
        error: &'static str,
        nullable: bool,
    }
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::DateTime32(None), false),
        DataField::new("b", DataType::Date32, false),
        DataField::new("c", DataType::Date16, false),
    ]);

    let tests = vec![
        Test {
            name: "test_toyyyymm_date16",
            display: "c()",
            arg_names: vec!["c"],
            func: ToYYYYMMFunction::try_create("c")?,
            columns: vec![Series::new(vec![0u16]).into()],
            nullable: false,
            expect: Series::new(vec![197001]).into(),
            error: "",
        },
        Test {
            name: "test_toyyyymm_date32",
            display: "b()",
            arg_names: vec!["b"],
            func: ToYYYYMMFunction::try_create("b")?,
            columns: vec![Series::new(vec![0i32, 1, 2, 3]).into()],
            nullable: false,
            expect: Series::new(vec![197001u32, 197001u32, 197001u32, 197001u32]).into(),
            error: "",
        },
        Test {
            name: "test_toyyyymm_datetime",
            display: "a()",
            arg_names: vec!["a"],
            func: ToYYYYMMFunction::try_create("a")?,
            columns: vec![Series::new(vec![0u32]).into()],
            nullable: false,
            expect: Series::new(vec![197001]).into(),
            error: "",
        },
        Test {
            name: "test_toyyyymm_constant_date16",
            display: "c()",
            arg_names: vec!["c"],
            func: ToYYYYMMFunction::try_create("c")?,
            columns: vec![DataColumn::Constant(DataValue::Int32(Some(0i32)), 10)],
            nullable: false,
            expect: Series::new(vec![197001]).into(),
            error: "",
        },
        Test {
            name: "test_toyyyymm_constant_date32",
            display: "b()",
            arg_names: vec!["b"],
            func: ToYYYYMMFunction::try_create("b")?,
            columns: vec![DataColumn::Constant(DataValue::Int32(Some(0i32)), 10)],
            nullable: false,
            expect: Series::new(vec![197001]).into(),
            error: "",
        },
        Test {
            name: "test_toyyyymm_constant_datetime",
            display: "a()",
            arg_names: vec!["a"],
            func: ToYYYYMMFunction::try_create("a")?,
            columns: vec![DataColumn::Constant(DataValue::UInt32(Some(0u32)), 15)],
            nullable: false,
            expect: Series::new(vec![197001]).into(),
            error: "",
        },
        Test {
            name: "test_toyyyymmdd_date16",
            display: "c()",
            arg_names: vec!["c"],
            func: ToYYYYMMDDFunction::try_create("c")?,
            columns: vec![Series::new(vec![0u16]).into()],
            nullable: false,
            expect: Series::new(vec![19700101]).into(),
            error: "",
        },
        Test {
            name: "test_toyyyymmdd_date32",
            display: "b()",
            arg_names: vec!["b"],
            func: ToYYYYMMDDFunction::try_create("b")?,
            columns: vec![Series::new(vec![0i32]).into()],
            nullable: false,
            expect: Series::new(vec![19700101]).into(),
            error: "",
        },
        Test {
            name: "test_toyyyymmdd_datetime",
            display: "a()",
            arg_names: vec!["a"],
            func: ToYYYYMMDDFunction::try_create("a")?,
            columns: vec![Series::new(vec![1630833797u32]).into()],
            nullable: false,
            expect: Series::new(vec![20210905]).into(),
            error: "",
        },
    ];

    for t in tests {
        let rows = t.columns[0].len();

        // Type check
        let mut args = vec![];
        let mut fields = vec![];
        for name in t.arg_names {
            args.push(schema.field_with_name(name)?.data_type().clone());
            fields.push(schema.field_with_name(name)?.clone());
        }

        let columns: Vec<DataColumnWithField> = t
            .columns
            .iter()
            .zip(fields.iter())
            .map(|(c, f)| DataColumnWithField::new(c.clone(), f.clone()))
            .collect();

        let func = t.func;
        if let Err(e) = func.eval(&columns, rows) {
            assert_eq!(t.error, e.to_string());
        }

        // Display check
        let expect_display = t.display.to_string();
        let actual_display = format!("{}", func);
        assert_eq!(expect_display, actual_display);

        // Nullable check.
        let expect_null = t.nullable;
        let actual_null = func.nullable(&schema)?;
        assert_eq!(expect_null, actual_null);

        let expect_type = func.return_type(&args)?.clone();
        let v = &(func.eval(&columns, rows)?);
        let actual_type = v.data_type().clone();
        assert_eq!(expect_type, actual_type);

        assert_eq!(v, &t.expect);
    }

    Ok(())
}

#[test]
fn test_toyyyymmdd_function() -> Result<()> {
    #[allow(dead_code)]
    struct Test {
        name: &'static str,
        display: &'static str,
        arg_names: Vec<&'static str>,
        func: Box<dyn Function>,
        columns: Vec<DataColumn>,
        expect: DataColumn,
        error: &'static str,
        nullable: bool,
    }
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::DateTime32(None), false),
        DataField::new("b", DataType::Date32, false),
        DataField::new("c", DataType::Date16, false),
    ]);

    let tests = vec![
        Test {
            name: "test_toyyyymmdd_date16",
            display: "c()",
            arg_names: vec!["c"],
            func: ToYYYYMMDDFunction::try_create("c")?,
            columns: vec![Series::new(vec![0u16]).into()],
            nullable: false,
            expect: Series::new(vec![19700101]).into(),
            error: "",
        },
        Test {
            name: "test_toyyyymmdd_date32",
            display: "b()",
            arg_names: vec!["b"],
            func: ToYYYYMMDDFunction::try_create("b")?,
            columns: vec![Series::new(vec![0i32]).into()],
            nullable: false,
            expect: Series::new(vec![19700101]).into(),
            error: "",
        },
        Test {
            name: "test_toyyyymmdd_datetime",
            display: "a()",
            arg_names: vec!["a"],
            func: ToYYYYMMDDFunction::try_create("a")?,
            columns: vec![Series::new(vec![1630833797u32]).into()],
            nullable: false,
            expect: Series::new(vec![20210905]).into(),
            error: "",
        },
        Test {
            name: "test_toyyyymmdd_constant_date16",
            display: "c()",
            arg_names: vec!["c"],
            func: ToYYYYMMDDFunction::try_create("c")?,
            columns: vec![DataColumn::Constant(DataValue::UInt16(Some(0u16)), 5)],
            nullable: false,
            expect: Series::new(vec![19700101]).into(),
            error: "",
        },
        Test {
            name: "test_toyyyymmdd_constant_date32",
            display: "b()",
            arg_names: vec!["b"],
            func: ToYYYYMMDDFunction::try_create("b")?,
            columns: vec![DataColumn::Constant(DataValue::Int32(Some(0i32)), 10)],
            nullable: false,
            expect: Series::new(vec![19700101]).into(),
            error: "",
        },
        Test {
            name: "test_toyyyymmdd_constant_datetime",
            display: "a()",
            arg_names: vec!["a"],
            func: ToYYYYMMDDFunction::try_create("a")?,
            columns: vec![DataColumn::Constant(
                DataValue::UInt32(Some(1630833797u32)),
                15,
            )],
            nullable: false,
            expect: Series::new(vec![20210905]).into(),
            error: "",
        },
    ];

    for t in tests {
        let rows = t.columns[0].len();

        // Type check
        let mut args = vec![];
        let mut fields = vec![];
        for name in t.arg_names {
            args.push(schema.field_with_name(name)?.data_type().clone());
            fields.push(schema.field_with_name(name)?.clone());
        }

        let columns: Vec<DataColumnWithField> = t
            .columns
            .iter()
            .zip(fields.iter())
            .map(|(c, f)| DataColumnWithField::new(c.clone(), f.clone()))
            .collect();

        let func = t.func;
        if let Err(e) = func.eval(&columns, rows) {
            assert_eq!(t.error, e.to_string());
        }

        // Display check
        let expect_display = t.display.to_string();
        let actual_display = format!("{}", func);
        assert_eq!(expect_display, actual_display);

        // Nullable check.
        let expect_null = t.nullable;
        let actual_null = func.nullable(&schema)?;
        assert_eq!(expect_null, actual_null);

        let expect_type = func.return_type(&args)?.clone();
        let v = &(func.eval(&columns, rows)?);
        let actual_type = v.data_type().clone();
        assert_eq!(expect_type, actual_type);

        assert_eq!(v, &t.expect);
    }

    Ok(())
}

#[test]
fn test_toyyyymmddhhmmss_function() -> Result<()> {
    // date16
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0u16])]);

    {
        let col = ToYYYYMMDDhhmmssFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt64);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt64Array>().unwrap();
        let expected = UInt64Array::from_slice([19700101000000; 1]);
        assert_eq!(actual, &expected);
    }

    // date32
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0i32])]);

    {
        let col = ToYYYYMMDDhhmmssFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt64);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt64Array>().unwrap();
        let expected = UInt64Array::from_slice([19700101000000; 1]);

        assert_eq!(actual, &expected);
    }

    // dateTime
    // 2021-09-05 09:23:17 --- 1630833797
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![1630833797u32])]);

    {
        let col = ToYYYYMMDDhhmmssFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt64);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt64Array>().unwrap();
        let expected = UInt64Array::from_slice([20210905092317; 1]);

        assert_eq!(actual, &expected);
    }

    Ok(())
}

#[test]
fn test_toyyyymmhhmmss_constant_function() -> Result<()> {
    // date16
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::UInt16(Some(0u16)),
        5,
    )]);
    {
        let col = ToYYYYMMDDhhmmssFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 5);
        assert_eq!(result.data_type(), DataType::UInt64);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt64Array>().unwrap();
        let expected = UInt64Array::from_slice([19700101000000; 5]);

        assert_eq!(actual, &expected);
    }

    // date32
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::Int32(Some(0i32)),
        10,
    )]);
    {
        let col = ToYYYYMMDDhhmmssFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 10);
        assert_eq!(result.data_type(), DataType::UInt64);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt64Array>().unwrap();
        let expected = UInt64Array::from_slice([19700101000000; 10]);

        assert_eq!(actual, &expected);
    }

    // datetime
    // 2021-09-05 09:23:17 --- 1630833797
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::UInt32(Some(1630833797u32)),
        15,
    )]);
    {
        let col = ToYYYYMMDDhhmmssFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 15);
        assert_eq!(result.data_type(), DataType::UInt64);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt64Array>().unwrap();
        let expected = UInt64Array::from_slice([20210905092317; 15]);

        assert_eq!(actual, &expected);
    }

    Ok(())
}

#[test]
fn test_tomonth_function() -> Result<()> {
    // date16
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0u16])]);

    {
        let col = ToMonthFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([1; 1]);
        assert_eq!(actual, &expected);
    }

    // date32
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0i32])]);

    {
        let col = ToMonthFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([1; 1]);

        assert_eq!(actual, &expected);
    }

    // dateTime
    // 2021-10-01 17:50:17 --- 1633081817
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![1633081817u32])]);

    {
        let col = ToMonthFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([10; 1]);

        assert_eq!(actual, &expected);
    }

    Ok(())
}

#[test]
fn test_tomonth_constant_function() -> Result<()> {
    // date16
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::UInt16(Some(0u16)),
        5,
    )]);
    {
        let col = ToMonthFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 5);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([1; 5]);

        assert_eq!(actual, &expected);
    }

    // date32
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::Int32(Some(0i32)),
        10,
    )]);
    {
        let col = ToMonthFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 10);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([1; 10]);

        assert_eq!(actual, &expected);
    }

    // datetime
    // 2021-10-01 17:50:17 --- 1633081817
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::UInt32(Some(1633081817u32)),
        15,
    )]);
    {
        let col = ToMonthFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 15);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([10; 15]);

        assert_eq!(actual, &expected);
    }

    Ok(())
}

#[test]
fn test_todayofyear_function() -> Result<()> {
    // date16
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0u16])]);

    {
        let col = ToDayOfYearFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt16);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt16Array>().unwrap();
        let expected = UInt16Array::from_slice([1; 1]);
        assert_eq!(actual, &expected);
    }

    // date32
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0i32])]);

    {
        let col = ToDayOfYearFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt16);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt16Array>().unwrap();
        let expected = UInt16Array::from_slice([1; 1]);

        assert_eq!(actual, &expected);
    }

    // dateTime
    // 2021-10-02 19:15:24 --- 1633173324
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![1633173324u32])]);

    {
        let col = ToDayOfYearFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt16);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt16Array>().unwrap();
        let expected = UInt16Array::from_slice([275; 1]);

        assert_eq!(actual, &expected);
    }

    Ok(())
}

#[test]
fn test_todayofyear_constant_function() -> Result<()> {
    // date16
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::UInt16(Some(0u16)),
        5,
    )]);
    {
        let col = ToDayOfYearFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 5);
        assert_eq!(result.data_type(), DataType::UInt16);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt16Array>().unwrap();
        let expected = UInt16Array::from_slice([1; 5]);

        assert_eq!(actual, &expected);
    }

    // date32
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::Int32(Some(0i32)),
        10,
    )]);
    {
        let col = ToDayOfYearFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 10);
        assert_eq!(result.data_type(), DataType::UInt16);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt16Array>().unwrap();
        let expected = UInt16Array::from_slice([1; 10]);

        assert_eq!(actual, &expected);
    }

    // datetime
    // 2021-10-02 19:15:24 --- 1633173324
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::UInt32(Some(1633173324u32)),
        15,
    )]);
    {
        let col = ToDayOfYearFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 15);
        assert_eq!(result.data_type(), DataType::UInt16);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt16Array>().unwrap();
        let expected = UInt16Array::from_slice([275; 15]);

        assert_eq!(actual, &expected);
    }

    Ok(())
}

#[test]
fn test_todayofweek_function() -> Result<()> {
    // date16
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0u16])]);

    {
        let col = ToDayOfWeekFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([4; 1]);
        assert_eq!(actual, &expected);
    }

    // date32
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0i32])]);

    {
        let col = ToDayOfWeekFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([4; 1]);

        assert_eq!(actual, &expected);
    }

    // dateTime
    // 2021-10-02 19:15:24 --- 1633173324
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![1633173324u32])]);

    {
        let col = ToDayOfWeekFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([6; 1]);

        assert_eq!(actual, &expected);
    }

    Ok(())
}

#[test]
fn test_todayofweek_constant_function() -> Result<()> {
    // date16
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::UInt16(Some(0u16)),
        5,
    )]);
    {
        let col = ToDayOfWeekFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 5);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([4; 5]);

        assert_eq!(actual, &expected);
    }

    // date32
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::Int32(Some(0i32)),
        10,
    )]);
    {
        let col = ToDayOfWeekFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 10);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([4; 10]);

        assert_eq!(actual, &expected);
    }

    // datetime
    // 2021-10-02 19:15:24 --- 1633173324
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::UInt32(Some(1633173324u32)),
        15,
    )]);
    {
        let col = ToDayOfWeekFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 15);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([6; 15]);

        assert_eq!(actual, &expected);
    }

    Ok(())
}

#[test]
fn test_todayofmonth_function() -> Result<()> {
    // date16
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0u16])]);

    {
        let col = ToDayOfMonthFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([1; 1]);
        assert_eq!(actual, &expected);
    }

    // date32
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0i32])]);

    {
        let col = ToDayOfMonthFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([1; 1]);

        assert_eq!(actual, &expected);
    }

    // dateTime
    // 2021-10-02 19:15:24 --- 1633173324
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![1633173324u32])]);

    {
        let col = ToDayOfMonthFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([2; 1]);

        assert_eq!(actual, &expected);
    }

    Ok(())
}

#[test]
fn test_todayofmonth_constant_function() -> Result<()> {
    // date16
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::UInt16(Some(0u16)),
        5,
    )]);
    {
        let col = ToDayOfMonthFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 5);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([1; 5]);

        assert_eq!(actual, &expected);
    }

    // date32
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::Int32(Some(0i32)),
        10,
    )]);
    {
        let col = ToDayOfMonthFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 10);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([1; 10]);

        assert_eq!(actual, &expected);
    }

    // datetime
    // 2021-10-02 19:15:24 --- 1633173324
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::UInt32(Some(1633173324u32)),
        15,
    )]);
    {
        let col = ToDayOfMonthFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 15);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([2; 15]);

        assert_eq!(actual, &expected);
    }

    Ok(())
}

#[test]
fn test_tohour_function() -> Result<()> {
    // date16
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0u16])]);

    {
        let col = ToHourFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([0; 1]);
        assert_eq!(actual, &expected);
    }

    // date32
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0i32])]);

    {
        let col = ToHourFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([0; 1]);

        assert_eq!(actual, &expected);
    }

    // dateTime
    // 2021-10-18 18:05:42 --- 1634551542
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![1634551542u32])]);

    {
        let col = ToHourFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([10; 1]);

        assert_eq!(actual, &expected);
    }

    Ok(())
}

#[test]
fn test_tohour_constant_function() -> Result<()> {
    // date16
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::UInt16(Some(0u16)),
        5,
    )]);
    {
        let col = ToHourFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 5);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([0; 5]);

        assert_eq!(actual, &expected);
    }

    // date32
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::Int32(Some(0i32)),
        10,
    )]);
    {
        let col = ToHourFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 10);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([0; 10]);

        assert_eq!(actual, &expected);
    }

    // datetime
    // 2021-10-18 18:05:42 --- 1634551542
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::UInt32(Some(1634551542u32)),
        15,
    )]);
    {
        let col = ToHourFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 15);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([10; 15]);

        assert_eq!(actual, &expected);
    }

    Ok(())
}

#[test]
fn test_tominute_function() -> Result<()> {
    // date16
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0u16])]);

    {
        let col = ToMinuteFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([0; 1]);
        assert_eq!(actual, &expected);
    }

    // date32
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0i32])]);

    {
        let col = ToMinuteFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([0; 1]);

        assert_eq!(actual, &expected);
    }

    // dateTime
    // 2021-10-18 18:05:42 --- 1634551542
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![1634551542u32])]);

    {
        let col = ToMinuteFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([5; 1]);

        assert_eq!(actual, &expected);
    }

    Ok(())
}

#[test]
fn test_tominute_constant_function() -> Result<()> {
    // date16
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::UInt16(Some(0u16)),
        5,
    )]);
    {
        let col = ToMinuteFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 5);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([0; 5]);

        assert_eq!(actual, &expected);
    }

    // date32
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::Int32(Some(0i32)),
        10,
    )]);
    {
        let col = ToMinuteFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 10);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([0; 10]);

        assert_eq!(actual, &expected);
    }

    // datetime
    // 2021-10-18 18:05:42 --- 1634551542
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::UInt32(Some(1634551542u32)),
        15,
    )]);
    {
        let col = ToMinuteFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 15);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([5; 15]);

        assert_eq!(actual, &expected);
    }

    Ok(())
}

#[test]
fn test_tosecond_function() -> Result<()> {
    // date16
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0u16])]);

    {
        let col = ToSecondFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([0; 1]);
        assert_eq!(actual, &expected);
    }

    // date32
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0i32])]);

    {
        let col = ToSecondFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([0; 1]);

        assert_eq!(actual, &expected);
    }

    // dateTime
    // 2021-10-18 18:05:42 --- 1634551542
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![1634551542u32])]);

    {
        let col = ToSecondFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([42; 1]);

        assert_eq!(actual, &expected);
    }

    Ok(())
}

#[test]
fn test_tosecond_constant_function() -> Result<()> {
    // date16
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::UInt16(Some(0u16)),
        5,
    )]);
    {
        let col = ToSecondFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 5);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([0; 5]);

        assert_eq!(actual, &expected);
    }

    // date32
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::Int32(Some(0i32)),
        10,
    )]);
    {
        let col = ToSecondFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 10);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([0; 10]);

        assert_eq!(actual, &expected);
    }

    // datetime
    // 2021-10-18 18:05:42 --- 1634551542
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::UInt32(Some(1634551542u32)),
        15,
    )]);
    {
        let col = ToSecondFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 15);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([42; 15]);

        assert_eq!(actual, &expected);
    }

    Ok(())
}

#[test]
fn test_tomonday_function() -> Result<()> {
    // date16 2021-10-19 -> 18919
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![18919u16])]);

    {
        let col = ToMondayFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt16);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt16Array>().unwrap();
        let expected = UInt16Array::from_slice([18918u16; 1]);
        assert_eq!(actual, &expected);
    }

    // date32
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![18919i32])]);

    {
        let col = ToMondayFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt16);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt16Array>().unwrap();
        let expected = UInt16Array::from_slice([18918u16; 1]);

        assert_eq!(actual, &expected);
    }

    // dateTime
    // 2021-10-19 11:31:58 --- 1634614318
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![1634614318u32])]);

    {
        let col = ToMondayFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt16);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt16Array>().unwrap();
        let expected = UInt16Array::from_slice([18918u16; 1]);

        assert_eq!(actual, &expected);
    }

    Ok(())
}
