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

use common_arrow::arrow::array::Float64Array;
use common_arrow::arrow::array::Int16Array;
use common_arrow::arrow::array::Int32Array;
use common_arrow::arrow::array::Int64Array;
use common_datablocks::*;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_functions::scalars::RunningDifferenceFunction;

macro_rules! run_difference_constant_test {
    ($method_name:ident, $primitive_type:ty, $logic_type:ident, $result_primitive_type:ty, $result_logic_type:ident, $array_type:ident) => {
        #[test]
        fn $method_name() -> Result<()> {
            let schema =
                DataSchemaRefExt::create(vec![DataField::new("a", DataType::$logic_type, false)]);
            let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
                DataValue::$logic_type(Some(10_i8 as $primitive_type)),
                5,
            )]);

            // Ok.
            {
                let run_difference_function = RunningDifferenceFunction::try_create("a")?;
                let columns = vec![DataColumnWithField::new(
                    block.try_column_by_name("a")?.clone(),
                    schema.field_with_name("a")?.clone(),
                )];

                // eval
                let result = run_difference_function.eval(&columns, block.num_rows())?;
                let actual_ref = result.get_array_ref().unwrap();
                let actual = actual_ref.as_any().downcast_ref::<$array_type>().unwrap();
                let expected = $array_type::from_slice([0i8 as $result_primitive_type; 5]);

                assert_eq!(&expected, actual);

                // result type
                let args_type_array = [DataType::$logic_type; 1];
                let result_type = run_difference_function.return_type(&args_type_array[..])?;
                assert_eq!(result_type, DataType::$result_logic_type);
            }

            Ok(())
        }
    };
}

run_difference_constant_test!(
    test_running_difference_constant_i8,
    i8,
    Int8,
    i16,
    Int16,
    Int16Array
);
run_difference_constant_test!(
    test_running_difference_constant_u8,
    u8,
    UInt8,
    i16,
    Int16,
    Int16Array
);
run_difference_constant_test!(
    test_running_difference_constant_i16,
    i16,
    Int16,
    i32,
    Int32,
    Int32Array
);
run_difference_constant_test!(
    test_running_difference_constant_u16,
    u16,
    UInt16,
    i32,
    Int32,
    Int32Array
);
run_difference_constant_test!(
    test_running_difference_constant_i32,
    i32,
    Int32,
    i64,
    Int64,
    Int64Array
);
run_difference_constant_test!(
    test_running_difference_constant_u32,
    u32,
    UInt32,
    i64,
    Int64,
    Int64Array
);
run_difference_constant_test!(
    test_running_difference_constant_i64,
    i64,
    Int64,
    i64,
    Int64,
    Int64Array
);
run_difference_constant_test!(
    test_running_difference_constant_u64,
    u64,
    UInt64,
    i64,
    Int64,
    Int64Array
);
run_difference_constant_test!(
    test_running_difference_constant_f32,
    f32,
    Float32,
    f64,
    Float64,
    Float64Array
);
run_difference_constant_test!(
    test_running_difference_constant_f64,
    f64,
    Float64,
    f64,
    Float64,
    Float64Array
);

macro_rules! run_difference_first_not_null_test {
    ($method_name:ident, $primitive_type:ty, $logic_type:ident, $result_primitive_type:ty, $result_logic_type:ident, $array_type:ident) => {
        #[test]
        fn $method_name() -> Result<()> {
            let schema =
                DataSchemaRefExt::create(vec![DataField::new("a", DataType::$logic_type, true)]);
            let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![
                Some(2_i8 as $primitive_type),
                Some(3_i8 as $primitive_type),
                None,
                Some(4_i8 as $primitive_type),
                Some(10_i8 as $primitive_type),
            ])]);

            // Ok.
            {
                let run_difference_function = RunningDifferenceFunction::try_create("a")?;
                let columns = vec![DataColumnWithField::new(
                    block.try_column_by_name("a")?.clone(),
                    schema.field_with_name("a")?.clone(),
                )];

                // eval
                let result = run_difference_function.eval(&columns, block.num_rows())?;
                let actual_ref = result.get_array_ref().unwrap();
                let actual = actual_ref.as_any().downcast_ref::<$array_type>().unwrap();
                let expected = $array_type::from([
                    Some(0_i8 as $result_primitive_type),
                    Some(1_i8 as $result_primitive_type),
                    None,
                    None,
                    Some(6_i8 as $result_primitive_type),
                ]);

                assert_eq!(&expected, actual);

                // result type
                let args_type_array = [DataType::$logic_type; 1];
                let result_type = run_difference_function.return_type(&args_type_array[..])?;

                assert_eq!(result_type, DataType::$result_logic_type);
            }

            Ok(())
        }
    };
}

run_difference_first_not_null_test!(
    test_running_difference_i8_first_not_null,
    i8,
    Int8,
    i16,
    Int16,
    Int16Array
);
run_difference_first_not_null_test!(
    test_running_difference_u8_first_not_null,
    u8,
    UInt8,
    i16,
    Int16,
    Int16Array
);
run_difference_first_not_null_test!(
    test_running_difference_i16_first_not_null,
    i16,
    Int16,
    i32,
    Int32,
    Int32Array
);
run_difference_first_not_null_test!(
    test_running_difference_u16_first_not_null,
    u16,
    UInt16,
    i32,
    Int32,
    Int32Array
);
run_difference_first_not_null_test!(
    test_running_difference_i32_first_not_null,
    i32,
    Int32,
    i64,
    Int64,
    Int64Array
);
run_difference_first_not_null_test!(
    test_running_difference_u32_first_not_null,
    u32,
    UInt32,
    i64,
    Int64,
    Int64Array
);
run_difference_first_not_null_test!(
    test_running_difference_i64_first_not_null,
    i64,
    Int64,
    i64,
    Int64,
    Int64Array
);
run_difference_first_not_null_test!(
    test_running_difference_u64_first_not_null,
    u64,
    UInt64,
    i64,
    Int64,
    Int64Array
);
run_difference_first_not_null_test!(
    test_running_difference_data16_first_not_null,
    u16,
    Date16,
    i32,
    Int32,
    Int32Array
);
run_difference_first_not_null_test!(
    test_running_difference_data32_first_not_null,
    i32,
    Date32,
    i64,
    Int64,
    Int64Array
);
run_difference_first_not_null_test!(
    test_running_difference_f32_first_not_null,
    f32,
    Float32,
    f64,
    Float64,
    Float64Array
);
run_difference_first_not_null_test!(
    test_running_difference_f64_first_not_null,
    f64,
    Float64,
    f64,
    Float64,
    Float64Array
);

macro_rules! run_difference_first_null_test {
    ($method_name:ident, $primitive_type:ty, $logic_type:ident, $result_primitive_type:ty, $result_logic_type:ident, $array_type:ident) => {
        #[test]
        fn $method_name() -> Result<()> {
            let schema =
                DataSchemaRefExt::create(vec![DataField::new("a", DataType::$logic_type, true)]);
            let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![
                None,
                Some(1_i8 as $primitive_type),
                None,
                Some(3),
                Some(7),
            ])]);

            // Ok.
            {
                let run_difference_function = RunningDifferenceFunction::try_create("a")?;
                let columns = vec![DataColumnWithField::new(
                    block.try_column_by_name("a")?.clone(),
                    schema.field_with_name("a")?.clone(),
                )];

                // eval
                let result = run_difference_function.eval(&columns, block.num_rows())?;
                let actual_ref = result.get_array_ref().unwrap();
                let actual = actual_ref.as_any().downcast_ref::<$array_type>().unwrap();
                let expected = $array_type::from([
                    None,
                    None,
                    None,
                    None,
                    Some(4_i8 as $result_primitive_type),
                ]);

                assert_eq!(&expected, actual);

                // result type
                let args_type_array = [DataType::$logic_type; 1];
                let result_type = run_difference_function.return_type(&args_type_array[..])?;
                assert_eq!(result_type, DataType::$result_logic_type);
            }

            Ok(())
        }
    };
}

run_difference_first_null_test!(
    test_running_difference_i8_first_null,
    i8,
    Int8,
    i16,
    Int16,
    Int16Array
);

run_difference_first_null_test!(
    test_running_difference_u8_first_null,
    u8,
    UInt8,
    i16,
    Int16,
    Int16Array
);

run_difference_first_null_test!(
    test_running_difference_i16_first_null,
    i16,
    Int16,
    i32,
    Int32,
    Int32Array
);

run_difference_first_null_test!(
    test_running_difference_u16_first_null,
    u16,
    UInt16,
    i32,
    Int32,
    Int32Array
);

run_difference_first_null_test!(
    test_running_difference_i32_first_null,
    i32,
    Int32,
    i64,
    Int64,
    Int64Array
);

run_difference_first_null_test!(
    test_running_difference_u32_first_null,
    u32,
    UInt32,
    i64,
    Int64,
    Int64Array
);

run_difference_first_null_test!(
    test_running_difference_i64_first_null,
    i64,
    Int64,
    i64,
    Int64,
    Int64Array
);

run_difference_first_null_test!(
    test_running_difference_u64_first_null,
    u64,
    UInt64,
    i64,
    Int64,
    Int64Array
);

run_difference_first_null_test!(
    test_running_difference_date16_first_null,
    u16,
    UInt16,
    i32,
    Int32,
    Int32Array
);

run_difference_first_null_test!(
    test_running_difference_date32_first_null,
    i32,
    Date32,
    i64,
    Int64,
    Int64Array
);

#[test]
fn test_running_difference_datetime32_first_not_null() -> Result<()> {
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), true)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![
        Some(2_u32),
        Some(3),
        None,
        Some(4),
        Some(10),
    ])]);

    // Ok.
    {
        let run_difference_function = RunningDifferenceFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];

        // eval
        let result = run_difference_function.eval(&columns, block.num_rows())?;
        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<Int64Array>().unwrap();
        let expected = Int64Array::from([Some(0i64), Some(1), None, None, Some(6)]);
        assert_eq!(&expected, actual);

        // result type
        let args_type_array = [DataType::DateTime32(None); 1];
        let result_type = run_difference_function.return_type(&args_type_array[..])?;
        assert_eq!(result_type, DataType::Int64);
    }

    Ok(())
}

#[test]
fn test_running_difference_datetime32_first_null() -> Result<()> {
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), true)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![
        Some(2_u32),
        Some(3),
        None,
        Some(4),
        Some(10),
    ])]);

    // Ok.
    {
        let run_difference_function = RunningDifferenceFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];

        // eval
        let result = run_difference_function.eval(&columns, block.num_rows())?;
        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<Int64Array>().unwrap();
        let expected = Int64Array::from([Some(0), Some(1_i64), None, None, Some(6_i64)]);

        assert_eq!(&expected, actual);

        // result type
        let args_type_array = [DataType::DateTime32(None); 1];
        let result_type = run_difference_function.return_type(&args_type_array[..])?;
        assert_eq!(result_type, DataType::Int64);
    }

    Ok(())
}
