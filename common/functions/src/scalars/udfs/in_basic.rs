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

use std::collections::HashSet;
use std::fmt;

use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::DataColumnsWithField;
use common_datavalues::prelude::MutableArrayBuilder;
use common_datavalues::prelude::MutableBooleanArrayBuilder;
use common_datavalues::types::merge_types;
use common_datavalues::DataType;
use common_datavalues::DataTypeAndNullable;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
pub struct InFunction<const NEGATED: bool>;

impl<const NEGATED: bool> InFunction<NEGATED> {
    pub fn try_create(_display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(InFunction::<NEGATED> {}))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .bool_function()
                .variadic_arguments(2, usize::MAX),
        )
    }
}

macro_rules! basic_contains {
    // bool no_null_iter returns bool not &bool, other types no_null_iter return &T
    ($INPUT_DT: expr, $INPUT_ARRAY: expr, $CHECK_ARRAY: expr, $NEGATED: expr, $BUILDER: expr, $CAST_TYPE: ident, bool) => {
        let mut vals_set = HashSet::new();
        for array in $CHECK_ARRAY {
            let array = array.column().cast_with_type($INPUT_DT)?;
            let data = array.try_get(0)?;
            match data {
                DataValue::$CAST_TYPE(Some(val)) => {
                    vals_set.insert(val);
                }
                DataValue::$CAST_TYPE(None) => {
                    continue;
                }
                DataValue::Null => {
                    continue;
                }
                _ => {
                    return Err(ErrorCode::LogicalError("it's a bug"));
                }
            }
        }
        let arr = $INPUT_ARRAY.bool()?;
        for val in arr.into_no_null_iter() {
            let contains = vals_set.contains(&val);
            $BUILDER.push((contains && !NEGATED) || (!contains && NEGATED));
        }
    };
    ($INPUT_DT: expr, $INPUT_ARRAY: expr, $CHECK_ARRAY: expr, $NEGATED: expr, $BUILDER: expr, $CAST_TYPE: ident, $PRIMITIVE_TYPE: ident) => {
        let mut vals_set = HashSet::new();
        for array in $CHECK_ARRAY {
            let array = array.column().cast_with_type($INPUT_DT)?;
            let data = array.try_get(0)?;
            match data {
                DataValue::$CAST_TYPE(Some(val)) => {
                    vals_set.insert(val);
                }
                DataValue::$CAST_TYPE(None) => {
                    continue;
                }
                DataValue::Null => {
                    continue;
                }
                _ => {
                    return Err(ErrorCode::LogicalError("it's a bug"));
                }
            }
        }
        let arr = $INPUT_ARRAY.$PRIMITIVE_TYPE()?;
        for val in arr.into_no_null_iter() {
            let contains = vals_set.contains(val);
            $BUILDER.push((contains && !NEGATED) || (!contains && NEGATED));
        }
    };
}

// float type can not impl Hash and Eq trait, so it can not use HashSet
// maybe we can find some more efficient way to make it.
macro_rules! float_contains {
    ($INPUT_DT: expr, $INPUT_ARRAY: expr, $CHECK_ARRAY: expr, $NEGATED: expr, $BUILDER: expr, $CAST_TYPE: ident, $PRIMITIVE_TYPE: ident) => {
        let mut vals_set = Vec::new();
        for array in $CHECK_ARRAY {
            let array = array.column().cast_with_type($INPUT_DT)?;
            let data = array.try_get(0)?;
            match data {
                DataValue::$CAST_TYPE(Some(val)) => {
                    vals_set.push(val);
                }
                DataValue::$CAST_TYPE(None) => {
                    continue;
                }
                DataValue::Null => {
                    continue;
                }
                _ => {
                    return Err(ErrorCode::LogicalError("it's a bug"));
                }
            }
        }
        let arr = $INPUT_ARRAY.$PRIMITIVE_TYPE()?;
        for val in arr.into_no_null_iter() {
            let contains = vals_set.contains(val);
            $BUILDER.push((contains && !NEGATED) || (!contains && NEGATED));
        }
    };
}

impl<const NEGATED: bool> Function for InFunction<NEGATED> {
    fn name(&self) -> &str {
        "InFunction"
    }

    fn return_type(&self, args: &[DataTypeAndNullable]) -> Result<DataTypeAndNullable> {
        let input_dt = args[0].data_type();
        if input_dt == &DataType::Null {
            return Ok(DataTypeAndNullable::create(input_dt, false));
        }

        let dt = DataType::Boolean;
        Ok(DataTypeAndNullable::create(&dt, false))
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        let input_column = columns[0].column();

        let input_array = match input_column {
            DataColumn::Array(array) => array.to_owned(),
            DataColumn::Constant(scalar, _) => scalar.to_array()?,
        };

        let input_dt = input_array.data_type();
        if input_dt == &DataType::Null {
            return Ok(DataColumn::Constant(DataValue::Null, input_rows));
        }
        let mut builder = MutableBooleanArrayBuilder::<false>::with_capacity(input_column.len());

        let check_arrays = &columns[1..];

        let mut least_super_dt = input_dt.clone();
        for array in check_arrays {
            least_super_dt = merge_types(&least_super_dt, array.data_type())?;
        }
        let input_array = input_array.cast_with_type(&least_super_dt)?;

        match least_super_dt {
            DataType::Boolean => {
                basic_contains!(
                    &least_super_dt,
                    input_array,
                    check_arrays,
                    NEGATED,
                    builder,
                    Boolean,
                    bool
                );
            }
            DataType::UInt8 => {
                basic_contains!(
                    &least_super_dt,
                    input_array,
                    check_arrays,
                    NEGATED,
                    builder,
                    UInt8,
                    u8
                );
            }
            DataType::UInt16 => {
                basic_contains!(
                    &least_super_dt,
                    input_array,
                    check_arrays,
                    NEGATED,
                    builder,
                    UInt16,
                    u16
                );
            }
            DataType::UInt32 => {
                basic_contains!(
                    &least_super_dt,
                    input_array,
                    check_arrays,
                    NEGATED,
                    builder,
                    UInt32,
                    u32
                );
            }
            DataType::UInt64 => {
                basic_contains!(
                    &least_super_dt,
                    input_array,
                    check_arrays,
                    NEGATED,
                    builder,
                    UInt64,
                    u64
                );
            }
            DataType::Int8 => {
                basic_contains!(
                    &least_super_dt,
                    input_array,
                    check_arrays,
                    NEGATED,
                    builder,
                    Int8,
                    i8
                );
            }
            DataType::Int16 => {
                basic_contains!(
                    &least_super_dt,
                    input_array,
                    check_arrays,
                    NEGATED,
                    builder,
                    Int16,
                    i16
                );
            }
            DataType::Int32 => {
                basic_contains!(
                    &least_super_dt,
                    input_array,
                    check_arrays,
                    NEGATED,
                    builder,
                    Int32,
                    i32
                );
            }
            DataType::Int64 => {
                basic_contains!(
                    &least_super_dt,
                    input_array,
                    check_arrays,
                    NEGATED,
                    builder,
                    Int64,
                    i64
                );
            }
            DataType::Float32 => {
                float_contains!(
                    &least_super_dt,
                    input_array,
                    check_arrays,
                    NEGATED,
                    builder,
                    Float32,
                    f32
                );
            }
            DataType::Float64 => {
                float_contains!(
                    &least_super_dt,
                    input_array,
                    check_arrays,
                    NEGATED,
                    builder,
                    Float64,
                    f64
                );
            }
            DataType::String => {
                basic_contains!(
                    &least_super_dt,
                    input_array,
                    check_arrays,
                    NEGATED,
                    builder,
                    String,
                    string
                );
            }
            DataType::Struct(_) => {}
            _ => {
                unimplemented!()
            }
        }

        Ok(DataColumn::Array(builder.as_series()))
    }

    fn passthrough_null(&self) -> bool {
        false
    }
}

impl<const NEGATED: bool> fmt::Display for InFunction<NEGATED> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if NEGATED {
            write!(f, "NOT IN")
        } else {
            write!(f, "IN")
        }
    }
}
