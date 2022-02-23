// Copyright 2022 Datafuse Labs.
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

use std::fmt;
use std::ops::Sub;
use std::str;
use std::sync::Arc;

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;

#[derive(Clone)]
pub struct RunningDifferenceFunction {
    display_name: String,
}

impl RunningDifferenceFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(RunningDifferenceFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().num_arguments(1))
    }
}

impl Function for RunningDifferenceFunction {
    fn name(&self) -> &str {
        self.display_name.as_str()
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        let nullable = args.iter().any(|arg| arg.is_nullable());
        let dt = remove_nullable(args[0]);

        let output_type = match dt.data_type_id() {
            TypeID::Int8 | TypeID::UInt8 => Ok(type_primitive::Int16Type::arc()),
            TypeID::Int16 | TypeID::UInt16 | TypeID::Date16 => Ok(type_primitive::Int32Type::arc()),
            TypeID::Int32
            | TypeID::UInt32
            | TypeID::Int64
            | TypeID::UInt64
            | TypeID::Date32
            | TypeID::DateTime32
            | TypeID::DateTime64
            | TypeID::Interval => Ok(type_primitive::Int64Type::arc()),
            TypeID::Float32 | TypeID::Float64 => Ok(type_primitive::Float64Type::arc()),
            _ => Result::Err(ErrorCode::IllegalDataType(
                "Argument for function runningDifference must have numeric type",
            )),
        }?;

        if nullable {
            Ok(Arc::new(NullableType::create(output_type)))
        } else {
            Ok(output_type)
        }
    }

    fn eval(&self, columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        let dt = remove_nullable(columns[0].data_type());
        let col = columns[0].column();
        match dt.data_type_id() {
            TypeID::Int8 => compute_i8(col, input_rows),
            TypeID::UInt8 => compute_u8(col, input_rows),
            TypeID::Int16 => compute_i16(col, input_rows),
            TypeID::UInt16 | TypeID::Date16 => compute_u16(col, input_rows),
            TypeID::Int32 | TypeID::Date32 => compute_i32(col, input_rows),
            TypeID::UInt32 | TypeID::DateTime32 => compute_u32(col, input_rows),
            TypeID::Int64 | TypeID::Interval | TypeID::DateTime64 => compute_i64(col, input_rows),
            TypeID::UInt64 => compute_u64(col, input_rows),
            TypeID::Float32 => compute_f32(col, input_rows),
            TypeID::Float64 => compute_f64(col, input_rows),

            _ => Result::Err(ErrorCode::IllegalDataType(
                format!(
                    "Argument for function runningDifference must have numeric type.: While processing runningDifference({})",
                    columns[0].field().name(),
                )))
        }
    }

    // The function runningDifference compares the value[index] and value[index-1].
    // If we set passthrough_null to be true, the input will be optimized with non-null + masking.
    // Considering tht we actually need two masks(one for value[index], the other value[index-1]) for validity,
    // we choose to handler the nullable ourselves.
    fn passthrough_null(&self) -> bool {
        false
    }
}

macro_rules! run_difference_compute {
    ($method_name:ident, $source_primitive_type:ident, $result_data_type:ident, $result_primitive_type:ty, $sub_op:ident) => {
        fn $method_name(column: &ColumnRef, input_rows: usize) -> Result<ColumnRef> {
            if column.is_const() || column.is_empty() {
                let ty = $result_data_type::arc();
                let column = ty.create_constant_column(&DataValue::Int64(0), input_rows)?;
                return Ok(column);
            }

            let viewer = <$source_primitive_type>::try_create_viewer(column)?;
            let viewer_iter_a = viewer.iter();
            let viewer_iter_b = viewer.iter();

            let size = viewer.size();
            if column.is_nullable() {
                let mut builder: NullableColumnBuilder<$result_primitive_type> =
                    NullableColumnBuilder::with_capacity(input_rows);

                builder.append(0 as $result_primitive_type, viewer.valid_at(0));
                for (a, (index, b)) in viewer_iter_a
                    .skip(1)
                    .zip(viewer_iter_b.take(size - 1).enumerate())
                {
                    if viewer.null_at(index + 1) || viewer.null_at(index) {
                        builder.append_null();
                    } else {
                        let a = a as $result_primitive_type;
                        let b = b as $result_primitive_type;
                        // For Int64/UInt64 subtraction we need to use wrapping
                        let diff = a.$sub_op(b);
                        builder.append(diff, true);
                    }
                }

                Ok(builder.build(input_rows))
            } else {
                let mut builder: ColumnBuilder<$result_primitive_type> =
                    ColumnBuilder::with_capacity(input_rows);
                builder.append(0 as $result_primitive_type);

                for (a, b) in viewer_iter_a.skip(1).zip(viewer_iter_b.take(size - 1)) {
                    let a = a as $result_primitive_type;
                    let b = b as $result_primitive_type;
                    // For Int64/UInt64 subtraction we need to use wrapping
                    let diff = a.$sub_op(b);
                    builder.append(diff);
                }
                Ok(builder.build(input_rows))
            }
        }
    };
}

run_difference_compute!(compute_i8, i8, Int16Type, i16, sub);
run_difference_compute!(compute_u8, u8, Int16Type, i16, sub);
run_difference_compute!(compute_i16, i16, Int32Type, i32, sub);
run_difference_compute!(compute_u16, u16, Int32Type, i32, sub);
run_difference_compute!(compute_i32, i32, Int64Type, i64, wrapping_sub);
run_difference_compute!(compute_u32, u32, Int64Type, i64, wrapping_sub);
run_difference_compute!(compute_i64, i64, Int64Type, i64, wrapping_sub);
run_difference_compute!(compute_u64, u64, Int64Type, i64, wrapping_sub);
run_difference_compute!(compute_f32, f32, Float64Type, f64, sub);
run_difference_compute!(compute_f64, f64, Float64Type, f64, sub);

impl fmt::Display for RunningDifferenceFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}()", self.display_name)
    }
}
