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

use common_arrow::arrow::bitmap::MutableBitmap;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::FactoryCreator;
use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

#[derive(Clone)]
pub struct AsFunction {
    display_name: String,
    data_type: DataTypeImpl,
    type_id: TypeID,
}

impl AsFunction {
    pub fn try_create(
        display_name: String,
        type_id: TypeID,
        args: &[&DataTypeImpl],
    ) -> Result<Box<dyn Function>> {
        Ok(Box::new(AsFunction {
            display_name,
            data_type: args[0].clone(),
            type_id,
        }))
    }
}

impl Function for AsFunction {
    fn name(&self) -> &str {
        self.display_name.as_str()
    }

    fn return_type(&self) -> DataTypeImpl {
        if !self.data_type.data_type_id().is_variant() {
            return NullType::new_impl();
        }
        match self.type_id {
            TypeID::Boolean => NullableType::new_impl(BooleanType::new_impl()),
            TypeID::Int64 => NullableType::new_impl(Int64Type::new_impl()),
            TypeID::Float64 => NullableType::new_impl(Float64Type::new_impl()),
            TypeID::String => NullableType::new_impl(StringType::new_impl()),
            TypeID::VariantArray => NullableType::new_impl(VariantArrayType::new_impl()),
            TypeID::VariantObject => NullableType::new_impl(VariantObjectType::new_impl()),
            _ => unreachable!(),
        }
    }

    fn eval(
        &self,
        _func_ctx: FunctionContext,
        columns: &ColumnsWithField,
        input_rows: usize,
    ) -> Result<ColumnRef> {
        if !columns[0].field().data_type().data_type_id().is_variant() {
            return NullType::new_impl().create_constant_column(&DataValue::Null, input_rows);
        }

        let variant_column: &VariantColumn = if columns[0].column().is_const() {
            let const_column: &ConstColumn = Series::check_get(columns[0].column())?;
            Series::check_get(const_column.inner())?
        } else {
            Series::check_get(columns[0].column())?
        };

        match self.type_id {
            TypeID::Boolean => {
                let mut builder = NullableColumnBuilder::<bool>::with_capacity(input_rows);
                for val in variant_column.scalar_iter() {
                    match val.as_bool() {
                        Some(v) => builder.append(v, true),
                        None => builder.append_null(),
                    }
                }
                return Ok(builder.build(input_rows));
            }
            TypeID::Int64 => {
                let mut builder = NullableColumnBuilder::<i64>::with_capacity(input_rows);
                for val in variant_column.scalar_iter() {
                    match val.as_i64() {
                        Some(v) => builder.append(v, true),
                        None => builder.append_null(),
                    }
                }
                return Ok(builder.build(input_rows));
            }
            TypeID::Float64 => {
                let mut builder = NullableColumnBuilder::<f64>::with_capacity(input_rows);
                for val in variant_column.scalar_iter() {
                    match val.as_f64() {
                        Some(v) => builder.append(v, true),
                        None => builder.append_null(),
                    }
                }
                return Ok(builder.build(input_rows));
            }
            TypeID::String => {
                let mut builder = NullableColumnBuilder::<Vu8>::with_capacity(input_rows);
                for val in variant_column.scalar_iter() {
                    match val.as_str() {
                        Some(v) => builder.append(v.as_bytes(), true),
                        None => builder.append_null(),
                    }
                }
                return Ok(builder.build(input_rows));
            }
            TypeID::VariantArray => {
                let mut bitmap = MutableBitmap::with_capacity(input_rows);
                bitmap.extend_constant(input_rows, true);
                for (i, val) in variant_column.scalar_iter().enumerate() {
                    if !val.is_array() {
                        bitmap.set(i, false);
                    }
                }
                return Ok(NullableColumn::wrap_inner(
                    variant_column.arc(),
                    bitmap.into(),
                ));
            }
            TypeID::VariantObject => {
                let mut bitmap = MutableBitmap::with_capacity(input_rows);
                bitmap.extend_constant(input_rows, true);
                for (i, val) in variant_column.scalar_iter().enumerate() {
                    if !val.is_object() {
                        bitmap.set(i, false);
                    }
                }
                return Ok(NullableColumn::wrap_inner(
                    variant_column.arc(),
                    bitmap.into(),
                ));
            }
            _ => {
                return Err(ErrorCode::UnknownFunction(format!(
                    "Unsupported Function {} as {}",
                    self.display_name, self.type_id
                )));
            }
        }
    }
}

impl fmt::Display for AsFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}()", self.display_name)
    }
}

pub fn as_function_creator(type_id: TypeID) -> FunctionDescription {
    let creator: FactoryCreator = Box::new(move |display_name, args| {
        AsFunction::try_create(display_name.to_string(), type_id, args)
    });

    FunctionDescription::creator(creator).features(FunctionFeatures::default().num_arguments(1))
}
