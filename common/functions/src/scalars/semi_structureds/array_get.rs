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

use common_datavalues::prelude::*;
use common_datavalues::with_match_integer_types_error;
use common_datavalues::with_match_scalar_types_error;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

#[derive(Clone)]
pub struct ArrayGetFunction {
    array_type: ArrayType,
    display_name: String,
}

impl ArrayGetFunction {
    pub fn try_create(display_name: &str, args: &[&DataTypeImpl]) -> Result<Box<dyn Function>> {
        let data_type = args[0];
        let path_type = args[1];

        if !data_type.data_type_id().is_array() || !path_type.data_type_id().is_integer() {
            return Err(ErrorCode::IllegalDataType(format!(
                "Invalid argument types for function '{}': ({:?}, {:?})",
                display_name.to_uppercase(),
                data_type.data_type_id(),
                path_type.data_type_id()
            )));
        }

        let array_type: ArrayType = data_type.clone().try_into()?;
        Ok(Box::new(ArrayGetFunction {
            array_type,
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(2))
    }
}

impl Function for ArrayGetFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self) -> DataTypeImpl {
        NullableType::new_impl(self.array_type.inner_type().clone())
    }

    fn eval(
        &self,
        _func_ctx: FunctionContext,
        columns: &ColumnsWithField,
        input_rows: usize,
    ) -> Result<ColumnRef> {
        let array_column: &ArrayColumn = if columns[0].column().is_const() {
            let const_column: &ConstColumn = Series::check_get(columns[0].column())?;
            Series::check_get(const_column.inner())?
        } else {
            Series::check_get(columns[0].column())?
        };

        let inner_type = self.array_type.inner_type().data_type_id();
        let index_type = columns[1].data_type().data_type_id();
        with_match_scalar_types_error!(inner_type.to_physical_type(), |$T1| {
            with_match_integer_types_error!(index_type, |$T2| {
                let inner_column: &<$T1 as Scalar>::ColumnType = Series::check_get(array_column.values())?;
                let mut builder = NullableColumnBuilder::<$T1>::with_capacity(input_rows);
                if columns[0].column().is_const() {
                    let index_column: &PrimitiveColumn<$T2> = if columns[1].column().is_const() {
                        let const_column: &ConstColumn = Series::check_get(columns[1].column())?;
                        Series::check_get(const_column.inner())?
                    } else {
                        Series::check_get(columns[1].column())?
                    };
                    let len = array_column.size_at_index(0);
                    for (i, index) in index_column.iter().enumerate() {
                        let index = usize::try_from(*index)?;
                        let _ = check_index(index, len)?;
                        builder.append(inner_column.get_data(index), true);
                    }
                } else if columns[1].column().is_const() {
                    let index_column: &ConstColumn = Series::check_get(columns[1].column())?;
                    let index = index_column.get(0).as_u64()? as usize;
                    let mut offset = 0;
                    for i in 0..input_rows {
                        let len = array_column.size_at_index(i);
                        let _ = check_index(index, len)?;
                        builder.append(inner_column.get_data(offset + index), true);
                        offset += len;
                    }
                } else {
                    let index_column: &PrimitiveColumn<$T2> = Series::check_get(columns[1].column())?;
                    let mut offset = 0;
                    for (i, index) in index_column.iter().enumerate() {
                        let index = usize::try_from(*index)?;
                        let len = array_column.size_at_index(i);
                        let _ = check_index(index, len)?;
                        builder.append(inner_column.get_data(offset + index), true);
                        offset += len;
                    }
                }
                Ok(builder.build(input_rows))
            })
        })
    }
}

impl fmt::Display for ArrayGetFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name.to_uppercase())
    }
}

fn check_index(index: usize, len: usize) -> Result<()> {
    if index >= len {
        return Err(ErrorCode::BadArguments(format!(
            "Index out of array column bounds: the len is {} but the index is {}",
            len, index
        )));
    }
    Ok(())
}
