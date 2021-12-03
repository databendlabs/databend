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

use std::fmt;

use common_datavalues::prelude::*;
use common_exception::Result;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
pub struct ConcatFunction {
    _display_name: String,
}

impl ConcatFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(ConcatFunction {
            _display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic())
    }

    fn concat_column(lhs: DataColumn, rhs: &DataColumnWithField) -> Result<DataColumn> {
        let lhs = lhs.cast_with_type(&DataType::String)?;
        let rhs = rhs.column().cast_with_type(&DataType::String)?;
        let array = match (lhs, rhs) {
            (DataColumn::Array(lhs), DataColumn::Array(rhs)) => {
                let l_array = lhs.string()?;
                let r_array = rhs.string()?;
                DFStringArray::new_from_iter_validity(
                    l_array
                        .into_no_null_iter()
                        .zip(r_array.into_no_null_iter())
                        .map(|(l, r)| [l, r].concat()),
                    combine_validities(l_array.inner().validity(), r_array.inner().validity()),
                )
            }
            (DataColumn::Array(l_array), DataColumn::Constant(rhs, len)) => {
                let l_array = l_array.string()?;
                let r_array = rhs.to_series_with_size(len)?;
                let r_array = r_array.string()?;
                DFStringArray::new_from_iter_validity(
                    l_array
                        .into_no_null_iter()
                        .zip(r_array.into_no_null_iter())
                        .map(|(l, r)| [l, r].concat()),
                    combine_validities(l_array.inner().validity(), r_array.inner().validity()),
                )
            }
            (DataColumn::Constant(l_array, len), DataColumn::Array(rhs)) => {
                let l_array = l_array.to_series_with_size(len)?;
                let l_array = l_array.string()?;
                let r_array = rhs.string()?;
                DFStringArray::new_from_iter_validity(
                    l_array
                        .into_no_null_iter()
                        .zip(r_array.into_no_null_iter())
                        .map(|(l, r)| [l, r].concat()),
                    combine_validities(l_array.inner().validity(), r_array.inner().validity()),
                )
            }
            (DataColumn::Constant(l_array, l_len), DataColumn::Constant(rhs, r_len)) => {
                let l_array = l_array.to_series_with_size(l_len)?;
                let l_array = l_array.string()?;
                let r_array = rhs.to_series_with_size(r_len)?;
                let r_array = r_array.string()?;
                DFStringArray::new_from_iter_validity(
                    l_array
                        .into_no_null_iter()
                        .zip(r_array.into_no_null_iter())
                        .map(|(l, r)| [l, r].concat()),
                    combine_validities(l_array.inner().validity(), r_array.inner().validity()),
                )
            }
        };
        let result = DataColumn::Array(array.into_series());
        Ok(result)
    }
}

impl Function for ConcatFunction {
    fn name(&self) -> &str {
        "concat"
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        for arg in args {
            if arg.is_null() {
                return Ok(DataType::Null);
            }
        }
        Ok(DataType::String)
    }

    fn variadic_arguments(&self) -> Option<(usize, usize)> {
        Some((1, 1024))
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn> {
        let len = columns[0].column().len();
        let mut result = DataColumn::Constant(DataValue::String(Some(vec![0u8; 0])), len);
        for column in columns {
            result = Self::concat_column(result, column)?;
        }
        Ok(result)
    }
}

impl fmt::Display for ConcatFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CONCAT")
    }
}
