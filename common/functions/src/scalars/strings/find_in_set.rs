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
use common_exception::ErrorCode;
use common_exception::Result;
use itertools::izip;
use common_datavalues::DataTypeAndNullable;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
pub struct FindInSetFunction {
    display_name: String,
}

impl FindInSetFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(Self {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(2))
    }
}

impl Function for FindInSetFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, args: &[DataTypeAndNullable]) -> Result<DataType> {
        if !args[0].is_integer() && !args[0].is_string() && !args[0].is_null() {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected integer or string or null, but got {}",
                args[0]
            )));
        }
        if !args[1].is_integer() && !args[1].is_string() && !args[1].is_null() {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected integer or string or null, but got {}",
                args[1]
            )));
        }
        Ok(DataType::UInt64)
    }

    fn eval(&self, columns: &DataColumnsWithField, r: usize) -> Result<DataColumn> {
        let e_column = columns[0].column().cast_with_type(&DataType::String)?;
        let d_column = columns[1].column().cast_with_type(&DataType::String)?;

        match (e_column, d_column) {
            (
                DataColumn::Constant(DataValue::String(e), _),
                DataColumn::Constant(DataValue::String(d), _),
            ) => {
                if let (Some(e), Some(d)) = (e, d) {
                    let l = find(&e, &d);
                    Ok(DataColumn::Constant(DataValue::UInt64(Some(l)), r))
                } else {
                    Ok(DataColumn::Constant(DataValue::Null, r))
                }
            }
            (DataColumn::Array(e), DataColumn::Constant(DataValue::String(d), _)) => {
                if let Some(d) = d {
                    Ok(DataColumn::from(
                        e.string()?.apply_cast_numeric(|e| find(e, &d)),
                    ))
                } else {
                    Ok(DataColumn::Constant(DataValue::Null, r))
                }
            }
            (DataColumn::Constant(DataValue::String(e), _), DataColumn::Array(d)) => {
                if let Some(e) = e {
                    Ok(DataColumn::from(
                        d.string()?.apply_cast_numeric(|d| find(&e, d)),
                    ))
                } else {
                    Ok(DataColumn::Constant(DataValue::Null, r))
                }
            }
            (DataColumn::Array(e), DataColumn::Array(d)) => {
                let e = e.string()?;
                let d = d.string()?;
                Ok(DataColumn::from(DFUInt64Array::new_from_iter_validity(
                    izip!(e.into_no_null_iter(), d.into_no_null_iter()).map(|(e, d)| find(e, d)),
                    combine_validities(e.inner().validity(), d.inner().validity()),
                )))
            }
            _ => Err(ErrorCode::IllegalDataType(
                "Expected args a const, an expression, an expression, a const and a const",
            )),
        }
    }
}

impl fmt::Display for FindInSetFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

#[inline]
fn find<'a>(str: &'a [u8], list: &'a [u8]) -> u64 {
    if str.is_empty() || str.len() > list.len() {
        return 0;
    }
    let mut pos = 1;
    for (p, w) in list.windows(str.len()).enumerate() {
        if w[0] == 44 {
            pos += 1;
        } else if w == str && (p + w.len() == list.len() || list[p + w.len()] == 44) {
            return pos;
        }
    }
    0
}
