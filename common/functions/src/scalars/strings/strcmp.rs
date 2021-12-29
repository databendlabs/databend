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

use std::cmp::Ordering;
use std::fmt;

use common_datavalues::prelude::*;
use common_datavalues::DataTypeAndNullable;
use common_exception::ErrorCode;
use common_exception::Result;
use itertools::izip;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
pub struct StrcmpFunction {
    display_name: String,
}

impl StrcmpFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(StrcmpFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(2))
    }
}

impl Function for StrcmpFunction {
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
        Ok(DataType::Int8)
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        let s1_column = columns[0].column().cast_with_type(&DataType::String)?;
        let s2_column = columns[1].column().cast_with_type(&DataType::String)?;

        let r_column: DataColumn = match (s1_column, s2_column) {
            // #00
            (
                DataColumn::Constant(DataValue::String(s1), _),
                DataColumn::Constant(DataValue::String(s2), _),
            ) => {
                if let (Some(s1), Some(s2)) = (s1, s2) {
                    DataColumn::Constant(DataValue::Int8(Some(strcmp(&s1, &s2))), input_rows)
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #10
            (DataColumn::Array(s_series), DataColumn::Constant(DataValue::String(s2), _)) => {
                if let Some(s2) = s2 {
                    let mut r_array = DFInt8ArrayBuilder::with_capacity(input_rows);
                    for os1 in s_series.string()? {
                        r_array.append_option(os1.map(|s1| strcmp(s1, &s2)));
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #01
            (DataColumn::Constant(DataValue::String(s1), _), DataColumn::Array(s2_series)) => {
                if let Some(s1) = s1 {
                    let mut r_array = DFInt8ArrayBuilder::with_capacity(input_rows);
                    for os2 in s2_series.string()? {
                        r_array.append_option(os2.map(|s2| strcmp(&s1, s2)));
                    }
                    r_array.finish().into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            // #11
            (DataColumn::Array(s1_series), DataColumn::Array(s2_series)) => {
                let mut r_array = DFInt8ArrayBuilder::with_capacity(input_rows);
                for s1_s2 in izip!(s1_series.string()?, s2_series.string()?) {
                    r_array.append_option(match s1_s2 {
                        (Some(s1), Some(s2)) => Some(strcmp(s1, s2)),
                        _ => None,
                    });
                }
                r_array.finish().into()
            }
            _ => DataColumn::Constant(DataValue::Null, input_rows),
        };
        Ok(r_column)
    }
}

impl fmt::Display for StrcmpFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

#[inline]
fn strcmp<'a>(s1: &'a [u8], s2: &'a [u8]) -> i8 {
    let res = match s1.len().cmp(&s2.len()) {
        Ordering::Equal => {
            let mut res = Ordering::Equal;
            for (s1i, s2i) in izip!(s1, s2) {
                match s1i.cmp(s2i) {
                    Ordering::Equal => continue,
                    ord => {
                        res = ord;
                        break;
                    }
                }
            }
            res
        }
        ord => ord,
    };
    match res {
        Ordering::Equal => 0,
        Ordering::Greater => 1,
        Ordering::Less => -1,
    }
}
