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
use itertools::izip;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
pub struct LocateFunction {
    display_name: String,
}

impl LocateFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(LocateFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic())
    }
}

impl Function for LocateFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn num_arguments(&self) -> usize {
        0
    }

    fn variadic_arguments(&self) -> Option<(usize, usize)> {
        Some((2, 3))
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(true)
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        let ss_column = columns[0].column().cast_with_type(&DataType::String)?;
        let s_column = columns[1].column().cast_with_type(&DataType::String)?;
        let p_column = if columns.len() == 3 {
            columns[2].column().cast_with_type(&DataType::UInt64)?
        } else {
            DataColumn::Constant(DataValue::UInt64(Some(1)), input_rows)
        };

        let r_column: DataColumn = match (ss_column, s_column, p_column) {
            (
                DataColumn::Constant(DataValue::String(ss), _),
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Constant(DataValue::UInt64(p), _),
            ) => {
                if let (Some(ss), Some(s), Some(p)) = (ss, s, p) {
                    DataColumn::Constant(DataValue::UInt64(Some(find_at(&s, &ss, &p))), input_rows)
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            (
                DataColumn::Constant(DataValue::String(ss), _),
                DataColumn::Array(s_series),
                DataColumn::Constant(DataValue::UInt64(p), _),
            ) => {
                if let (Some(ss), Some(p)) = (ss, p) {
                    DFUInt64Array::from_iter(
                        s_series
                            .string()?
                            .into_no_null_iter()
                            .map(|s| find_at(s, &ss, &p)),
                    )
                    .into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            (
                DataColumn::Array(ss_series),
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Constant(DataValue::UInt64(p), _),
            ) => {
                if let (Some(s), Some(p)) = (s, p) {
                    DFUInt64Array::from_iter(
                        ss_series
                            .string()?
                            .into_no_null_iter()
                            .map(|ss| find_at(&s, ss, &p)),
                    )
                    .into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            (
                DataColumn::Array(ss_series),
                DataColumn::Array(s_series),
                DataColumn::Constant(DataValue::UInt64(p), _),
            ) => {
                if let Some(p) = p {
                    DFUInt64Array::from_iter(
                        ss_series
                            .string()?
                            .into_no_null_iter()
                            .zip(s_series.string()?.into_no_null_iter())
                            .map(|(ss, s)| find_at(s, ss, &p)),
                    )
                    .into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            (
                DataColumn::Constant(DataValue::String(ss), _),
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Array(p_series),
            ) => {
                if let (Some(ss), Some(s)) = (ss, s) {
                    DFUInt64Array::from_iter(
                        p_series
                            .u64()?
                            .into_no_null_iter()
                            .map(|p| find_at(&s, &ss, p)),
                    )
                    .into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            (
                DataColumn::Constant(DataValue::String(ss), _),
                DataColumn::Array(s_series),
                DataColumn::Array(p_series),
            ) => {
                if let Some(ss) = ss {
                    DFUInt64Array::from_iter(
                        s_series
                            .string()?
                            .into_no_null_iter()
                            .zip(p_series.u64()?.into_no_null_iter())
                            .map(|(s, p)| find_at(s, &ss, p)),
                    )
                    .into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            (
                DataColumn::Array(ss_series),
                DataColumn::Constant(DataValue::String(s), _),
                DataColumn::Array(p_series),
            ) => {
                if let Some(s) = s {
                    DFUInt64Array::from_iter(
                        ss_series
                            .string()?
                            .into_no_null_iter()
                            .zip(p_series.u64()?.into_no_null_iter())
                            .map(|(ss, p)| find_at(&s, ss, p)),
                    )
                    .into()
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            (
                DataColumn::Array(ss_series),
                DataColumn::Array(s_series),
                DataColumn::Array(p_series),
            ) => DFUInt64Array::from_iter(
                izip!(
                    ss_series.string()?.into_no_null_iter(),
                    s_series.string()?.into_no_null_iter(),
                    p_series.u64()?.into_no_null_iter(),
                )
                .map(|(ss, s, p)| find_at(s, ss, p)),
            )
            .into(),
            _ => DataColumn::Constant(DataValue::Null, input_rows),
        };
        Ok(r_column)
    }
}

impl fmt::Display for LocateFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

#[inline]
fn find_at(str: &[u8], substr: &[u8], pos: &u64) -> u64 {
    let pos = (*pos) as usize;
    if pos == 0 {
        return 0_u64;
    }
    let p = pos - 1;
    if p + substr.len() <= str.len() {
        str[p..]
            .windows(substr.len())
            .position(|w| w == substr)
            .map(|i| i + 1 + p)
            .unwrap_or(0) as u64
    } else {
        0_u64
    }
}
