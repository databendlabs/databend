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

const FUNC_LOCATE: u8 = 1;
const FUNC_POSITION: u8 = 2;
const FUNC_INSTR: u8 = 3;

pub struct LocateFunction {}

impl LocateFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        LocatingFunction::<FUNC_LOCATE>::try_create(display_name)
    }

    pub fn desc() -> FunctionDescription {
        LocatingFunction::<FUNC_LOCATE>::desc()
    }
}

pub struct InstrFunction {}

impl InstrFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        LocatingFunction::<FUNC_INSTR>::try_create(display_name)
    }

    pub fn desc() -> FunctionDescription {
        LocatingFunction::<FUNC_INSTR>::desc()
    }
}

pub struct PositionFunction {}

impl PositionFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        LocatingFunction::<FUNC_POSITION>::try_create(display_name)
    }

    pub fn desc() -> FunctionDescription {
        LocatingFunction::<FUNC_POSITION>::desc()
    }
}

#[derive(Clone)]
struct LocatingFunction<const T: u8> {
    display_name: String,
}

impl<const T: u8> LocatingFunction<T> {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(LocatingFunction::<T> {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic())
    }
}

impl<const T: u8> Function for LocatingFunction<T> {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn variadic_arguments(&self) -> Option<(usize, usize)> {
        if T == FUNC_LOCATE {
            Some((2, 3))
        } else {
            Some((2, 2))
        }
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(true)
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        let (ss_column, s_column) = if T == FUNC_INSTR {
            (
                columns[1].column().cast_with_type(&DataType::String)?,
                columns[0].column().cast_with_type(&DataType::String)?,
            )
        } else {
            (
                columns[0].column().cast_with_type(&DataType::String)?,
                columns[1].column().cast_with_type(&DataType::String)?,
            )
        };

        let p_column = if T == FUNC_LOCATE && columns.len() == 3 {
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
                    let s_array = s_series.string()?;
                    DFUInt64Array::new_from_iter_validity(
                        s_array.into_no_null_iter().map(|s| find_at(s, &ss, &p)),
                        s_array.inner().validity().cloned(),
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
                    let ss_array = ss_series.string()?;
                    DFUInt64Array::new_from_iter_validity(
                        ss_array.into_no_null_iter().map(|ss| find_at(&s, ss, &p)),
                        ss_array.inner().validity().cloned(),
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
                    let ss_array = ss_series.string()?;
                    let s_array = s_series.string()?;
                    DFUInt64Array::new_from_iter_validity(
                        ss_array
                            .into_no_null_iter()
                            .zip(s_array.into_no_null_iter())
                            .map(|(ss, s)| find_at(s, ss, &p)),
                        combine_validities(ss_array.inner().validity(), s_array.inner().validity()),
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
                    let p_array = p_series.u64()?;
                    DFUInt64Array::new_from_iter_validity(
                        p_array.into_no_null_iter().map(|p| find_at(&s, &ss, p)),
                        p_array.inner().validity().cloned(),
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
                    let s_array = s_series.string()?;
                    let p_array = p_series.u64()?;
                    DFUInt64Array::new_from_iter_validity(
                        s_array
                            .into_no_null_iter()
                            .zip(p_array.into_no_null_iter())
                            .map(|(s, p)| find_at(s, &ss, p)),
                        combine_validities(s_array.inner().validity(), p_array.inner().validity()),
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
                    let ss_array = ss_series.string()?;
                    let p_array = p_series.u64()?;
                    DFUInt64Array::new_from_iter_validity(
                        ss_array
                            .into_no_null_iter()
                            .zip(p_array.into_no_null_iter())
                            .map(|(ss, p)| find_at(&s, ss, p)),
                        combine_validities(ss_array.inner().validity(), p_array.inner().validity()),
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
            ) => {
                let ss_array = ss_series.string()?;
                let s_array = s_series.string()?;
                let p_array = p_series.u64()?;

                DFUInt64Array::new_from_iter_validity(
                    izip!(
                        ss_array.into_no_null_iter(),
                        s_array.into_no_null_iter(),
                        p_array.into_no_null_iter(),
                    )
                    .map(|(ss, s, p)| find_at(s, ss, p)),
                    combine_validities(
                        combine_validities(
                            ss_array.inner().validity(),
                            ss_array.inner().validity(),
                        )
                        .as_ref(),
                        p_array.inner().validity(),
                    ),
                )
                .into()
            }
            _ => DataColumn::Constant(DataValue::Null, input_rows),
        };
        Ok(r_column)
    }
}

impl<const T: u8> fmt::Display for LocatingFunction<T> {
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
