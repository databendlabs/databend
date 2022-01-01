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
use common_datavalues::DataTypeAndNullable;
use common_exception::Result;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
pub struct FieldFunction {
    display_name: String,
}

impl FieldFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(FieldFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .deterministic()
                .variadic_arguments(2, usize::MAX - 1),
        )
    }
}

impl Function for FieldFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, args: &[DataTypeAndNullable]) -> Result<DataTypeAndNullable> {
        let nullable = args.iter().any(|arg| arg.is_nullable());
        let dt = DataType::UInt64(nullable);
        Ok(DataTypeAndNullable::create(&dt, nullable))
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        let s_nullable = columns[0].field().is_nullable();
        let s_column = columns[0]
            .column()
            .cast_with_type(&DataType::String(s_nullable))?;
        let r_column: DataColumn = match s_column {
            DataColumn::Constant(DataValue::String(s), _) => {
                let mut r_column = DataColumn::Constant(DataValue::UInt64(Some(0)), input_rows);
                if let Some(s) = s {
                    for (i, c) in columns.iter().enumerate().skip(1) {
                        match c
                            .column()
                            .cast_with_type(&DataType::String(c.field().is_nullable()))?
                        {
                            DataColumn::Constant(DataValue::String(ss), _) => {
                                if let Some(ss) = ss {
                                    if s == ss {
                                        r_column = DataColumn::Constant(
                                            DataValue::UInt64(Some(i as u64)),
                                            input_rows,
                                        );
                                        break;
                                    }
                                }
                            }
                            DataColumn::Array(ss_series) => {
                                let s = Some(&s[..]);
                                let ss_array = ss_series.string()?;
                                if ss_array.into_iter().any(|ss| ss == s) {
                                    r_column = ss_array
                                        .apply_cast_numeric(|ss| {
                                            (if Some(ss) == s { i } else { 0 }) as u64
                                        })
                                        .into();
                                    break;
                                }
                            }
                            _ => continue,
                        }
                    }
                }
                r_column
            }
            DataColumn::Array(s_series) => {
                let mut r_column = DataColumn::Constant(DataValue::UInt64(Some(0)), input_rows);
                let s_array = s_series.string()?;
                for (i, c) in columns.iter().enumerate().skip(1) {
                    match c
                        .column()
                        .cast_with_type(&DataType::String(c.field().is_nullable()))?
                    {
                        DataColumn::Constant(DataValue::String(ss), _) => {
                            if let Some(ss) = ss {
                                let ss = Some(&ss[..]);
                                if s_array.into_iter().any(|s| s == ss) {
                                    r_column = s_array
                                        .apply_cast_numeric(|s| {
                                            (if Some(s) == ss { i } else { 0 }) as u64
                                        })
                                        .into();
                                    break;
                                }
                            }
                        }
                        DataColumn::Array(ss_series) => {
                            let ss_array = ss_series.string()?;
                            if s_array
                                .into_iter()
                                .zip(ss_array.into_iter())
                                .any(|(s, ss)| s.is_some() && s == ss)
                            {
                                r_column = DFUInt64Array::from_iter(
                                    s_array
                                        .into_iter()
                                        .zip(ss_array.into_iter())
                                        .map(|(s, ss)| {
                                            (if s.is_some() && s == ss { i } else { 0 }) as u64
                                        }),
                                )
                                .into();
                                break;
                            }
                        }
                        _ => continue,
                    }
                }
                r_column
            }
            _ => DataColumn::Constant(DataValue::UInt64(Some(0)), input_rows),
        };
        Ok(r_column)
    }
}

impl fmt::Display for FieldFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
