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

use common_arrow::arrow::temporal_conversions::EPOCH_DAYS_FROM_CE;
use common_datavalues2::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use super::cast_with_type;
use crate::scalars::function2::Function2;
use crate::with_match_primitive_type;

#[derive(Clone)]
pub struct CastFunction {
    _display_name: String,
    /// The data type to cast to
    cast_type: DataTypePtr,
}

impl CastFunction {
    pub fn create(display_name: String, cast_type: DataTypePtr) -> Result<Box<dyn Function2>> {
        Ok(Box::new(Self {
            _display_name: display_name,
            cast_type,
        }))
    }
}

impl Function2 for CastFunction {
    fn name(&self) -> &str {
        "CastFunction"
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        Ok(self.cast_type.clone())
    }

    fn eval(&self, columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        cast_with_type(columns[0].column(), &self.cast_type)
    }

    fn to() {
        let series = columns[0].column().clone().to_minimal_array()?;
        const DATE_FMT: &str = "%Y-%m-%d";
        const TIME_FMT: &str = "%Y-%m-%d %H:%M:%S";

        let error_fn = || -> Result<Series> {
            Err(ErrorCode::BadDataValueType(format!(
                "Unsupported cast_with_type from array: {:?} into data_type: {:?}",
                series, self.cast_type,
            )))
        };

        let array = match (columns[0].data_type(), &self.cast_type) {
            // Date/DateTime to others
            (DataType::Date16, _) => with_match_primitive_type!(&self.cast_type, |$T| {
                series.cast_with_type(&self.cast_type)
            }, {
               let arr = series.u16()?;
               match &self.cast_type {
                Date32 => Ok(arr.apply_cast_numeric(|v| v as i32).into_series()),
                DateTime32(_) => Ok(arr.apply_cast_numeric(|v|  Utc.timestamp(v as i64 * 24 * 3600, 0_u32).timestamp() as u32 ).into_series() ),
                String => Ok(DFStringArray::from_iter(arr.into_iter().map(|v| v.map(|x| datetime_to_string( Utc.timestamp(*x as i64 * 24 * 3600, 0_u32), DATE_FMT))) ).into_series()),
                _ => error_fn(),
               }
            }),

            (DataType::Date32, _) => with_match_primitive_type!(&self.cast_type, |$T| {
                series.cast_with_type(&self.cast_type)
            }, {
               let arr = series.i32()?;
               match &self.cast_type {
                Date32 => Ok(arr.apply_cast_numeric(|v| v as i32).into_series()),
                DateTime32(_) => Ok(arr.apply_cast_numeric(|v|  Utc.timestamp(v as i64 * 24 * 3600, 0_u32).timestamp()  as u32).into_series() ),
                String => Ok(DFStringArray::from_iter(arr.into_iter().map(|v| v.map(|x| datetime_to_string( Utc.timestamp(*x as i64 * 24 * 3600, 0_u32), DATE_FMT))) ).into_series()),
                _ => error_fn(),
               }
            }),

            (DataType::DateTime32(_), _) => with_match_primitive_type!(&self.cast_type, |$T| {
                series.cast_with_type(&self.cast_type)
            }, {
               let arr = series.u32()?;
               match &self.cast_type {
                Date16 => Ok(arr.apply_cast_numeric(|v| (v as i64 / 24/ 3600) as u16).into_series()),
                Date32 => Ok(arr.apply_cast_numeric(|v| (v as i64 / 24/ 3600) as i32).into_series()),
                String => Ok(DFStringArray::from_iter(arr.into_iter().map(|v| v.map(|x| datetime_to_string( Utc.timestamp(*x as i64, 0_u32), TIME_FMT))) ).into_series()),
                _ => error_fn(),
               }
            }),

            // others to Date/DateTime
            (_, DataType::Date16) => with_match_primitive_type!(columns[0].data_type(), |$T| {
                series.cast_with_type(&self.cast_type)
            }, {
               match columns[0].data_type() {
                String => {
                    let it = series.string()?.into_iter().map(|v| {
                        v.and_then(string_to_date).map(|d| (d.num_days_from_ce() - EPOCH_DAYS_FROM_CE) as u16 )
                    });
                    Ok(DFUInt16Array::from_iter(it).into_series())
                },
                _ => error_fn(),

               }
            }),

            (_, DataType::Date32) => with_match_primitive_type!(columns[0].data_type(), |$T| {
                series.cast_with_type(&self.cast_type)
            }, {
               match columns[0].data_type() {
                String => {
                    let it = series.string()?.into_iter().map(|v| {
                        v.and_then(string_to_date).map(|d| (d.num_days_from_ce() - EPOCH_DAYS_FROM_CE) as i32 )
                    });
                    Ok(DFInt32Array::from_iter(it).into_series())
                },
                _ => error_fn(),

               }
            }),

            (_, DataType::DateTime32(_)) => {
                with_match_primitive_type!(columns[0].data_type(), |$T| {
                    series.cast_with_type(&self.cast_type)
                }, {
                   match columns[0].data_type() {
                    String => {
                        let it = series.string()?.into_iter().map(|v| {
                            v.and_then(string_to_datetime).map(|t| t.timestamp() as u32)
                        });
                        Ok(DFUInt32Array::from_iter(it).into_series())
                    },
                    _ => error_fn(),
                   }
                })
            }
            (_, DataType::DateTime64(precision, _)) => {
                with_match_primitive_type!(columns[0].data_type(), |$T| {
                    series.cast_with_type(&self.cast_type)
                }, {
                   match columns[0].data_type() {
                    String => {
                        let it = series.string()?.into_iter().map(|v| {
                            v.and_then(string_to_datetime64).map(|t| -> u64 {
                                if *precision <= 3 {
                                    t.timestamp_millis() as u64
                                } else {
                                    t.timestamp_nanos() as u64
                                }
                            })
                        });
                        Ok(DFUInt64Array::from_iter(it).into_series())
                    },
                    _ => error_fn()
                   }
                })
            }
            _ => series.cast_with_type(&self.cast_type),
        }?;

        let column: ColumnRef = array.into();
        Ok(column.resize_constant(input_rows))
    }
}

impl fmt::Display for CastFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CAST")
    }
}
