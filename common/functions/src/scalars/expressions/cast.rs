// Copyright 2020 Datafuse Labs.
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
use common_datavalues::chrono::DateTime;
use common_datavalues::chrono::Datelike;
use common_datavalues::chrono::NaiveDate;
use common_datavalues::chrono::NaiveDateTime;
use common_datavalues::chrono::TimeZone;
use common_datavalues::chrono::Utc;
use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::ArrayApply;
use common_datavalues::prelude::DFInt32Array;
use common_datavalues::prelude::DFStringArray;
use common_datavalues::prelude::DFUInt16Array;
use common_datavalues::prelude::DFUInt32Array;
use common_datavalues::prelude::DataColumnsWithField;
use common_datavalues::series::IntoSeries;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::Function;
use crate::with_match_primitive_type;

#[derive(Clone)]
pub struct CastFunction {
    _display_name: String,
    /// The data type to cast to
    cast_type: DataType,
}

impl CastFunction {
    pub fn create(display_name: String, cast_type: DataType) -> Result<Box<dyn Function>> {
        Ok(Box::new(Self {
            _display_name: display_name,
            cast_type,
        }))
    }
}

impl Function for CastFunction {
    fn name(&self) -> &str {
        "CastFunction"
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(self.cast_type.clone())
    }

    // TODO
    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        if columns[0].data_type() == &self.cast_type {
            return Ok(columns[0].column().clone());
        }

        let series = columns[0].column().clone().to_minimal_array()?;
        const DATE_FMT: &str = "%Y-%m-%d";
        const TIME_FMT: &str = "%Y-%m-%d %H:%M:%S";

        let error = ErrorCode::BadDataValueType(format!(
            "Unsupported cast_with_type from array: {:?} into data_type: {:?}",
            series, self.cast_type,
        ));

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
                _ =>  Err(error)
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
                _ =>  Err(error)
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
                _ =>  Err(error)
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
                _ =>  Err(error)
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
                _ =>  Err(error)
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
                    _ =>  Err(error)
                   }
                })
            }

            _ => series.cast_with_type(&self.cast_type),
        }?;

        let column: DataColumn = array.into();
        Ok(column.resize_constant(input_rows))
    }

    fn num_arguments(&self) -> usize {
        1
    }
}

impl fmt::Display for CastFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CAST")
    }
}

#[inline]
fn datetime_to_string(date: DateTime<Utc>, fmt: &str) -> String {
    date.format(fmt).to_string()
}

// currently use UTC by default
// TODO support timezone
#[inline]
fn string_to_datetime(date_str: impl AsRef<[u8]>) -> Option<NaiveDateTime> {
    let s = std::str::from_utf8(date_str.as_ref()).ok();
    s.and_then(|c| NaiveDateTime::parse_from_str(c, "%Y-%m-%d %H:%M:%S").ok())
}

#[inline]
fn string_to_date(date_str: impl AsRef<[u8]>) -> Option<NaiveDate> {
    let s = std::str::from_utf8(date_str.as_ref()).ok();
    s.and_then(|c| c.parse::<NaiveDate>().ok())
}
