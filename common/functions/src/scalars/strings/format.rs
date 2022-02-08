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
use std::ops::AddAssign;

use common_datavalues::prelude::*;
use common_datavalues::DataTypeAndNullable;
use common_exception::ErrorCode;
use common_exception::Result;
use num_format::Locale;
use num_format::ToFormattedString;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

const FORMAT_MAX_DECIMALS: i64 = 30;

#[derive(Clone)]
pub struct FormatFunction {
    _display_name: String,
}

impl FormatFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(FormatFunction {
            _display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .deterministic()
                .variadic_arguments(2, 3),
        )
    }

    fn format_en_us(number: Option<f64>, precision: Option<i64>) -> Option<Vec<u8>> {
        if let (Some(number), Some(precision)) = (number, precision) {
            let precision = if precision > FORMAT_MAX_DECIMALS {
                FORMAT_MAX_DECIMALS
            } else if precision < 0 {
                0
            } else {
                precision
            };
            let trunc = number as i64;
            let fract = (number - trunc as f64).abs();
            let fract_str = format!("{0:.1$}", fract, precision as usize);
            let fract_str = fract_str.strip_prefix('0');
            let mut trunc_str = trunc.to_formatted_string(&Locale::en);
            if let Some(s) = fract_str {
                trunc_str.add_assign(s)
            }
            Some(Vec::from(trunc_str))
        } else {
            None
        }
    }
}

impl Function for FormatFunction {
    fn name(&self) -> &str {
        "format"
    }

    fn return_type(&self, args: &[DataTypeAndNullable]) -> Result<DataTypeAndNullable> {
        // Format(value, format, culture), the 'culture' is optional.
        // if 'value' or 'format' is nullable, the result should be nullable.
        let nullable = args[0].is_nullable() || args[1].is_nullable();

        if (args[0].is_numeric() || args[0].is_string() || args[0].is_null())
            && (args[1].is_numeric() || args[1].is_string() || args[1].is_null())
        {
            let dt = DataType::String;
            Ok(DataTypeAndNullable::create(&dt, nullable))
        } else {
            Err(ErrorCode::IllegalDataType(format!(
                "Expected string/numeric, but got {}",
                args[0]
            )))
        }
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        match (
            columns[0].column().cast_with_type(&DataType::Float64)?,
            columns[1].column().cast_with_type(&DataType::Int64)?,
        ) {
            (
                DataColumn::Constant(DataValue::Float64(number), _),
                DataColumn::Constant(DataValue::Int64(precision), _),
            ) => {
                // Currently we ignore the locale value and use en_US as default.
                let ret = Self::format_en_us(number, precision);
                Ok(DataColumn::Constant(DataValue::String(ret), input_rows))
            }
            (DataColumn::Array(number), DataColumn::Constant(DataValue::Int64(precision), _)) => {
                let mut string_builder = StringArrayBuilder::with_capacity(input_rows);
                for number in number.f64()? {
                    string_builder.append_option(Self::format_en_us(number.copied(), precision));
                }
                Ok(string_builder.finish().into())
            }
            (DataColumn::Constant(DataValue::Float64(number), _), DataColumn::Array(precision)) => {
                let mut string_builder = StringArrayBuilder::with_capacity(input_rows);
                for precision in precision.i64()? {
                    string_builder.append_option(Self::format_en_us(number, precision.copied()));
                }
                Ok(string_builder.finish().into())
            }
            (DataColumn::Array(number), DataColumn::Array(precision)) => {
                let mut string_builder = StringArrayBuilder::with_capacity(input_rows);
                for (number, precision) in number.f64()?.into_iter().zip(precision.i64()?) {
                    string_builder
                        .append_option(Self::format_en_us(number.copied(), precision.copied()));
                }
                Ok(string_builder.finish().into())
            }
            _ => Ok(DataColumn::Constant(DataValue::String(None), input_rows)),
        }
    }

    fn passthrough_null(&self) -> bool {
        true
    }
}

impl fmt::Display for FormatFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FORMAT")
    }
}
