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
use common_exception::Result;
use num_format::Locale;
use num_format::ToFormattedString;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

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
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic())
    }

    fn format_en_us(number: Vec<u8>, precision: Vec<u8>) -> Vec<u8> {
        let number = String::from_utf8(number).unwrap_or_default();
        let number = number.parse::<f64>().unwrap_or(0.0);
        let precision = String::from_utf8(precision).unwrap_or_default();
        let precision = precision.parse::<i64>().unwrap_or(0);
        let precision = if precision < 0 {
            0
        } else if precision > 30 {
            30
        } else {
            precision
        };
        let trunc = number as i64;
        let fract = (number - trunc as f64).abs();
        let fract_str = format!("{0:.1$}", fract, precision as usize);
        let fract_str = fract_str.strip_prefix('0').unwrap();
        let mut trunc_str = trunc.to_formatted_string(&Locale::en);
        trunc_str.add_assign(fract_str);
        Vec::from(trunc_str)
    }
}

impl Function for FormatFunction {
    fn name(&self) -> &str {
        "format"
    }

    fn num_arguments(&self) -> usize {
        0
    }

    fn variadic_arguments(&self) -> Option<(usize, usize)> {
        Some((2, 3))
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if args[0].is_null() || args[1].is_null() {
            Ok(DataType::Null)
        } else {
            Ok(DataType::String)
        }
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        match (
            columns[0].column().cast_with_type(&DataType::String)?,
            columns[1].column().cast_with_type(&DataType::String)?,
        ) {
            (
                DataColumn::Constant(DataValue::String(Some(number)), _),
                DataColumn::Constant(DataValue::String(Some(precision)), _),
            ) => {
                // Currently we ignore the locale value and use en_US as default.
                let ret = Self::format_en_us(number, precision);
                Ok(DataColumn::Constant(
                    DataValue::String(Some(ret)),
                    input_rows,
                ))
            }
            _ => Ok(DataColumn::Constant(DataValue::Null, input_rows)),
        }
    }
}

impl fmt::Display for FormatFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "FORMAT")
    }
}
