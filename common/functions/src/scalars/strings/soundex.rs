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
pub struct SoundexFunction {
    _display_name: String,
}

impl SoundexFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(SoundexFunction {
            _display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic())
    }

    fn soundex(data: &[u8]) -> Vec<u8> {
        let mut result = String::with_capacity(4);
        let mut last = None;
        let mut count = 0;

        for ch in String::from_utf8_lossy(data).chars() {
            let score = Self::number_map(ch);
            if last.is_none() {
                if !Self::is_uni_alphabetic(ch) {
                    continue;
                }

                last = score;
                result.push(ch.to_ascii_uppercase());
            } else {
                if !ch.is_ascii_alphabetic() || Self::is_drop(ch) || score.is_none() || score == last {
                    continue;
                }

                last = score;
                result.push(score.unwrap());
            }

            count += 1;
        }

        // add '0'
        if !result.is_empty() && count < 4 {
            result.extend(vec!['0'; 4 - count])
        }

        result.into_bytes()
    }

    #[inline(always)]
    fn number_map(i: char) -> Option<char> {
        match i.to_ascii_lowercase() {
            'b' | 'f' | 'p' | 'v' => Some('1'),
            'c' | 'g' | 'j' | 'k' | 'q' | 's' | 'x' | 'z' => Some('2'),
            'd' | 't' => Some('3'),
            'l' => Some('4'),
            'm' | 'n' => Some('5'),
            'r' => Some('6'),
            _ => Some('0'),
        }
    }

    #[inline(always)]
    fn is_drop(c: char) -> bool {
        matches!(c, 'a' | 'e' | 'i' | 'o' | 'u' | 'y' | 'h' | 'w')
    }

    // https://github.com/mysql/mysql-server/blob/3290a66c89eb1625a7058e0ef732432b6952b435/sql/item_strfunc.cc#L1919
    #[inline(always)]
    fn is_uni_alphabetic(c: char) -> bool {
        (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c as i32 >= 0xC0
    }
}

impl fmt::Display for SoundexFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SOUNDEX")
    }
}

impl Function for SoundexFunction {
    fn name(&self) -> &str {
        &*self._display_name
    }

    fn num_arguments(&self) -> usize {
        1
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        match args[0] {
            DataType::Null => Ok(DataType::Null),
            _ => Ok(DataType::String),
        }
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(true)
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        match columns[0].data_type() {
            DataType::String => {
                return match columns[0].column().cast_with_type(&DataType::String) {
                    Ok(DataColumn::Constant(DataValue::String(Some(ref data)), _)) => {
                        let r = Self::soundex(data);
                        Ok(
                            DataColumn::Constant(
                                DataValue::String(Some(r)),
                                input_rows,
                            )
                        )
                    }
                    _ => {
                        Ok(DataColumn::Constant(DataValue::Null, input_rows))
                    }
                };
            }
            DataType::Null => Ok(DataColumn::Constant(DataValue::Null, input_rows)),
            _ => {
                let mut s = StringArrayBuilder::with_capacity(0);
                s.append_value("");
                Ok(s.finish().into())
            }
        }
    }
}
