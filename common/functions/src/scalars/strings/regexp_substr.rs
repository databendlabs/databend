// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::fmt;

use bstr::ByteSlice;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use regex::bytes::Regex;

use crate::scalars::assert_string;
use crate::scalars::cast_column_field;
use crate::scalars::strings::regexp_instr::regexp_match_result;
use crate::scalars::strings::regexp_instr::validate_regexp_arguments;
use crate::scalars::strings::regexp_like::build_regexp_from_pattern;
use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

#[derive(Clone)]
pub struct RegexpSubStrFunction {
    display_name: String,
}

impl RegexpSubStrFunction {
    pub fn try_create(display_name: &str, args: &[&DataTypeImpl]) -> Result<Box<dyn Function>> {
        for (i, arg) in args.iter().enumerate() {
            if arg.is_null() {
                continue;
            }

            let arg = remove_nullable(arg);
            if i < 2 || i == 4 {
                assert_string(&arg)?;
            } else if !arg.data_type_id().is_integer() && !arg.data_type_id().is_string() {
                return Err(ErrorCode::IllegalDataType(format!(
                    "Expected integer or string or null, but got {}",
                    args[i].data_type_id()
                )));
            }
        }

        Ok(Box::new(Self {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .deterministic()
                .disable_passthrough_null() // disable passthrough null to validate the function arguments
                .variadic_arguments(2, 5),
        )
    }
}

impl Function for RegexpSubStrFunction {
    fn name(&self) -> &str {
        &self.display_name
    }

    fn return_type(&self) -> DataTypeImpl {
        NullableType::arc(StringType::arc())
    }

    // Notes: https://dev.mysql.com/doc/refman/8.0/en/regexp.html#function_regexp-substr
    fn eval(
        &self,
        _func_ctx: FunctionContext,
        columns: &ColumnsWithField,
        input_rows: usize,
    ) -> Result<ColumnRef> {
        let has_null = columns.iter().any(|col| col.column().is_null());
        if has_null {
            return Ok(NullColumn::new(input_rows).arc());
        }

        let mut pos = ConstColumn::new(Series::from_data(vec![1_i64]), input_rows).arc();
        let mut occurrence = ConstColumn::new(Series::from_data(vec![1_i64]), input_rows).arc();
        let mut match_type = ConstColumn::new(Series::from_data(vec![""]), input_rows).arc();

        for i in 2..columns.len() {
            match i {
                2 => pos = cast_column_field(&columns[2], &NullableType::arc(Int64Type::arc()))?,
                3 => {
                    occurrence =
                        cast_column_field(&columns[3], &NullableType::arc(Int64Type::arc()))?
                }
                _ => {
                    match_type =
                        cast_column_field(&columns[4], &NullableType::arc(StringType::arc()))?
                }
            }
        }

        let pat = columns[1].column();

        if pat.is_const() && match_type.is_const() {
            let pat_value = pat.get_string(0)?;
            let mt_value = match_type.get_string(0)?;
            let columns = [columns[0].column(), &pos, &occurrence];

            return self.a_regexp_substr_binary_scalar(&columns, &pat_value, &mt_value, input_rows);
        }

        let columns = [columns[0].column(), pat, &pos, &occurrence, &match_type];
        self.a_regexp_substr_binary(&columns, input_rows)
    }

    fn passthrough_constant(&self) -> bool {
        false
    }
}

impl RegexpSubStrFunction {
    fn a_regexp_substr_binary_scalar(
        &self,
        columns: &[&ColumnRef],
        pat: &[u8],
        mt: &[u8],
        input_rows: usize,
    ) -> Result<ColumnRef> {
        let mut builder = NullableColumnBuilder::<Vu8>::with_capacity(columns[0].len());

        let source = Vu8::try_create_viewer(columns[0])?;
        let pos = i64::try_create_viewer(columns[1])?;
        let occur = i64::try_create_viewer(columns[2])?;

        let re = build_regexp_from_pattern(self.name(), pat, Some(mt))?;

        for row in 0..input_rows {
            if source.null_at(row) || pos.null_at(row) || occur.null_at(row) {
                builder.append_null();
                continue;
            }

            let s_value = source.value_at(row);
            let pos_value = pos.value_at(row);
            let occur_value = occur.value_at(row);
            validate_regexp_arguments(self.name(), pos_value, Some(occur_value), None, None)?;

            if s_value.is_empty() || pat.is_empty() {
                builder.append_null();
                continue;
            }

            let substr = regexp_substr(s_value, &re, pos_value, occur_value);
            match substr {
                Some(ss) => builder.append(ss, true),
                None => builder.append_null(),
            }
        }

        Ok(builder.build(input_rows))
    }

    fn a_regexp_substr_binary(
        &self,
        columns: &[&ColumnRef],
        input_rows: usize,
    ) -> Result<ColumnRef> {
        let mut builder = NullableColumnBuilder::<Vu8>::with_capacity(columns[0].len());

        let mut map: HashMap<Vec<u8>, Regex> = HashMap::new();
        let mut key: Vec<u8> = Vec::new();

        let source = Vu8::try_create_viewer(columns[0])?;
        let pat = Vu8::try_create_viewer(columns[1])?;
        let pos = i64::try_create_viewer(columns[2])?;
        let occur = i64::try_create_viewer(columns[3])?;
        let mt = Vu8::try_create_viewer(columns[4])?;

        for row in 0..input_rows {
            if source.null_at(row)
                || pat.null_at(row)
                || pos.null_at(row)
                || occur.null_at(row)
                || mt.null_at(row)
            {
                builder.append_null();
                continue;
            }

            let s_value = source.value_at(row);
            let pat_value = pat.value_at(row);
            let pos_value = pos.value_at(row);
            let occur_value = occur.value_at(row);
            let mt_value = mt.value_at(row);
            validate_regexp_arguments(
                self.name(),
                pos_value,
                Some(occur_value),
                None,
                Some(mt_value),
            )?;

            if s_value.is_empty() || pat_value.is_empty() {
                builder.append_null();
                continue;
            }

            key.extend_from_slice(pat_value);
            key.extend_from_slice("-".as_bytes());
            key.extend_from_slice(mt_value);
            let re = if let Some(re) = map.get(&key) {
                re
            } else {
                let re = build_regexp_from_pattern(self.name(), pat_value, Some(mt_value))?;
                map.insert(key.clone(), re);
                map.get(&key).unwrap()
            };
            key.clear();

            let substr = regexp_substr(s_value, re, pos_value, occur_value);
            if let Some(ss) = substr {
                builder.append(ss, true);
            } else {
                builder.append_null();
            }
        }

        Ok(builder.build(input_rows))
    }
}

#[inline]
fn regexp_substr<'a>(s: &'a [u8], re: &Regex, pos: i64, occur: i64) -> Option<&'a [u8]> {
    let occur = if occur < 1 { 1 } else { occur };
    let pos = if pos < 1 { 0 } else { (pos - 1) as usize };

    // the 'pos' postion is the character index,
    // so we should iterate the character to find the byte index.
    let mut pos = match s.char_indices().nth(pos) {
        Some((start, _, _)) => start,
        None => return None,
    };

    let m = regexp_match_result(s, re, &mut pos, &occur);

    m.map(|m| m.as_bytes())
}

impl fmt::Display for RegexpSubStrFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
