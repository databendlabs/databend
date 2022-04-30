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
pub struct RegexpReplaceFunction {
    display_name: String,
}

impl RegexpReplaceFunction {
    pub fn try_create(display_name: &str, args: &[&DataTypeImpl]) -> Result<Box<dyn Function>> {
        for (i, arg) in args.iter().enumerate() {
            if arg.is_null() {
                continue;
            }

            let arg = remove_nullable(arg);
            if i < 3 || i == 5 {
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
                .variadic_arguments(3, 6),
        )
    }
}

impl Function for RegexpReplaceFunction {
    fn name(&self) -> &str {
        &self.display_name
    }

    fn return_type(&self) -> DataTypeImpl {
        NullableType::new_impl(StringType::new_impl())
    }

    // Notes: https://dev.mysql.com/doc/refman/8.0/en/regexp.html#function_regexp-replace
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
        let mut occurrence = ConstColumn::new(Series::from_data(vec![0_i64]), input_rows).arc();
        let mut match_type = ConstColumn::new(Series::from_data(vec![""]), input_rows).arc();

        for i in 3..columns.len() {
            match i {
                3 => {
                    pos = cast_column_field(
                        &columns[3],
                        &NullableType::new_impl(Int64Type::new_impl()),
                    )?
                }
                4 => {
                    occurrence = cast_column_field(
                        &columns[4],
                        &NullableType::new_impl(Int64Type::new_impl()),
                    )?
                }
                _ => {
                    match_type = cast_column_field(
                        &columns[5],
                        &NullableType::new_impl(StringType::new_impl()),
                    )?
                }
            }
        }

        let source = columns[0].column();
        let pat = columns[1].column();
        let repl = columns[2].column();

        if pat.is_const() && match_type.is_const() {
            let pat_value = pat.get_string(0)?;
            let mt_value = match_type.get_string(0)?;
            let columns = [source, repl, &pos, &occurrence];

            return self.a_regexp_replace_binary_scalar(
                &columns[..],
                &pat_value,
                &mt_value,
                input_rows,
            );
        }

        let columns = [source, pat, repl, &pos, &occurrence, &match_type];
        self.a_regexp_replace_binary(&columns[..], input_rows)
    }
}

impl RegexpReplaceFunction {
    fn a_regexp_replace_binary_scalar(
        &self,
        columns: &[&ColumnRef],
        pat: &[u8],
        mt: &[u8],
        input_rows: usize,
    ) -> Result<ColumnRef> {
        let re = build_regexp_from_pattern(self.name(), pat, Some(mt))?;

        let source = Vu8::try_create_viewer(columns[0])?;
        let repl = Vu8::try_create_viewer(columns[1])?;
        let pos = i64::try_create_viewer(columns[2])?;
        let occur = i64::try_create_viewer(columns[3])?;

        let mut_string_col = MutableStringColumn::with_values_capacity(
            source.value_at(0).len() * input_rows,
            input_rows + 1,
        );
        let mut builder = MutableNullableColumn::new(Box::new(mut_string_col), self.return_type());

        let mut buf = Vec::with_capacity(source.value_at(0).len());
        for row in 0..input_rows {
            if source.null_at(row) || repl.null_at(row) || pos.null_at(row) || occur.null_at(row) {
                builder.append_default();
                continue;
            }

            let s_value = source.value_at(row);
            let repl_value = repl.value_at(row);
            let pos_value = pos.value_at(row);
            let occur_value = occur.value_at(row);

            validate_regexp_arguments(self.name(), pos_value, None, None, None)?;
            if occur_value < 0 {
                // the occurrence argument for regexp_replace is different with other regexp_* function
                // the value of '0' is valid, so check the value here separately
                return Err(ErrorCode::BadArguments(format!(
                    "Incorrect arguments to {}: occurrence must not be negative, but got {}",
                    self.name(),
                    occur_value
                )));
            }

            if s_value.is_empty() || pat.is_empty() {
                builder.append_data_value(s_value.into())?;
                continue;
            }

            regexp_replace(s_value, &re, repl_value, pos_value, occur_value, &mut buf);
            builder.append_data_value(buf.clone().into())?;
            buf.clear();
        }

        Ok(builder.to_column())
    }

    fn a_regexp_replace_binary(
        &self,
        columns: &[&ColumnRef],
        input_rows: usize,
    ) -> Result<ColumnRef> {
        let mut map: HashMap<Vec<u8>, Regex> = HashMap::new();
        let mut key: Vec<u8> = Vec::new();

        let source = Vu8::try_create_viewer(columns[0])?;
        let pat = Vu8::try_create_viewer(columns[1])?;
        let repl = Vu8::try_create_viewer(columns[2])?;
        let pos = i64::try_create_viewer(columns[3])?;
        let occur = i64::try_create_viewer(columns[4])?;
        let mt = Vu8::try_create_viewer(columns[5])?;

        let mut_string_col = MutableStringColumn::with_values_capacity(
            source.value_at(0).len() * input_rows,
            input_rows + 1,
        );
        let mut builder = MutableNullableColumn::new(Box::new(mut_string_col), self.return_type());

        let mut buf = Vec::with_capacity(source.value_at(0).len());
        for row in 0..input_rows {
            if source.null_at(row)
                || pat.null_at(row)
                || repl.null_at(row)
                || pos.null_at(row)
                || occur.null_at(row)
                || mt.null_at(row)
            {
                builder.append_default();
                continue;
            }

            let s_value = source.value_at(row);
            let pat_value = pat.value_at(row);
            let repl_value = repl.value_at(row);
            let pos_value = pos.value_at(row);
            let occur_value = occur.value_at(row);
            let mt_value = mt.value_at(row);

            validate_regexp_arguments(self.name(), pos_value, None, None, Some(mt_value))?;
            if occur_value < 0 {
                // the occurrence argument for regexp_replace is different with other regexp_* function
                // the value of '0' is valid, so check the value here separately
                return Err(ErrorCode::BadArguments(format!(
                    "Incorrect arguments to {}: occurrence must not be negative, but got {}",
                    self.name(),
                    occur_value
                )));
            }
            validate_regexp_arguments(self.name(), pos_value, None, None, Some(mt_value))?;

            if s_value.is_empty() || pat_value.is_empty() {
                builder.append_data_value(s_value.into())?;
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

            regexp_replace(s_value, re, repl_value, pos_value, occur_value, &mut buf);
            builder.append_data_value(buf.clone().into())?;
            buf.clear();
        }

        Ok(builder.to_column())
    }
}

#[inline]
fn regexp_replace(s: &[u8], re: &Regex, repl: &[u8], pos: i64, occur: i64, buf: &mut Vec<u8>) {
    let pos = (pos - 1) as usize; // set the index start from 0

    // the 'pos' postion is the character index,
    // so we should iterate the character to find the byte index.
    let mut pos = match s.char_indices().nth(pos) {
        Some((start, _, _)) => start,
        None => {
            buf.extend_from_slice(s);
            return;
        }
    };

    let m = regexp_match_result(s, re, &mut pos, &occur);
    if m.is_none() {
        buf.extend_from_slice(s);
        return;
    }

    buf.extend_from_slice(&s[..m.unwrap().start()]);

    if occur == 0 {
        let s = &s[m.unwrap().start()..];
        buf.extend_from_slice(&re.replace_all(s, repl));
    } else {
        buf.extend_from_slice(repl);
        buf.extend_from_slice(&s[m.unwrap().end()..])
    }
}

impl fmt::Display for RegexpReplaceFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
