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
use regex::bytes::Match;
use regex::bytes::Regex;

use crate::scalars::assert_string;
use crate::scalars::cast_column_field;
use crate::scalars::strings::regexp_like::build_regexp_from_pattern;
use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

#[derive(Clone)]
pub struct RegexpInStrFunction {
    display_name: String,
}

impl RegexpInStrFunction {
    pub fn try_create(display_name: &str, args: &[&DataTypePtr]) -> Result<Box<dyn Function>> {
        for (i, arg) in args.iter().enumerate() {
            if arg.is_null() {
                continue;
            }

            let arg = remove_nullable(arg);
            if i < 2 || i == 5 {
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
                .variadic_arguments(2, 6),
        )
    }
}

impl Function for RegexpInStrFunction {
    fn name(&self) -> &str {
        &self.display_name
    }

    fn return_type(&self) -> DataTypePtr {
        NullableType::arc(u64::to_data_type())
    }

    // Notes: https://dev.mysql.com/doc/refman/8.0/en/regexp.html#function_regexp-instr
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
        let mut return_option = ConstColumn::new(Series::from_data(vec![0_i64]), input_rows).arc();
        let mut match_type = ConstColumn::new(Series::from_data(vec![""]), input_rows).arc();

        for i in 2..columns.len() {
            match i {
                2 => pos = cast_column_field(&columns[2], &NullableType::arc(Int64Type::arc()))?,
                3 => {
                    occurrence =
                        cast_column_field(&columns[3], &NullableType::arc(Int64Type::arc()))?
                }
                4 => {
                    return_option =
                        cast_column_field(&columns[4], &NullableType::arc(Int64Type::arc()))?
                }
                _ => {
                    match_type =
                        cast_column_field(&columns[5], &NullableType::arc(StringType::arc()))?
                }
            }
        }

        let source = columns[0].column();
        let pat = columns[1].column();

        if pat.is_const() && match_type.is_const() {
            let pat_value = pat.get_string(0)?;
            let mt_value = match_type.get_string(0)?;
            let columns = [source, &pos, &occurrence, &return_option];

            return self.a_regexp_instr_binary_scalar(&columns, &pat_value, &mt_value, input_rows);
        }

        let columns = [source, pat, &pos, &occurrence, &return_option, &match_type];
        self.a_regexp_instr_binary(&columns, input_rows)
    }
}

impl RegexpInStrFunction {
    fn a_regexp_instr_binary_scalar(
        &self,
        columns: &[&ColumnRef],
        pat: &[u8],
        mt: &[u8],
        input_rows: usize,
    ) -> Result<ColumnRef> {
        let mut builder = NullableColumnBuilder::<u64>::with_capacity(columns[0].len());

        let source = Vu8::try_create_viewer(columns[0])?;
        let pos = i64::try_create_viewer(columns[1])?;
        let occur = i64::try_create_viewer(columns[2])?;
        let ro = i64::try_create_viewer(columns[3])?;

        let re = build_regexp_from_pattern(self.name(), pat, Some(mt))?;

        for row in 0..input_rows {
            if source.null_at(row) || pos.null_at(row) || occur.null_at(row) || ro.null_at(row) {
                builder.append_null();
                continue;
            }

            let s_value = source.value_at(row);
            let pos_value = pos.value_at(row);
            let occur_value = occur.value_at(row);
            let ro_value = ro.value_at(row);
            validate_regexp_arguments(
                self.name(),
                pos_value,
                Some(occur_value),
                Some(ro_value),
                None,
            )?;

            if s_value.is_empty() || pat.is_empty() {
                builder.append(0, true);
                continue;
            }

            let instr = regexp_instr(s_value, &re, pos_value, occur_value, ro_value);
            builder.append(instr, true);
        }

        Ok(builder.build(input_rows))
    }

    fn a_regexp_instr_binary(
        &self,
        columns: &[&ColumnRef],
        input_rows: usize,
    ) -> Result<ColumnRef> {
        let mut builder = NullableColumnBuilder::<u64>::with_capacity(columns[0].len());

        let mut map: HashMap<Vec<u8>, Regex> = HashMap::new();
        let mut key: Vec<u8> = Vec::new();

        let source = Vu8::try_create_viewer(columns[0])?;
        let pat = Vu8::try_create_viewer(columns[1])?;
        let pos = i64::try_create_viewer(columns[2])?;
        let occur = i64::try_create_viewer(columns[3])?;
        let ro = i64::try_create_viewer(columns[4])?;
        let mt = Vu8::try_create_viewer(columns[5])?;

        for row in 0..input_rows {
            if source.null_at(row)
                || pat.null_at(row)
                || pos.null_at(row)
                || occur.null_at(row)
                || ro.null_at(row)
                || mt.null_at(row)
            {
                builder.append_null();
                continue;
            }

            let s_value = source.value_at(row);
            let pat_value = pat.value_at(row);
            let pos_value = pos.value_at(row);
            let occur_value = occur.value_at(row);
            let ro_value = ro.value_at(row);
            let mt_value = mt.value_at(row);
            validate_regexp_arguments(
                self.name(),
                pos_value,
                Some(occur_value),
                Some(ro_value),
                Some(mt_value),
            )?;

            if s_value.is_empty() || pat_value.is_empty() {
                builder.append(0, true);
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

            let instr = regexp_instr(s_value, re, pos_value, occur_value, ro_value);

            builder.append(instr, true);
        }

        Ok(builder.build(input_rows))
    }
}

#[inline]
fn regexp_instr(s: &[u8], re: &Regex, pos: i64, occur: i64, ro: i64) -> u64 {
    let pos = (pos - 1) as usize; // set the index start from 0

    // the 'pos' postion is the character index,
    // so we should iterate the character to find the byte index.
    let mut pos = match s.char_indices().nth(pos) {
        Some((start, _, _)) => start,
        None => return 0,
    };

    let m = regexp_match_result(s, re, &mut pos, &occur);
    if m.is_none() {
        return 0;
    }

    // the matched result is the byte index, but the 'regexp_instr' function returns the character index,
    // so we should iterate the character to find the character index.
    let mut instr = 0_usize;
    for (p, (start, end, _)) in s.char_indices().enumerate() {
        if ro == 0 {
            if start == m.unwrap().start() {
                instr = p + 1;
                break;
            }
        } else if end == m.unwrap().end() {
            instr = p + 2;
            break;
        }
    }

    instr as u64
}

#[inline]
pub fn regexp_match_result<'a>(
    s: &'a [u8],
    re: &Regex,
    pos: &mut usize,
    occur: &i64,
) -> Option<Match<'a>> {
    let mut i = 1_i64;
    let m = loop {
        let m = re.find_at(s, *pos);
        if i >= *occur || m.is_none() {
            break m;
        }

        i += 1;
        if let Some(m) = m {
            // set the start postion of 'find_at' function to the position following the matched substring
            *pos = m.end();
        }
    };

    m
}

/// Validates the arguments of 'regexp_*' functions, returns error if any of arguments is invalid
/// and make the error logic the same as snowflake, since it is more reasonable and consistent
#[inline]
pub fn validate_regexp_arguments(
    fn_name: &str,
    pos: i64,
    occur: Option<i64>,
    ro: Option<i64>,
    mt: Option<&[u8]>,
) -> Result<()> {
    if pos < 1 {
        return Err(ErrorCode::BadArguments(format!(
            "Incorrect arguments to {}: position must be positive, but got {}",
            fn_name, pos
        )));
    }
    if let Some(occur) = occur {
        if occur < 1 {
            return Err(ErrorCode::BadArguments(format!(
                "Incorrect arguments to {}: occurrence must be positive, but got {}",
                fn_name, occur
            )));
        }
    }
    if let Some(ro) = ro {
        if ro != 0 && ro != 1 {
            return Err(ErrorCode::BadArguments(format!(
                "Incorrect arguments to {}: return_option must be 1 or 0, but got {}",
                fn_name, ro
            )));
        }
    }
    if let Some(mt) = mt {
        if mt.starts_with_str("-") {
            return Err(ErrorCode::BadArguments(format!(
                "Incorrect arguments to {} match type: {}",
                fn_name,
                mt.to_str_lossy(),
            )));
        }
    }

    Ok(())
}

impl fmt::Display for RegexpInStrFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
