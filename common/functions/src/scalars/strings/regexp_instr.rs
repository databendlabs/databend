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
use std::sync::Arc;

use bstr::ByteSlice;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use itertools::izip;
use regex::bytes::Regex;

use crate::scalars::assert_string;
use crate::scalars::cast_column_field;
use crate::scalars::strings::regexp_like::build_regexp_from_pattern;
use crate::scalars::Function;
use crate::scalars::FunctionFeatures;
use crate::scalars::TypedFunctionDescription;

#[derive(Clone)]
pub struct RegexpInStrFunction {
    display_name: String,
}

impl RegexpInStrFunction {
    pub fn try_create(display_name: &str, args: &[&DataTypePtr]) -> Result<Box<dyn Function>> {
        for (i, arg) in args.iter().enumerate() {
            if i < 2 || i == 5 {
                assert_string(*arg)?;
            } else if !arg.data_type_id().is_integer()
                && !arg.data_type_id().is_string()
                && !arg.data_type_id().is_null()
            {
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

    pub fn desc() -> TypedFunctionDescription {
        TypedFunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .deterministic()
                .variadic_arguments(2, 6),
        )
    }
}

impl Function for RegexpInStrFunction {
    fn name(&self) -> &str {
        &self.display_name
    }

    fn return_type(&self, _args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        Ok(u64::to_data_type())
    }

    // Notes: https://dev.mysql.com/doc/refman/8.0/en/regexp.html#function_regexp-instr
    fn eval(
        &self,
        columns: &ColumnsWithField,
        input_rows: usize,
        _func_ctx: FunctionContext,
    ) -> Result<ColumnRef> {
        let mut pos = ConstColumn::new(Series::from_data(vec![1_i64]), input_rows).arc();
        let mut occurrence = ConstColumn::new(Series::from_data(vec![1_i64]), input_rows).arc();
        let mut return_option = ConstColumn::new(Series::from_data(vec![0_i64]), input_rows).arc();
        let mut match_type = ConstColumn::new(Series::from_data(vec![""]), input_rows).arc();

        for i in 2..columns.len() {
            match i {
                2 => pos = cast_column_field(&columns[2], &Int64Type::arc())?,
                3 => occurrence = cast_column_field(&columns[3], &Int64Type::arc())?,
                4 => return_option = cast_column_field(&columns[4], &Int64Type::arc())?,
                _ => match_type = cast_column_field(&columns[5], &StringType::arc())?,
            }
        }

        let pat = columns[1].column();

        if pat.is_const() && match_type.is_const() {
            let pat_value = pat.get_string(0)?;
            let mt_value = match_type.get_string(0)?;

            return Ok(Arc::new(self.a_regexp_instr_binary_scalar(
                columns[0].column(),
                &pat_value,
                &pos,
                &occurrence,
                &return_option,
                &mt_value,
            )?));
        }

        Ok(Arc::new(self.a_regexp_instr_binary(
            columns[0].column(),
            pat,
            &pos,
            &occurrence,
            &return_option,
            &match_type,
        )?))
    }
}

impl RegexpInStrFunction {
    fn a_regexp_instr_binary_scalar(
        &self,
        source: &ColumnRef,
        pat: &[u8],
        pos: &ColumnRef,
        occurrence: &ColumnRef,
        return_option: &ColumnRef,
        mt: &[u8],
    ) -> Result<UInt64Column> {
        let mut builder: ColumnBuilder<u64> = ColumnBuilder::with_capacity(source.len());

        let source = Vu8::try_create_viewer(source)?;
        let pos = i64::try_create_viewer(pos)?;
        let occur = i64::try_create_viewer(occurrence)?;
        let ro = i64::try_create_viewer(return_option)?;

        let re = build_regexp_from_pattern(self.name(), pat, Some(mt))?;

        let iter = izip!(source, pos, occur, ro);
        for (s_value, pos_value, occur_value, ro_value) in iter {
            if ro_value != 0 && ro_value != 1 {
                return Err(ErrorCode::BadArguments(format!(
                    "Incorrect arguments to {}: return_option must be 1 or 0, but got {}",
                    self.name(),
                    ro_value
                )));
            }
            if s_value.is_empty() || pat.is_empty() {
                builder.append(0);
                continue;
            }

            let instr = regexp_instr(s_value, &re, pos_value, occur_value, ro_value);

            builder.append(instr);
        }

        Ok(builder.build_column())
    }

    fn a_regexp_instr_binary(
        &self,
        source: &ColumnRef,
        pat: &ColumnRef,
        pos: &ColumnRef,
        occurrence: &ColumnRef,
        return_option: &ColumnRef,
        match_type: &ColumnRef,
    ) -> Result<UInt64Column> {
        let mut builder: ColumnBuilder<u64> = ColumnBuilder::with_capacity(source.len());

        let mut map: HashMap<Vec<u8>, Regex> = HashMap::new();
        let mut key: Vec<u8> = Vec::new();

        let source = Vu8::try_create_viewer(source)?;
        let pat = Vu8::try_create_viewer(pat)?;
        let pos = i64::try_create_viewer(pos)?;
        let occur = i64::try_create_viewer(occurrence)?;
        let ro = i64::try_create_viewer(return_option)?;
        let mt = Vu8::try_create_viewer(match_type)?;

        let iter = izip!(source, pat, pos, occur, ro, mt);
        for (s_value, pat_value, pos_value, occur_value, ro_value, mt_value) in iter {
            if ro_value != 0 && ro_value != 1 {
                return Err(ErrorCode::BadArguments(format!(
                    "Incorrect arguments to {}: return_option must be 1 or 0, but got {}",
                    self.name(),
                    ro_value
                )));
            }
            if mt_value.starts_with_str("-") {
                return Err(ErrorCode::BadArguments(format!(
                    "Incorrect arguments to {} match type: {}",
                    self.name(),
                    mt_value.to_str_lossy(),
                )));
            }
            if s_value.is_empty() || pat_value.is_empty() {
                builder.append(0);
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

            builder.append(instr);
        }

        Ok(builder.build_column())
    }
}

#[inline]
fn regexp_instr(s: &[u8], re: &Regex, pos: i64, occur: i64, ro: i64) -> u64 {
    let occur = if occur < 1 { 1 } else { occur };
    let pos = if pos < 1 { 0 } else { (pos - 1) as usize };

    // the 'pos' postion is the character index,
    // so we should iterate the character to find the byte index.
    let mut pos = match s.char_indices().nth(pos) {
        Some((start, _, _)) => start,
        None => return 0,
    };

    let mut i = 1_i64;
    let m = loop {
        let m = re.find_at(s, pos);
        if i == occur || m.is_none() {
            break m;
        }

        i += 1;
        if let Some(m) = m {
            // set the start postion of 'find_at' function to the position following the matched substring
            pos = m.end();
        }
    };

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

impl fmt::Display for RegexpInStrFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
use crate::scalars::FunctionContext;
