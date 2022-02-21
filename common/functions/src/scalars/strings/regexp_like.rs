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
use common_datavalues::TypeID;
use common_exception::ErrorCode;
use common_exception::Result;
use itertools::izip;
use regex::bytes::Regex as BytesRegex;
use regex::bytes::RegexBuilder as BytesRegexBuilder;

use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;

#[derive(Clone)]
pub struct RegexpLikeFunction {
    display_name: String,
}

impl RegexpLikeFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(Self {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .deterministic()
                .variadic_arguments(2, 3),
        )
    }
}

impl Function for RegexpLikeFunction {
    fn name(&self) -> &str {
        &self.display_name
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        let not_string = args.iter().any(|arg| arg.data_type_id() != TypeID::String);
        if not_string {
            return Err(ErrorCode::BadArguments(format!(
                "Expected string arg, but got {:?}",
                args,
            )));
        }
        Ok(Int8Type::arc())
    }

    fn eval(&self, columns: &ColumnsWithField, _input_rows: usize) -> Result<ColumnRef> {
        let mut mt: Option<&ColumnRef> = None;

        if columns.len() == 3 {
            mt = Some(columns[2].column())
        }
        Ok(Arc::new(a_regexp_binary(
            columns[0].column(),
            columns[1].column(),
            mt,
        )?))
    }
}

impl fmt::Display for RegexpLikeFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

#[inline]
fn a_regexp_binary(lhs: &ColumnRef, rhs: &ColumnRef, mt: Option<&ColumnRef>) -> Result<Int8Column> {
    let mut map = HashMap::new();

    let mut builder: ColumnBuilder<i8> = ColumnBuilder::with_capacity(lhs.len());

    let lhs = Vu8::try_create_viewer(lhs)?;
    let rhs = Vu8::try_create_viewer(rhs)?;

    if let Some(mt) = mt {
        let mut key: Vec<u8> = Vec::new();

        let mt = Vu8::try_create_viewer(mt)?;
        let iter = izip!(lhs, rhs, mt);
        for (lhs_value, rhs_value, mt_value) in iter {
            key.extend_from_slice(rhs_value);
            key.extend_from_slice("-".as_bytes());
            key.extend_from_slice(mt_value);

            let pattern = if let Some(pattern) = map.get(key.as_slice()) {
                pattern
            } else {
                let re = build_regexp_from_pattern(rhs_value, Some(mt_value))?;
                map.insert(rhs_value, re);
                map.get(rhs_value).unwrap()
            };
            key.clear();

            builder.append(match pattern.is_match(lhs_value) {
                true => 1,
                false => 0,
            });
        }
    } else {
        for (lhs_value, rhs_value) in lhs.zip(rhs) {
            let pattern = if let Some(pattern) = map.get(rhs_value) {
                pattern
            } else {
                let re = build_regexp_from_pattern(rhs_value, None)?;
                map.insert(rhs_value, re);
                map.get(rhs_value).unwrap()
            };

            builder.append(match pattern.is_match(lhs_value) {
                true => 1,
                false => 0,
            });
        }
    }

    Ok(builder.build_column())
}

#[inline]
fn build_regexp_from_pattern(pat: &[u8], mt: Option<&[u8]>) -> Result<BytesRegex> {
    let pattern = match pat.is_empty() {
        true => "^$",
        false => simdutf8::basic::from_utf8(pat).map_err(|e| {
            ErrorCode::BadArguments(format!(
                "Unable to convert the REGEXP_LIKE pattern to string: {}",
                e
            ))
        })?,
    };

    let mt = match mt {
        Some(mt) => {
            if mt.is_empty() {
                "i".as_bytes()
            } else {
                mt
            }
        }
        None => "i".as_bytes(),
    };

    let mut builder = BytesRegexBuilder::new(pattern);

    for c in mt.chars() {
        let r = match c {
            'c' => Ok(builder.case_insensitive(false)),
            'i' => Ok(builder.case_insensitive(true)),
            'm' => Ok(builder.multi_line(true)),
            'n' => Ok(builder.dot_matches_new_line(true)),
            // Notes: https://dev.mysql.com/doc/refman/8.0/en/regexp.html#function_regexp-like
            // Notes: https://docs.rs/regex/1.5.4/regex/bytes/struct.RegexBuilder.html
            // Notes: https://github.com/rust-lang/regex/issues/244
            // It seems that the regexp crate doesn't support the 'u' match type.
            'u' => Err(ErrorCode::BadArguments(format!(
                "Unsupported arguments to REGEXP_LIKE match type: {}",
                c,
            ))),
            _ => Err(ErrorCode::BadArguments(format!(
                "Incorrect arguments to REGEXP_LIKE match type: {}",
                c,
            ))),
        };
        if let Err(e) = r {
            return Err(e);
        }
    }
    builder.build().map_err(|e| {
        ErrorCode::BadArguments(format!(
            "Unable to build regex from REGEXP_LIKE pattern: {}",
            e
        ))
    })
}
