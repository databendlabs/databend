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
use regex::bytes::Regex as BytesRegex;
use regex::bytes::RegexBuilder as BytesRegexBuilder;

use crate::scalars::assert_string;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

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
                .bool_function()
                .variadic_arguments(2, 3),
        )
    }
}

impl Function for RegexpLikeFunction {
    fn name(&self) -> &str {
        &self.display_name
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        for arg in args {
            assert_string(*arg)?;
        }

        Ok(BooleanType::arc())
    }
    // Notes: https://dev.mysql.com/doc/refman/8.0/en/regexp.html#function_regexp-like
    fn eval(
        &self,
        columns: &ColumnsWithField,
        _input_rows: usize,
        _func_ctx: FunctionContext,
    ) -> Result<ColumnRef> {
        let col1: Result<&ConstColumn> = Series::check_get(columns[1].column());
        if let Ok(col1) = col1 {
            let lhs = columns[0].column();
            let rhs = col1.get_string(0)?;

            if columns.len() == 3 {
                if columns[2].column().is_const() {
                    let mt = columns[2].column().get_string(0)?;
                    return Ok(Arc::new(self.a_regexp_binary_scalar(
                        lhs,
                        &rhs,
                        Some(&mt),
                    )?));
                }
            } else {
                return Ok(Arc::new(self.a_regexp_binary_scalar(lhs, &rhs, None)?));
            }
        }

        let mut mt: Option<&ColumnRef> = None;
        if columns.len() == 3 {
            mt = Some(columns[2].column())
        }
        Ok(Arc::new(self.a_regexp_binary(
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

impl RegexpLikeFunction {
    fn a_regexp_binary_scalar(
        &self,
        lhs: &ColumnRef,
        rhs: &[u8],
        mt: Option<&[u8]>,
    ) -> Result<BooleanColumn> {
        let mut builder: ColumnBuilder<bool> = ColumnBuilder::with_capacity(lhs.len());

        let re = build_regexp_from_pattern(self.name(), rhs, mt)?;

        let lhs = Vu8::try_create_viewer(lhs)?;
        for lhs_value in lhs.iter() {
            builder.append(re.is_match(lhs_value));
        }

        Ok(builder.build_column())
    }

    fn a_regexp_binary(
        &self,
        lhs: &ColumnRef,
        rhs: &ColumnRef,
        mt: Option<&ColumnRef>,
    ) -> Result<BooleanColumn> {
        let mut builder: ColumnBuilder<bool> = ColumnBuilder::with_capacity(lhs.len());

        let mut map = HashMap::new();
        let mut key: Vec<u8> = Vec::new();

        let lhs = Vu8::try_create_viewer(lhs)?;
        let rhs = Vu8::try_create_viewer(rhs)?;

        if let Some(mt) = mt {
            let mt = Vu8::try_create_viewer(mt)?;
            let iter = izip!(lhs, rhs, mt);
            for (lhs_value, rhs_value, mt_value) in iter {
                if mt_value.starts_with_str("-") {
                    return Err(ErrorCode::BadArguments(format!(
                        "Incorrect arguments to {} match type: {}",
                        self.name(),
                        mt_value.to_str_lossy(),
                    )));
                }
                key.extend_from_slice(rhs_value);
                key.extend_from_slice("-".as_bytes());
                key.extend_from_slice(mt_value);

                let pattern = if let Some(pattern) = map.get(&key) {
                    pattern
                } else {
                    let re = build_regexp_from_pattern(self.name(), rhs_value, Some(mt_value))?;
                    map.insert(key.clone(), re);
                    map.get(&key).unwrap()
                };
                key.clear();

                builder.append(pattern.is_match(lhs_value));
            }
        } else {
            for (lhs_value, rhs_value) in lhs.zip(rhs) {
                key.extend_from_slice(rhs_value);
                let pattern = if let Some(pattern) = map.get(&key) {
                    pattern
                } else {
                    let re = build_regexp_from_pattern(self.name(), rhs_value, None)?;
                    map.insert(key.clone(), re);
                    map.get(&key).unwrap()
                };
                key.clear();

                builder.append(pattern.is_match(lhs_value));
            }
        }

        Ok(builder.build_column())
    }
}

#[inline]
pub fn build_regexp_from_pattern(
    fn_name: &str,
    pat: &[u8],
    mt: Option<&[u8]>,
) -> Result<BytesRegex> {
    let pattern = match pat.is_empty() {
        true => "^$",
        false => simdutf8::basic::from_utf8(pat).map_err(|e| {
            ErrorCode::BadArguments(format!(
                "Unable to convert the {} pattern to string: {}",
                fn_name, e
            ))
        })?,
    };
    // the default match type value is 'i', if it is empty
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
                "Unsupported arguments to {} match type: {}",
                fn_name, c,
            ))),
            _ => Err(ErrorCode::BadArguments(format!(
                "Incorrect arguments to {} match type: {}",
                fn_name, c,
            ))),
        };
        if let Err(e) = r {
            return Err(e);
        }
    }
    builder.build().map_err(|e| {
        ErrorCode::BadArguments(format!(
            "Unable to build regex from {} pattern: {}",
            fn_name, e
        ))
    })
}
use crate::scalars::FunctionContext;
