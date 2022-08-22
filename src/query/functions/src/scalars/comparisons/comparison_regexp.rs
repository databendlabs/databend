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

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use regex::bytes::Regex as BytesRegex;
use regex::bytes::RegexBuilder as BytesRegexBuilder;

use super::comparison::StringSearchCreator;
use super::utils::StringSearchImpl;

pub type ComparisonRegexpFunction = StringSearchCreator<false, StringSearchRegex>;
pub type ComparisonNotRegexpFunction = StringSearchCreator<true, StringSearchRegex>;

#[derive(Clone)]
pub struct StringSearchRegex;

impl StringSearchImpl for StringSearchRegex {
    fn vector_vector(
        lhs: &StringColumn,
        rhs: &StringColumn,
        op: impl Fn(bool) -> bool,
    ) -> BooleanColumn {
        let mut map = HashMap::new();

        let mut builder: ColumnBuilder<bool> = ColumnBuilder::with_capacity(lhs.len());

        for (lhs_value, rhs_value) in lhs.scalar_iter().zip(rhs.scalar_iter()) {
            let pattern = if let Some(pattern) = map.get(rhs_value) {
                pattern
            } else {
                let re = build_regexp_from_pattern(rhs_value).unwrap();
                map.insert(rhs_value, re);
                map.get(rhs_value).unwrap()
            };

            builder.append(op(pattern.is_match(lhs_value)));
        }
        builder.build_column()
    }

    fn vector_const(lhs: &StringColumn, rhs: &[u8], op: impl Fn(bool) -> bool) -> BooleanColumn {
        let re = build_regexp_from_pattern(rhs).unwrap();
        BooleanColumn::from_iterator(lhs.scalar_iter().map(|x| op(re.is_match(x))))
    }
}

#[inline]
fn build_regexp_from_pattern(pat: &[u8]) -> Result<BytesRegex> {
    let pattern = match pat.is_empty() {
        true => "^$",
        false => simdutf8::basic::from_utf8(pat).map_err(|e| {
            ErrorCode::BadArguments(format!(
                "Unable to convert the REGEXP pattern to string: {}",
                e
            ))
        })?,
    };

    BytesRegexBuilder::new(pattern)
        .case_insensitive(true)
        .build()
        .map_err(|e| {
            ErrorCode::BadArguments(format!("Unable to build regex from REGEXP pattern: {}", e))
        })
}
