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

use std::collections::HashMap;

use common_datavalues::prelude::*;
use regex::bytes::Regex as BytesRegex;

use super::comparison::StringSearchCreator;
use super::utils::StringSearchImpl;

pub type ComparisonLikeFunction = StringSearchCreator<false, StringSearchLike>;
pub type ComparisonNotLikeFunction = StringSearchCreator<true, StringSearchLike>;

#[derive(Clone)]
pub struct StringSearchLike;

impl StringSearchImpl for StringSearchLike {
    /// QUOTE: (From arrow2::arrow::compute::like::a_like_binary)
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
                let pattern_str = simdutf8::basic::from_utf8(rhs_value)
                    .expect("Unable to convert the LIKE pattern to string: {}");
                let re_pattern = like_pattern_to_regex(pattern_str);
                let re = BytesRegex::new(&re_pattern)
                    .expect("Unable to build regex from LIKE pattern: {}");
                map.insert(rhs_value, re);
                map.get(rhs_value).unwrap()
            };

            builder.append(op(pattern.is_match(lhs_value)));
        }
        builder.build_column()
    }

    /// QUOTE: (From arrow2::arrow::compute::like::a_like_binary_scalar)
    fn vector_const(lhs: &StringColumn, rhs: &[u8], op: impl Fn(bool) -> bool) -> BooleanColumn {
        match check_pattern_type(rhs, false) {
            PatternType::OrdinalStr => {
                BooleanColumn::from_iterator(lhs.scalar_iter().map(|x| op(x == rhs)))
            }
            PatternType::EndOfPercent => {
                // fast path, can use starts_with
                let starts_with = &rhs[..rhs.len() - 1];
                BooleanColumn::from_iterator(
                    lhs.scalar_iter().map(|x| op(x.starts_with(starts_with))),
                )
            }
            PatternType::StartOfPercent => {
                // fast path, can use ends_with
                let ends_with = &rhs[1..];
                BooleanColumn::from_iterator(lhs.scalar_iter().map(|x| op(x.ends_with(ends_with))))
            }
            PatternType::PatternStr => {
                let pattern = simdutf8::basic::from_utf8(rhs)
                    .expect("Unable to convert the LIKE pattern to string: {}");
                let re_pattern = like_pattern_to_regex(pattern);
                let re = BytesRegex::new(&re_pattern)
                    .expect("Unable to build regex from LIKE pattern: {}");
                BooleanColumn::from_iterator(lhs.scalar_iter().map(|x| op(re.is_match(x))))
            }
        }
    }
}

#[inline]
fn is_like_pattern_escape(c: u8) -> bool {
    c == b'%' || c == b'_' || c == b'\\'
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum PatternType {
    // e.g. 'Arrow'
    OrdinalStr,
    // e.g. 'A%row'
    PatternStr,
    // e.g. '%rrow'
    StartOfPercent,
    // e.g. 'Arro%'
    EndOfPercent,
}

/// Check the like pattern type.
///
/// is_pruning: indicate whether to be called on range_filter for pruning.
///
/// For example:
///
/// 'a\\%row'
/// '\\%' will be escaped to a percent. Need transform to `a%row`.
///
/// If is_pruning is true, will be called on range_filter:L379.
/// OrdinalStr is returned, because the pattern can be transformed by range_filter:L382.
///
/// If is_pruning is false, will be called on like.rs:L74.
/// PatternStr is returned, because the pattern cannot be used directly on like.rs:L76.
#[inline]
pub fn check_pattern_type(pattern: &[u8], is_pruning: bool) -> PatternType {
    let len = pattern.len();
    if len == 0 {
        return PatternType::OrdinalStr;
    }

    let mut index = 0;
    let start_percent = pattern[0] == b'%';
    if start_percent {
        if is_pruning {
            return PatternType::PatternStr;
        }
        index += 1;
    }

    while index < len {
        match pattern[index] {
            b'_' => return PatternType::PatternStr,
            b'%' => {
                if index == len - 1 && !start_percent {
                    return PatternType::EndOfPercent;
                }
                return PatternType::PatternStr;
            }
            b'\\' => {
                if index < len - 1 {
                    index += 1;
                    if !is_pruning && is_like_pattern_escape(pattern[index]) {
                        return PatternType::PatternStr;
                    }
                }
            }
            _ => {}
        }
        index += 1;
    }

    if start_percent {
        PatternType::StartOfPercent
    } else {
        PatternType::OrdinalStr
    }
}

/// Transform the like pattern to regex pattern.
/// e.g. 'Hello\._World%\%' tranform to '^Hello\\\..World.*%$'.
#[inline]
pub fn like_pattern_to_regex(pattern: &str) -> String {
    let mut regex = String::with_capacity(pattern.len() * 2);
    regex.push('^');

    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            // Use double backslash to escape special character.
            '^' | '$' | '(' | ')' | '*' | '+' | '.' | '[' | '?' | '{' | '|' => {
                regex.push('\\');
                regex.push(c);
            }
            '%' => regex.push_str(".*"),
            '_' => regex.push('.'),
            '\\' => match chars.peek().cloned() {
                Some('%') => {
                    regex.push('%');
                    chars.next();
                }
                Some('_') => {
                    regex.push('_');
                    chars.next();
                }
                Some('\\') => {
                    regex.push_str("\\\\");
                    chars.next();
                }
                _ => regex.push_str("\\\\"),
            },
            _ => regex.push(c),
        }
    }

    regex.push('$');
    regex
}
