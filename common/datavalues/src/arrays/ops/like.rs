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

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use regex::bytes::Regex as BytesRegex;

use crate::prelude::*;

impl DFStringArray {
    /// QUOTE: (From arrow2::arrow::compute::like::a_like_binary)
    pub fn a_like_binary<F>(&self, rhs: &DFStringArray, op: F) -> Result<DFBooleanArray>
    where F: Fn(bool) -> bool {
        if self.len() != rhs.len() {
            return Err(ErrorCode::BadArguments(
                "Cannot perform comparison operation on arrays of different length".to_string(),
            ));
        }

        let validity = combine_validities(self.array.validity(), rhs.array.validity());

        let mut map = HashMap::new();

        let values = Bitmap::try_from_trusted_len_iter(
            self.into_no_null_iter()
                .zip(rhs.into_no_null_iter())
                .map::<Result<bool>, _>(|(lhs, rhs)| {
                    let pattern = if let Some(pattern) = map.get(rhs) {
                        pattern
                    } else {
                        let pattern_str = simdutf8::basic::from_utf8(rhs).map_err(|e| {
                            ErrorCode::BadArguments(format!(
                                "Unable to convert the LIKE pattern to string: {}",
                                e
                            ))
                        })?;
                        let re_pattern = like_pattern_to_regex(pattern_str);
                        let re = BytesRegex::new(&re_pattern).map_err(|e| {
                            ErrorCode::BadArguments(format!(
                                "Unable to build regex from LIKE pattern: {}",
                                e
                            ))
                        })?;
                        map.insert(rhs, re);
                        map.get(rhs).unwrap()
                    };
                    Ok(op(pattern.is_match(lhs)))
                }),
        )?;

        Ok(DFBooleanArray::from_arrow_data(values, validity))
    }

    /// QUOTE: (From arrow2::arrow::compute::like::a_like_binary_scalar)
    pub fn a_like_binary_scalar<F>(&self, rhs: &[u8], op: F) -> Result<DFBooleanArray>
    where F: Fn(bool) -> bool {
        let arr = self.inner();
        let validity = arr.validity();

        let values = match check_pattern_type(rhs, false) {
            PatternType::OrdinalStr => {
                Bitmap::from_trusted_len_iter(arr.values_iter().map(|x| x == rhs))
            }
            PatternType::EndOfPercent => {
                // fast path, can use starts_with
                let starts_with = &rhs[..rhs.len() - 1];
                Bitmap::from_trusted_len_iter(
                    arr.values_iter().map(|x| op(x.starts_with(starts_with))),
                )
            }
            PatternType::StartOfPercent => {
                // fast path, can use ends_with
                let ends_with = &rhs[1..];
                Bitmap::from_trusted_len_iter(arr.values_iter().map(|x| op(x.ends_with(ends_with))))
            }
            PatternType::PatternStr => {
                let pattern = simdutf8::basic::from_utf8(rhs).map_err(|e| {
                    ErrorCode::BadArguments(format!(
                        "Unable to convert the LIKE pattern to string: {}",
                        e
                    ))
                })?;
                let re_pattern = like_pattern_to_regex(pattern);
                let re = BytesRegex::new(&re_pattern).map_err(|e| {
                    ErrorCode::BadArguments(format!(
                        "Unable to build regex from LIKE pattern: {}",
                        e
                    ))
                })?;
                Bitmap::from_trusted_len_iter(arr.values_iter().map(|x| op(re.is_match(x))))
            }
        };
        Ok(DFBooleanArray::from_arrow_data(values, validity.cloned()))
    }
}

fn is_like_pattern_escape(c: u8) -> bool {
    c == b'%' || c == b'_' || c == b'\\'
}

#[derive(Clone, Debug, PartialEq)]
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
            '\\' => {
                if let Some(v) = chars.peek().cloned() {
                    match v {
                        '%' | '_' => {
                            regex.push(v);
                            chars.next();
                        }
                        '\\' => {
                            regex.push_str("\\\\");
                            chars.next();
                        }
                        _ => regex.push_str("\\\\"),
                    };
                } else {
                    regex.push_str("\\\\");
                }
            }
            _ => regex.push(c),
        }
    }

    regex.push('$');
    regex
}
