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
use std::fmt;
use std::sync::Arc;

use common_datavalues::prelude::*;
use common_datavalues::DataValueComparisonOperator;
use common_exception::ErrorCode;
use common_exception::Result;
use regex::bytes::Regex as BytesRegex;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
pub struct ComparisonLikeFunction {
    op: DataValueComparisonOperator,
}

impl ComparisonLikeFunction {
    pub fn try_create_like(_display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(ComparisonLikeFunction {
            op: DataValueComparisonOperator::Like,
        }))
    }

    pub fn try_create_nlike(_display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(ComparisonLikeFunction {
            op: DataValueComparisonOperator::NotLike,
        }))
    }

    pub fn desc_like() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_like)).features(
            FunctionFeatures::default()
                .deterministic()
                .negative_function("not like")
                .bool_function()
                .num_arguments(2),
        )
    }

    pub fn desc_unlike() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_nlike)).features(
            FunctionFeatures::default()
                .deterministic()
                .negative_function("like")
                .bool_function()
                .num_arguments(2),
        )
    }
}

impl Function for ComparisonLikeFunction {
    fn name(&self) -> &str {
        match self.op {
            DataValueComparisonOperator::Like => "like",
            DataValueComparisonOperator::NotLike => "not like",
            _ => unreachable!(),
        }
    }

    fn return_type(
        &self,
        args: &[&common_datavalues::DataTypePtr],
    ) -> Result<common_datavalues::DataTypePtr> {
        // expect array & struct
        let not_string = args.iter().all(|arg| arg.data_type_id() != TypeID::String);
        if not_string {
            return Err(ErrorCode::BadArguments(format!(
                "Illegal types {:?} of argument of function {}, must be strings",
                args,
                self.name()
            )));
        }
        Ok(BooleanType::arc())
    }

    fn eval(
        &self,
        columns: &common_datavalues::ColumnsWithField,
        _input_rows: usize,
    ) -> Result<common_datavalues::ColumnRef> {
        let col1: Result<&ConstColumn> = Series::check_get(columns[1].column());

        if let Ok(col1) = col1 {
            let rhs = col1.get_string(0)?;
            return self.eval_constant(columns[0].column(), &rhs);
        }

        let result = match self.op {
            DataValueComparisonOperator::Like => {
                a_like_binary(columns[0].column(), columns[1].column(), |x| x)
            }
            DataValueComparisonOperator::NotLike => {
                a_like_binary(columns[0].column(), columns[1].column(), |x| !x)
            }
            _ => unreachable!(),
        }?;

        Ok(Arc::new(result))
    }
}

impl ComparisonLikeFunction {
    fn eval_constant(&self, lhs: &ColumnRef, rhs: &[u8]) -> Result<common_datavalues::ColumnRef> {
        let result = match self.op {
            DataValueComparisonOperator::Like => a_like_binary_scalar(lhs, rhs, |x| x),
            DataValueComparisonOperator::NotLike => a_like_binary_scalar(lhs, rhs, |x| !x),
            _ => unreachable!(),
        }?;
        Ok(Arc::new(result))
    }
}

impl fmt::Display for ComparisonLikeFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.op)
    }
}

/// QUOTE: (From arrow2::arrow::compute::like::a_like_binary)
#[inline]
pub fn a_like_binary<F>(lhs: &ColumnRef, rhs: &ColumnRef, op: F) -> Result<BooleanColumn>
where F: Fn(bool) -> bool {
    let mut map = HashMap::new();

    let mut builder: ColumnBuilder<bool> = ColumnBuilder::with_capacity(lhs.len());

    let lhs = Vu8::try_create_viewer(lhs)?;
    let rhs = Vu8::try_create_viewer(rhs)?;

    for (lhs_value, rhs_value) in lhs.iter().zip(rhs.iter()) {
        let pattern = if let Some(pattern) = map.get(rhs_value) {
            pattern
        } else {
            let pattern_str = simdutf8::basic::from_utf8(rhs_value).map_err(|e| {
                ErrorCode::BadArguments(format!(
                    "Unable to convert the LIKE pattern to string: {}",
                    e
                ))
            })?;
            let re_pattern = like_pattern_to_regex(pattern_str);
            let re = BytesRegex::new(&re_pattern).map_err(|e| {
                ErrorCode::BadArguments(format!("Unable to build regex from LIKE pattern: {}", e))
            })?;
            map.insert(rhs_value, re);
            map.get(rhs_value).unwrap()
        };

        builder.append(op(pattern.is_match(lhs_value)));
    }
    Ok(builder.build_column())
}

/// QUOTE: (From arrow2::arrow::compute::like::a_like_binary_scalar)
#[inline]
pub fn a_like_binary_scalar<F>(lhs: &ColumnRef, rhs: &[u8], op: F) -> Result<BooleanColumn>
where F: Fn(bool) -> bool {
    let viewer = Vu8::try_create_viewer(lhs)?;
    let column = match check_pattern_type(rhs, false) {
        PatternType::OrdinalStr => BooleanColumn::from_iterator(viewer.iter().map(|x| x == rhs)),
        PatternType::EndOfPercent => {
            // fast path, can use starts_with
            let starts_with = &rhs[..rhs.len() - 1];
            BooleanColumn::from_iterator(viewer.iter().map(|x| op(x.starts_with(starts_with))))
        }
        PatternType::StartOfPercent => {
            // fast path, can use ends_with
            let ends_with = &rhs[1..];
            BooleanColumn::from_iterator(viewer.iter().map(|x| op(x.ends_with(ends_with))))
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
                ErrorCode::BadArguments(format!("Unable to build regex from LIKE pattern: {}", e))
            })?;
            BooleanColumn::from_iterator(viewer.iter().map(|x| op(re.is_match(x))))
        }
    };
    Ok(column)
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
