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

use common_datavalues::prelude::*;
use common_datavalues::DataValueComparisonOperator;
use common_exception::ErrorCode;
use common_exception::Result;
use regex::bytes::Regex as BytesRegex;
use regex::bytes::RegexBuilder as BytesRegexBuilder;

use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;

#[derive(Clone)]
pub struct ComparisonRegexpFunction {
    op: DataValueComparisonOperator,
}

impl ComparisonRegexpFunction {
    pub fn try_create_regexp(_display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(ComparisonRegexpFunction {
            op: DataValueComparisonOperator::Regexp,
        }))
    }

    pub fn try_create_nregexp(_display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(ComparisonRegexpFunction {
            op: DataValueComparisonOperator::NotRegexp,
        }))
    }

    pub fn desc_regexp() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_regexp)).features(
            FunctionFeatures::default()
                .deterministic()
                .negative_function("not regexp")
                .bool_function()
                .num_arguments(2),
        )
    }

    pub fn desc_unregexp() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_nregexp)).features(
            FunctionFeatures::default()
                .deterministic()
                .negative_function("regexp")
                .bool_function()
                .num_arguments(2),
        )
    }
}

impl Function for ComparisonRegexpFunction {
    fn name(&self) -> &str {
        match self.op {
            DataValueComparisonOperator::Regexp => "regexp",
            DataValueComparisonOperator::NotRegexp => "not regexp",
            _ => unreachable!(),
        }
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
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

    fn eval(&self, columns: &ColumnsWithField, _input_rows: usize) -> Result<ColumnRef> {
        let col1: Result<&ConstColumn> = Series::check_get(columns[1].column());

        if let Ok(col1) = col1 {
            let rhs = col1.get_string(0)?;
            return self.eval_constant(columns[0].column(), &rhs);
        }

        let result = match self.op {
            DataValueComparisonOperator::Regexp => {
                a_regexp_binary(columns[0].column(), columns[1].column(), |x| x)
            }
            DataValueComparisonOperator::NotRegexp => {
                a_regexp_binary(columns[0].column(), columns[1].column(), |x| !x)
            }
            _ => unreachable!(),
        }?;

        Ok(Arc::new(result))
    }
}

impl ComparisonRegexpFunction {
    fn eval_constant(&self, lhs: &ColumnRef, rhs: &[u8]) -> Result<common_datavalues::ColumnRef> {
        let result = match self.op {
            DataValueComparisonOperator::Regexp => a_regexp_binary_scalar(lhs, rhs, |x| x),
            DataValueComparisonOperator::NotRegexp => a_regexp_binary_scalar(lhs, rhs, |x| !x),
            _ => unreachable!(),
        }?;
        Ok(Arc::new(result))
    }
}

impl fmt::Display for ComparisonRegexpFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.op)
    }
}

#[inline]
fn a_regexp_binary<F>(lhs: &ColumnRef, rhs: &ColumnRef, op: F) -> Result<BooleanColumn>
where F: Fn(bool) -> bool {
    let mut map = HashMap::new();

    let mut builder: ColumnBuilder<bool> = ColumnBuilder::with_capacity(lhs.len());

    let lhs = Vu8::try_create_viewer(lhs)?;
    let rhs = Vu8::try_create_viewer(rhs)?;

    for (lhs_value, rhs_value) in lhs.iter().zip(rhs.iter()) {
        let pattern = if let Some(pattern) = map.get(rhs_value) {
            pattern
        } else {
            let re = build_regexp_from_pattern(rhs_value)?;
            map.insert(rhs_value, re);
            map.get(rhs_value).unwrap()
        };

        builder.append(op(pattern.is_match(lhs_value)));
    }
    Ok(builder.build_column())
}

#[inline]
fn a_regexp_binary_scalar<F>(lhs: &ColumnRef, rhs: &[u8], op: F) -> Result<BooleanColumn>
where F: Fn(bool) -> bool {
    let re = build_regexp_from_pattern(rhs)?;
    let viewer = Vu8::try_create_viewer(lhs)?;
    Ok(BooleanColumn::from_iterator(
        viewer.iter().map(|x| op(re.is_match(x))),
    ))
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
