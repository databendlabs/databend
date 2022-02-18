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

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::Expression;
use sqlparser::ast::DateTimeField;
use sqlparser::ast::Value;

pub struct ValueExprAnalyzer;

impl ValueExprAnalyzer {
    pub fn analyze(value: &Value) -> Result<Expression> {
        match value {
            Value::Null => Self::analyze_null_value(),
            Value::Boolean(value) => Self::analyze_bool_value(value),
            Value::Number(value, _) => Self::analyze_number_value(value, None),
            Value::HexStringLiteral(value) => Self::analyze_number_value(value, Some(16)),
            Value::SingleQuotedString(value) => Self::analyze_string_value(value),
            Value::Interval {
                leading_precision: Some(_),
                ..
            }
            | Value::Interval {
                fractional_seconds_precision: Some(_),
                ..
            }
            | Value::Interval {
                last_field: Some(_),
                ..
            } => Self::unsupported_interval(value),
            Value::Interval {
                value,
                leading_field,
                ..
            } => Self::analyze_interval(value, leading_field),
            other => Result::Err(ErrorCode::SyntaxException(format!(
                "Unsupported value expression: {}, type: {:?}",
                value, other
            ))),
        }
    }

    fn analyze_null_value() -> Result<Expression> {
        Ok(Expression::create_literal(DataValue::Null))
    }

    fn analyze_bool_value(value: &bool) -> Result<Expression> {
        Ok(Expression::create_literal(DataValue::Boolean(*value)))
    }

    fn analyze_number_value(value: &str, radix: Option<u32>) -> Result<Expression> {
        let literal = DataValue::try_from_literal(value, radix)?;
        Ok(Expression::create_literal(literal))
    }

    fn analyze_string_value(value: &str) -> Result<Expression> {
        let data_value = DataValue::String(value.to_string().into_bytes());
        Ok(Expression::create_literal(data_value))
    }

    fn unsupported_interval(interval: &Value) -> Result<Expression> {
        //TODO: support parsing literal interval like '1 hour'
        Err(ErrorCode::SyntaxException(format!(
            "Unsupported interval expression: {}.",
            interval
        )))
    }

    fn analyze_interval(value: &str, unit: &Option<DateTimeField>) -> Result<Expression> {
        // We only accept i32 for number in "interval [num] [year|month|day|hour|minute|second]"
        let num = value.parse::<i32>()?;

        //TODO: support default unit for interval
        match unit {
            None => Err(ErrorCode::SyntaxException(
                "Interval must have unit, e.g: '1 HOUR'",
            )),
            Some(DateTimeField::Year) => Self::interval_literal(num, IntervalKind::Year),
            Some(DateTimeField::Month) => Self::interval_literal(num, IntervalKind::Month),
            Some(DateTimeField::Day) => Self::interval_literal(num, IntervalKind::Day),
            Some(DateTimeField::Hour) => Self::interval_literal(num, IntervalKind::Hour),
            Some(DateTimeField::Minute) => Self::interval_literal(num, IntervalKind::Minute),
            Some(DateTimeField::Second) => Self::interval_literal(num, IntervalKind::Second),
        }
    }

    fn interval_literal(num: i32, kind: IntervalKind) -> Result<Expression> {
        Ok(Expression::Literal {
            value: DataValue::Int64(num as i64),
            column_name: Some(num.to_string()),
            data_type: IntervalType::arc(kind),
        })
    }
}
