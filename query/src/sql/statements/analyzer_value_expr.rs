use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_datavalues::IntervalUnit;
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
            Value::Number(value, _) => Self::analyze_number_value(value),
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
        Ok(Expression::create_literal(DataValue::Boolean(Some(*value))))
    }

    fn analyze_number_value(value: &str) -> Result<Expression> {
        let literal = DataValue::try_from_literal(value)?;
        Ok(Expression::create_literal(literal))
    }

    fn analyze_string_value(value: &str) -> Result<Expression> {
        let data_value = DataValue::String(Some(value.to_string().into_bytes()));
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
            Some(DateTimeField::Year) => Self::year_month_interval(num * 12),
            Some(DateTimeField::Month) => Self::year_month_interval(num * 12),
            Some(DateTimeField::Day) => Self::day_time_interval(num, 0),
            Some(DateTimeField::Hour) => Self::day_time_interval(0, num * 3600 * 1000),
            Some(DateTimeField::Minute) => Self::day_time_interval(0, num * 60 * 1000),
            Some(DateTimeField::Second) => Self::day_time_interval(0, num * 1000),
        }
    }

    fn year_month_interval(months: i32) -> Result<Expression> {
        Ok(Expression::Literal {
            value: DataValue::Int64(Some(months as i64)),
            column_name: Some(months.to_string()),
            data_type: DataType::Interval(IntervalUnit::YearMonth),
        })
    }

    fn day_time_interval(days: i32, ms: i32) -> Result<Expression> {
        static MILLISECONDS_PER_DAY: i64 = 24 * 3600 * 1000;
        let total_ms = days as i64 * MILLISECONDS_PER_DAY + ms as i64;

        Ok(Expression::Literal {
            value: DataValue::Int64(Some(total_ms)),
            column_name: Some(total_ms.to_string()),
            data_type: DataType::Interval(IntervalUnit::DayTime),
        })
    }
}
