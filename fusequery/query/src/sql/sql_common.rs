// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::str::FromStr;

use common_arrow::arrow::datatypes::TimeUnit;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::ErrorCodes;
use common_exception::Result;
use common_planners::ExpressionAction;
use sqlparser::ast::DataType as SQLDataType;
use sqlparser::ast::DateTimeField;

pub struct SQLCommon;

impl SQLCommon {
    /// Maps the SQL type to the corresponding Arrow `DataType`
    pub fn make_data_type(sql_type: &SQLDataType) -> Result<DataType> {
        match sql_type {
            SQLDataType::BigInt => Ok(DataType::Int64),
            SQLDataType::Int => Ok(DataType::Int32),
            SQLDataType::SmallInt => Ok(DataType::Int16),
            SQLDataType::Char(_) => Ok(DataType::Utf8),
            SQLDataType::Varchar(_) => Ok(DataType::Utf8),
            SQLDataType::Text => Ok(DataType::Utf8),
            SQLDataType::Decimal(_, _) => Ok(DataType::Float64),
            SQLDataType::Float(_) => Ok(DataType::Float32),
            SQLDataType::Real | SQLDataType::Double => Ok(DataType::Float64),
            SQLDataType::Boolean => Ok(DataType::Boolean),
            SQLDataType::Date => Ok(DataType::Date32),
            SQLDataType::Time => Ok(DataType::Time64(TimeUnit::Millisecond)),
            SQLDataType::Timestamp => Ok(DataType::Date64),

            _ => Result::Err(ErrorCodes::IllegalDataType(format!(
                "The SQL data type {:?} is not implemented",
                sql_type
            )))
        }
    }

    /// INTERVAL '3' MONTH
    /// type: Value(Interval { value: "3", leading_field: Some(Month), leading_precision: None, last_field: None, fractional_seconds_precision: None })
    pub fn make_sql_interval_to_literal(
        value: &str,
        leading_field: &Option<DateTimeField>,
        leading_precision: &Option<u64>,
        last_field: &Option<DateTimeField>,
        fractional_seconds_precision: &Option<u64>
    ) -> Result<ExpressionAction> {
        if leading_field.is_some() {
            return Result::Err(ErrorCodes::SyntaxException(format!(
                "Unsupported Interval Expression with leading_field {:?}",
                leading_field
            )));
        }

        if leading_precision.is_some() {
            return Result::Err(ErrorCodes::SyntaxException(format!(
                "Unsupported Interval Expression with leading_precision {:?}",
                leading_precision
            )));
        }

        if last_field.is_some() {
            return Result::Err(ErrorCodes::SyntaxException(format!(
                "Unsupported Interval Expression with last_field {:?}",
                last_field
            )));
        }

        if fractional_seconds_precision.is_some() {
            return Result::Err(ErrorCodes::SyntaxException(format!(
                "Unsupported Interval Expression with fractional_seconds_precision {:?}",
                fractional_seconds_precision
            )));
        }

        const SECONDS_PER_HOUR: f32 = 3_600_f32;
        const MILLIS_PER_SECOND: f32 = 1_000_f32;

        // We are storing parts as integers, it's why we need to align parts fractional
        // INTERVAL '0.5 MONTH' = 15 days, INTERVAL '1.5 MONTH' = 1 month 15 days
        // INTERVAL '0.5 DAY' = 12 hours, INTERVAL '1.5 DAY' = 1 day 12 hours
        let align_interval_parts =
            |month_part: f32, mut day_part: f32, mut milles_part: f32| -> (i32, i32, f32) {
                // Convert fractional month to days, It's not supported by Arrow types, but anyway
                day_part += (month_part - (month_part as i32) as f32) * 30_f32;

                // Convert fractional days to hours
                milles_part += (day_part - ((day_part as i32) as f32))
                    * 24_f32
                    * SECONDS_PER_HOUR
                    * MILLIS_PER_SECOND;

                (month_part as i32, day_part as i32, milles_part)
            };

        let calculate_from_part = |interval_period_str: &str,
                                   interval_type: &str|
         -> Result<(i32, i32, f32)> {
            // @todo It's better to use Decimal in order to protect rounding errors
            // Wait https://github.com/apache/arrow/pull/9232
            let interval_period = match f32::from_str(interval_period_str) {
                Ok(n) => n,
                Err(_) => {
                    return Result::Err(ErrorCodes::SyntaxException(format!(
                        "Unsupported Interval Expression with value {:?}",
                        value
                    )))
                }
            };

            if interval_period > (i32::MAX as f32) {
                return Result::Err(ErrorCodes::SyntaxException(format!(
                    "Interval field value out of range: {:?}",
                    value
                )));
            }

            match interval_type.to_lowercase().as_str() {
                "year" => Ok(align_interval_parts(interval_period * 12_f32, 0.0, 0.0)),
                "month" => Ok(align_interval_parts(interval_period, 0.0, 0.0)),
                "day" | "days" => Ok(align_interval_parts(0.0, interval_period, 0.0)),
                "hour" | "hours" => {
                    Ok((0, 0, interval_period * SECONDS_PER_HOUR * MILLIS_PER_SECOND))
                }
                "minutes" | "minute" => Ok((0, 0, interval_period * 60_f32 * MILLIS_PER_SECOND)),
                "seconds" | "second" => Ok((0, 0, interval_period * MILLIS_PER_SECOND)),
                "milliseconds" | "millisecond" => Ok((0, 0, interval_period)),
                _ => {
                    return Result::Err(ErrorCodes::SyntaxException(format!(
                        "Invalid input syntax for type interval: {:?}",
                        value
                    )))
                }
            }
        };

        let mut result_month: i64 = 0;
        let mut result_days: i64 = 0;
        let mut result_millis: i64 = 0;

        let mut parts = value.split_whitespace();

        loop {
            let interval_period_str = parts.next();
            if interval_period_str.is_none() {
                break;
            }

            let (diff_month, diff_days, diff_millis) = calculate_from_part(
                interval_period_str.unwrap(),
                parts.next().unwrap_or("second")
            )?;

            result_month += diff_month as i64;

            if result_month > (i32::MAX as i64) {
                return Result::Err(ErrorCodes::SyntaxException(format!(
                    "Interval field value out of range: {:?}",
                    value
                )));
            }

            result_days += diff_days as i64;

            if result_days > (i32::MAX as i64) {
                return Result::Err(ErrorCodes::SyntaxException(format!(
                    "Interval field value out of range: {:?}",
                    value
                )));
            }

            result_millis += diff_millis as i64;

            if result_millis > (i32::MAX as i64) {
                return Result::Err(ErrorCodes::SyntaxException(format!(
                    "Interval field value out of range: {:?}",
                    value
                )));
            }
        }

        // Interval is tricky thing
        // 1 day is not 24 hours because timezones, 1 year != 365/364! 30 days != 1 month
        // The true way to store and calculate intervals is to store it as it defined
        // Due the fact that Arrow supports only two types YearMonth (month) and DayTime (day, time)
        // It's not possible to store complex intervals
        // It's possible to do select (NOW() + INTERVAL '1 year') + INTERVAL '1 day'; as workaround
        if result_month != 0 && (result_days != 0 || result_millis != 0) {
            return Result::Err(ErrorCodes::SyntaxException(
                format!(
                    "DF does not support intervals that have both a Year/Month part as well as Days/Hours/Mins/Seconds: {:?}. Hint: try breaking the interval into two parts, one with Year/Month and the other with Days/Hours/Mins/Seconds - e.g. (NOW() + INTERVAL '1 year') + INTERVAL '1 day'",
                    value
                )
            ));
        }

        if result_month != 0 {
            return Ok(ExpressionAction::Literal(DataValue::IntervalYearMonth(
                Some(result_month as i32)
            )));
        }

        let result: i64 = (result_days << 32) | result_millis;
        Ok(ExpressionAction::Literal(DataValue::IntervalDayTime(Some(
            result
        ))))
    }
}
