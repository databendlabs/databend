// Copyright 2021 Datafuse Labs
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

use databend_common_ast::Span;
use databend_common_ast::ast::BinaryOperator;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::IntervalKind as ASTIntervalKind;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::Weekday as ASTWeekday;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Constant;
use databend_common_expression::ConstantFolder;
use databend_common_expression::Expr as EExpr;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use smallvec::smallvec;

use super::TypeChecker;
use super::core_expr::CoreExprArena;
use crate::binder::wrap_cast;
use crate::planner::semantic::type_check::core_expr::CoreExprId;
use crate::plans::ScalarExpr;

pub(super) enum DateArithmeticFunction {
    Add,
    Diff,
    Between,
}

pub(super) enum AdjacentDayFunction {
    Previous,
    Next,
}

impl<'a, P> TypeChecker<'a, P> {
    pub(super) fn adjust_date_interval_operands(
        &self,
        op: &BinaryOperator,
        left_expr: &mut ScalarExpr,
        left_type: &DataType,
        right_expr: &mut ScalarExpr,
        right_type: &DataType,
    ) -> Result<()> {
        match op {
            BinaryOperator::Plus => {
                self.adjust_single_date_interval_operand(
                    left_expr, left_type, right_expr, right_type,
                )?;
                self.adjust_single_date_interval_operand(
                    right_expr, right_type, left_expr, left_type,
                )?;
            }
            BinaryOperator::Minus => {
                self.adjust_single_date_interval_operand(
                    left_expr, left_type, right_expr, right_type,
                )?;
            }
            _ => {}
        }
        Ok(())
    }

    pub(super) fn adjust_date_interval_function_args(
        &self,
        func_name: &str,
        args: &mut [ScalarExpr],
    ) -> Result<()> {
        if args.len() != 2 {
            return Ok(());
        }
        let op = if func_name.eq_ignore_ascii_case("plus") {
            BinaryOperator::Plus
        } else if func_name.eq_ignore_ascii_case("minus") {
            BinaryOperator::Minus
        } else {
            return Ok(());
        };
        let (left_slice, right_slice) = args.split_at_mut(1);
        let left_expr = &mut left_slice[0];
        let right_expr = &mut right_slice[0];
        let left_type = left_expr.data_type()?;
        let right_type = right_expr.data_type()?;
        self.adjust_date_interval_operands(&op, left_expr, &left_type, right_expr, &right_type)
    }

    fn adjust_single_date_interval_operand(
        &self,
        date_expr: &mut ScalarExpr,
        date_type: &DataType,
        interval_expr: &ScalarExpr,
        interval_type: &DataType,
    ) -> Result<()> {
        if date_type.remove_nullable() != DataType::Date
            || interval_type.remove_nullable() != DataType::Interval
        {
            return Ok(());
        }

        if self.interval_contains_only_date_parts(interval_expr)? {
            return Ok(());
        }

        // Preserve nullability when casting DATE to TIMESTAMP
        let target_type = if date_type.is_nullable_or_null() {
            DataType::Timestamp.wrap_nullable()
        } else {
            DataType::Timestamp
        };
        *date_expr = wrap_cast(date_expr, &target_type);
        Ok(())
    }

    fn interval_contains_only_date_parts(&self, interval_expr: &ScalarExpr) -> Result<bool> {
        let expr = interval_expr.as_expr()?;
        let (folded, _) = ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
        if let EExpr::Constant(Constant {
            scalar: Scalar::Interval(value),
            ..
        }) = folded
        {
            return Ok(value.microseconds() == 0);
        }
        Ok(false)
    }
}

impl<'a> CoreExprArena<'a> {
    pub(super) fn lower_extract_expr(
        &mut self,
        span: Span,
        interval_kind: &ASTIntervalKind,
        arg: &'a Expr,
    ) -> Result<CoreExprId> {
        let func_name = match interval_kind {
            ASTIntervalKind::ISOYear => "to_iso_year",
            ASTIntervalKind::Year => "to_year",
            ASTIntervalKind::Quarter => "to_quarter",
            ASTIntervalKind::Month => "to_month",
            ASTIntervalKind::Day => "to_day_of_month",
            ASTIntervalKind::Hour => "to_hour",
            ASTIntervalKind::Minute => "to_minute",
            ASTIntervalKind::Second => "to_second",
            ASTIntervalKind::Doy => "to_day_of_year",
            // Day of the week (Sunday = 0, Saturday = 6)
            ASTIntervalKind::Dow => "dayofweek",
            ASTIntervalKind::Week => "to_week_of_year",
            ASTIntervalKind::Epoch => "epoch",
            ASTIntervalKind::MicroSecond => "to_microsecond",
            // ISO day of the week (Monday = 1, Sunday = 7)
            ASTIntervalKind::ISODow => "to_day_of_week",
            ASTIntervalKind::YearWeek => "yearweek",
            ASTIntervalKind::Millennium => "millennium",
            _ => Err(ErrorCode::SemanticError(
                "Only support interval type [ISOYear, Year, Quarter, Month, Day, Hour, Minute, Second, Doy, Dow, Week, Epoch, MicroSecond, ISODow, YearWeek, Millennium]".to_string(),
            )
            .set_span(span))?,
        };
        let arg = self.lower_ast_expr(arg)?;
        Ok(self.call(span, func_name, smallvec![arg]))
    }

    pub(super) fn lower_date_arith_expr(
        &mut self,
        span: Span,
        interval_kind: &ASTIntervalKind,
        date_rhs: &'a Expr,
        date_lhs: &'a Expr,
        function: DateArithmeticFunction,
    ) -> Result<CoreExprId> {
        let func_name = date_arith_function(span, interval_kind, function)?;
        let date_lhs = self.lower_ast_expr(date_lhs)?;
        let date_rhs = self.lower_ast_expr(date_rhs)?;
        Ok(self.call(span, func_name, smallvec![date_lhs, date_rhs]))
    }

    pub(super) fn lower_date_sub_expr(
        &mut self,
        span: Span,
        interval_kind: &ASTIntervalKind,
        interval: &'a Expr,
        date: &'a Expr,
    ) -> Result<CoreExprId> {
        let func_name = date_sub_function(span, interval_kind)?;
        let date = self.lower_ast_expr(date)?;
        let interval = self.lower_ast_expr(interval)?;
        let interval = self.call(span, "minus", smallvec![interval]);
        Ok(self.call(span, func_name, smallvec![date, interval]))
    }

    pub(super) fn lower_date_trunc_expr(
        &mut self,
        span: Span,
        kind: &ASTIntervalKind,
        date: &'a Expr,
        week_start: u64,
    ) -> Result<CoreExprId> {
        let func_name = match kind {
            ASTIntervalKind::Year => "to_start_of_year",
            ASTIntervalKind::ISOYear => "to_start_of_iso_year",
            ASTIntervalKind::Quarter => "to_start_of_quarter",
            ASTIntervalKind::Month => "to_start_of_month",
            ASTIntervalKind::Week => {
                let date = self.lower_ast_expr(date)?;
                let week_start = self.literal(None, Literal::UInt64(week_start));
                return Ok(self.call(span, "to_start_of_week", smallvec![date, week_start]));
            }
            ASTIntervalKind::ISOWeek => "to_start_of_iso_week",
            ASTIntervalKind::Day => "to_start_of_day",
            ASTIntervalKind::Hour => "to_start_of_hour",
            ASTIntervalKind::Minute => "to_start_of_minute",
            ASTIntervalKind::Second => "to_start_of_second",
            _ => {
                return Err(ErrorCode::SemanticError("Only these interval types are currently supported: [year, quarter, month, day, hour, minute, second, week]".to_string()).set_span(span));
            }
        };
        let date = self.lower_ast_expr(date)?;
        Ok(self.call(span, func_name, smallvec![date]))
    }

    pub(super) fn lower_time_slice_expr(
        &mut self,
        span: Span,
        date: &'a Expr,
        slice_length: u64,
        kind: &ASTIntervalKind,
        start_or_end: String,
    ) -> Result<CoreExprId> {
        if slice_length < 1 {
            return Err(ErrorCode::BadArguments(
                "slice_length must be greater than or equal to 1",
            ));
        }
        let start_or_end = if start_or_end.eq_ignore_ascii_case("start")
            || start_or_end.eq_ignore_ascii_case("end")
        {
            start_or_end
        } else {
            return Err(ErrorCode::BadArguments(
                "time_slice only support start or end",
            ));
        };
        let kind = match kind {
            ASTIntervalKind::Year
            | ASTIntervalKind::Quarter
            | ASTIntervalKind::Month
            | ASTIntervalKind::Week
            | ASTIntervalKind::ISOWeek
            | ASTIntervalKind::Day
            | ASTIntervalKind::Hour
            | ASTIntervalKind::Minute
            | ASTIntervalKind::Second => kind.to_string(),
            _ => return Err(ErrorCode::SemanticError("Only these interval types are currently supported: [year, quarter, month, day, hour, minute, second, week]".to_string()).set_span(span)),
        };
        let date = self.lower_ast_expr(date)?;
        let slice_length = self.literal(None, Literal::UInt64(slice_length));
        let start_or_end = self.literal(None, Literal::String(start_or_end));
        let kind = self.literal(None, Literal::String(kind));
        Ok(self.call(span, "time_slice", smallvec![
            date,
            slice_length,
            start_or_end,
            kind
        ]))
    }

    pub(super) fn lower_last_day_expr(
        &mut self,
        span: Span,
        date: &'a Expr,
        kind: &ASTIntervalKind,
    ) -> Result<CoreExprId> {
        let func_name = match kind {
            ASTIntervalKind::Year => "to_last_of_year",
            ASTIntervalKind::Quarter => "to_last_of_quarter",
            ASTIntervalKind::Month => "to_last_of_month",
            ASTIntervalKind::Week => "to_last_of_week",
            _ => {
                return Err(ErrorCode::SemanticError(
                    "Only these interval types are currently supported: [year, quarter, month, week]"
                        .to_string(),
                )
                .set_span(span));
            }
        };
        let date = self.lower_ast_expr(date)?;
        Ok(self.call(span, func_name, smallvec![date]))
    }

    pub(super) fn lower_previous_or_next_day_expr(
        &mut self,
        span: Span,
        date: &'a Expr,
        weekday: &ASTWeekday,
        function: AdjacentDayFunction,
    ) -> Result<CoreExprId> {
        let func_name = adjacent_day_function(weekday, function);
        let date = self.lower_ast_expr(date)?;
        Ok(self.call(span, func_name, smallvec![date]))
    }
}

fn date_arith_function(
    span: Span,
    interval_kind: &ASTIntervalKind,
    function: DateArithmeticFunction,
) -> Result<&'static str> {
    match function {
        DateArithmeticFunction::Diff => date_diff_function(span, interval_kind),
        DateArithmeticFunction::Add => date_sub_function(span, interval_kind),
        DateArithmeticFunction::Between => date_between_function(span, interval_kind),
    }
}

fn date_diff_function(span: Span, interval_kind: &ASTIntervalKind) -> Result<&'static str> {
    Ok(match interval_kind {
        ASTIntervalKind::ISOYear => "diff_isoyears",
        ASTIntervalKind::Year => "diff_years",
        ASTIntervalKind::Quarter => "diff_quarters",
        ASTIntervalKind::Month => "diff_months",
        ASTIntervalKind::Day => "diff_days",
        ASTIntervalKind::Hour => "diff_hours",
        ASTIntervalKind::Minute => "diff_minutes",
        ASTIntervalKind::Second => "diff_seconds",
        ASTIntervalKind::Doy => "diff_doys",
        ASTIntervalKind::Week => "diff_weeks",
        ASTIntervalKind::Dow => "diff_dows",
        ASTIntervalKind::Epoch => "diff_epochs",
        ASTIntervalKind::MicroSecond => "diff_microseconds",
        ASTIntervalKind::ISODow => "diff_isodows",
        ASTIntervalKind::YearWeek => "diff_yearweeks",
        ASTIntervalKind::Millennium => "diff_millenniums",
        ASTIntervalKind::ISOWeek | ASTIntervalKind::UnknownIntervalKind => {
            return unsupported_date_interval(span, "date_diff", interval_kind);
        }
    })
}

fn date_between_function(span: Span, interval_kind: &ASTIntervalKind) -> Result<&'static str> {
    Ok(match interval_kind {
        ASTIntervalKind::ISOYear => "between_isoyears",
        ASTIntervalKind::Year => "between_years",
        ASTIntervalKind::Quarter => "between_quarters",
        ASTIntervalKind::Month => "between_months",
        ASTIntervalKind::Day => "between_days",
        ASTIntervalKind::Hour => "between_hours",
        ASTIntervalKind::Minute => "between_minutes",
        ASTIntervalKind::Second => "between_seconds",
        ASTIntervalKind::Doy => "between_doys",
        ASTIntervalKind::Week => "between_weeks",
        ASTIntervalKind::Dow => "between_dows",
        ASTIntervalKind::Epoch => "between_epochs",
        ASTIntervalKind::MicroSecond => "between_microseconds",
        ASTIntervalKind::ISODow => "between_isodows",
        ASTIntervalKind::YearWeek => "between_yearweeks",
        ASTIntervalKind::Millennium => "between_millenniums",
        ASTIntervalKind::ISOWeek | ASTIntervalKind::UnknownIntervalKind => {
            return unsupported_date_interval(span, "date_between", interval_kind);
        }
    })
}

fn date_sub_function(span: Span, interval_kind: &ASTIntervalKind) -> Result<&'static str> {
    Ok(match interval_kind {
        ASTIntervalKind::Year => "add_years",
        ASTIntervalKind::Quarter => "add_quarters",
        ASTIntervalKind::Month => "date_add_months",
        ASTIntervalKind::Day => "add_days",
        ASTIntervalKind::Hour => "add_hours",
        ASTIntervalKind::Minute => "add_minutes",
        ASTIntervalKind::Second => "add_seconds",
        ASTIntervalKind::Week => "add_weeks",
        ASTIntervalKind::ISOYear
        | ASTIntervalKind::Doy
        | ASTIntervalKind::ISOWeek
        | ASTIntervalKind::Dow
        | ASTIntervalKind::Epoch
        | ASTIntervalKind::MicroSecond
        | ASTIntervalKind::ISODow
        | ASTIntervalKind::YearWeek
        | ASTIntervalKind::Millennium
        | ASTIntervalKind::UnknownIntervalKind => {
            return unsupported_date_interval(span, "date_add/date_sub", interval_kind);
        }
    })
}

fn unsupported_date_interval(
    span: Span,
    function_name: &str,
    interval_kind: &ASTIntervalKind,
) -> Result<&'static str> {
    Err(ErrorCode::SemanticError(format!(
        "Unsupported interval type {} for {}",
        interval_kind, function_name
    ))
    .set_span(span))
}

fn adjacent_day_function(weekday: &ASTWeekday, function: AdjacentDayFunction) -> &'static str {
    match function {
        AdjacentDayFunction::Previous => match weekday {
            ASTWeekday::Monday => "to_previous_monday",
            ASTWeekday::Tuesday => "to_previous_tuesday",
            ASTWeekday::Wednesday => "to_previous_wednesday",
            ASTWeekday::Thursday => "to_previous_thursday",
            ASTWeekday::Friday => "to_previous_friday",
            ASTWeekday::Saturday => "to_previous_saturday",
            ASTWeekday::Sunday => "to_previous_sunday",
        },
        AdjacentDayFunction::Next => match weekday {
            ASTWeekday::Monday => "to_next_monday",
            ASTWeekday::Tuesday => "to_next_tuesday",
            ASTWeekday::Wednesday => "to_next_wednesday",
            ASTWeekday::Thursday => "to_next_thursday",
            ASTWeekday::Friday => "to_next_friday",
            ASTWeekday::Saturday => "to_next_saturday",
            ASTWeekday::Sunday => "to_next_sunday",
        },
    }
}
