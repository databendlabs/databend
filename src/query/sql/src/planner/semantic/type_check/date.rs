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

use super::TypeChecker;
use crate::binder::wrap_cast;
use crate::plans::ScalarExpr;

impl<'a> TypeChecker<'a> {
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

    pub(super) fn resolve_extract_expr(
        &mut self,
        span: Span,
        interval_kind: &ASTIntervalKind,
        arg: &Expr,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        match interval_kind {
            ASTIntervalKind::ISOYear => self.resolve_function(span, "to_iso_year", vec![], &[arg]),
            ASTIntervalKind::Year => self.resolve_function(span, "to_year", vec![], &[arg]),
            ASTIntervalKind::Quarter => self.resolve_function(span, "to_quarter", vec![], &[arg]),
            ASTIntervalKind::Month => self.resolve_function(span, "to_month", vec![], &[arg]),
            ASTIntervalKind::Day => self.resolve_function(span, "to_day_of_month", vec![], &[arg]),
            ASTIntervalKind::Hour => self.resolve_function(span, "to_hour", vec![], &[arg]),
            ASTIntervalKind::Minute => self.resolve_function(span, "to_minute", vec![], &[arg]),
            ASTIntervalKind::Second => self.resolve_function(span, "to_second", vec![], &[arg]),
            ASTIntervalKind::Doy => self.resolve_function(span, "to_day_of_year", vec![], &[arg]),
            // Day of the week (Sunday = 0, Saturday = 6)
            ASTIntervalKind::Dow => self.resolve_function(span, "dayofweek", vec![], &[arg]),
            ASTIntervalKind::Week => self.resolve_function(span, "to_week_of_year", vec![], &[arg]),
            ASTIntervalKind::Epoch => self.resolve_function(span, "epoch", vec![], &[arg]),
            ASTIntervalKind::MicroSecond => {
                self.resolve_function(span, "to_microsecond", vec![], &[arg])
            }
            // ISO day of the week (Monday = 1, Sunday = 7)
            ASTIntervalKind::ISODow => {
                self.resolve_function(span, "to_day_of_week", vec![], &[arg])
            }
            ASTIntervalKind::YearWeek => self.resolve_function(span, "yearweek", vec![], &[arg]),
            ASTIntervalKind::Millennium => {
                self.resolve_function(span, "millennium", vec![], &[arg])
            }
            _ => Err(ErrorCode::SemanticError(
                "Only support interval type [ISOYear, Year, Quarter, Month, Day, Hour, Minute, Second, Doy, Dow, Week, Epoch, MicroSecond, ISODow, YearWeek, Millennium]".to_string(),
            )
            .set_span(span)),
        }
    }

    pub(super) fn resolve_date_arith(
        &mut self,
        span: Span,
        interval_kind: &ASTIntervalKind,
        date_rhs: &Expr,
        date_lhs: &Expr,
        is_diff: &Expr,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let func_name = match is_diff {
            Expr::DateDiff { .. } => format!("diff_{}s", interval_kind.to_string().to_lowercase()),
            Expr::DateSub { .. } | Expr::DateAdd { .. } => {
                let interval_kind = interval_kind.to_string().to_lowercase();
                if interval_kind == "month" {
                    format!("date_add_{}s", interval_kind.to_string().to_lowercase())
                } else {
                    format!("add_{}s", interval_kind.to_string().to_lowercase())
                }
            }
            Expr::DateBetween { .. } => {
                format!("between_{}s", interval_kind.to_string().to_lowercase())
            }
            _ => {
                return Err(ErrorCode::Internal(
                    "Only support resolve datesub, date_sub, date_diff, date_add",
                ));
            }
        };
        let mut args = vec![];
        let mut arg_types = vec![];

        let (date_lhs, date_lhs_type) = *self.resolve(date_lhs)?;
        args.push(date_lhs);
        arg_types.push(date_lhs_type);

        let (date_rhs, date_rhs_type) = *self.resolve(date_rhs)?;

        args.push(date_rhs);
        arg_types.push(date_rhs_type);

        self.resolve_scalar_function_call(span, &func_name, vec![], args)
    }

    pub(super) fn resolve_date_trunc(
        &mut self,
        span: Span,
        date: &Expr,
        kind: &ASTIntervalKind,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        match kind {
            ASTIntervalKind::Year => {
                self.resolve_function(
                    span,
                    "to_start_of_year", vec![],
                    &[date],
                )
            }
            ASTIntervalKind::ISOYear => {
                self.resolve_function(
                    span,
                    "to_start_of_iso_year", vec![],
                    &[date],
                )
            }
            ASTIntervalKind::Quarter => {
                self.resolve_function(
                    span,
                    "to_start_of_quarter", vec![],
                    &[date],
                )
            }
            ASTIntervalKind::Month => {
                self.resolve_function(
                    span,
                    "to_start_of_month", vec![],
                    &[date],
                )
            }
            ASTIntervalKind::Week => {
                let week_start = self.func_ctx.week_start;
                self.resolve_function(
                    span,
                    "to_start_of_week", vec![],
                    &[date, &Expr::Literal {
                        span: None,
                        value: Literal::UInt64(week_start as u64)
                    }],
                )
            }
            ASTIntervalKind::ISOWeek => {
                self.resolve_function(
                    span,
                    "to_start_of_iso_week", vec![],
                    &[date],
                )
            }
            ASTIntervalKind::Day => {
                self.resolve_function(
                    span,
                    "to_start_of_day", vec![],
                    &[date],
                )
            }
            ASTIntervalKind::Hour => {
                self.resolve_function(
                    span,
                    "to_start_of_hour", vec![],
                    &[date],
                )
            }
            ASTIntervalKind::Minute => {
                self.resolve_function(
                    span,
                    "to_start_of_minute", vec![],
                    &[date],
                )
            }
            ASTIntervalKind::Second => {
                self.resolve_function(
                    span,
                    "to_start_of_second", vec![],
                    &[date],
                )
            }
            _ => Err(ErrorCode::SemanticError("Only these interval types are currently supported: [year, quarter, month, day, hour, minute, second, week]".to_string()).set_span(span)),
        }
    }

    pub(super) fn resolve_time_slice(
        &mut self,
        span: Span,
        date: &Expr,
        slice_length: u64,
        kind: &ASTIntervalKind,
        start_or_end: String,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if slice_length < 1 {
            return Err(ErrorCode::BadArguments(
                "slice_length must be greater than or equal to 1",
            ));
        }
        let slice_length = &Expr::Literal {
            span: None,
            value: Literal::UInt64(slice_length),
        };
        let start_or_end = if start_or_end.eq_ignore_ascii_case("start")
            || start_or_end.eq_ignore_ascii_case("end")
        {
            &Expr::Literal {
                span: None,
                value: Literal::String(start_or_end),
            }
        } else {
            return Err(ErrorCode::BadArguments(
                "time_slice only support start or end",
            ));
        };

        let kind = match kind {
            ASTIntervalKind::Year |
            ASTIntervalKind::Quarter |
            ASTIntervalKind::Month|
            ASTIntervalKind::Week| ASTIntervalKind::ISOWeek | ASTIntervalKind::Day | ASTIntervalKind::Hour | ASTIntervalKind::Minute | ASTIntervalKind::Second => {
                    &Expr::Literal {
                    span: None,
                    value: Literal::String(kind.to_string())
                }
            }
            _ => return Err(ErrorCode::SemanticError("Only these interval types are currently supported: [year, quarter, month, day, hour, minute, second, week]".to_string()).set_span(span)),
        };
        self.resolve_function(span, "time_slice", vec![], &[
            date,
            slice_length,
            start_or_end,
            kind,
        ])
    }

    pub(super) fn resolve_last_day(
        &mut self,
        span: Span,
        date: &Expr,
        kind: &ASTIntervalKind,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        match kind {
            ASTIntervalKind::Year => {
                self.resolve_function(span, "to_last_of_year", vec![], &[date])
            }
            ASTIntervalKind::Quarter => {
                self.resolve_function(span, "to_last_of_quarter", vec![], &[date])
            }
            ASTIntervalKind::Month => {
                self.resolve_function(span, "to_last_of_month", vec![], &[date])
            }
            ASTIntervalKind::Week => {
                self.resolve_function(span, "to_last_of_week", vec![], &[date])
            }
            _ => Err(ErrorCode::SemanticError(
                "Only these interval types are currently supported: [year, quarter, month, week]"
                    .to_string(),
            )
            .set_span(span)),
        }
    }

    pub(super) fn resolve_previous_or_next_day(
        &mut self,
        span: Span,
        date: &Expr,
        weekday: &ASTWeekday,
        is_previous: bool,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let prefix = if is_previous {
            "to_previous_"
        } else {
            "to_next_"
        };

        let func_name = match weekday {
            ASTWeekday::Monday => format!("{}monday", prefix),
            ASTWeekday::Tuesday => format!("{}tuesday", prefix),
            ASTWeekday::Wednesday => format!("{}wednesday", prefix),
            ASTWeekday::Thursday => format!("{}thursday", prefix),
            ASTWeekday::Friday => format!("{}friday", prefix),
            ASTWeekday::Saturday => format!("{}saturday", prefix),
            ASTWeekday::Sunday => format!("{}sunday", prefix),
        };

        self.resolve_function(span, &func_name, vec![], &[date])
    }
}
