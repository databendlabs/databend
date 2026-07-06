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
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::UnaryOperator;
use databend_common_exception::Result;
use smallvec::smallvec;

use super::CoreExprArena;
use super::CoreExprArgs;
use super::CoreExprId;
use super::TypeChecker;

impl<'a> CoreExprArena<'a> {
    pub(super) fn lower_unary_op_expr(
        &mut self,
        span: Span,
        op: &'a UnaryOperator,
        child: &'a Expr,
    ) -> Result<CoreExprId> {
        if matches!(op, UnaryOperator::Plus) {
            return self.lower_ast_expr(child);
        }

        let func_name = unary_op_core_function(op).expect("unary plus should have returned");
        let child = self.lower_ast_expr(child)?;
        Ok(self.call(span, func_name, smallvec![child]))
    }

    pub(super) fn lower_is_null_expr(
        &mut self,
        span: Span,
        child: &'a Expr,
        not: bool,
    ) -> Result<CoreExprId> {
        let child = self.lower_ast_expr(child)?;
        let is_not_null = self.call(span, "is_not_null", smallvec![child]);
        Ok(if not {
            is_not_null
        } else {
            self.call(span, "not", smallvec![is_not_null])
        })
    }

    pub(super) fn lower_is_distinct_from_expr(
        &mut self,
        span: Span,
        left: &'a Expr,
        right: &'a Expr,
        not: bool,
    ) -> Result<CoreExprId> {
        let left_null = self.lower_is_null_expr(span, left, false)?;
        let right_null = self.lower_is_null_expr(span, right, false)?;
        let both_null = self.call(span, "and", smallvec![left_null, right_null]);

        let left_null = self.lower_is_null_expr(span, left, false)?;
        let right_null = self.lower_is_null_expr(span, right, false)?;
        let either_null = self.call(span, "or", smallvec![left_null, right_null]);

        let op = if not { "eq" } else { "noteq" };
        let left = self.lower_ast_expr(left)?;
        let right = self.lower_ast_expr(right)?;
        let compare = self.call(span, op, smallvec![left, right]);

        let both_null_result = self.literal(span, Literal::Boolean(not));
        let either_null_result = self.literal(span, Literal::Boolean(!not));
        let result = self.call(span, "if", smallvec![
            both_null,
            both_null_result,
            either_null,
            either_null_result,
            compare,
        ]);
        Ok(self.call(span, "assume_not_null", smallvec![result]))
    }

    pub(super) fn lower_between_expr(
        &mut self,
        span: Span,
        expr: &'a Expr,
        low: &'a Expr,
        high: &'a Expr,
        not: bool,
    ) -> Result<CoreExprId> {
        let expr_low = self.lower_ast_expr(expr)?;
        let low = self.lower_ast_expr(low)?;
        let low_compare = self.call(span, if not { "lt" } else { "gte" }, smallvec![
            expr_low, low
        ]);

        let expr_high = self.lower_ast_expr(expr)?;
        let high = self.lower_ast_expr(high)?;
        let high_compare = self.call(span, if not { "gt" } else { "lte" }, smallvec![
            expr_high, high
        ]);

        Ok(self.call(span, if not { "or" } else { "and" }, smallvec![
            low_compare,
            high_compare
        ]))
    }

    pub(super) fn lower_case_expr(
        &mut self,
        span: Span,
        operand: Option<&'a Expr>,
        conditions: &'a [Expr],
        results: &'a [Expr],
        else_result: Option<&'a Expr>,
    ) -> Result<CoreExprId> {
        let mut args = CoreExprArgs::with_capacity(conditions.len() * 2 + 1);
        for (condition, result) in conditions.iter().zip(results.iter()) {
            let condition = if let Some(operand) = operand {
                let operand = self.lower_ast_expr(operand)?;
                let condition = self.lower_ast_expr(condition)?;
                self.call(span, "eq", smallvec![operand, condition])
            } else {
                self.lower_ast_expr(condition)?
            };
            args.push(condition);
            args.push(self.lower_ast_expr(result)?);
        }
        args.push(match else_result {
            Some(else_result) => self.lower_ast_expr(else_result)?,
            None => self.literal(None, Literal::Null),
        });
        Ok(self.call(span, "if", args))
    }
}

pub(super) fn can_lower_binary_op(op: &BinaryOperator, right: &Expr) -> bool {
    if matches!(right, Expr::Subquery {
        modifier: Some(_),
        ..
    }) {
        return false;
    }

    if matches!(
        op,
        BinaryOperator::Like(_)
            | BinaryOperator::NotLike(_)
            | BinaryOperator::LikeAny(_)
            | BinaryOperator::ILike(_)
            | BinaryOperator::NotILike(_)
            | BinaryOperator::ILikeAny(_)
            | BinaryOperator::Regexp
            | BinaryOperator::PgRegexpMatch
            | BinaryOperator::RLike
            | BinaryOperator::NotRegexp
            | BinaryOperator::NotRLike
            | BinaryOperator::SoundsLike
    ) {
        return false;
    }

    binary_op_core_function(op)
        .map(TypeChecker::<super::FullTypeCheckAdapter>::can_lower_core_scalar_function)
        .unwrap_or(false)
}

pub(super) fn binary_op_core_function(op: &BinaryOperator) -> Option<&'static str> {
    let func_name = match op {
        BinaryOperator::Plus => "plus",
        BinaryOperator::Minus => "minus",
        BinaryOperator::Multiply => "multiply",
        BinaryOperator::Div => "div",
        BinaryOperator::Divide => "divide",
        BinaryOperator::IntDiv => "intdiv",
        BinaryOperator::Modulo => "modulo",
        BinaryOperator::StringConcat => "concat",
        BinaryOperator::Gt => "gt",
        BinaryOperator::Lt => "lt",
        BinaryOperator::Gte => "gte",
        BinaryOperator::Lte => "lte",
        BinaryOperator::Eq => "eq",
        BinaryOperator::NotEq => "noteq",
        BinaryOperator::Caret => "pow",
        BinaryOperator::And => "and",
        BinaryOperator::Or => "or",
        BinaryOperator::Xor => "xor",
        BinaryOperator::LikeAny(_) => "like_any",
        BinaryOperator::Like(_) => "like",
        BinaryOperator::ILike(_) => "ilike",
        BinaryOperator::ILikeAny(_) => "ilike_any",
        BinaryOperator::Regexp => "regexp",
        BinaryOperator::PgRegexpMatch => "regexp",
        BinaryOperator::RLike => "rlike",
        BinaryOperator::BitwiseOr => "bit_or",
        BinaryOperator::BitwiseAnd => "bit_and",
        BinaryOperator::BitwiseXor => "bit_xor",
        BinaryOperator::BitwiseShiftLeft => "bit_shift_left",
        BinaryOperator::BitwiseShiftRight => "bit_shift_right",
        BinaryOperator::CosineDistance => "cosine_distance",
        BinaryOperator::L1Distance => "l1_distance",
        BinaryOperator::L2Distance => "l2_distance",
        BinaryOperator::NotLike(_)
        | BinaryOperator::NotILike(_)
        | BinaryOperator::NotRegexp
        | BinaryOperator::NotRLike
        | BinaryOperator::SoundsLike => return None,
    };
    Some(func_name)
}

pub(super) fn like_op_core_function(op: &BinaryOperator) -> Option<&'static str> {
    match op {
        BinaryOperator::Like(_) => Some("like"),
        BinaryOperator::NotLike(_) => Some("notlike"),
        BinaryOperator::LikeAny(_) => Some("like_any"),
        BinaryOperator::ILike(_) => Some("ilike"),
        BinaryOperator::NotILike(_) => Some("notilike"),
        BinaryOperator::ILikeAny(_) => Some("ilike_any"),
        BinaryOperator::Regexp => Some("regexp"),
        BinaryOperator::PgRegexpMatch => Some("regexp"),
        BinaryOperator::RLike => Some("rlike"),
        BinaryOperator::NotRegexp => Some("notregexp"),
        BinaryOperator::NotRLike => Some("notrlike"),
        _ => None,
    }
}

pub(super) fn unary_op_core_function(op: &UnaryOperator) -> Option<&'static str> {
    let func_name = match op {
        UnaryOperator::Plus => return None,
        UnaryOperator::Minus => "minus",
        UnaryOperator::Not => "not",
        UnaryOperator::Factorial => "factorial",
        UnaryOperator::SquareRoot => "sqrt",
        UnaryOperator::CubeRoot => "cbrt",
        UnaryOperator::Abs => "abs",
        UnaryOperator::BitwiseNot => "bit_not",
    };
    Some(func_name)
}
