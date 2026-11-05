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

//! Reading arguments that must be constants, such as function parameters.
//! Bind-time folding has already turned most of them into constants, so no
//! subtree is folded here.

use databend_common_ast::Span;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Expr as EExpr;
use databend_common_expression::FoldRules;
use databend_common_expression::RawExpr;
use databend_common_expression::Scalar;
use databend_common_expression::expr;
use databend_common_expression::type_check;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Number;
use databend_common_functions::BUILTIN_FUNCTIONS;

use super::TypeCheckAdapter;
use super::TypeChecker;
use super::scalar_fold::ScalarFacts;
use crate::plans::ScalarExpr;

impl<'a, A> TypeChecker<'a, A>
where A: TypeCheckAdapter
{
    /// Like `ScalarExpr::constant_value`, with facts the caller already has.
    pub(super) fn bound_constant_value(
        &self,
        scalar: &ScalarExpr,
        facts: &ScalarFacts,
    ) -> Option<(Scalar, DataType)> {
        if let ScalarExpr::ConstantExpr(_) | ScalarExpr::TypedConstantExpr(..) = scalar {
            return scalar.constant_value(&self.func_ctx);
        }
        if !facts.column_free {
            return None;
        }
        let expr =
            type_check::check(&Self::placeholder(0, scalar, facts), &BUILTIN_FUNCTIONS).ok()?;
        let value = FoldRules::eval_constant(&self.func_ctx, &BUILTIN_FUNCTIONS, &expr)?;
        if !facts.deterministic {
            self.mark_query_specific_value();
        }
        Some((value, expr.into_data_type()))
    }

    /// Records that type checking stores a value computed from a non-deterministic
    /// expression, such as `now()`, in the logical plan. The value is valid only for this
    /// query, so the plan must not enter the plan cache.
    pub(super) fn mark_query_specific_value(&self) {
        self.metadata.write().set_holds_query_specific_value();
    }

    /// Like `ScalarExpr::constant_value`, and records a value computed from a
    /// non-deterministic expression.
    pub(super) fn scalar_constant_value(&self, scalar: &ScalarExpr) -> Option<(Scalar, DataType)> {
        let value = scalar.constant_value(&self.func_ctx)?;
        if !scalar.is_deterministic() {
            self.mark_query_specific_value();
        }
        Some(value)
    }

    /// Casts a constant as `CAST(value AS to)` would, or `None` if the cast
    /// fails.
    pub(super) fn cast_constant(
        &self,
        span: Span,
        value: Scalar,
        from: DataType,
        to: &DataType,
    ) -> Option<Scalar> {
        if &from == to {
            return Some(value);
        }
        let raw = RawExpr::<usize>::Cast {
            span,
            is_try: false,
            expr: Box::new(RawExpr::Constant {
                span,
                scalar: value,
                data_type: Some(from),
            }),
            dest_type: to.clone(),
        };
        let expr = type_check::check(&raw, &BUILTIN_FUNCTIONS).ok()?;
        FoldRules::eval_constant(&self.func_ctx, &BUILTIN_FUNCTIONS, &expr)
    }

    /// The value of an expression that must be a constant number of type
    /// `T`, converted as an explicit cast would. Errors match
    /// `type_check::check_number`.
    pub(super) fn constant_number<T: Number>(&self, span: Span, scalar: &ScalarExpr) -> Result<T> {
        let target = DataType::Number(T::data_type());
        let origin_type = scalar.data_type().into_owned();
        let value = self
            .scalar_constant_value(scalar)
            .and_then(|(value, data_type)| self.cast_constant(span, value, data_type, &target));
        match value {
            Some(Scalar::Number(number)) => T::try_downcast_scalar(&number).ok_or_else(|| {
                ErrorCode::InvalidArgument(format!(
                    "Expect {}, but got {}",
                    T::data_type(),
                    origin_type
                ))
                .set_span(span)
            }),
            _ => {
                let expr = scalar.as_expr()?;
                let display = if expr.data_type() == &target {
                    expr.sql_display()
                } else {
                    EExpr::Cast(expr::Cast {
                        span,
                        is_try: false,
                        expr: Box::new(expr),
                        dest_type: target,
                    })
                    .sql_display()
                };
                Err(
                    ErrorCode::InvalidArgument(format!("Need constant number, but got {display}"))
                        .set_span(span),
                )
            }
        }
    }
}
