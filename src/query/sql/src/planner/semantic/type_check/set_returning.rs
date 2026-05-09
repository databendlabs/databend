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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;

use super::TypeChecker;
use super::core_expr::CoreExprArena;
use super::core_expr::CoreExprArgs;
use crate::binder::ExprContext;
use crate::plans::FunctionCall;
use crate::plans::ScalarExpr;

impl<'a, P> TypeChecker<'a, P>
where P: super::TypeCheckPolicy
{
    pub(super) fn resolve_core_set_returning_function(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        func_name: &str,
        args: &CoreExprArgs,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        match self.bind_context.expr_context {
            ExprContext::InSetReturningFunction => {
                return Err(ErrorCode::SemanticError(
                    "set-returning functions cannot be nested".to_string(),
                )
                .set_span(span));
            }
            ExprContext::WhereClause => {
                return Err(ErrorCode::SemanticError(
                    "set-returning functions are not allowed in WHERE clause".to_string(),
                )
                .set_span(span));
            }
            ExprContext::HavingClause => {
                return Err(ErrorCode::SemanticError(
                    "set-returning functions cannot be used in HAVING clause".to_string(),
                )
                .set_span(span));
            }
            ExprContext::QualifyClause => {
                return Err(ErrorCode::SemanticError(
                    "set-returning functions cannot be used in QUALIFY clause".to_string(),
                )
                .set_span(span));
            }
            _ => {}
        }

        if self.in_window_function {
            return Err(ErrorCode::SemanticError(
                "set-returning functions cannot be used in window spec",
            )
            .set_span(span));
        }

        let original_context = self
            .bind_context
            .replace_expr_context(ExprContext::InSetReturningFunction);
        let arguments_result = self.resolve_core_expr_args(arena, args);
        self.bind_context.expr_context = original_context;
        let (arguments, _) = arguments_result?;

        let srf_scalar = ScalarExpr::FunctionCall(FunctionCall {
            span,
            func_name: func_name.to_string(),
            params: vec![],
            arguments,
        });
        let srf_expr = srf_scalar.as_expr()?;
        let srf_tuple_types = srf_expr.data_type().as_tuple().ok_or_else(|| {
            ErrorCode::Internal(format!(
                "The return type of srf should be tuple, but got {}",
                srf_expr.data_type()
            ))
        })?;

        let (return_scalar, return_type) = if srf_tuple_types.len() > 1 {
            (srf_scalar, srf_expr.data_type().clone())
        } else {
            let child_scalar = ScalarExpr::FunctionCall(FunctionCall {
                span,
                func_name: "get".to_string(),
                params: vec![Scalar::Number(NumberScalar::Int64(1))],
                arguments: vec![srf_scalar],
            });
            (child_scalar, srf_tuple_types[0].clone())
        };

        Ok(Box::new((return_scalar, return_type)))
    }
}
