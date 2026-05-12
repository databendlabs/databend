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
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FunctionCall as ASTFunctionCall;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_functions::GENERAL_LAMBDA_FUNCTIONS;
use databend_common_functions::GENERAL_WINDOW_FUNCTIONS;
use databend_common_functions::GENERAL_WITHIN_GROUP_FUNCTIONS;
use databend_common_functions::is_builtin_function;
use unicase::Ascii;

use super::CoreExprArena;
use super::CoreExprId;
use super::TypeChecker;
use super::lambda::general_lambda_function_name;
use super::rewrite_function::rewrite_function_name;
use super::scalar_function::builtin_scalar_function_name;
use super::window::general_window_function_name;
use crate::planner::semantic::type_check::CoreExpr;

impl<'a> CoreExprArena<'a> {
    pub(super) fn lower_function_call_expr(
        &mut self,
        original_expr: &'a Expr,
        span: Span,
        func: &'a ASTFunctionCall,
    ) -> Result<CoreExprId> {
        let ASTFunctionCall {
            distinct,
            name,
            args,
            params,
            order_by,
            window,
            lambda,
        } = func;
        let func_name = name.name.to_ascii_lowercase();
        if self.in_lambda_function && window.is_some() {
            return Err(ErrorCode::SemanticError(
                "window functions can not be used in lambda function".to_string(),
            )
            .set_span(span));
        }
        if !is_builtin_function(&func_name)
            && !TypeChecker::<()>::all_special_functions().contains(&Ascii::new(func_name.as_str()))
            && rewrite_function_name(&func_name).is_none()
        {
            return self.runtime_call(span, name, args);
        }

        if lambda.is_none()
            && let Some(func_name) = general_window_function_name(&func_name)
        {
            return self.lower_general_window_function_call(
                format!("{original_expr:#}"),
                span,
                func_name,
                func,
            );
        }

        if lambda.is_none() && self.aggregate_function_factory.contains(&func_name) {
            return self.lower_aggregate_function_call(
                format!("{original_expr:#}"),
                span,
                func_name,
                func,
            );
        }

        let uni_case_func_name = Ascii::new(func_name.as_str());
        if !order_by.is_empty() && !GENERAL_WITHIN_GROUP_FUNCTIONS.contains(&uni_case_func_name) {
            return Err(ErrorCode::SemanticError(
                "only aggregate functions allowed in within group syntax",
            )
            .set_span(span));
        }
        if window.is_some()
            && !self.aggregate_function_factory.contains(&func_name)
            && !GENERAL_WINDOW_FUNCTIONS.contains(&uni_case_func_name)
        {
            return Err(ErrorCode::SemanticError(
                "only window and aggregate functions allowed in window syntax",
            )
            .set_span(span));
        }
        if lambda.is_some() && !GENERAL_LAMBDA_FUNCTIONS.contains(&uni_case_func_name) {
            return Err(
                ErrorCode::SemanticError("only lambda functions allowed in lambda syntax")
                    .set_span(span),
            );
        }

        if let Some(func_name) = general_lambda_function_name(&func_name) {
            return match lambda.as_ref() {
                Some(lambda) => self.lambda_function(span, func_name, args, lambda),
                None => Err(ErrorCode::SemanticError(format!(
                    "function {func_name} must have a lambda expression",
                ))
                .set_span(span)),
            };
        }

        if let Some(expr) = self.search_function(span, &func_name, args)? {
            return Ok(expr);
        }

        if let Some(expr) = self.async_function(span, &func_name, args)? {
            return Ok(expr);
        }

        if let Some(expr) = self.set_returning_function(span, &func_name, args)? {
            return Ok(expr);
        }

        if !*distinct
            && params.is_empty()
            && order_by.is_empty()
            && window.is_none()
            && lambda.is_none()
        {
            if let Some(expr) = self.lower_rewrite_function(span, &func_name, args)? {
                return Ok(expr);
            }

            if let Some(expr) = self.special_function(span, &func_name, args)? {
                return Ok(expr);
            }

            if let Some(func_name) = builtin_scalar_function_name(&func_name) {
                let args = self.lower_expr_args(args)?;
                return Ok(self.call(span, func_name, args));
            }
        }

        let Some(func_name) = builtin_scalar_function_name(&func_name) else {
            return Err(ErrorCode::Internal(format!(
                "function {func_name} should have been classified before scalar lowering",
            )));
        };
        let params = self.lower_function_params(params)?;
        let args = self.lower_expr_args(args)?;
        Ok(self.alloc(CoreExpr::ScalarFunction {
            span,
            func_name,
            params,
            args,
        }))
    }
}
