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
use databend_common_ast::ast::Literal;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use smallvec::smallvec;
use unicase::Ascii;

use super::TypeChecker;
use super::core_expr::CoreExprArena;
use super::core_expr::CoreExprArgs;
use super::core_expr::CoreExprId;
use super::function_arity::check_function_arity;

pub(super) fn all_rewrite_functions() -> &'static [Ascii<&'static str>] {
    static FUNCTIONS: &[Ascii<&'static str>] = &[
        Ascii::new("nullif"),
        Ascii::new("iff"),
        Ascii::new("ifnull"),
        Ascii::new("nvl"),
        Ascii::new("nvl2"),
        Ascii::new("is_null"),
        Ascii::new("isnull"),
        Ascii::new("is_error"),
        Ascii::new("error_or"),
        Ascii::new("equal_null"),
    ];
    FUNCTIONS
}

impl<'a> TypeChecker<'a, super::FullTypeCheckAdapter> {
    pub fn all_rewrite_functions() -> &'static [Ascii<&'static str>] {
        all_rewrite_functions()
    }
}

pub(super) fn rewrite_function_name(func_name: &str) -> Option<&'static str> {
    let func_name = Ascii::new(func_name);
    all_rewrite_functions()
        .iter()
        .cloned()
        .find(|name| *name == func_name)
        .map(Ascii::into_inner)
}

impl<'a> CoreExprArena<'a> {
    pub(super) fn lower_rewrite_function(
        &mut self,
        span: Span,
        func_name: &str,
        args: &'a [Expr],
    ) -> Result<Option<CoreExprId>> {
        let Some(func_name) = rewrite_function_name(func_name) else {
            return Ok(None);
        };

        let lowered = match (func_name, args) {
            ("nullif", [arg_x, arg_y]) => {
                let arg_x_eq = self.lower_ast_expr(arg_x)?;
                let arg_y = self.lower_ast_expr(arg_y)?;
                let eq = self.call(span, "eq", smallvec![arg_x_eq, arg_y]);
                let null = self.literal(span, Literal::Null);
                let arg_x = self.lower_ast_expr(arg_x)?;
                Some(self.call(span, "if", smallvec![eq, null, arg_x]))
            }
            ("equal_null", [arg_x, arg_y]) => {
                let arg_x_eq = self.lower_ast_expr(arg_x)?;
                let arg_y_eq = self.lower_ast_expr(arg_y)?;
                let eq = self.call(span, "eq", smallvec![arg_x_eq, arg_y_eq]);
                let eq_is_not_null = self.call(span, "is_not_null", smallvec![eq]);
                let eq_is_true = self.call(span, "is_true", smallvec![eq]);

                let arg_x = self.lower_ast_expr(arg_x)?;
                let arg_x_is_not_null = self.call(span, "is_not_null", smallvec![arg_x]);
                let arg_x_is_null = self.call(span, "not", smallvec![arg_x_is_not_null]);
                let arg_y = self.lower_ast_expr(arg_y)?;
                let arg_y_is_not_null = self.call(span, "is_not_null", smallvec![arg_y]);
                let arg_y_is_null = self.call(span, "not", smallvec![arg_y_is_not_null]);
                let both_null =
                    self.call(span, "and_filters", smallvec![arg_x_is_null, arg_y_is_null]);

                Some(self.call(span, "if", smallvec![eq_is_not_null, eq_is_true, both_null]))
            }
            ("iff", [condition, then_expr, else_expr]) => {
                let condition = self.lower_ast_expr(condition)?;
                let then_expr = self.lower_ast_expr(then_expr)?;
                let else_expr = self.lower_ast_expr(else_expr)?;
                Some(self.call(span, "if", smallvec![condition, then_expr, else_expr]))
            }
            ("ifnull" | "nvl", [arg_x, arg_y]) => {
                let arg_x_null_check = self.lower_ast_expr(arg_x)?;
                let arg_x_is_not_null = self.call(span, "is_not_null", smallvec![arg_x_null_check]);
                let arg_x_is_null = self.call(span, "not", smallvec![arg_x_is_not_null]);
                let arg_y = self.lower_ast_expr(arg_y)?;
                let arg_x = self.lower_ast_expr(arg_x)?;
                Some(self.call(span, "if", smallvec![arg_x_is_null, arg_y, arg_x]))
            }
            ("nvl2", [arg_x, arg_y, arg_z]) => {
                let arg_x = self.lower_ast_expr(arg_x)?;
                let arg_x_is_not_null = self.call(span, "is_not_null", smallvec![arg_x]);
                let arg_y = self.lower_ast_expr(arg_y)?;
                let arg_z = self.lower_ast_expr(arg_z)?;
                Some(self.call(span, "if", smallvec![arg_x_is_not_null, arg_y, arg_z]))
            }
            ("is_null" | "isnull", [arg_x]) => {
                let arg_x = self.lower_ast_expr(arg_x)?;
                let arg_x_is_not_null = self.call(span, "is_not_null", smallvec![arg_x]);
                Some(self.call(span, "not", smallvec![arg_x_is_not_null]))
            }
            ("is_error", [arg_x]) => {
                let arg_x = self.lower_ast_expr(arg_x)?;
                let arg_x_is_not_error = self.call(span, "is_not_error", smallvec![arg_x]);
                Some(self.call(span, "not", smallvec![arg_x_is_not_error]))
            }
            ("error_or", args) => {
                check_function_arity(span, func_name, args.len(), 1, None)?;
                let mut new_args = CoreExprArgs::with_capacity(args.len() * 2 + 1);
                for arg in args {
                    let arg_error_check = self.lower_ast_expr(arg)?;
                    let is_not_error = self.call(span, "is_not_error", smallvec![arg_error_check]);
                    let arg = self.lower_ast_expr(arg)?;
                    new_args.push(is_not_error);
                    new_args.push(arg);
                }
                new_args.push(self.literal(span, Literal::Null));
                Some(self.call(span, "if", new_args))
            }
            ("ifnull" | "nvl", [arg]) => Some(self.lower_ast_expr(arg)?),
            ("nullif", _) | ("equal_null", _) | ("ifnull" | "nvl", _) => {
                check_function_arity(span, func_name, args.len(), 2, Some(2))?;
                None
            }
            ("iff", _) | ("nvl2", _) => {
                check_function_arity(span, func_name, args.len(), 3, Some(3))?;
                None
            }
            ("is_null" | "isnull" | "is_error", _) => {
                check_function_arity(span, func_name, args.len(), 1, Some(1))?;
                None
            }
            _ => {
                return Err(ErrorCode::Internal(format!(
                    "rewrite function {func_name} should have been classified before lowering"
                )));
            }
        };

        Ok(lowered)
    }
}
