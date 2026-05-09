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

use std::collections::VecDeque;

use databend_common_ast::Span;
use databend_common_ast::ast::BinaryOperator;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FunctionCall as ASTFunctionCall;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::TypeName;
use databend_common_ast::ast::UnaryOperator;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::shrink_scalar;
use databend_common_expression::type_check::check_number;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use itertools::Itertools;
use serde_json::json;
use serde_json::to_string;
use smallvec::smallvec;
use unicase::Ascii;

use super::TypeChecker;
use super::core_expr::CoreExprArena;
use super::core_expr::CoreExprArgs;
use super::core_expr::CoreExprId;
use super::core_expr::CoreSearchFunctionArgs;
use crate::planner::semantic::normalize_identifier;
use crate::plans::CastExpr;
use crate::plans::ConstantExpr;
use crate::plans::ScalarExpr;

impl<'a, P> TypeChecker<'a, P> {
    pub fn all_sugar_functions() -> &'static [Ascii<&'static str>] {
        static FUNCTIONS: &[Ascii<&'static str>] = &[
            Ascii::new("current_catalog"),
            Ascii::new("database"),
            Ascii::new("currentdatabase"),
            Ascii::new("current_database"),
            Ascii::new("version"),
            Ascii::new("user"),
            Ascii::new("currentuser"),
            Ascii::new("current_user"),
            Ascii::new("current_role"),
            Ascii::new("current_secondary_roles"),
            Ascii::new("current_available_roles"),
            Ascii::new("connection_id"),
            Ascii::new("client_session_id"),
            Ascii::new("timezone"),
            Ascii::new("nullif"),
            Ascii::new("iff"),
            Ascii::new("ifnull"),
            Ascii::new("nvl"),
            Ascii::new("nvl2"),
            Ascii::new("is_null"),
            Ascii::new("isnull"),
            Ascii::new("is_error"),
            Ascii::new("error_or"),
            Ascii::new("coalesce"),
            Ascii::new("decode"),
            Ascii::new("last_query_id"),
            Ascii::new("array_sort"),
            Ascii::new("array_aggregate"),
            Ascii::new("to_variant"),
            Ascii::new("try_to_variant"),
            Ascii::new("greatest"),
            Ascii::new("least"),
            Ascii::new("greatest_ignore_nulls"),
            Ascii::new("least_ignore_nulls"),
            Ascii::new("stream_has_data"),
            Ascii::new("getvariable"),
            Ascii::new("equal_null"),
            Ascii::new("hex_decode_string"),
            Ascii::new("base64_decode_string"),
            Ascii::new("try_hex_decode_string"),
            Ascii::new("try_base64_decode_string"),
        ];
        FUNCTIONS
    }

    pub(super) fn can_lower_core_sugar_function(func_name: &str) -> bool {
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
        FUNCTIONS.contains(&Ascii::new(func_name))
    }
}

impl<'a> CoreExprArena<'a> {
    pub(super) fn lower_sugar_function(
        &mut self,
        span: Span,
        func_name: &'static str,
        args: &'a [Expr],
    ) -> Result<CoreExprId> {
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
            ("iff", args) => {
                let args = args
                    .iter()
                    .map(|arg| self.lower_ast_expr(arg))
                    .collect::<Result<_>>()?;
                Some(self.call(span, "if", args))
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
            _ => None,
        };

        lowered.ok_or_else(|| {
            ErrorCode::Internal(format!(
                "sugar function {func_name} should have been classified before core lowering"
            ))
        })
    }
}

impl<'a, P> TypeChecker<'a, P>
where P: super::TypeCheckPolicy
{
    pub(super) fn resolve_core_sugar_function(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        func_name: &str,
        args: &CoreSearchFunctionArgs,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let mut resolved_args = Vec::with_capacity(args.len());
        for (display, arg) in args {
            let box (scalar, data_type) = self.resolve_core(arena, *arg)?;
            resolved_args.push((display.as_str(), scalar, data_type));
        }

        match (func_name, resolved_args.as_slice()) {
            ("current_catalog", []) => self.resolve_core_sugar_literal(
                span,
                Scalar::String(self.table_ctx().get_current_catalog()),
            ),
            ("database" | "currentdatabase" | "current_database", []) => self
                .resolve_core_sugar_literal(
                    span,
                    Scalar::String(self.table_ctx().get_current_database()),
                ),
            ("version", []) => self.resolve_core_sugar_literal(
                span,
                Scalar::String(self.table_ctx().get_fuse_version()),
            ),
            ("user" | "currentuser" | "current_user", []) => {
                let user = self.table_ctx().get_current_user()?;
                self.resolve_core_sugar_literal(
                    span,
                    Scalar::String(user.identity().display().to_string()),
                )
            }
            ("current_role", []) => self.resolve_core_sugar_literal(
                span,
                Scalar::String(
                    self.table_ctx()
                        .get_current_role()
                        .map(|role| role.name)
                        .unwrap_or_default(),
                ),
            ),
            ("current_secondary_roles", []) => {
                let mut roles = databend_common_base::runtime::block_on(
                    self.table_ctx().get_all_effective_roles(),
                )
                .unwrap_or_default()
                .iter()
                .map(|role| role.name.clone())
                .collect::<Vec<_>>();
                roles.sort();
                let roles_comma_separated_string = roles.iter().join(",");
                let value = if self.table_ctx().get_secondary_roles().is_none() {
                    json!({ "roles": roles_comma_separated_string, "value": "ALL" })
                } else {
                    json!({ "roles": roles_comma_separated_string, "value": "None" })
                };
                self.resolve_core_sugar_literal(span, Scalar::String(to_string(&value)?))
            }
            ("current_available_roles", []) => {
                let mut roles = databend_common_base::runtime::block_on(
                    self.table_ctx().get_all_available_roles(),
                )
                .unwrap_or_default()
                .iter()
                .map(|role| role.name.clone())
                .collect::<Vec<_>>();
                roles.sort();
                self.resolve_core_sugar_literal(span, Scalar::String(to_string(&roles)?))
            }
            ("connection_id", []) => self.resolve_core_sugar_literal(
                span,
                Scalar::String(self.table_ctx().get_connection_id()),
            ),
            ("client_session_id", []) => self.resolve_core_sugar_literal(
                span,
                Scalar::String(
                    self.table_ctx()
                        .get_current_client_session_id()
                        .unwrap_or_default(),
                ),
            ),
            ("timezone", []) => self.resolve_core_sugar_literal(
                span,
                Scalar::String(self.table_ctx().get_settings().get_timezone().unwrap()),
            ),
            ("last_query_id", args) => self.resolve_core_last_query_id(span, args),
            ("coalesce", args) => self.resolve_core_coalesce(span, args),
            ("decode", args) => self.resolve_core_decode(span, args),
            ("array_sort", args) => self.resolve_core_array_sort(span, args),
            ("array_aggregate", args) => self.resolve_core_array_aggregate(span, args),
            ("to_variant", [(_, scalar, data_type)]) => self
                .resolve_cast_to_variant(span, data_type, scalar, false)
                .unwrap_or_else(|| {
                    self.resolve_scalar_function_call(span, "to_variant", vec![], vec![
                        scalar.clone(),
                    ])
                }),
            ("try_to_variant", [(_, scalar, data_type)]) => self
                .resolve_cast_to_variant(span, data_type, scalar, true)
                .unwrap_or_else(|| {
                    self.resolve_scalar_function_call(span, "try_to_variant", vec![], vec![
                        scalar.clone(),
                    ])
                }),
            (name @ ("greatest" | "least"), args) => {
                self.resolve_core_greatest_or_least(span, name, args, false)
            }
            (name @ ("greatest_ignore_nulls" | "least_ignore_nulls"), args) => {
                self.resolve_core_greatest_or_least(span, name, args, true)
            }
            ("getvariable", [(_, scalar, _)]) => self.resolve_core_getvariable(span, scalar),
            (
                name @ ("hex_decode_string"
                | "try_hex_decode_string"
                | "base64_decode_string"
                | "try_base64_decode_string"),
                [(_, scalar, _)],
            ) => self.resolve_core_decode_string_function(span, name, scalar),
            _ => {
                let scalars = resolved_args
                    .into_iter()
                    .map(|(_, scalar, _)| scalar)
                    .collect();
                self.resolve_scalar_function_call(span, func_name, vec![], scalars)
            }
        }
    }

    fn resolve_core_sugar_literal(
        &self,
        span: Span,
        value: Scalar,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let data_type = value.as_ref().infer_data_type();
        Ok(Box::new((
            ScalarExpr::ConstantExpr(ConstantExpr { span, value }),
            data_type,
        )))
    }

    fn resolve_core_last_query_id(
        &mut self,
        span: Span,
        args: &[(&str, ScalarExpr, DataType)],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if args.len() > 1 {
            return Err(ErrorCode::BadArguments(
                "last_query_id needs at most one integer argument",
            )
            .set_span(span));
        }
        let index = if args.is_empty() {
            -1
        } else {
            let scalar = &args[0].1;
            let expr = scalar.as_expr()?;
            if expr.as_constant().is_none() {
                return Err(ErrorCode::BadArguments(
                    "last_query_id argument only support constant argument",
                )
                .set_span(span));
            }
            check_number(span, &self.func_ctx, &expr, &BUILTIN_FUNCTIONS)?
        };
        let value = self
            .table_ctx()
            .get_last_query_id(index as i32)
            .map(Scalar::String)
            .unwrap_or(Scalar::Null);
        self.resolve_core_sugar_literal(span, value)
    }

    fn resolve_core_coalesce(
        &mut self,
        span: Span,
        args: &[(&str, ScalarExpr, DataType)],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let mut new_args = Vec::with_capacity(args.len() * 2 + 1);
        for (_, arg, _) in args {
            if matches!(
                arg,
                ScalarExpr::ConstantExpr(ConstantExpr {
                    value: Scalar::Null,
                    ..
                })
            ) {
                continue;
            }
            let box (is_not_null, _) =
                self.resolve_scalar_function_call(span, "is_not_null", vec![], vec![arg.clone()])?;
            if let ScalarExpr::ConstantExpr(ConstantExpr {
                value: Scalar::Boolean(false),
                ..
            }) = &is_not_null
            {
                continue;
            }
            let box (assume_not_null, _) =
                self.resolve_scalar_function_call(span, "assume_not_null", vec![], vec![
                    arg.clone(),
                ])?;
            new_args.push(is_not_null);
            new_args.push(assume_not_null);
        }
        new_args.push(ScalarExpr::ConstantExpr(ConstantExpr {
            span,
            value: Scalar::Null,
        }));
        if new_args.len() == 1 {
            new_args.push(ScalarExpr::ConstantExpr(ConstantExpr {
                span,
                value: Scalar::Null,
            }));
            new_args.push(ScalarExpr::ConstantExpr(ConstantExpr {
                span,
                value: Scalar::Null,
            }));
        }
        self.resolve_scalar_function_call(span, "if", vec![], new_args)
    }

    fn resolve_core_decode(
        &mut self,
        span: Span,
        args: &[(&str, ScalarExpr, DataType)],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if args.len() < 3 {
            return Err(
                ErrorCode::BadArguments("DECODE requires at least 3 arguments").set_span(span),
            );
        }
        let search_expr = &args[0].1;
        let mut new_args = Vec::with_capacity(args.len() * 2 + 1);
        let mut i = 1;
        while i < args.len() {
            if i + 1 >= args.len() {
                break;
            }
            let search = &args[i].1;
            let result = args[i + 1].1.clone();
            let box (eq, _) = self.resolve_scalar_function_call(span, "eq", vec![], vec![
                search_expr.clone(),
                search.clone(),
            ])?;
            let box (a_not_null, _) =
                self.resolve_scalar_function_call(span, "is_not_null", vec![], vec![
                    search_expr.clone(),
                ])?;
            let box (a_null, _) =
                self.resolve_scalar_function_call(span, "not", vec![], vec![a_not_null])?;
            let box (b_not_null, _) =
                self.resolve_scalar_function_call(span, "is_not_null", vec![], vec![
                    search.clone(),
                ])?;
            let box (b_null, _) =
                self.resolve_scalar_function_call(span, "not", vec![], vec![b_not_null])?;
            let box (both_null, _) =
                self.resolve_scalar_function_call(span, "and", vec![], vec![a_null, b_null])?;
            let box (condition, _) =
                self.resolve_scalar_function_call(span, "or", vec![], vec![eq, both_null])?;
            new_args.push(condition);
            new_args.push(result);
            i += 2;
        }
        if i + 1 == args.len() {
            new_args.push(args[i].1.clone());
        } else {
            new_args.push(ScalarExpr::ConstantExpr(ConstantExpr {
                span,
                value: Scalar::Null,
            }));
        }
        self.resolve_scalar_function_call(span, "if", vec![], new_args)
    }

    fn resolve_core_array_sort(
        &mut self,
        span: Span,
        args: &[(&str, ScalarExpr, DataType)],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if args.is_empty() || args.len() > 3 {
            return self.resolve_scalar_function_call(
                span,
                "array_sort",
                vec![],
                args.iter().map(|(_, scalar, _)| scalar.clone()).collect(),
            );
        }
        let mut asc = true;
        let mut nulls_first = None;
        if args.len() >= 2 {
            let Ok(arg) = ConstantExpr::try_from(args[1].1.clone()) else {
                return Err(ErrorCode::SemanticError(
                    "Sorting order must be a constant string",
                ));
            };
            let Scalar::String(sort_order) = arg.value else {
                return Err(ErrorCode::SemanticError(
                    "Sorting order must be either ASC or DESC",
                ));
            };
            if sort_order.eq_ignore_ascii_case("asc") {
                asc = true;
            } else if sort_order.eq_ignore_ascii_case("desc") {
                asc = false;
            } else {
                return Err(ErrorCode::SemanticError(
                    "Sorting order must be either ASC or DESC",
                ));
            }
        }
        if args.len() == 3 {
            let Ok(arg) = ConstantExpr::try_from(args[2].1.clone()) else {
                return Err(ErrorCode::SemanticError(
                    "Null sorting order must be a constant string",
                ));
            };
            let Scalar::String(nulls_order) = arg.value else {
                return Err(ErrorCode::SemanticError(
                    "Null sorting order must be either NULLS FIRST or NULLS LAST",
                ));
            };
            if nulls_order.eq_ignore_ascii_case("nulls first") {
                nulls_first = Some(true);
            } else if nulls_order.eq_ignore_ascii_case("nulls last") {
                nulls_first = Some(false);
            } else {
                return Err(ErrorCode::SemanticError(
                    "Null sorting order must be either NULLS FIRST or NULLS LAST",
                ));
            }
        }
        let nulls_first = nulls_first.unwrap_or_else(|| {
            let settings = self.table_ctx().get_settings();
            settings.get_nulls_first()(asc)
        });
        let func_name = match (asc, nulls_first) {
            (true, true) => "array_sort_asc_null_first",
            (false, true) => "array_sort_desc_null_first",
            (true, false) => "array_sort_asc_null_last",
            (false, false) => "array_sort_desc_null_last",
        };
        self.resolve_scalar_function_call(span, func_name, vec![], vec![args[0].1.clone()])
    }

    fn resolve_core_array_aggregate(
        &mut self,
        span: Span,
        args: &[(&str, ScalarExpr, DataType)],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if args.len() != 2 {
            return self.resolve_scalar_function_call(
                span,
                "array_aggregate",
                vec![],
                args.iter().map(|(_, scalar, _)| scalar.clone()).collect(),
            );
        }
        let Ok(arg) = ConstantExpr::try_from(args[1].1.clone()) else {
            return Err(ErrorCode::SemanticError(
                "Array aggregate function name be must a constant string",
            ));
        };
        let Scalar::String(aggr_func_name) = arg.value else {
            return Err(ErrorCode::SemanticError(
                "Array aggregate function name be must a constant string",
            ));
        };
        let func_name = format!("array_{}", aggr_func_name);
        self.resolve_scalar_function_call(span, &func_name, vec![], vec![args[0].1.clone()])
    }

    fn resolve_core_greatest_or_least(
        &mut self,
        span: Span,
        name: &str,
        args: &[(&str, ScalarExpr, DataType)],
        ignore_nulls: bool,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let array_func = if name.starts_with("greatest") {
            "array_max"
        } else {
            "array_min"
        };
        let scalars = args.iter().map(|(_, scalar, _)| scalar.clone()).collect();
        let box (array, _) = self.resolve_scalar_function_call(span, "array", vec![], scalars)?;
        if ignore_nulls {
            return self.resolve_scalar_function_call(span, array_func, vec![], vec![array]);
        }
        let null_scalar = ScalarExpr::ConstantExpr(ConstantExpr {
            span: None,
            value: Scalar::Null,
        });
        let box (contains_null, _) =
            self.resolve_scalar_function_call(span, "array_contains", vec![], vec![
                array.clone(),
                null_scalar.clone(),
            ])?;
        let box (max_or_min, _) =
            self.resolve_scalar_function_call(span, array_func, vec![], vec![array])?;
        self.resolve_scalar_function_call(span, "if", vec![], vec![
            contains_null,
            null_scalar,
            max_or_min,
        ])
    }

    fn resolve_core_getvariable(
        &self,
        span: Span,
        scalar: &ScalarExpr,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if let Ok(arg) = ConstantExpr::try_from(scalar.clone())
            && let Scalar::String(var_name) = arg.value
        {
            let var_value = self
                .table_ctx()
                .get_variable(&var_name)
                .unwrap_or(Scalar::Null);
            let var_value = shrink_scalar(var_value);
            let data_type = var_value.as_ref().infer_data_type();
            return Ok(Box::new((
                ScalarExpr::ConstantExpr(ConstantExpr {
                    span,
                    value: var_value,
                }),
                data_type,
            )));
        }
        Err(ErrorCode::SemanticError(
            "Variable name must be a constant string",
        ))
    }

    fn resolve_core_decode_string_function(
        &self,
        span: Span,
        func_name: &str,
        scalar: &ScalarExpr,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let binary_func_name = func_name.replace("_string", "_binary");
        let box (binary, _) =
            self.resolve_scalar_function_call(span, &binary_func_name, vec![], vec![
                scalar.clone(),
            ])?;
        let target_type = DataType::String;
        let scalar = ScalarExpr::CastExpr(CastExpr {
            span,
            is_try: false,
            argument: Box::new(binary),
            target_type: Box::new(target_type.clone()),
        });
        Ok(Box::new((scalar, target_type)))
    }

    pub(super) async fn try_rewrite_sugar_function(
        &mut self,
        span: Span,
        func_name: &str,
        args: &[&Expr],
    ) -> Option<Result<Box<(ScalarExpr, DataType)>>> {
        match (func_name.to_lowercase().as_str(), args) {
            ("current_catalog", &[]) => Some(self.resolve(&Expr::Literal {
                span,
                value: Literal::String(self.table_ctx().get_current_catalog()),
            })),
            ("database" | "currentdatabase" | "current_database", &[]) => {
                Some(self.resolve(&Expr::Literal {
                    span,
                    value: Literal::String(self.table_ctx().get_current_database()),
                }))
            }
            ("version", &[]) => Some(self.resolve(&Expr::Literal {
                span,
                value: Literal::String(self.table_ctx().get_fuse_version()),
            })),
            ("user" | "currentuser" | "current_user", &[]) => {
                match self.table_ctx().get_current_user() {
                    Ok(user) => Some(self.resolve(&Expr::Literal {
                        span,
                        value: Literal::String(user.identity().display().to_string()),
                    })),
                    Err(e) => Some(Err(e)),
                }
            }
            ("current_role", &[]) => Some(
                self.resolve(&Expr::Literal {
                    span,
                    value: Literal::String(
                        self.table_ctx()
                            .get_current_role()
                            .map(|r| r.name)
                            .unwrap_or_default(),
                    ),
                }),
            ),
            ("current_secondary_roles", &[]) => {
                let mut res = self
                    .table_ctx()
                    .get_all_effective_roles()
                    .await
                    .unwrap_or_default()
                    .iter()
                    .map(|r| r.name.clone())
                    .collect::<Vec<String>>();
                res.sort();
                let roles_comma_separated_string = res.iter().join(",");
                let res = if self.table_ctx().get_secondary_roles().is_none() {
                    json!({
                        "roles": roles_comma_separated_string,
                        "value": "ALL"
                    })
                } else {
                    json!({
                        "roles": roles_comma_separated_string,
                        "value": "None"
                    })
                };
                match to_string(&res) {
                    Ok(res) => Some(self.resolve(&Expr::Literal {
                        span,
                        value: Literal::String(res),
                    })),
                    Err(e) => Some(Err(ErrorCode::IllegalRole(format!(
                        "Failed to serialize secondary roles into JSON string: {}",
                        e
                    )))),
                }
            }
            ("current_available_roles", &[]) => {
                let mut res = self
                    .table_ctx()
                    .get_all_available_roles()
                    .await
                    .unwrap_or_default()
                    .iter()
                    .map(|r| r.name.clone())
                    .collect::<Vec<String>>();
                res.sort();
                match to_string(&res) {
                    Ok(res) => Some(self.resolve(&Expr::Literal {
                        span,
                        value: Literal::String(res),
                    })),
                    Err(e) => Some(Err(ErrorCode::IllegalRole(format!(
                        "Failed to serialize available roles into JSON string: {}",
                        e
                    )))),
                }
            }
            ("connection_id", &[]) => Some(self.resolve(&Expr::Literal {
                span,
                value: Literal::String(self.table_ctx().get_connection_id()),
            })),
            ("client_session_id", &[]) => Some(
                self.resolve(&Expr::Literal {
                    span,
                    value: Literal::String(
                        self.table_ctx()
                            .get_current_client_session_id()
                            .unwrap_or_default(),
                    ),
                }),
            ),
            ("timezone", &[]) => {
                let tz = self.table_ctx().get_settings().get_timezone().unwrap();
                Some(self.resolve(&Expr::Literal {
                    span,
                    value: Literal::String(tz),
                }))
            }
            ("nullif", &[arg_x, arg_y]) => {
                // Rewrite nullif(x, y) to if(x = y, null, x)
                Some(self.resolve_function(span, "if", vec![], &[
                    &Expr::BinaryOp {
                        span,
                        op: BinaryOperator::Eq,
                        left: Box::new(arg_x.clone()),
                        right: Box::new(arg_y.clone()),
                    },
                    &Expr::Literal {
                        span,
                        value: Literal::Null,
                    },
                    arg_x,
                ]))
            }
            ("equal_null", &[arg_x, arg_y]) => {
                // Rewrite equal_null(x, y) to if(is_not_null( x = y ), is_true( x = y ), x is null and y is null)
                let eq_expr = Expr::BinaryOp {
                    span,
                    op: BinaryOperator::Eq,
                    left: Box::new(arg_x.clone()),
                    right: Box::new(arg_y.clone()),
                };

                let is_null_x = Expr::IsNull {
                    span,
                    expr: Box::new(arg_x.clone()),
                    not: false,
                };
                let is_null_y = Expr::IsNull {
                    span,
                    expr: Box::new(arg_y.clone()),
                    not: false,
                };

                Some(self.resolve_function(span, "if", vec![], &[
                    &Expr::IsNull {
                        span,
                        expr: Box::new(eq_expr.clone()),
                        not: true,
                    },
                    &Expr::FunctionCall {
                        span,
                        func: ASTFunctionCall {
                            name: Identifier::from_name(span, "is_true"),
                            args: vec![eq_expr],
                            ..Default::default()
                        },
                    },
                    &Expr::FunctionCall {
                        span,
                        func: ASTFunctionCall {
                            name: Identifier::from_name(span, "and_filters"),
                            args: vec![is_null_x, is_null_y],
                            ..Default::default()
                        },
                    },
                ]))
            }
            ("iff", args) => Some(self.resolve_function(span, "if", vec![], args)),
            ("ifnull" | "nvl", args) => {
                if args.len() == 2 {
                    // Rewrite ifnull(x, y) | nvl(x, y) to if(is_null(x), y, x)
                    Some(self.resolve_function(span, "if", vec![], &[
                        &Expr::IsNull {
                            span,
                            expr: Box::new(args[0].clone()),
                            not: false,
                        },
                        args[1],
                        args[0],
                    ]))
                } else {
                    // Rewrite ifnull(args) to coalesce(x, y)
                    // Rewrite nvl(args) to coalesce(args)
                    // nvl is essentially an alias for ifnull.
                    Some(self.resolve_function(span, "coalesce", vec![], args))
                }
            }
            ("nvl2", &[arg_x, arg_y, arg_z]) => {
                // Rewrite nvl2(x, y, z) to if(is_not_null(x), y, z)
                Some(self.resolve_function(span, "if", vec![], &[
                    &Expr::IsNull {
                        span,
                        expr: Box::new(arg_x.clone()),
                        not: true,
                    },
                    arg_y,
                    arg_z,
                ]))
            }
            ("is_null", &[arg_x]) | ("isnull", &[arg_x]) => {
                // Rewrite is_null(x) to not(is_not_null(x))
                Some(
                    self.resolve_unary_op(span, &UnaryOperator::Not, &Expr::FunctionCall {
                        span,
                        func: ASTFunctionCall {
                            distinct: false,
                            name: Identifier::from_name(span, "is_not_null"),
                            args: vec![arg_x.clone()],
                            params: vec![],
                            order_by: vec![],
                            window: None,
                            lambda: None,
                        },
                    }),
                )
            }
            ("is_error", &[arg_x]) => {
                // Rewrite is_error(x) to not(is_not_error(x))
                Some(
                    self.resolve_unary_op(span, &UnaryOperator::Not, &Expr::FunctionCall {
                        span,
                        func: ASTFunctionCall {
                            distinct: false,
                            name: Identifier::from_name(span, "is_not_error"),
                            args: vec![arg_x.clone()],
                            params: vec![],
                            order_by: vec![],
                            window: None,
                            lambda: None,
                        },
                    }),
                )
            }
            ("error_or", args) => {
                // error_or(arg0, arg1, ..., argN) is essentially
                // if(is_not_error(arg0), arg0, is_not_error(arg1), arg1, ..., argN)
                let mut new_args = Vec::with_capacity(args.len() * 2 + 1);

                for arg in args.iter() {
                    let is_not_error = Expr::FunctionCall {
                        span,
                        func: ASTFunctionCall {
                            distinct: false,
                            name: Identifier::from_name(span, "is_not_error"),
                            args: vec![(*arg).clone()],
                            params: vec![],
                            order_by: vec![],
                            window: None,
                            lambda: None,
                        },
                    };
                    new_args.push(is_not_error);
                    new_args.push((*arg).clone());
                }
                new_args.push(Expr::Literal {
                    span,
                    value: Literal::Null,
                });

                let args_ref: Vec<&Expr> = new_args.iter().collect();
                Some(self.resolve_function(span, "if", vec![], &args_ref))
            }
            ("coalesce", args) => {
                // coalesce(arg0, arg1, ..., argN) is essentially
                // if(is_not_null(arg0), assume_not_null(arg0), is_not_null(arg1), assume_not_null(arg1), ..., argN)
                // with constant Literal::Null arguments removed.
                let mut new_args = Vec::with_capacity(args.len() * 2 + 1);
                for arg in args.iter() {
                    if let Expr::Literal {
                        span: _,
                        value: Literal::Null,
                    } = arg
                    {
                        continue;
                    }

                    let is_not_null_expr = Expr::IsNull {
                        span,
                        expr: Box::new((*arg).clone()),
                        not: true,
                    };
                    if let Ok(res) = self.resolve(&is_not_null_expr) {
                        if let ScalarExpr::ConstantExpr(c) = res.0 {
                            if Scalar::Boolean(false) == c.value {
                                continue;
                            }
                        }
                    }

                    let assume_not_null_expr = Expr::FunctionCall {
                        span,
                        func: ASTFunctionCall {
                            distinct: false,
                            name: Identifier::from_name(span, "assume_not_null"),
                            args: vec![(*arg).clone()],
                            params: vec![],
                            order_by: vec![],
                            window: None,
                            lambda: None,
                        },
                    };

                    new_args.push(is_not_null_expr);
                    new_args.push(assume_not_null_expr);
                }
                new_args.push(Expr::Literal {
                    span,
                    value: Literal::Null,
                });

                // coalesce(all_null) => null
                if new_args.len() == 1 {
                    new_args.push(Expr::Literal {
                        span,
                        value: Literal::Null,
                    });
                    new_args.push(Expr::Literal {
                        span,
                        value: Literal::Null,
                    });
                }

                let args_ref: Vec<&Expr> = new_args.iter().collect();
                Some(self.resolve_function(span, "if", vec![], &args_ref))
            }
            ("decode", args) => {
                // DECODE( <expr> , <search1> , <result1> [ , <search2> , <result2> ... ] [ , <default> ] )
                // Note that, contrary to CASE, a NULL value in the select expression matches a NULL value in the search expressions.
                if args.len() < 3 {
                    return Some(Err(ErrorCode::BadArguments(
                        "DECODE requires at least 3 arguments",
                    )
                    .set_span(span)));
                }

                let mut new_args = Vec::with_capacity(args.len() * 2 + 1);
                let search_expr = args[0].clone();
                let mut i = 1;

                while i < args.len() {
                    let search = args[i].clone();
                    let result = if i + 1 < args.len() {
                        args[i + 1].clone()
                    } else {
                        // If we're at the last argument and it's odd, it's the default value
                        break;
                    };

                    // (a = b) or (a is null and b is null)
                    let is_null_a = Expr::IsNull {
                        span,
                        expr: Box::new(search_expr.clone()),
                        not: false,
                    };
                    let is_null_b = Expr::IsNull {
                        span,
                        expr: Box::new(search.clone()),
                        not: false,
                    };
                    let and_expr = Expr::BinaryOp {
                        span,
                        op: BinaryOperator::And,
                        left: Box::new(is_null_a),
                        right: Box::new(is_null_b),
                    };

                    let eq_expr = Expr::BinaryOp {
                        span,
                        op: BinaryOperator::Eq,
                        left: Box::new(search_expr.clone()),
                        right: Box::new(search),
                    };

                    let or_expr = Expr::BinaryOp {
                        span,
                        op: BinaryOperator::Or,
                        left: Box::new(eq_expr),
                        right: Box::new(and_expr),
                    };

                    new_args.push(or_expr);
                    new_args.push(result);
                    i += 2;
                }

                // Add default value if it exists
                if i + 1 == args.len() {
                    new_args.push(args[i].clone());
                } else {
                    new_args.push(Expr::Literal {
                        span,
                        value: Literal::Null,
                    });
                }

                let args_ref: Vec<&Expr> = new_args.iter().collect();
                Some(self.resolve_function(span, "if", vec![], &args_ref))
            }
            ("last_query_id", args) => {
                // last_query_id(index) returns query_id in current session by index
                let res: Result<i64> = try {
                    if args.len() > 1 {
                        return Some(Err(ErrorCode::BadArguments(
                            "last_query_id needs at most one integer argument",
                        )
                        .set_span(span)));
                    }
                    if args.is_empty() {
                        -1
                    } else {
                        let box (scalar, _) = self.resolve(args[0])?;

                        let expr = scalar.as_expr()?;
                        match expr.as_constant() {
                            Some(_) => {
                                check_number(span, &self.func_ctx, &expr, &BUILTIN_FUNCTIONS)?
                            }
                            None => {
                                return Some(Err(ErrorCode::BadArguments(
                                    "last_query_id argument only support constant argument",
                                )
                                .set_span(span)));
                            }
                        }
                    }
                };

                Some(match res {
                    Ok(index) => {
                        if let Some(query_id) = self.table_ctx().get_last_query_id(index as i32) {
                            self.resolve(&Expr::Literal {
                                span,
                                value: Literal::String(query_id),
                            })
                        } else {
                            self.resolve(&Expr::Literal {
                                span,
                                value: Literal::Null,
                            })
                        }
                    }
                    Err(e) => Err(e),
                })
            }
            ("array_sort", args) => {
                if args.is_empty() || args.len() > 3 {
                    return None;
                }
                let mut asc = true;
                let mut nulls_first = None;
                if args.len() >= 2 {
                    let box (arg, _) = self.resolve(args[1]).ok()?;
                    if let Ok(arg) = ConstantExpr::try_from(arg) {
                        if let Scalar::String(sort_order) = arg.value {
                            if sort_order.eq_ignore_ascii_case("asc") {
                                asc = true;
                            } else if sort_order.eq_ignore_ascii_case("desc") {
                                asc = false;
                            } else {
                                return Some(Err(ErrorCode::SemanticError(
                                    "Sorting order must be either ASC or DESC",
                                )));
                            }
                        } else {
                            return Some(Err(ErrorCode::SemanticError(
                                "Sorting order must be either ASC or DESC",
                            )));
                        }
                    } else {
                        return Some(Err(ErrorCode::SemanticError(
                            "Sorting order must be a constant string",
                        )));
                    }
                }
                if args.len() == 3 {
                    let box (arg, _) = self.resolve(args[2]).ok()?;
                    if let Ok(arg) = ConstantExpr::try_from(arg) {
                        if let Scalar::String(nulls_order) = arg.value {
                            if nulls_order.eq_ignore_ascii_case("nulls first") {
                                nulls_first = Some(true);
                            } else if nulls_order.eq_ignore_ascii_case("nulls last") {
                                nulls_first = Some(false);
                            } else {
                                return Some(Err(ErrorCode::SemanticError(
                                    "Null sorting order must be either NULLS FIRST or NULLS LAST",
                                )));
                            }
                        } else {
                            return Some(Err(ErrorCode::SemanticError(
                                "Null sorting order must be either NULLS FIRST or NULLS LAST",
                            )));
                        }
                    } else {
                        return Some(Err(ErrorCode::SemanticError(
                            "Null sorting order must be a constant string",
                        )));
                    }
                }

                let nulls_first = nulls_first.unwrap_or_else(|| {
                    let settings = self.table_ctx().get_settings();
                    settings.get_nulls_first()(asc)
                });

                let func_name = match (asc, nulls_first) {
                    (true, true) => "array_sort_asc_null_first",
                    (false, true) => "array_sort_desc_null_first",
                    (true, false) => "array_sort_asc_null_last",
                    (false, false) => "array_sort_desc_null_last",
                };
                let args_ref: Vec<&Expr> = vec![args[0]];
                Some(self.resolve_function(span, func_name, vec![], &args_ref))
            }
            ("array_aggregate", args) => {
                if args.len() != 2 {
                    return None;
                }
                let box (arg, _) = self.resolve(args[1]).ok()?;
                if let Ok(arg) = ConstantExpr::try_from(arg) {
                    if let Scalar::String(aggr_func_name) = arg.value {
                        let func_name = format!("array_{}", aggr_func_name);
                        let args_ref: Vec<&Expr> = vec![args[0]];
                        return Some(self.resolve_function(span, &func_name, vec![], &args_ref));
                    }
                }
                Some(Err(ErrorCode::SemanticError(
                    "Array aggregate function name be must a constant string",
                )))
            }
            ("to_variant", args) => {
                if args.len() != 1 {
                    return None;
                }
                let box (scalar, data_type) = self.resolve(args[0]).ok()?;
                self.resolve_cast_to_variant(span, &data_type, &scalar, false)
            }
            ("try_to_variant", args) => {
                if args.len() != 1 {
                    return None;
                }
                let box (scalar, data_type) = self.resolve(args[0]).ok()?;
                self.resolve_cast_to_variant(span, &data_type, &scalar, true)
            }
            (name @ ("greatest" | "least"), args) => {
                let array_func = if name == "greatest" {
                    "array_max"
                } else {
                    "array_min"
                };
                let (array, _) = *self.resolve_function(span, "array", vec![], args).ok()?;
                let null_scalar = ScalarExpr::ConstantExpr(ConstantExpr {
                    span: None,
                    value: Scalar::Null,
                });

                let contains_null = self
                    .resolve_scalar_function_call(span, "array_contains", vec![], vec![
                        array.clone(),
                        null_scalar.clone(),
                    ])
                    .ok()?;

                let max = self
                    .resolve_scalar_function_call(span, array_func, vec![], vec![array])
                    .ok()?;

                Some(self.resolve_scalar_function_call(span, "if", vec![], vec![
                    contains_null.0.clone(),
                    null_scalar.clone(),
                    max.0.clone(),
                ]))
            }
            ("greatest_ignore_nulls", args) => {
                let (array, _) = *self.resolve_function(span, "array", vec![], args).ok()?;
                Some(self.resolve_scalar_function_call(span, "array_max", vec![], vec![array]))
            }
            ("least_ignore_nulls", args) => {
                let (array, _) = *self.resolve_function(span, "array", vec![], args).ok()?;
                Some(self.resolve_scalar_function_call(span, "array_min", vec![], vec![array]))
            }
            ("getvariable", args) => {
                if args.len() != 1 {
                    return None;
                }
                let box (scalar, _) = self.resolve(args[0]).ok()?;

                if let Ok(arg) = ConstantExpr::try_from(scalar) {
                    if let Scalar::String(var_name) = arg.value {
                        let var_value = self
                            .table_ctx()
                            .get_variable(&var_name)
                            .unwrap_or(Scalar::Null);
                        let var_value = shrink_scalar(var_value);
                        let data_type = var_value.as_ref().infer_data_type();
                        return Some(Ok(Box::new((
                            ScalarExpr::ConstantExpr(ConstantExpr {
                                span,
                                value: var_value,
                            }),
                            data_type,
                        ))));
                    }
                }
                Some(Err(ErrorCode::SemanticError(
                    "Variable name must be a constant string",
                )))
            }
            ("get" | "get_string", &[arg_x, arg_y]) => {
                if !self.bind_context.allow_virtual_column {
                    return None;
                }

                let mut expr = arg_x;
                let mut path_exprs = VecDeque::new();
                path_exprs.push_back(arg_y);
                while let Expr::FunctionCall { func, .. } = expr {
                    let func_name =
                        normalize_identifier(&func.name, self.name_resolution_ctx).to_string();
                    let func_name = func_name.as_str();
                    if func_name == "get" && func.args.len() == 2 {
                        expr = &func.args[0];
                        path_exprs.push_back(&func.args[1]);
                    } else {
                        return None;
                    }
                }
                let mut paths = VecDeque::with_capacity(path_exprs.len());
                while let Some(path_expr) = path_exprs.pop_back() {
                    if let Expr::Literal { span, value } = path_expr {
                        if matches!(value, Literal::UInt64(_) | Literal::String(_)) {
                            paths.push_back((*span, value.clone()));
                        } else {
                            return Some(Err(ErrorCode::SemanticError(format!(
                                "Unsupported argument: {:?}",
                                value
                            ))
                            .set_span(*span)));
                        }
                    } else {
                        return None;
                    }
                }
                if func_name == "get_string" {
                    if let Ok(box (scalar, data_type)) = self.resolve_map_access(span, expr, paths)
                    {
                        if data_type.remove_nullable() == DataType::Variant {
                            let target_type = DataType::Nullable(Box::new(DataType::String));
                            let new_scalar = ScalarExpr::CastExpr(CastExpr {
                                span: scalar.span(),
                                is_try: false,
                                argument: Box::new(scalar),
                                target_type: Box::new(target_type.clone()),
                            });
                            return Some(Ok(Box::new((new_scalar, target_type))));
                        }
                    }
                    None
                } else {
                    Some(self.resolve_map_access(span, expr, paths))
                }
            }
            (func_name, &[expr])
                if matches!(
                    func_name,
                    "hex_decode_string"
                        | "try_hex_decode_string"
                        | "base64_decode_string"
                        | "try_base64_decode_string"
                ) =>
            {
                Some(self.resolve(&Expr::Cast {
                    span,
                    expr: Box::new(Expr::FunctionCall {
                        span,
                        func: ASTFunctionCall {
                            distinct: false,
                            name: Identifier::from_name(
                                span,
                                func_name.replace("_string", "_binary"),
                            ),
                            args: vec![expr.clone()],
                            params: vec![],
                            order_by: vec![],
                            window: None,
                            lambda: None,
                        },
                    }),
                    target_type: TypeName::String,
                    pg_style: false,
                }))
            }
            _ => None,
        }
    }
}
