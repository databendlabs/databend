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
use crate::plans::CastExpr;
use crate::plans::ConstantExpr;
use crate::plans::ScalarExpr;

impl<'a> TypeChecker<'a, super::FullTypeCheckAdapter> {
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

impl<'a, A> TypeChecker<'a, A>
where A: super::TypeCheckAdapter
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
                Scalar::String(self.adapter.table_access_context()?.get_current_catalog()),
            ),
            ("database" | "currentdatabase" | "current_database", []) => self
                .resolve_core_sugar_literal(
                    span,
                    Scalar::String(self.adapter.table_access_context()?.get_current_database()),
                ),
            ("version", []) => {
                self.resolve_core_sugar_literal(span, Scalar::String(self.adapter.fuse_version()?))
            }
            ("user" | "currentuser" | "current_user", []) => self.resolve_core_sugar_literal(
                span,
                Scalar::String(
                    self.adapter
                        .authorization_context()?
                        .get_current_user()?
                        .identity()
                        .display()
                        .to_string(),
                ),
            ),
            ("current_role", []) => self.resolve_core_sugar_literal(
                span,
                Scalar::String(
                    self.adapter
                        .authorization_context()?
                        .get_current_role()
                        .map(|role| role.name)
                        .unwrap_or_default(),
                ),
            ),
            ("current_secondary_roles", []) => {
                let auth_ctx = self.adapter.authorization_context()?;
                let mut roles = self
                    .block_on(auth_ctx.get_all_effective_roles())??
                    .into_iter()
                    .map(|role| role.name)
                    .collect::<Vec<_>>();
                roles.sort();
                let roles_comma_separated_string = roles.iter().join(",");
                let value = if auth_ctx.get_secondary_roles().is_none() {
                    json!({ "roles": roles_comma_separated_string, "value": "ALL" })
                } else {
                    json!({ "roles": roles_comma_separated_string, "value": "None" })
                };
                self.resolve_core_sugar_literal(span, Scalar::String(to_string(&value)?))
            }
            ("current_available_roles", []) => {
                let auth_ctx = self.adapter.authorization_context()?;
                let mut roles = self
                    .block_on(auth_ctx.get_all_available_roles())??
                    .into_iter()
                    .map(|role| role.name)
                    .collect::<Vec<_>>();
                roles.sort();
                self.resolve_core_sugar_literal(span, Scalar::String(to_string(&roles)?))
            }
            ("connection_id", []) => {
                self.resolve_core_sugar_literal(span, Scalar::String(self.adapter.connection_id()?))
            }
            ("client_session_id", []) => self.resolve_core_sugar_literal(
                span,
                Scalar::String(
                    self.adapter
                        .current_client_session_id()?
                        .unwrap_or_default(),
                ),
            ),
            ("timezone", []) => self.resolve_core_sugar_literal(
                span,
                Scalar::String(self.adapter.settings().get_timezone().unwrap()),
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
            .adapter
            .last_query_id(index as i32)?
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
        let nulls_first =
            nulls_first.unwrap_or_else(|| self.adapter.settings().get_nulls_first()(asc));
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
            let var_value = self.adapter.variable(&var_name)?.unwrap_or(Scalar::Null);
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
}
