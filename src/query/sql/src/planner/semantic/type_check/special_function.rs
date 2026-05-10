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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::shrink_scalar;
use databend_common_expression::type_check::check_number;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use unicase::Ascii;

use super::TypeCheckAuthorizationFunction;
use super::TypeCheckNamespaceFunction;
use super::TypeCheckSessionFunction;
use super::TypeChecker;
use super::core_expr::CoreExpr;
use super::core_expr::CoreExprArena;
use super::core_expr::CoreExprId;
use super::core_expr::CoreSearchFunctionArgs;
use super::function_arity::check_function_arity;
use crate::plans::CastExpr;
use crate::plans::ConstantExpr;
use crate::plans::ScalarExpr;

pub(super) enum CoreSpecialFunction {
    Namespace(CoreNamespaceSpecialFunction),
    Session(CoreSessionSpecialFunction),
    Authorization(CoreAuthorizationSpecialFunction),
    Timezone,
    LastQueryId {
        args: CoreSearchFunctionArgs,
    },
    Coalesce {
        args: CoreSearchFunctionArgs,
    },
    Decode {
        args: CoreSearchFunctionArgs,
    },
    ArraySort {
        args: CoreSearchFunctionArgs,
    },
    ArrayAggregate {
        args: CoreSearchFunctionArgs,
    },
    CastToVariant {
        func_name: &'static str,
        args: CoreSearchFunctionArgs,
        is_try: bool,
    },
    GreatestOrLeast {
        func_name: &'static str,
        args: CoreSearchFunctionArgs,
        ignore_nulls: bool,
    },
    GetVariable {
        args: CoreSearchFunctionArgs,
    },
    DecodeString {
        func_name: &'static str,
        args: CoreSearchFunctionArgs,
    },
    Scalar {
        func_name: &'static str,
        args: CoreSearchFunctionArgs,
    },
}

pub(super) enum CoreNamespaceSpecialFunction {
    CurrentCatalog,
    CurrentDatabase,
}

pub(super) enum CoreSessionSpecialFunction {
    Version,
    ConnectionId,
    ClientSessionId,
}

pub(super) enum CoreAuthorizationSpecialFunction {
    User,
    Role,
    SecondaryRoles,
    AvailableRoles,
}

impl CoreNamespaceSpecialFunction {
    fn type_check_function(&self) -> TypeCheckNamespaceFunction {
        match self {
            CoreNamespaceSpecialFunction::CurrentCatalog => {
                TypeCheckNamespaceFunction::CurrentCatalog
            }
            CoreNamespaceSpecialFunction::CurrentDatabase => {
                TypeCheckNamespaceFunction::CurrentDatabase
            }
        }
    }
}

impl CoreSessionSpecialFunction {
    fn type_check_function(&self) -> TypeCheckSessionFunction<'static> {
        match self {
            CoreSessionSpecialFunction::Version => TypeCheckSessionFunction::Version,
            CoreSessionSpecialFunction::ConnectionId => TypeCheckSessionFunction::ConnectionId,
            CoreSessionSpecialFunction::ClientSessionId => {
                TypeCheckSessionFunction::ClientSessionId
            }
        }
    }
}

impl CoreAuthorizationSpecialFunction {
    fn type_check_function(&self) -> TypeCheckAuthorizationFunction {
        match self {
            CoreAuthorizationSpecialFunction::User => TypeCheckAuthorizationFunction::CurrentUser,
            CoreAuthorizationSpecialFunction::Role => TypeCheckAuthorizationFunction::CurrentRole,
            CoreAuthorizationSpecialFunction::SecondaryRoles => {
                TypeCheckAuthorizationFunction::CurrentSecondaryRoles
            }
            CoreAuthorizationSpecialFunction::AvailableRoles => {
                TypeCheckAuthorizationFunction::CurrentAvailableRoles
            }
        }
    }
}

impl<'a> TypeChecker<'a, super::FullTypeCheckAdapter> {
    pub fn all_special_functions() -> &'static [Ascii<&'static str>] {
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
            Ascii::new("hex_decode_string"),
            Ascii::new("base64_decode_string"),
            Ascii::new("try_hex_decode_string"),
            Ascii::new("try_base64_decode_string"),
        ];
        FUNCTIONS
    }
}

impl<'a> CoreExprArena<'a> {
    pub(super) fn special_function(
        &mut self,
        span: Span,
        func_name: &str,
        args: &'a [Expr],
    ) -> Result<Option<CoreExprId>> {
        let function = match func_name {
            "current_catalog" => {
                check_function_arity(span, func_name, args.len(), 0, Some(0))?;
                CoreSpecialFunction::Namespace(CoreNamespaceSpecialFunction::CurrentCatalog)
            }
            "database" => {
                check_function_arity(span, func_name, args.len(), 0, Some(0))?;
                CoreSpecialFunction::Namespace(CoreNamespaceSpecialFunction::CurrentDatabase)
            }
            "currentdatabase" => {
                check_function_arity(span, func_name, args.len(), 0, Some(0))?;
                CoreSpecialFunction::Namespace(CoreNamespaceSpecialFunction::CurrentDatabase)
            }
            "current_database" => {
                check_function_arity(span, func_name, args.len(), 0, Some(0))?;
                CoreSpecialFunction::Namespace(CoreNamespaceSpecialFunction::CurrentDatabase)
            }
            "version" => {
                check_function_arity(span, func_name, args.len(), 0, Some(0))?;
                CoreSpecialFunction::Session(CoreSessionSpecialFunction::Version)
            }
            "connection_id" => {
                check_function_arity(span, func_name, args.len(), 0, Some(0))?;
                CoreSpecialFunction::Session(CoreSessionSpecialFunction::ConnectionId)
            }
            "client_session_id" => {
                check_function_arity(span, func_name, args.len(), 0, Some(0))?;
                CoreSpecialFunction::Session(CoreSessionSpecialFunction::ClientSessionId)
            }
            "user" => {
                check_function_arity(span, func_name, args.len(), 0, Some(0))?;
                CoreSpecialFunction::Authorization(CoreAuthorizationSpecialFunction::User)
            }
            "currentuser" => {
                check_function_arity(span, func_name, args.len(), 0, Some(0))?;
                CoreSpecialFunction::Authorization(CoreAuthorizationSpecialFunction::User)
            }
            "current_user" => {
                check_function_arity(span, func_name, args.len(), 0, Some(0))?;
                CoreSpecialFunction::Authorization(CoreAuthorizationSpecialFunction::User)
            }
            "current_role" => {
                check_function_arity(span, func_name, args.len(), 0, Some(0))?;
                CoreSpecialFunction::Authorization(CoreAuthorizationSpecialFunction::Role)
            }
            "current_secondary_roles" => {
                check_function_arity(span, func_name, args.len(), 0, Some(0))?;
                CoreSpecialFunction::Authorization(CoreAuthorizationSpecialFunction::SecondaryRoles)
            }
            "current_available_roles" => {
                check_function_arity(span, func_name, args.len(), 0, Some(0))?;
                CoreSpecialFunction::Authorization(CoreAuthorizationSpecialFunction::AvailableRoles)
            }
            "timezone" => {
                check_function_arity(span, func_name, args.len(), 0, Some(0))?;
                CoreSpecialFunction::Timezone
            }
            "last_query_id" => CoreSpecialFunction::LastQueryId {
                args: self.lower_checked_special_args(span, func_name, args, 0, Some(1))?,
            },
            "coalesce" => CoreSpecialFunction::Coalesce {
                args: self.lower_checked_special_args(span, func_name, args, 1, None)?,
            },
            "decode" => CoreSpecialFunction::Decode {
                args: self.lower_checked_special_args(span, func_name, args, 3, None)?,
            },
            "array_sort" => CoreSpecialFunction::ArraySort {
                args: self.lower_checked_special_args(span, func_name, args, 1, Some(3))?,
            },
            "array_aggregate" => CoreSpecialFunction::ArrayAggregate {
                args: self.lower_checked_special_args(span, func_name, args, 2, Some(2))?,
            },
            "to_variant" => CoreSpecialFunction::CastToVariant {
                func_name: "to_variant",
                args: self.lower_checked_special_args(span, func_name, args, 1, Some(1))?,
                is_try: false,
            },
            "try_to_variant" => CoreSpecialFunction::CastToVariant {
                func_name: "try_to_variant",
                args: self.lower_checked_special_args(span, func_name, args, 1, Some(1))?,
                is_try: true,
            },
            "greatest" => CoreSpecialFunction::GreatestOrLeast {
                func_name: "greatest",
                args: self.lower_checked_special_args(span, func_name, args, 1, None)?,
                ignore_nulls: false,
            },
            "least" => CoreSpecialFunction::GreatestOrLeast {
                func_name: "least",
                args: self.lower_checked_special_args(span, func_name, args, 1, None)?,
                ignore_nulls: false,
            },
            "greatest_ignore_nulls" => CoreSpecialFunction::GreatestOrLeast {
                func_name: "greatest_ignore_nulls",
                args: self.lower_checked_special_args(span, func_name, args, 1, None)?,
                ignore_nulls: true,
            },
            "least_ignore_nulls" => CoreSpecialFunction::GreatestOrLeast {
                func_name: "least_ignore_nulls",
                args: self.lower_checked_special_args(span, func_name, args, 1, None)?,
                ignore_nulls: true,
            },
            "getvariable" => CoreSpecialFunction::GetVariable {
                args: self.lower_checked_special_args(span, func_name, args, 1, Some(1))?,
            },
            "hex_decode_string" => CoreSpecialFunction::DecodeString {
                func_name: "hex_decode_string",
                args: self.lower_checked_special_args(span, func_name, args, 1, Some(1))?,
            },
            "try_hex_decode_string" => CoreSpecialFunction::DecodeString {
                func_name: "try_hex_decode_string",
                args: self.lower_checked_special_args(span, func_name, args, 1, Some(1))?,
            },
            "base64_decode_string" => CoreSpecialFunction::DecodeString {
                func_name: "base64_decode_string",
                args: self.lower_checked_special_args(span, func_name, args, 1, Some(1))?,
            },
            "try_base64_decode_string" => CoreSpecialFunction::DecodeString {
                func_name: "try_base64_decode_string",
                args: self.lower_checked_special_args(span, func_name, args, 1, Some(1))?,
            },
            "stream_has_data" => CoreSpecialFunction::Scalar {
                func_name: "stream_has_data",
                args: self.lower_checked_special_args(span, func_name, args, 1, Some(1))?,
            },
            _ => return Ok(None),
        };
        Ok(Some(
            self.alloc(CoreExpr::SpecialFunction { span, function }),
        ))
    }

    fn lower_checked_special_args(
        &mut self,
        span: Span,
        func_name: &str,
        args: &'a [Expr],
        min_args: usize,
        max_args: Option<usize>,
    ) -> Result<CoreSearchFunctionArgs> {
        check_function_arity(span, func_name, args.len(), min_args, max_args)?;
        self.lower_display_expr_args(args)
    }
}

fn invalid_lowered_special_function(func_name: &str) -> ErrorCode {
    ErrorCode::Internal(format!(
        "special function {func_name} should have been validated before resolving"
    ))
}

impl CoreSpecialFunction {
    pub(super) fn requires_full_context(&self) -> bool {
        matches!(
            self,
            CoreSpecialFunction::Namespace(_)
                | CoreSpecialFunction::Session(_)
                | CoreSpecialFunction::Authorization(_)
                | CoreSpecialFunction::LastQueryId { .. }
                | CoreSpecialFunction::GetVariable { .. }
        )
    }

    pub(super) fn resolve<'tc, A>(
        &self,
        type_checker: &mut TypeChecker<'tc, A>,
        arena: &CoreExprArena<'_>,
        span: Span,
    ) -> Result<Box<(ScalarExpr, DataType)>>
    where
        A: super::TypeCheckAdapter,
    {
        type_checker.resolve_special_function(arena, span, self)
    }
}

impl<'a, A> TypeChecker<'a, A>
where A: super::TypeCheckAdapter
{
    fn resolve_special_function(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        function: &CoreSpecialFunction,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        match function {
            CoreSpecialFunction::Namespace(namespace_function) => self.resolve_special_literal(
                span,
                self.adapter
                    .resolve_namespace_function(namespace_function.type_check_function())?,
            ),
            CoreSpecialFunction::Session(session_function) => self.resolve_special_literal(
                span,
                self.adapter
                    .resolve_session_function(session_function.type_check_function())?,
            ),
            CoreSpecialFunction::Authorization(authorization_function) => self
                .resolve_special_literal(
                    span,
                    self.adapter.resolve_authorization_function(
                        authorization_function.type_check_function(),
                    )?,
                ),
            CoreSpecialFunction::Timezone => self.resolve_special_literal(
                span,
                Scalar::String(self.adapter.settings().get_timezone().unwrap()),
            ),
            CoreSpecialFunction::LastQueryId { args } => {
                let args = self.resolve_special_args(arena, args)?;
                self.resolve_last_query_id(span, &args)
            }
            CoreSpecialFunction::Coalesce { args } => {
                let args = self.resolve_special_args(arena, args)?;
                self.resolve_coalesce(span, &args)
            }
            CoreSpecialFunction::Decode { args } => {
                let args = self.resolve_special_args(arena, args)?;
                self.resolve_decode(span, &args)
            }
            CoreSpecialFunction::ArraySort { args } => {
                let args = self.resolve_special_args(arena, args)?;
                self.resolve_array_sort(span, &args)
            }
            CoreSpecialFunction::ArrayAggregate { args } => {
                let args = self.resolve_special_args(arena, args)?;
                self.resolve_array_aggregate(span, &args)
            }
            CoreSpecialFunction::CastToVariant {
                func_name,
                args,
                is_try,
            } => {
                let args = self.resolve_special_args(arena, args)?;
                let [(_, scalar, data_type)] = args.as_slice() else {
                    return Err(invalid_lowered_special_function(func_name));
                };
                self.resolve_cast_to_variant(span, data_type, scalar, *is_try)
                    .unwrap_or_else(|| {
                        self.resolve_scalar_function_call(span, func_name, vec![], vec![
                            scalar.clone(),
                        ])
                    })
            }
            CoreSpecialFunction::GreatestOrLeast {
                func_name,
                args,
                ignore_nulls,
            } => {
                let args = self.resolve_special_args(arena, args)?;
                self.resolve_greatest_or_least(span, func_name, &args, *ignore_nulls)
            }
            CoreSpecialFunction::GetVariable { args } => {
                let args = self.resolve_special_args(arena, args)?;
                let [(_, scalar, _)] = args.as_slice() else {
                    return Err(invalid_lowered_special_function("getvariable"));
                };
                self.resolve_get_variable(span, scalar)
            }
            CoreSpecialFunction::DecodeString { func_name, args } => {
                let args = self.resolve_special_args(arena, args)?;
                let [(_, scalar, _)] = args.as_slice() else {
                    return Err(invalid_lowered_special_function(func_name));
                };
                self.resolve_decode_string(span, func_name, scalar)
            }
            CoreSpecialFunction::Scalar { func_name, args } => {
                let args = self.resolve_special_args(arena, args)?;
                self.resolve_special_scalar_function(span, func_name, &args)
            }
        }
    }

    fn resolve_special_args<'f>(
        &mut self,
        arena: &CoreExprArena<'_>,
        args: &'f CoreSearchFunctionArgs,
    ) -> Result<Vec<(&'f str, ScalarExpr, DataType)>> {
        let mut resolved_args = Vec::with_capacity(args.len());
        for (display, arg) in args {
            let box (scalar, data_type) = self.resolve_core(arena, *arg)?;
            resolved_args.push((display.as_str(), scalar, data_type));
        }
        Ok(resolved_args)
    }

    fn resolve_special_scalar_function(
        &mut self,
        span: Span,
        func_name: &str,
        args: &[(&str, ScalarExpr, DataType)],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let scalars = args.iter().map(|(_, scalar, _)| scalar.clone()).collect();
        self.resolve_scalar_function_call(span, func_name, vec![], scalars)
    }

    fn resolve_special_literal(
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

    fn resolve_last_query_id(
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
        self.resolve_special_literal(
            span,
            self.adapter
                .resolve_session_function(TypeCheckSessionFunction::LastQueryId(index as i32))?,
        )
    }

    fn resolve_coalesce(
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

    fn resolve_decode(
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

    fn resolve_array_sort(
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

    fn resolve_array_aggregate(
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

    fn resolve_greatest_or_least(
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

    fn resolve_get_variable(
        &self,
        span: Span,
        scalar: &ScalarExpr,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if let Ok(arg) = ConstantExpr::try_from(scalar.clone())
            && let Scalar::String(var_name) = arg.value
        {
            let var_value = self
                .adapter
                .resolve_session_function(TypeCheckSessionFunction::Variable(&var_name))?;
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

    fn resolve_decode_string(
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
