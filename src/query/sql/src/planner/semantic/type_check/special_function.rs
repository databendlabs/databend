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

pub(super) struct CoreSpecialFunction {
    func_name: &'static str,
    kind: CoreSpecialFunctionKind,
    args: CoreSearchFunctionArgs,
}

pub(super) enum CoreSpecialFunctionKind {
    Namespace(CoreNamespaceSpecialFunction),
    Session(CoreSessionSpecialFunction),
    Authorization(CoreAuthorizationSpecialFunction),
    Timezone,
    LastQueryId,
    Coalesce,
    Decode,
    ArraySort,
    ArrayAggregate,
    CastToVariant { is_try: bool },
    GreatestOrLeast { ignore_nulls: bool },
    GetVariable,
    DecodeString,
    Scalar,
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
        let (func_name, kind, min_args, max_args) = match func_name {
            "current_catalog" => (
                "current_catalog",
                CoreSpecialFunctionKind::Namespace(CoreNamespaceSpecialFunction::CurrentCatalog),
                0,
                Some(0),
            ),
            "database" => (
                "database",
                CoreSpecialFunctionKind::Namespace(CoreNamespaceSpecialFunction::CurrentDatabase),
                0,
                Some(0),
            ),
            "currentdatabase" => (
                "currentdatabase",
                CoreSpecialFunctionKind::Namespace(CoreNamespaceSpecialFunction::CurrentDatabase),
                0,
                Some(0),
            ),
            "current_database" => (
                "current_database",
                CoreSpecialFunctionKind::Namespace(CoreNamespaceSpecialFunction::CurrentDatabase),
                0,
                Some(0),
            ),
            "version" => (
                "version",
                CoreSpecialFunctionKind::Session(CoreSessionSpecialFunction::Version),
                0,
                Some(0),
            ),
            "connection_id" => (
                "connection_id",
                CoreSpecialFunctionKind::Session(CoreSessionSpecialFunction::ConnectionId),
                0,
                Some(0),
            ),
            "client_session_id" => (
                "client_session_id",
                CoreSpecialFunctionKind::Session(CoreSessionSpecialFunction::ClientSessionId),
                0,
                Some(0),
            ),
            "user" => (
                "user",
                CoreSpecialFunctionKind::Authorization(CoreAuthorizationSpecialFunction::User),
                0,
                Some(0),
            ),
            "currentuser" => (
                "currentuser",
                CoreSpecialFunctionKind::Authorization(CoreAuthorizationSpecialFunction::User),
                0,
                Some(0),
            ),
            "current_user" => (
                "current_user",
                CoreSpecialFunctionKind::Authorization(CoreAuthorizationSpecialFunction::User),
                0,
                Some(0),
            ),
            "current_role" => (
                "current_role",
                CoreSpecialFunctionKind::Authorization(CoreAuthorizationSpecialFunction::Role),
                0,
                Some(0),
            ),
            "current_secondary_roles" => (
                "current_secondary_roles",
                CoreSpecialFunctionKind::Authorization(
                    CoreAuthorizationSpecialFunction::SecondaryRoles,
                ),
                0,
                Some(0),
            ),
            "current_available_roles" => (
                "current_available_roles",
                CoreSpecialFunctionKind::Authorization(
                    CoreAuthorizationSpecialFunction::AvailableRoles,
                ),
                0,
                Some(0),
            ),
            "timezone" => ("timezone", CoreSpecialFunctionKind::Timezone, 0, Some(0)),
            "last_query_id" => (
                "last_query_id",
                CoreSpecialFunctionKind::LastQueryId,
                0,
                Some(1),
            ),
            "coalesce" => ("coalesce", CoreSpecialFunctionKind::Coalesce, 1, None),
            "decode" => ("decode", CoreSpecialFunctionKind::Decode, 3, None),
            "array_sort" => ("array_sort", CoreSpecialFunctionKind::ArraySort, 1, Some(3)),
            "array_aggregate" => (
                "array_aggregate",
                CoreSpecialFunctionKind::ArrayAggregate,
                2,
                Some(2),
            ),
            "to_variant" => (
                "to_variant",
                CoreSpecialFunctionKind::CastToVariant { is_try: false },
                1,
                Some(1),
            ),
            "try_to_variant" => (
                "try_to_variant",
                CoreSpecialFunctionKind::CastToVariant { is_try: true },
                1,
                Some(1),
            ),
            "greatest" => (
                "greatest",
                CoreSpecialFunctionKind::GreatestOrLeast {
                    ignore_nulls: false,
                },
                1,
                None,
            ),
            "least" => (
                "least",
                CoreSpecialFunctionKind::GreatestOrLeast {
                    ignore_nulls: false,
                },
                1,
                None,
            ),
            "greatest_ignore_nulls" => (
                "greatest_ignore_nulls",
                CoreSpecialFunctionKind::GreatestOrLeast { ignore_nulls: true },
                1,
                None,
            ),
            "least_ignore_nulls" => (
                "least_ignore_nulls",
                CoreSpecialFunctionKind::GreatestOrLeast { ignore_nulls: true },
                1,
                None,
            ),
            "getvariable" => (
                "getvariable",
                CoreSpecialFunctionKind::GetVariable,
                1,
                Some(1),
            ),
            "hex_decode_string" => (
                "hex_decode_string",
                CoreSpecialFunctionKind::DecodeString,
                1,
                Some(1),
            ),
            "try_hex_decode_string" => (
                "try_hex_decode_string",
                CoreSpecialFunctionKind::DecodeString,
                1,
                Some(1),
            ),
            "base64_decode_string" => (
                "base64_decode_string",
                CoreSpecialFunctionKind::DecodeString,
                1,
                Some(1),
            ),
            "try_base64_decode_string" => (
                "try_base64_decode_string",
                CoreSpecialFunctionKind::DecodeString,
                1,
                Some(1),
            ),
            "stream_has_data" => (
                "stream_has_data",
                CoreSpecialFunctionKind::Scalar,
                1,
                Some(1),
            ),
            _ => return Ok(None),
        };
        check_function_arity(span, func_name, args.len(), min_args, max_args)?;
        let function = CoreSpecialFunction {
            func_name,
            kind,
            args: self.lower_display_expr_args(args)?,
        };
        Ok(Some(
            self.alloc(CoreExpr::SpecialFunction { span, function }),
        ))
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
            self.kind,
            CoreSpecialFunctionKind::Namespace(_)
                | CoreSpecialFunctionKind::Session(_)
                | CoreSpecialFunctionKind::Authorization(_)
                | CoreSpecialFunctionKind::LastQueryId
                | CoreSpecialFunctionKind::GetVariable
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
        let mut resolved_args = Vec::with_capacity(function.args.len());
        for (display, arg) in &function.args {
            let box (scalar, data_type) = self.resolve_core(arena, *arg)?;
            resolved_args.push((display.as_str(), scalar, data_type));
        }

        let args = resolved_args.as_slice();
        match &function.kind {
            CoreSpecialFunctionKind::Namespace(namespace_function) => self.resolve_special_literal(
                span,
                self.adapter
                    .resolve_namespace_function(namespace_function.type_check_function())?,
            ),
            CoreSpecialFunctionKind::Session(session_function) => self.resolve_special_literal(
                span,
                self.adapter
                    .resolve_session_function(session_function.type_check_function())?,
            ),
            CoreSpecialFunctionKind::Authorization(authorization_function) => self
                .resolve_special_literal(
                    span,
                    self.adapter.resolve_authorization_function(
                        authorization_function.type_check_function(),
                    )?,
                ),
            CoreSpecialFunctionKind::Timezone => self.resolve_special_literal(
                span,
                Scalar::String(self.adapter.settings().get_timezone().unwrap()),
            ),
            CoreSpecialFunctionKind::LastQueryId => self.resolve_last_query_id(span, args),
            CoreSpecialFunctionKind::Coalesce => self.resolve_coalesce(span, args),
            CoreSpecialFunctionKind::Decode => self.resolve_decode(span, args),
            CoreSpecialFunctionKind::ArraySort => self.resolve_array_sort(span, args),
            CoreSpecialFunctionKind::ArrayAggregate => self.resolve_array_aggregate(span, args),
            CoreSpecialFunctionKind::CastToVariant { is_try } => {
                let [(_, scalar, data_type)] = args else {
                    return Err(invalid_lowered_special_function(function.func_name));
                };
                self.resolve_cast_to_variant(span, data_type, scalar, *is_try)
                    .unwrap_or_else(|| {
                        self.resolve_scalar_function_call(span, function.func_name, vec![], vec![
                            scalar.clone(),
                        ])
                    })
            }
            CoreSpecialFunctionKind::GreatestOrLeast { ignore_nulls } => {
                self.resolve_greatest_or_least(span, function.func_name, args, *ignore_nulls)
            }
            CoreSpecialFunctionKind::GetVariable => {
                let [(_, scalar, _)] = args else {
                    return Err(invalid_lowered_special_function(function.func_name));
                };
                self.resolve_get_variable(span, scalar)
            }
            CoreSpecialFunctionKind::DecodeString => {
                let [(_, scalar, _)] = args else {
                    return Err(invalid_lowered_special_function(function.func_name));
                };
                self.resolve_decode_string(span, function.func_name, scalar)
            }
            CoreSpecialFunctionKind::Scalar => {
                self.resolve_special_scalar_function(span, function.func_name, args)
            }
        }
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
