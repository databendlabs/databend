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

use super::AuthFunction;
use super::CoreDisplayExprArg;
use super::CoreDisplayExprArgs;
use super::CoreExpr;
use super::CoreExprArena;
use super::CoreExprId;
use super::NamespaceFunction;
use super::SessionFunction;
use super::TypeChecker;
use super::function_arity::check_function_arity;
use crate::TypeCheckAdapter;
use crate::plans::CastExpr;
use crate::plans::ConstantExpr;
use crate::plans::ScalarExpr;

pub(super) enum SpecialFunction {
    Namespace(NamespaceFunction),
    Session(SessionFunction<'static>),
    Auth(AuthFunction),
    Timezone,
    LastQueryId {
        arg: Option<CoreDisplayExprArg>,
    },
    Coalesce {
        args: CoreDisplayExprArgs,
    },
    Decode {
        args: CoreDisplayExprArgs,
    },
    ArraySort {
        array: CoreDisplayExprArg,
        sort_order: Option<CoreDisplayExprArg>,
        nulls_order: Option<CoreDisplayExprArg>,
    },
    ArrayAggregate {
        array: CoreDisplayExprArg,
        function: CoreDisplayExprArg,
    },
    CastToVariant {
        func_name: &'static str,
        arg: CoreDisplayExprArg,
        is_try: bool,
    },
    GreatestOrLeast {
        func_name: &'static str,
        args: CoreDisplayExprArgs,
        ignore_nulls: bool,
    },
    GetVariable {
        arg: CoreDisplayExprArg,
    },
    DecodeString {
        func_name: &'static str,
        arg: CoreDisplayExprArg,
    },
    Scalar {
        func_name: &'static str,
        arg: CoreDisplayExprArg,
    },
}

impl<'a, A> TypeChecker<'a, A> {
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
                SpecialFunction::Namespace(NamespaceFunction::CurrentCatalog)
            }
            "database" => {
                check_function_arity(span, func_name, args.len(), 0, Some(0))?;
                SpecialFunction::Namespace(NamespaceFunction::CurrentDatabase)
            }
            "currentdatabase" => {
                check_function_arity(span, func_name, args.len(), 0, Some(0))?;
                SpecialFunction::Namespace(NamespaceFunction::CurrentDatabase)
            }
            "current_database" => {
                check_function_arity(span, func_name, args.len(), 0, Some(0))?;
                SpecialFunction::Namespace(NamespaceFunction::CurrentDatabase)
            }
            "version" => {
                check_function_arity(span, func_name, args.len(), 0, Some(0))?;
                SpecialFunction::Session(SessionFunction::Version)
            }
            "connection_id" => {
                check_function_arity(span, func_name, args.len(), 0, Some(0))?;
                SpecialFunction::Session(SessionFunction::ConnectionId)
            }
            "client_session_id" => {
                check_function_arity(span, func_name, args.len(), 0, Some(0))?;
                SpecialFunction::Session(SessionFunction::ClientSessionId)
            }
            "user" => {
                check_function_arity(span, func_name, args.len(), 0, Some(0))?;
                SpecialFunction::Auth(AuthFunction::CurrentUser)
            }
            "currentuser" => {
                check_function_arity(span, func_name, args.len(), 0, Some(0))?;
                SpecialFunction::Auth(AuthFunction::CurrentUser)
            }
            "current_user" => {
                check_function_arity(span, func_name, args.len(), 0, Some(0))?;
                SpecialFunction::Auth(AuthFunction::CurrentUser)
            }
            "current_role" => {
                check_function_arity(span, func_name, args.len(), 0, Some(0))?;
                SpecialFunction::Auth(AuthFunction::CurrentRole)
            }
            "current_secondary_roles" => {
                check_function_arity(span, func_name, args.len(), 0, Some(0))?;
                SpecialFunction::Auth(AuthFunction::CurrentSecondaryRoles)
            }
            "current_available_roles" => {
                check_function_arity(span, func_name, args.len(), 0, Some(0))?;
                SpecialFunction::Auth(AuthFunction::CurrentAvailableRoles)
            }
            "timezone" => {
                check_function_arity(span, func_name, args.len(), 0, Some(0))?;
                SpecialFunction::Timezone
            }
            "last_query_id" => {
                check_function_arity(span, func_name, args.len(), 0, Some(1))?;
                SpecialFunction::LastQueryId {
                    arg: args
                        .first()
                        .map(|arg| self.lower_display_expr_arg(arg))
                        .transpose()?,
                }
            }
            "coalesce" => {
                check_function_arity(span, func_name, args.len(), 1, None)?;
                SpecialFunction::Coalesce {
                    args: self.lower_display_expr_args(args)?,
                }
            }
            "decode" => {
                check_function_arity(span, func_name, args.len(), 3, None)?;
                SpecialFunction::Decode {
                    args: self.lower_display_expr_args(args)?,
                }
            }
            "array_sort" => {
                check_function_arity(span, func_name, args.len(), 1, Some(3))?;
                SpecialFunction::ArraySort {
                    array: self.lower_display_expr_arg(&args[0])?,
                    sort_order: args
                        .get(1)
                        .map(|arg| self.lower_display_expr_arg(arg))
                        .transpose()?,
                    nulls_order: args
                        .get(2)
                        .map(|arg| self.lower_display_expr_arg(arg))
                        .transpose()?,
                }
            }
            "array_aggregate" => {
                check_function_arity(span, func_name, args.len(), 2, Some(2))?;
                SpecialFunction::ArrayAggregate {
                    array: self.lower_display_expr_arg(&args[0])?,
                    function: self.lower_display_expr_arg(&args[1])?,
                }
            }
            "to_variant" => {
                check_function_arity(span, func_name, args.len(), 1, Some(1))?;
                SpecialFunction::CastToVariant {
                    func_name: "to_variant",
                    arg: self.lower_display_expr_arg(&args[0])?,
                    is_try: false,
                }
            }
            "try_to_variant" => {
                check_function_arity(span, func_name, args.len(), 1, Some(1))?;
                SpecialFunction::CastToVariant {
                    func_name: "try_to_variant",
                    arg: self.lower_display_expr_arg(&args[0])?,
                    is_try: true,
                }
            }
            "greatest" => {
                check_function_arity(span, func_name, args.len(), 1, None)?;
                SpecialFunction::GreatestOrLeast {
                    func_name: "greatest",
                    args: self.lower_display_expr_args(args)?,
                    ignore_nulls: false,
                }
            }
            "least" => {
                check_function_arity(span, func_name, args.len(), 1, None)?;
                SpecialFunction::GreatestOrLeast {
                    func_name: "least",
                    args: self.lower_display_expr_args(args)?,
                    ignore_nulls: false,
                }
            }
            "greatest_ignore_nulls" => {
                check_function_arity(span, func_name, args.len(), 1, None)?;
                SpecialFunction::GreatestOrLeast {
                    func_name: "greatest_ignore_nulls",
                    args: self.lower_display_expr_args(args)?,
                    ignore_nulls: true,
                }
            }
            "least_ignore_nulls" => {
                check_function_arity(span, func_name, args.len(), 1, None)?;
                SpecialFunction::GreatestOrLeast {
                    func_name: "least_ignore_nulls",
                    args: self.lower_display_expr_args(args)?,
                    ignore_nulls: true,
                }
            }
            "getvariable" => {
                check_function_arity(span, func_name, args.len(), 1, Some(1))?;
                SpecialFunction::GetVariable {
                    arg: self.lower_display_expr_arg(&args[0])?,
                }
            }
            "hex_decode_string" => {
                check_function_arity(span, func_name, args.len(), 1, Some(1))?;
                SpecialFunction::DecodeString {
                    func_name: "hex_decode_string",
                    arg: self.lower_display_expr_arg(&args[0])?,
                }
            }
            "try_hex_decode_string" => {
                check_function_arity(span, func_name, args.len(), 1, Some(1))?;
                SpecialFunction::DecodeString {
                    func_name: "try_hex_decode_string",
                    arg: self.lower_display_expr_arg(&args[0])?,
                }
            }
            "base64_decode_string" => {
                check_function_arity(span, func_name, args.len(), 1, Some(1))?;
                SpecialFunction::DecodeString {
                    func_name: "base64_decode_string",
                    arg: self.lower_display_expr_arg(&args[0])?,
                }
            }
            "try_base64_decode_string" => {
                check_function_arity(span, func_name, args.len(), 1, Some(1))?;
                SpecialFunction::DecodeString {
                    func_name: "try_base64_decode_string",
                    arg: self.lower_display_expr_arg(&args[0])?,
                }
            }
            "stream_has_data" => {
                check_function_arity(span, func_name, args.len(), 1, Some(1))?;
                SpecialFunction::Scalar {
                    func_name: "stream_has_data",
                    arg: self.lower_display_expr_arg(&args[0])?,
                }
            }
            _ => return Ok(None),
        };
        Ok(Some(
            self.alloc(CoreExpr::SpecialFunction { span, function }),
        ))
    }
}

impl SpecialFunction {
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
where A: TypeCheckAdapter
{
    fn resolve_special_function(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        function: &SpecialFunction,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        match function {
            SpecialFunction::Namespace(namespace_function) => self.resolve_special_literal(
                span,
                self.adapter
                    .resolve_namespace_function(*namespace_function)?,
            ),
            SpecialFunction::Session(session_function) => self.resolve_special_literal(
                span,
                self.adapter.resolve_session_function(*session_function)?,
            ),
            SpecialFunction::Auth(authorization_function) => self.resolve_special_literal(
                span,
                self.adapter
                    .resolve_authorization_function(*authorization_function)?,
            ),
            SpecialFunction::Timezone => self.resolve_special_literal(
                span,
                Scalar::String(self.adapter.settings().get_timezone().unwrap()),
            ),
            SpecialFunction::LastQueryId { arg } => {
                let scalar = match arg {
                    Some((_, arg)) => {
                        let box (scalar, _) = self.resolve_core(arena, *arg)?;
                        Some(scalar)
                    }
                    None => None,
                };
                self.resolve_last_query_id(span, scalar.as_ref())
            }
            SpecialFunction::Coalesce { args } => {
                let args = self.resolve_special_args(arena, args)?;
                self.resolve_coalesce(span, &args)
            }
            SpecialFunction::Decode { args } => {
                let args = self.resolve_special_args(arena, args)?;
                self.resolve_decode(span, &args)
            }
            SpecialFunction::ArraySort {
                array,
                sort_order,
                nulls_order,
            } => {
                let (_, array, _) = self.resolve_display_arg(arena, array)?;
                let sort_order = self.resolve_optional_display_arg(arena, sort_order.as_ref())?;
                let nulls_order = self.resolve_optional_display_arg(arena, nulls_order.as_ref())?;
                self.resolve_array_sort(span, &array, sort_order.as_ref(), nulls_order.as_ref())
            }
            SpecialFunction::ArrayAggregate { array, function } => {
                let (_, array, _) = self.resolve_display_arg(arena, array)?;
                let (_, function, _) = self.resolve_display_arg(arena, function)?;
                self.resolve_array_aggregate(span, &array, &function)
            }
            SpecialFunction::CastToVariant {
                func_name,
                arg,
                is_try,
            } => {
                let (_, scalar, data_type) = self.resolve_display_arg(arena, arg)?;
                self.resolve_cast_to_variant(span, &data_type, &scalar, *is_try)
                    .unwrap_or_else(|| {
                        self.resolve_scalar_function_call(span, func_name, vec![], vec![
                            scalar.clone(),
                        ])
                    })
            }
            SpecialFunction::GreatestOrLeast {
                func_name,
                args,
                ignore_nulls,
            } => {
                let args = self.resolve_special_args(arena, args)?;
                self.resolve_greatest_or_least(span, func_name, &args, *ignore_nulls)
            }
            SpecialFunction::GetVariable { arg } => {
                let (_, scalar, _) = self.resolve_display_arg(arena, arg)?;
                self.resolve_get_variable(span, &scalar)
            }
            SpecialFunction::DecodeString { func_name, arg } => {
                let (_, scalar, _) = self.resolve_display_arg(arena, arg)?;
                self.resolve_decode_string(span, func_name, &scalar)
            }
            SpecialFunction::Scalar { func_name, arg } => {
                let (_, scalar, _) = self.resolve_display_arg(arena, arg)?;
                self.resolve_scalar_function_call(span, func_name, vec![], vec![scalar])
            }
        }
    }

    fn resolve_display_arg<'f>(
        &mut self,
        arena: &CoreExprArena<'_>,
        arg: &'f CoreDisplayExprArg,
    ) -> Result<(&'f str, ScalarExpr, DataType)> {
        let (display, arg) = arg;
        let box (scalar, data_type) = self.resolve_core(arena, *arg)?;
        Ok((display.as_str(), scalar, data_type))
    }

    fn resolve_optional_display_arg(
        &mut self,
        arena: &CoreExprArena<'_>,
        arg: Option<&CoreDisplayExprArg>,
    ) -> Result<Option<ScalarExpr>> {
        arg.map(|arg| {
            self.resolve_display_arg(arena, arg)
                .map(|(_, scalar, _)| scalar)
        })
        .transpose()
    }

    fn resolve_special_args<'f>(
        &mut self,
        arena: &CoreExprArena<'_>,
        args: &'f CoreDisplayExprArgs,
    ) -> Result<Vec<(&'f str, ScalarExpr, DataType)>> {
        let mut resolved_args = Vec::with_capacity(args.len());
        for (display, arg) in args {
            let box (scalar, data_type) = self.resolve_core(arena, *arg)?;
            resolved_args.push((display.as_str(), scalar, data_type));
        }
        Ok(resolved_args)
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
        scalar: Option<&ScalarExpr>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let index = if let Some(scalar) = scalar {
            let expr = scalar.as_expr()?;
            if expr.as_constant().is_none() {
                return Err(ErrorCode::BadArguments(
                    "last_query_id argument only support constant argument",
                )
                .set_span(span));
            }
            check_number(span, &self.func_ctx, &expr, &BUILTIN_FUNCTIONS)?
        } else {
            -1
        };
        self.resolve_special_literal(
            span,
            self.adapter
                .resolve_session_function(SessionFunction::LastQueryId(index as i32))?,
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
        array: &ScalarExpr,
        sort_order: Option<&ScalarExpr>,
        nulls_order: Option<&ScalarExpr>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let mut asc = true;
        let mut nulls_first = None;
        if let Some(sort_order) = sort_order {
            let Ok(arg) = ConstantExpr::try_from(sort_order.clone()) else {
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
        if let Some(nulls_order) = nulls_order {
            let Ok(arg) = ConstantExpr::try_from(nulls_order.clone()) else {
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
        self.resolve_scalar_function_call(span, func_name, vec![], vec![array.clone()])
    }

    fn resolve_array_aggregate(
        &mut self,
        span: Span,
        array: &ScalarExpr,
        function: &ScalarExpr,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let Ok(arg) = ConstantExpr::try_from(function.clone()) else {
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
        self.resolve_scalar_function_call(span, &func_name, vec![], vec![array.clone()])
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
                .resolve_session_function(SessionFunction::Variable(&var_name))?;
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
