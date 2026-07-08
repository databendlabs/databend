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

use std::collections::HashMap;

use databend_common_ast::Span;
use databend_common_expression::ConstantFolder;
use databend_common_expression::Domain;
use databend_common_expression::ExprVisitor;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::Scalar;
use databend_common_expression::conversion::classify_conversion;
use databend_common_expression::expr::*;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::visit_expr;
use databend_common_expression::with_integer_mapped_type;
use databend_common_functions::BUILTIN_FUNCTIONS;

pub(super) struct RewriteVisitor<'a> {
    pub input_domains: HashMap<String, Domain>,
    pub func_ctx: &'a FunctionContext,
    pub fn_registry: &'a FunctionRegistry,
}

type RewriteResult = std::result::Result<Option<Expr<String>>, !>;

impl ExprVisitor<String> for RewriteVisitor<'_> {
    fn enter_function_call(&mut self, call: &FunctionCall<String>) -> RewriteResult {
        if call.id.name() == "eq" {
            let result = match call.args.as_slice() {
                [Expr::Cast(cast), Expr::Constant(constant)]
                | [Expr::Constant(constant), Expr::Cast(cast)]
                    if self.check_no_throw(cast) =>
                {
                    self.try_rewrite(call.span, cast, constant.clone())?
                }
                [Expr::ColumnRef(column), expr] | [expr, Expr::ColumnRef(column)] => {
                    self.try_rewrite_integer_column_string_constant(call.span, column, expr)?
                }
                _ => None,
            };
            if result.is_some() {
                return Ok(result);
            }
        }
        match Self::visit_function_call(call, self)? {
            Some(Expr::FunctionCall(func)) => {
                let name = func.id.name();
                match check_function(
                    func.span,
                    name.as_ref(),
                    func.id.params(),
                    &func.args,
                    self.fn_registry,
                ) {
                    Ok(expr) => Ok(Some(expr)),
                    Err(_) => Ok(None),
                }
            }
            result => Ok(result),
        }
    }
}

impl RewriteVisitor<'_> {
    fn try_rewrite(&self, span: Span, cast: &Cast<String>, constant: Constant) -> RewriteResult {
        if cast.is_try {
            return Ok(None);
        }

        let Cast {
            expr, dest_type, ..
        } = cast;
        let src_type = expr.data_type();
        if !classify_conversion(src_type, dest_type).is_lossless_injective() {
            return Ok(None);
        }

        let Some(scalar) = cast_const(self.func_ctx, src_type.to_owned(), constant.clone()) else {
            return Ok(None);
        };
        let constant = Constant {
            span: None,
            scalar,
            data_type: src_type.clone(),
        };

        match expr.as_cast() {
            Some(cast) => self.try_rewrite(span, cast, constant),
            None => {
                let Ok(func_expr) = check_function(
                    span,
                    "eq",
                    &[],
                    &[(**expr).clone(), constant.into()],
                    self.fn_registry,
                ) else {
                    return Ok(None);
                };

                Ok(Some(
                    ConstantFolder::fold_with_domain(
                        &func_expr,
                        &self.input_domains,
                        self.func_ctx,
                        self.fn_registry,
                    )
                    .0,
                ))
            }
        }
    }

    fn try_rewrite_integer_column_string_constant(
        &self,
        span: Span,
        column: &ColumnRef<String>,
        expr: &Expr<String>,
    ) -> RewriteResult {
        let Some(constant) = self.constant_from_expr(expr) else {
            return Ok(None);
        };
        let Some(scalar) = cast_integer_string_constant(
            self.func_ctx,
            &column.data_type,
            &constant.data_type,
            &constant.scalar,
        ) else {
            return Ok(None);
        };
        let constant = Constant {
            span: None,
            scalar,
            data_type: column.data_type.clone(),
        };

        let Ok(func_expr) = check_function(
            span,
            "eq",
            &[],
            &[column.clone().into(), constant.into()],
            self.fn_registry,
        ) else {
            return Ok(None);
        };

        Ok(Some(
            ConstantFolder::fold_with_domain(
                &func_expr,
                &self.input_domains,
                self.func_ctx,
                self.fn_registry,
            )
            .0,
        ))
    }

    fn constant_from_expr(&self, expr: &Expr<String>) -> Option<Constant> {
        match expr {
            Expr::Constant(constant) => Some(constant.clone()),
            Expr::Cast(cast) if !cast.is_try => {
                let Expr::Constant(constant) = cast.expr.as_ref() else {
                    return None;
                };
                let scalar = cast_const(self.func_ctx, cast.dest_type.clone(), constant.clone())?;
                Some(Constant {
                    span: None,
                    scalar,
                    data_type: cast.dest_type.clone(),
                })
            }
            _ => None,
        }
    }

    fn check_no_throw(&self, cast: &Cast<String>) -> bool {
        if cast.is_try {
            return false;
        }

        // check domain for possible overflow
        ConstantFolder::<String>::fold_with_domain(
            &cast.clone().into(),
            &self.input_domains,
            self.func_ctx,
            &BUILTIN_FUNCTIONS,
        )
        .1
        .is_some()
    }
}

pub(super) fn cast_const(
    func_ctx: &FunctionContext,
    dest_type: DataType,
    constant: Constant,
) -> Option<Scalar> {
    let (_, Some(domain)) = ConstantFolder::<String>::fold(
        &Cast {
            span: None,
            is_try: false,
            expr: Box::new(constant.into()),
            dest_type,
        }
        .into(),
        func_ctx,
        &BUILTIN_FUNCTIONS,
    ) else {
        return None;
    };

    domain.as_singleton()
}

fn cast_integer_string_constant(
    func_ctx: &FunctionContext,
    column_type: &DataType,
    scalar_type: &DataType,
    scalar: &Scalar,
) -> Option<Scalar> {
    if scalar.is_null() || scalar_type.remove_nullable() != DataType::String {
        return None;
    }

    let DataType::Number(num_ty) = column_type.remove_nullable() else {
        return None;
    };
    if !num_ty.is_integer() || !string_scalar_parses_as_integer_type(scalar, &num_ty) {
        return None;
    }

    let scalar = cast_const(func_ctx, column_type.clone(), Constant {
        span: None,
        scalar: scalar.clone(),
        data_type: scalar_type.clone(),
    })?;
    (!scalar.is_null()).then_some(scalar)
}

fn string_scalar_parses_as_integer_type(scalar: &Scalar, num_ty: &NumberDataType) -> bool {
    let Some(value) = scalar.as_string() else {
        return false;
    };

    with_integer_mapped_type!(|NUM_TYPE| match num_ty {
        NumberDataType::NUM_TYPE => value.parse::<NUM_TYPE>().is_ok(),
        _ => false,
    })
}

pub fn eliminate_cast(
    expr: &Expr<String>,
    input_domains: HashMap<String, Domain>,
) -> Option<Expr<String>> {
    let mut visitor = RewriteVisitor {
        input_domains,
        func_ctx: &FunctionContext::default(),
        fn_registry: &BUILTIN_FUNCTIONS,
    };

    visit_expr(expr, &mut visitor).unwrap()
}
