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
use databend_common_expression::expr::*;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::DataType;
use databend_common_expression::visit_expr;
use databend_common_functions::BUILTIN_FUNCTIONS;

pub(super) fn is_injective_cast(src: &DataType, dest: &DataType) -> bool {
    if src == dest {
        return true;
    }

    match (src, dest) {
        (DataType::Boolean, DataType::String | DataType::Number(_) | DataType::Decimal(_)) => true,

        (DataType::Number(src), DataType::Number(dest))
            if src.is_integer() && dest.is_integer() =>
        {
            true
        }
        (DataType::Number(src), DataType::Decimal(_)) if src.is_integer() => true,
        (DataType::Number(_), DataType::String) => true,
        (DataType::Decimal(_), DataType::String) => true,

        (DataType::Date, DataType::Timestamp) => true,

        // (_, DataType::Boolean) => false,
        // (DataType::String, _) => false,
        // (DataType::Decimal(_), DataType::Number(_)) => false,
        // (DataType::Number(src), DataType::Number(dest))
        //     if src.is_float() && dest.is_integer() =>
        // {
        //     false
        // }
        (DataType::Nullable(src), DataType::Nullable(dest)) => is_injective_cast(src, dest),
        _ => false,
    }
}

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
                _ => None,
            };
            if result.is_some() {
                return Ok(result);
            }
        }
        Self::visit_function_call(call, self)
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
        if !is_injective_cast(src_type, dest_type) {
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
