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
use databend_common_ast::ast::BinaryOperator;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FunctionCall as ASTFunctionCall;
use databend_common_ast::ast::Literal;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use smallvec::SmallVec;
use smallvec::smallvec;
use unicase::Ascii;

use super::TypeChecker;
use crate::plans::ScalarExpr;
use crate::plans::SubqueryType;

#[derive(Clone, Copy)]
pub(super) struct CoreExprId {
    index: usize,
}

pub(super) type CoreExprArgs = SmallVec<[CoreExprId; 4]>;
pub(super) type SugarFunctionArgs<'a> = SmallVec<[&'a Expr; 4]>;

pub(super) struct CoreExprArena<'a> {
    nodes: Vec<CoreExpr<'a>>,
}

impl<'a> CoreExprArena<'a> {
    pub(super) fn new() -> Self {
        Self { nodes: Vec::new() }
    }

    pub(super) fn legacy_ast(&mut self, expr: &'a Expr) -> CoreExprId {
        self.alloc(CoreExpr::LegacyAst(expr))
    }

    pub(super) fn lower_ast_expr(&mut self, expr: &'a Expr) -> CoreExprId {
        match expr {
            Expr::Literal { span, value } => self.literal(*span, value.clone()),
            Expr::IsNull {
                span,
                expr: child,
                not,
            } => {
                let child = self.lower_ast_expr(child);
                let is_not_null = self.call(*span, "is_not_null", smallvec![child]);
                if *not {
                    is_not_null
                } else {
                    self.call(*span, "not", smallvec![is_not_null])
                }
            }
            Expr::BinaryOp {
                span,
                op: op @ (BinaryOperator::Eq | BinaryOperator::NotEq),
                left,
                right,
            } => {
                let left = self.lower_ast_expr(left);
                let right = self.lower_ast_expr(right);
                self.call(*span, op.to_func_name(), smallvec![left, right])
            }
            Expr::FunctionCall {
                span,
                func:
                    ASTFunctionCall {
                        distinct,
                        name,
                        args,
                        params,
                        order_by,
                        window,
                        lambda,
                    },
            } if !*distinct
                && params.is_empty()
                && order_by.is_empty()
                && window.is_none()
                && lambda.is_none()
                && {
                    let func_name = normalized_func_name(&name.name);
                    TypeChecker::all_sugar_functions().contains(&Ascii::new(func_name.as_str()))
                } =>
            {
                let func_name = normalized_func_name(&name.name);
                if TypeChecker::can_lower_core_sugar_function(&func_name) {
                    self.lower_sugar_function(*span, &func_name, args.iter().collect())
                } else {
                    self.sugar_function(*span, func_name, args.iter().collect())
                }
            }
            Expr::FunctionCall {
                span,
                func:
                    ASTFunctionCall {
                        distinct,
                        name,
                        args,
                        params,
                        order_by,
                        window,
                        lambda,
                    },
            } if !*distinct
                && params.is_empty()
                && order_by.is_empty()
                && window.is_none()
                && lambda.is_none()
                && {
                    let func_name = normalized_func_name(&name.name);
                    TypeChecker::can_lower_core_scalar_function(&func_name)
                } =>
            {
                let args = args.iter().map(|arg| self.lower_ast_expr(arg)).collect();
                self.call(*span, normalized_func_name(&name.name), args)
            }
            _ => self.legacy_ast(expr),
        }
    }

    pub(super) fn literal(&mut self, span: Span, value: Literal) -> CoreExprId {
        self.alloc(CoreExpr::Literal { span, value })
    }

    pub(super) fn sugar_function(
        &mut self,
        span: Span,
        func_name: impl Into<String>,
        args: SugarFunctionArgs<'a>,
    ) -> CoreExprId {
        self.alloc(CoreExpr::SugarFunction {
            span,
            func_name: func_name.into(),
            args,
        })
    }

    pub(super) fn call(
        &mut self,
        span: Span,
        func_name: impl Into<String>,
        args: CoreExprArgs,
    ) -> CoreExprId {
        self.alloc(CoreExpr::Call {
            span,
            func_name: func_name.into(),
            args,
        })
    }

    fn alloc(&mut self, expr: CoreExpr<'a>) -> CoreExprId {
        let index = self.nodes.len();
        self.nodes.push(expr);
        CoreExprId { index }
    }

    fn get(&self, id: CoreExprId) -> &CoreExpr<'a> {
        &self.nodes[id.index]
    }
}

pub(super) enum CoreExpr<'a> {
    LegacyAst(&'a Expr),
    Literal {
        span: Span,
        value: Literal,
    },
    Call {
        span: Span,
        func_name: String,
        args: CoreExprArgs,
    },
    SugarFunction {
        span: Span,
        func_name: String,
        args: SugarFunctionArgs<'a>,
    },
}

impl<'a> TypeChecker<'a> {
    pub(super) fn resolve_core_function(
        &mut self,
        span: Span,
        func_name: impl Into<String>,
        args: &[&Expr],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let func_name = func_name.into();
        if Self::all_sugar_functions().contains(&Ascii::new(func_name.as_str())) {
            let mut arena = CoreExprArena::new();
            let root = if Self::can_lower_core_sugar_function(&func_name) {
                arena.lower_sugar_function(span, &func_name, args.iter().copied().collect())
            } else {
                arena.sugar_function(span, func_name, args.iter().copied().collect())
            };
            return self.resolve_core(&arena, root);
        }

        let mut arena = CoreExprArena::new();
        let args = args
            .iter()
            .map(|arg| arena.lower_ast_expr(arg))
            .collect::<CoreExprArgs>();
        let root = arena.call(span, func_name, args);
        self.resolve_core(&arena, root)
    }

    pub(super) fn resolve_core(
        &mut self,
        arena: &CoreExprArena<'_>,
        id: CoreExprId,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        match arena.get(id) {
            CoreExpr::LegacyAst(expr) => self.resolve(expr),
            CoreExpr::Literal { span, value } => self.resolve_literal(*span, value),
            CoreExpr::SugarFunction {
                span,
                func_name,
                args,
            } => {
                if let Some(rewritten_func_result) = databend_common_base::runtime::block_on(
                    self.try_rewrite_sugar_function(*span, func_name, args),
                ) {
                    return rewritten_func_result;
                }
                self.resolve_function(*span, func_name, vec![], args)
            }
            CoreExpr::Call {
                span,
                func_name,
                args,
            } => {
                if Self::all_sugar_functions().contains(&Ascii::new(func_name.as_str())) {
                    return Err(ErrorCode::Internal(format!(
                        "sugar function {} should not be represented as core call",
                        func_name
                    )));
                }

                let mut scalars = SmallVec::<[ScalarExpr; 4]>::with_capacity(args.len());
                for arg in args {
                    let box (scalar, _) = self.resolve_core(arena, *arg)?;
                    scalars.push(scalar);
                }

                if self.should_try_rewrite_variant_function(func_name) {
                    let mut arg_types = SmallVec::<[DataType; 4]>::with_capacity(scalars.len());
                    for scalar in &scalars {
                        let mut data_type = scalar.data_type()?;
                        if let ScalarExpr::SubqueryExpr(subquery) = scalar
                            && subquery.typ == SubqueryType::Scalar
                            && !data_type.is_nullable()
                        {
                            data_type = data_type.wrap_nullable();
                        }
                        arg_types.push(data_type);
                    }
                    if let Some(rewritten_variant_expr) =
                        self.try_rewrite_variant_function(*span, func_name, &scalars, &arg_types)
                    {
                        return rewritten_variant_expr;
                    }
                }
                if Self::is_vector_function(func_name) {
                    if let Some(rewritten_vector_expr) =
                        self.try_rewrite_vector_function(*span, func_name, &scalars)
                    {
                        return rewritten_vector_expr;
                    }
                }
                let box (scalar, data_type) = self.resolve_scalar_function_call(
                    *span,
                    func_name,
                    vec![],
                    scalars.into_vec(),
                )?;
                if func_name == "eq" || func_name == "noteq" {
                    self.rewrite_variant_compare_constant(scalar, data_type)
                } else {
                    Ok(Box::new((scalar, data_type)))
                }
            }
        }
    }
}

fn normalized_func_name(func_name: &str) -> String {
    if func_name.chars().any(char::is_uppercase) {
        func_name.to_lowercase()
    } else {
        func_name.to_string()
    }
}
