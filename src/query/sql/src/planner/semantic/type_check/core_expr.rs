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

use std::marker::PhantomData;

use databend_common_ast::Span;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Literal;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;

use super::TypeChecker;
use crate::plans::ScalarExpr;
use crate::plans::SubqueryType;

pub(super) enum Raw {}

pub(super) struct CoreExprId<C> {
    index: usize,
    _marker: PhantomData<C>,
}

impl<C> Copy for CoreExprId<C> {}

impl<C> Clone for CoreExprId<C> {
    fn clone(&self) -> Self {
        *self
    }
}

pub(super) struct CoreExprArena<'a> {
    nodes: Vec<CoreExpr<'a>>,
}

impl<'a> CoreExprArena<'a> {
    pub(super) fn new() -> Self {
        Self { nodes: Vec::new() }
    }

    pub(super) fn ast(&mut self, expr: &'a Expr) -> CoreExprId<Raw> {
        self.alloc(CoreExpr::Ast(expr))
    }

    pub(super) fn literal(&mut self, span: Span, value: Literal) -> CoreExprId<Raw> {
        self.alloc(CoreExpr::Literal { span, value })
    }

    pub(super) fn call(
        &mut self,
        span: Span,
        func_name: impl Into<String>,
        args: Vec<CoreExprId<Raw>>,
    ) -> CoreExprId<Raw> {
        self.alloc(CoreExpr::Call {
            span,
            func_name: func_name.into(),
            args,
        })
    }

    fn alloc(&mut self, expr: CoreExpr<'a>) -> CoreExprId<Raw> {
        let index = self.nodes.len();
        self.nodes.push(expr);
        CoreExprId {
            index,
            _marker: PhantomData,
        }
    }

    fn get(&self, id: CoreExprId<Raw>) -> &CoreExpr<'a> {
        &self.nodes[id.index]
    }
}

pub(super) enum CoreExpr<'a> {
    Ast(&'a Expr),
    Literal {
        span: Span,
        value: Literal,
    },
    Call {
        span: Span,
        func_name: String,
        args: Vec<CoreExprId<Raw>>,
    },
}

impl<'a> TypeChecker<'a> {
    pub(super) fn resolve_core_function<'e>(
        &mut self,
        span: Span,
        func_name: impl Into<String>,
        args: &[&'e Expr],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let func_name = func_name.into();
        if let Some(rewritten_func_result) = databend_common_base::runtime::block_on(
            self.try_rewrite_sugar_function(span, &func_name, args),
        ) {
            return rewritten_func_result;
        }

        let mut arena = CoreExprArena::new();
        let args = args.iter().map(|arg| arena.ast(*arg)).collect();
        let root = arena.call(span, func_name, args);
        self.resolve_core(&arena, root)
    }

    pub(super) fn resolve_core(
        &mut self,
        arena: &CoreExprArena<'_>,
        id: CoreExprId<Raw>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        match arena.get(id) {
            CoreExpr::Ast(expr) => self.resolve(expr),
            CoreExpr::Literal { span, value } => self.resolve_literal(*span, value),
            CoreExpr::Call {
                span,
                func_name,
                args,
            } => {
                let mut scalars = Vec::with_capacity(args.len());
                let mut arg_types = Vec::with_capacity(args.len());
                for arg in args {
                    let box (scalar, mut data_type) = self.resolve_core(arena, *arg)?;
                    if let ScalarExpr::SubqueryExpr(subquery) = &scalar
                        && subquery.typ == SubqueryType::Scalar
                        && !scalar.data_type()?.is_nullable()
                    {
                        data_type = data_type.wrap_nullable();
                    }
                    scalars.push(scalar);
                    arg_types.push(data_type);
                }

                if let Some(rewritten_variant_expr) =
                    self.try_rewrite_variant_function(*span, func_name, &scalars, &arg_types)
                {
                    return rewritten_variant_expr;
                }
                if let Some(rewritten_vector_expr) =
                    self.try_rewrite_vector_function(*span, func_name, &scalars)
                {
                    return rewritten_vector_expr;
                }
                self.resolve_scalar_function_call(*span, func_name, vec![], scalars)
            }
        }
    }
}
