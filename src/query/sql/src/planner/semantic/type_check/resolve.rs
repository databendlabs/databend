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
use databend_common_ast::ast::UnaryOperator;
use databend_common_exception::Result;
use databend_common_expression::ColumnIndex;
use databend_common_expression::ConstantFolder;
use databend_common_expression::Expr as EExpr;
use databend_common_expression::FunctionKind;
use databend_common_expression::expr;
use databend_common_expression::shrink_scalar;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use unicase::Ascii;

use super::CoreExprArena;
use super::CoreExprId;
use super::TypeCheckAdapter;
use super::TypeChecker;
use super::rewrite_function;
use crate::BindContext;
use crate::MetadataRef;
use crate::NameResolutionContext;
use crate::plans::ConstantExpr;
use crate::plans::ScalarExpr;

impl<'a, A> TypeChecker<'a, A>
where A: TypeCheckAdapter
{
    pub fn try_create_with_adapter(
        bind_context: &'a mut BindContext,
        adapter: A,
        name_resolution_ctx: &'a NameResolutionContext,
        metadata: MetadataRef,
        aliases: &'a [(String, ScalarExpr)],
    ) -> Result<Self> {
        let func_ctx = adapter.function_context()?;
        let dialect = adapter.settings().get_sql_dialect()?;
        Ok(Self {
            bind_context,
            adapter,
            dialect,
            func_ctx,
            name_resolution_ctx,
            metadata,
            aliases,
            in_aggregate_function: false,
            in_window_function: false,
            in_masking_policy: false,
        })
    }

    pub(super) fn core_expr_arena(&self) -> CoreExprArena<'a> {
        CoreExprArena::with_aggregate_function_factory(
            self.func_ctx.week_start as u64,
            self.adapter.aggregate_function_factory(),
        )
    }

    pub(super) fn resolve_checked_core(
        &mut self,
        arena: &CoreExprArena<'_>,
        root: CoreExprId,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        self.adapter.check_core_expr_context(arena)?;
        self.resolve_core(arena, root)
    }

    pub(super) fn can_lower_core_scalar_function(func_name: &str) -> bool {
        if TypeChecker::<()>::all_special_functions().contains(&Ascii::new(func_name))
            || rewrite_function::rewrite_function_name(func_name).is_some()
        {
            return false;
        }
        BUILTIN_FUNCTIONS
            .get_property(func_name)
            .map(|property| property.kind != FunctionKind::SRF)
            .unwrap_or(false)
    }

    #[recursive::recursive]
    pub fn resolve(&mut self, expr: &Expr) -> Result<Box<(ScalarExpr, DataType)>> {
        let mut arena = self.core_expr_arena();
        let root = arena.lower_ast_expr(expr)?;
        self.resolve_checked_core(&arena, root)
    }

    /// Resolve binary expressions. Most of the binary expressions
    /// would be transformed into `FunctionCall`, except comparison
    /// expressions, conjunction(`AND`) and disjunction(`OR`).
    pub fn resolve_binary_op(
        &mut self,
        span: Span,
        op: &BinaryOperator,
        left: &Expr,
        right: &Expr,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let mut arena = self.core_expr_arena();
        let root = arena.lower_binary_op_expr(span, op, left, right)?;
        self.resolve_checked_core(&arena, root)
    }

    /// Resolve unary expressions.
    pub fn resolve_unary_op(
        &mut self,
        span: Span,
        op: &UnaryOperator,
        child: &Expr,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let mut arena = self.core_expr_arena();
        let root = arena.lower_unary_op_expr(span, op, child)?;
        self.resolve_checked_core(&arena, root)
    }

    pub(super) fn try_fold_constant<Index: ColumnIndex>(
        &self,
        expr: &EExpr<Index>,
        enable_shrink: bool,
    ) -> Option<Box<(ScalarExpr, DataType)>> {
        if expr.is_deterministic(&BUILTIN_FUNCTIONS) && enable_shrink {
            if let (EExpr::Constant(expr::Constant { scalar, .. }), _) =
                ConstantFolder::fold(expr, &self.func_ctx, &BUILTIN_FUNCTIONS)
            {
                let scalar = if enable_shrink {
                    shrink_scalar(scalar)
                } else {
                    scalar
                };
                let ty = scalar.as_ref().infer_data_type();
                return Some(Box::new((
                    ConstantExpr {
                        span: expr.span(),
                        value: scalar,
                    }
                    .into(),
                    ty,
                )));
            }
        }

        None
    }
}
