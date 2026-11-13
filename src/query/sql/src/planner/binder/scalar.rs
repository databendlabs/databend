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

use std::sync::Arc;

use databend_common_ast::ast::Expr;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::FunctionContext;
use databend_common_expression::types::DataType;

use crate::MetadataRef;
use crate::planner::binder::BindContext;
use crate::planner::binder::SelectItems;
use crate::planner::semantic::CoreExprArena;
use crate::planner::semantic::CoreExprId;
use crate::planner::semantic::FullTypeCheckAdapter;
use crate::planner::semantic::NameResolutionContext;
use crate::planner::semantic::TypeCheckAdapter;
use crate::planner::semantic::TypeChecker;
use crate::plans::ScalarExpr;

#[derive(Clone, Copy)]
struct MaskingWhereAliases<'a> {
    arena: &'a CoreExprArena<'a>,
    aliases: &'a [(String, CoreExprId)],
}

/// Helper for binding scalar expressions with explicitly supplied capabilities.
pub struct ScalarBinder<'a, A = FullTypeCheckAdapter> {
    bind_context: &'a mut BindContext,
    // Preserve the infallible legacy constructor, reporting initialization errors
    // when binding. `with_adapter` always stores an already constructed adapter.
    adapter: Result<A>,
    name_resolution_ctx: &'a NameResolutionContext,
    metadata: MetadataRef,
    aliases: &'a [(String, ScalarExpr)],
    forbid_virtual_computed_column: bool,
    apply_masking_policy: bool,
    masking_where_aliases: Option<MaskingWhereAliases<'a>>,
}

impl<'a> ScalarBinder<'a, FullTypeCheckAdapter> {
    pub fn new(
        bind_context: &'a mut BindContext,
        ctx: Arc<dyn TableContext>,
        name_resolution_ctx: &'a NameResolutionContext,
        metadata: MetadataRef,
        aliases: &'a [(String, ScalarExpr)],
    ) -> Self {
        Self {
            bind_context,
            adapter: FullTypeCheckAdapter::new(ctx),
            name_resolution_ctx,
            metadata,
            aliases,
            forbid_virtual_computed_column: false,
            apply_masking_policy: true,
            masking_where_aliases: None,
        }
    }

    pub fn forbid_udf(&mut self) {
        self.adapter = self
            .adapter
            .clone()
            .map(|adapter| adapter.with_forbid_udf(true));
    }
}

impl<'a, A: TypeCheckAdapter> ScalarBinder<'a, A> {
    pub fn with_adapter(
        bind_context: &'a mut BindContext,
        adapter: A,
        name_resolution_ctx: &'a NameResolutionContext,
        metadata: MetadataRef,
        aliases: &'a [(String, ScalarExpr)],
    ) -> Self {
        Self {
            bind_context,
            adapter: Ok(adapter),
            name_resolution_ctx,
            metadata,
            aliases,
            forbid_virtual_computed_column: false,
            apply_masking_policy: true,
            masking_where_aliases: None,
        }
    }

    pub fn forbid_virtual_computed_column(&mut self) {
        self.forbid_virtual_computed_column = true;
    }

    /// Table maintenance predicates evaluate stored values, not user-visible masked values.
    pub fn without_masking_policy(&mut self) {
        self.apply_masking_policy = false;
    }

    /// Let WHERE aliases stand for the expressions at their nodes in `arena`,
    /// see `TypeChecker::set_masking_where_aliases`.
    pub(crate) fn set_masking_where_aliases(
        &mut self,
        arena: &'a CoreExprArena<'a>,
        aliases: &'a [(String, CoreExprId)],
    ) {
        self.masking_where_aliases = Some(MaskingWhereAliases { arena, aliases });
    }

    pub fn bind(&mut self, expr: &Expr) -> Result<(ScalarExpr, DataType)> {
        self.with_type_checker(None, |type_checker| type_checker.resolve(expr))
    }

    /// Bind the node `root` of an expression lowered by the caller, letting
    /// aliases resolve to the pre-aggregate forms of the SELECT items of a
    /// grouped query, see `TypeChecker::set_select_items`.
    pub(crate) fn bind_lowered(
        &mut self,
        arena: &CoreExprArena<'_>,
        root: CoreExprId,
        select_items: Option<SelectItems<'_>>,
    ) -> Result<(ScalarExpr, DataType)> {
        self.with_type_checker(select_items, |type_checker| {
            type_checker.resolve_checked_core(arena, root)
        })
    }

    fn with_type_checker(
        &mut self,
        select_items: Option<SelectItems<'_>>,
        resolve: impl FnOnce(&mut TypeChecker<'_, A>) -> Result<Box<(ScalarExpr, DataType)>>,
    ) -> Result<(ScalarExpr, DataType)> {
        let mut type_checker = TypeChecker::try_create_with_adapter(
            self.bind_context,
            self.adapter.clone()?,
            self.name_resolution_ctx,
            self.metadata.clone(),
            self.aliases,
        )?;
        type_checker.set_apply_masking_policy(self.apply_masking_policy);
        if let Some(MaskingWhereAliases { arena, aliases }) = self.masking_where_aliases {
            type_checker.set_masking_where_aliases(arena, aliases);
        }
        type_checker.set_forbid_virtual_computed_column(self.forbid_virtual_computed_column);
        if let Some(items) = select_items {
            type_checker.set_select_items(items);
        }
        Ok(*resolve(&mut type_checker)?)
    }

    pub fn get_func_ctx(&self) -> Result<FunctionContext> {
        self.adapter.clone()?.function_context()
    }
}
