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
use databend_common_expression::ColumnIndex;
use databend_common_expression::Expr as EExpr;
use databend_common_expression::FunctionContext;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::MetadataRef;
use crate::planner::binder::BindContext;
use crate::planner::semantic::FullTypeCheckAdapter;
use crate::planner::semantic::NameResolutionContext;
use crate::planner::semantic::TypeCheckAdapter;
use crate::planner::semantic::TypeChecker;
use crate::plans::ScalarExpr;

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
    masking_where_aliases: Option<Vec<(String, Expr)>>,
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

    pub(crate) fn set_masking_where_aliases(&mut self, aliases: Vec<(String, Expr)>) {
        self.masking_where_aliases = Some(aliases);
    }

    pub fn bind(&mut self, expr: &Expr) -> Result<(ScalarExpr, DataType)> {
        let mut type_checker = TypeChecker::try_create_with_adapter(
            self.bind_context,
            self.adapter.clone()?,
            self.name_resolution_ctx,
            self.metadata.clone(),
            self.aliases,
        )?;
        type_checker.set_apply_masking_policy(self.apply_masking_policy);
        if let Some(aliases) = &self.masking_where_aliases {
            type_checker.set_masking_where_aliases(aliases);
        }
        type_checker.set_forbid_virtual_computed_column(self.forbid_virtual_computed_column);
        Ok(*type_checker.resolve(expr)?)
    }

    pub fn get_func_ctx(&self) -> Result<FunctionContext> {
        self.adapter.clone()?.function_context()
    }

    /// Records that binding stores the value of `expr` in the logical plan. A value computed
    /// from a non-deterministic expression such as `now()` is valid only for this query.
    pub(crate) fn mark_folded_value<Index: ColumnIndex>(&self, expr: &EExpr<Index>) {
        if !expr.is_deterministic(&BUILTIN_FUNCTIONS) {
            self.set_holds_query_specific_value();
        }
    }

    /// Records that the logical plan holds a value that is valid only for this query.
    pub(crate) fn set_holds_query_specific_value(&self) {
        self.metadata.write().set_holds_query_specific_value();
    }
}
