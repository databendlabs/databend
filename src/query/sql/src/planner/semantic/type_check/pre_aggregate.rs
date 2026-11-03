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

//! Binding the pre-aggregate forms of the SELECT items of a GROUPING SETS
//! query on reference, see `grouping_sets_select.rs`.

use databend_common_ast::Span;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use super::CoreExprArena;
use super::CoreExprId;
use super::TypeCheckAdapter;
use super::TypeChecker;
use crate::BindContext;
use crate::MetadataRef;
use crate::NameResolutionContext;
use crate::binder::ExprContext;
use crate::binder::NameResolutionResult;
use crate::binder::PreAggregateItemsMut;
use crate::binder::SetReturningAnalyzer;
use crate::plans::ScalarExpr;
use crate::plans::VisitorMut;

impl<'a, A> TypeChecker<'a, A>
where A: TypeCheckAdapter
{
    /// Let aliases resolve to the pre-aggregate forms of `items`. After the
    /// aggregate, the alias table of the type checker must list the SELECT
    /// items by position.
    pub(crate) fn set_pre_aggregate_items(&mut self, items: PreAggregateItemsMut<'a>) {
        self.pre_aggregate = Some(items);
    }

    /// Resolve an alias that names the SELECT items at `positions` before the
    /// aggregate.
    pub(super) fn resolve_pre_aggregate_alias(
        &mut self,
        span: Span,
        alias: &str,
        positions: &[usize],
    ) -> Result<ScalarExpr> {
        let mut resolved: Option<ScalarExpr> = None;
        for position in positions {
            let Some(form) = self.pre_aggregate_form(*position)? else {
                return Err(ErrorCode::Internal(format!(
                    "SELECT item {} named by alias {alias} has no pre-aggregate form",
                    position + 1
                ))
                .set_span(span));
            };
            match &resolved {
                Some(first) if first != &form => {
                    return Err(ErrorCode::SemanticError(format!(
                        "column {alias} reference or alias is ambiguous, please use another alias name",
                    ))
                    .set_span(span));
                }
                Some(_) => {}
                None => resolved = Some(form),
            }
        }
        resolved.ok_or_else(|| ErrorCode::Internal(format!("alias {alias} names no SELECT item")))
    }

    /// The pre-aggregate form of the SELECT item at `position` after the
    /// aggregate, if it has one.
    pub(super) fn post_aggregate_alias_form(
        &mut self,
        position: usize,
    ) -> Result<Option<ScalarExpr>> {
        match &self.pre_aggregate {
            Some(items) if items.is_after_aggregate() => self.pre_aggregate_form(position),
            _ => Ok(None),
        }
    }

    fn pre_aggregate_form(&mut self, position: usize) -> Result<Option<ScalarExpr>> {
        let Some(items) = self.pre_aggregate.as_mut() else {
            return Err(ErrorCode::Internal(
                "pre-aggregate SELECT items are not available",
            ));
        };
        Self::resolve_pre_aggregate_form(
            self.bind_context,
            &self.adapter,
            self.name_resolution_ctx,
            &self.metadata,
            items.reborrow(),
            position,
        )
    }

    /// The pre-aggregate form of the SELECT item at `position`, bound on the
    /// first call. The form only depends on the item: it is bound as in the
    /// SELECT list, seeing the aliases of the items before it, with
    /// column-dependent folding off and grouping keys unmatched, whichever
    /// clause refers to the item. After the aggregate, an item has no form,
    /// `None`, if binding it would register something.
    pub(crate) fn resolve_pre_aggregate_form(
        bind_context: &mut BindContext,
        adapter: &A,
        name_resolution_ctx: &NameResolutionContext,
        metadata: &MetadataRef,
        mut items: PreAggregateItemsMut<'_>,
        position: usize,
    ) -> Result<Option<ScalarExpr>> {
        if let Some(form) = items.form(position) {
            return Ok(Some(form.clone()));
        }
        if items.is_unavailable(position) {
            return Ok(None);
        }
        let after_aggregate = items.is_after_aggregate();
        let expr = items.start_binding(position)?;
        let list = items.list();

        let expr_context = bind_context.replace_expr_context(ExprContext::SelectClause);
        let defer_column_folding = std::mem::replace(&mut bind_context.defer_column_folding, true);
        let grouping_key_scope = bind_context.grouping_key_scope.take();
        let result = (|| -> Result<Option<ScalarExpr>> {
            let mut type_checker = TypeChecker::try_create_with_adapter_and_alias_fallback(
                &mut *bind_context,
                adapter.clone(),
                name_resolution_ctx,
                metadata.clone(),
                list.select_item_lookup(position),
                None,
            )?;
            type_checker.set_pre_aggregate_items(items.reborrow());
            let mut arena = type_checker.core_expr_arena();
            let root = arena.lower_ast_expr(expr)?;
            if after_aggregate && !type_checker.binds_nothing(&arena, root)? {
                return Ok(None);
            }
            let mut scalar = type_checker.resolve_checked_core(&arena, root)?.0;
            SetReturningAnalyzer::new(bind_context, metadata.clone()).visit(&mut scalar)?;
            Ok(Some(scalar))
        })();
        bind_context.expr_context = expr_context;
        bind_context.defer_column_folding = defer_column_folding;
        bind_context.grouping_key_scope = grouping_key_scope;

        items.finish_binding(position, &result);
        result
    }

    /// Whether binding the item lowered to `arena` registers nothing: the item
    /// is eligible as a grouping key, and the items it refers to by alias have
    /// pre-aggregate forms.
    fn binds_nothing(&mut self, arena: &CoreExprArena<'_>, root: CoreExprId) -> Result<bool> {
        if !arena.is_grouping_key_eligible(root) {
            return Ok(false);
        }
        for column_ref in arena.column_refs() {
            if let NameResolutionResult::SelectItems { positions, .. } =
                self.resolve_column_name(column_ref)?
            {
                for position in positions {
                    if self.pre_aggregate_form(position)?.is_none() {
                        return Ok(false);
                    }
                }
            }
        }
        Ok(true)
    }
}
