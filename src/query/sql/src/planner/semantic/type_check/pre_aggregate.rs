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
//! query on reference, see `grouped_select.rs`.

use databend_common_ast::Span;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;

use super::CoreExprArena;
use super::CoreExprId;
use super::TypeCheckAdapter;
use super::TypeChecker;
use crate::BindContext;
use crate::MetadataRef;
use crate::NameResolutionContext;
use crate::binder::ExprContext;
use crate::binder::ItemForm;
use crate::binder::NameResolutionResult;
use crate::binder::SelectItemSource;
use crate::binder::SelectItems;
use crate::binder::SetReturningAnalyzer;
use crate::plans::ScalarExpr;
use crate::plans::VisitorMut;

impl<'a, A> TypeChecker<'a, A>
where A: TypeCheckAdapter
{
    /// Let aliases resolve to the pre-aggregate forms of `items`. After the
    /// aggregate, the alias table of the type checker must list the SELECT
    /// items by position.
    pub(crate) fn set_select_items(&mut self, items: SelectItems<'a>) {
        self.select_items = Some(items);
    }

    /// Resolve the SELECT item at `position` of the items set with
    /// [`Self::set_select_items`], after the aggregate.
    pub(crate) fn resolve_select_item(
        &mut self,
        position: usize,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let Some(items) = &self.select_items else {
            return Err(ErrorCode::Internal("SELECT items are not available"));
        };
        let exprs = items.exprs();
        let SelectItemSource::Expr { root, .. } = exprs.item(position).source else {
            return Err(ErrorCode::Internal(format!(
                "SELECT item {} is not an expression",
                position + 1
            )));
        };
        self.resolve_checked_core(exprs.arena(), root)
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

    /// The pre-aggregate form of the SELECT item at `position` if it has one.
    pub(super) fn pre_aggregate_form(&mut self, position: usize) -> Result<Option<ScalarExpr>> {
        let Some(items) = &mut self.select_items else {
            return Ok(None);
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
    /// SELECT list, seeing the aliases of the items before it, whichever
    /// clause refers to the item. After the aggregate, an item has no form,
    /// `None`, if binding it would register something.
    pub(crate) fn resolve_pre_aggregate_form(
        bind_context: &mut BindContext,
        adapter: &A,
        name_resolution_ctx: &NameResolutionContext,
        metadata: &MetadataRef,
        mut items: SelectItems<'_>,
        position: usize,
    ) -> Result<Option<ScalarExpr>> {
        let exprs = items.exprs();
        let root = match &exprs.item(position).source {
            SelectItemSource::Star(scalar) => return Ok(Some(scalar.clone())),
            SelectItemSource::Expr { root, .. } => *root,
        };
        match items.form(position) {
            ItemForm::Unbound => {}
            ItemForm::Bound(scalar) => return Ok(Some(scalar.clone())),
            ItemForm::Unavailable => return Ok(None),
            ItemForm::Failed(err) => return Err(err.clone()),
            ItemForm::Binding => {
                return Err(ErrorCode::Internal(format!(
                    "SELECT item {} refers to itself",
                    position + 1
                )));
            }
        }
        items.set_form(position, ItemForm::Binding);
        let arena = exprs.arena();
        let after_aggregate = bind_context.grouping_key_scope.is_some();

        let expr_context = bind_context.replace_expr_context(ExprContext::SelectClause);
        let result = (|| -> Result<Option<ScalarExpr>> {
            let mut type_checker = TypeChecker::try_create_with_adapter_and_alias_fallback(
                &mut *bind_context,
                adapter.clone(),
                name_resolution_ctx,
                metadata.clone(),
                exprs.select_item_lookup(position),
                None,
            )?;
            // Before the aggregate, the form is bound with the policy of the
            // keys, also if WHERE refers to the item first; after it, the
            // scope of the keys has the same policy.
            type_checker.set_folding_policy(items.folding_policy());
            type_checker.set_select_items(items.reborrow());
            if after_aggregate && !type_checker.binds_nothing(arena, root)? {
                return Ok(None);
            }
            #[cfg(debug_assertions)]
            let effects = after_aggregate.then(|| type_checker.phase_effects());
            let Some(mut scalar) = type_checker.resolve_pre_aggregate_node(arena, root)? else {
                return Ok(None);
            };
            #[cfg(debug_assertions)]
            if let Some(effects) = effects {
                debug_assert_eq!(
                    effects,
                    type_checker.phase_effects(),
                    "a pre-aggregate form after the aggregate must not have phase-sensitive effects"
                );
            }
            SetReturningAnalyzer::new(bind_context, metadata.clone()).visit(&mut scalar)?;
            Ok(Some(scalar))
        })();
        bind_context.expr_context = expr_context;

        items.set_form(position, match &result {
            Ok(Some(scalar)) => ItemForm::Bound(scalar.clone()),
            Ok(None) => ItemForm::Unavailable,
            Err(err) => ItemForm::Failed(err.clone()),
        });
        result
    }

    /// Whether binding the item at `root` registers nothing: the item is
    /// eligible as a grouping key, and the items it refers to by alias have
    /// pre-aggregate forms.
    fn binds_nothing(&mut self, arena: &CoreExprArena<'_>, root: CoreExprId) -> Result<bool> {
        if !self.is_grouping_key_eligible(arena, root) {
            return Ok(false);
        }
        for column_ref in arena.grouping_key_column_refs(root) {
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
