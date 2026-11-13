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

//! The shared SELECT item forms for ordinary GROUP BY and grouping sets.
//!
//! The SELECT list of a grouped query is bound after its keys are known, see
//! `Binder::bind_select_list`, yet GROUP BY and WHERE, which are
//! evaluated before the aggregate, refer to the pre-aggregate forms of the
//! items by position or alias, and so do the expressions after the aggregate
//! wherever the expression around an alias is evaluated before the aggregate,
//! see `type_check/grouping_key.rs`. The pre-aggregate forms of the items are
//! therefore bound once for every clause, into [`SelectItemForms`], see
//! `type_check/pre_aggregate.rs`. The expressions are shared, while the forms
//! are owned by the binding of the query and lent to one clause at a time,
//! together with the expressions, as [`SelectItems`].
//!
//! An item is bound in its pre-aggregate form when a position or name first
//! resolves to it. Before the aggregate, the items bound so are exactly those
//! that GROUP BY and WHERE refer to, and whatever binding them registers, e.g.
//! the set-returning functions in them, is needed before the aggregate. After
//! the aggregate, the form of an item is only bound if that registers
//! nothing, i.e. if the item is eligible as a key and the items it refers to
//! have such forms too; other items have no form after the aggregate. Every
//! item, in particular every subquery, is bound once after the aggregate.

use databend_common_exception::ErrorCode;

use super::select_clauses::SelectClauseExprs;
use super::select_clauses::SelectItemSource;
use crate::planner::binder::ScalarFoldingPolicy;
use crate::plans::ScalarExpr;

/// The state of the pre-aggregate form of each SELECT item.
pub(crate) struct SelectItemForms {
    forms: Vec<ItemForm>,
    /// The folding policy of the grouping keys. Every pre-aggregate form is
    /// bound with it, whichever clause binds it first, so that a form equals
    /// the key it is written as.
    folding_policy: ScalarFoldingPolicy,
}

/// The state of the pre-aggregate form of a SELECT item.
#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
pub(crate) enum ItemForm {
    Unbound,
    /// Being bound. An item only refers to the items before it, so this is
    /// never reached again.
    Binding,
    Bound(ScalarExpr),
    /// The item has no pre-aggregate form after the aggregate.
    Unavailable,
    /// Binding the form failed with this error, deferred while binding it.
    Failed(ErrorCode),
}

impl SelectItemForms {
    pub(crate) fn new(exprs: &SelectClauseExprs<'_>, folding_policy: ScalarFoldingPolicy) -> Self {
        Self {
            forms: vec![ItemForm::Unbound; exprs.len()],
            folding_policy,
        }
    }
}

/// The SELECT items of a grouped query and their pre-aggregate forms,
/// owned by the binding of the query.
pub(crate) struct GroupedSelectItems<'a> {
    pub(crate) exprs: &'a SelectClauseExprs<'a>,
    pub(crate) forms: SelectItemForms,
}

impl<'a> GroupedSelectItems<'a> {
    pub(crate) fn new(
        exprs: &'a SelectClauseExprs<'a>,
        folding_policy: ScalarFoldingPolicy,
    ) -> Self {
        let forms = SelectItemForms::new(exprs, folding_policy);
        Self { exprs, forms }
    }

    /// Lend the items to the binding of a clause.
    pub(crate) fn lend(&mut self) -> SelectItems<'_> {
        SelectItems::new(self.exprs, &mut self.forms)
    }
}

/// The SELECT items with exclusive access to their pre-aggregate forms.
pub(crate) struct SelectItems<'s> {
    exprs: &'s SelectClauseExprs<'s>,
    forms: &'s mut SelectItemForms,
}

impl<'s> SelectItems<'s> {
    pub(crate) fn new(exprs: &'s SelectClauseExprs<'s>, forms: &'s mut SelectItemForms) -> Self {
        Self { exprs, forms }
    }

    pub(crate) fn reborrow(&mut self) -> SelectItems<'_> {
        SelectItems {
            exprs: self.exprs,
            forms: &mut *self.forms,
        }
    }

    pub(crate) fn exprs(&self) -> &'s SelectClauseExprs<'s> {
        self.exprs
    }

    /// The folding policy of the grouping keys and the pre-aggregate forms.
    pub(crate) fn folding_policy(&self) -> ScalarFoldingPolicy {
        self.forms.folding_policy
    }

    pub(crate) fn form(&self, position: usize) -> &ItemForm {
        &self.forms.forms[position]
    }

    pub(crate) fn set_form(&mut self, position: usize, form: ItemForm) {
        self.forms.forms[position] = form;
    }

    /// The pre-aggregate form of the item at `position` if it is bound.
    pub(crate) fn bound_form(&self, position: usize) -> Option<ScalarExpr> {
        match (&self.exprs.item(position).source, self.form(position)) {
            (SelectItemSource::Star(scalar), _) => Some(scalar.clone()),
            (SelectItemSource::Expr { .. }, ItemForm::Bound(scalar)) => Some(scalar.clone()),
            (SelectItemSource::Expr { .. }, _) => None,
        }
    }
}
