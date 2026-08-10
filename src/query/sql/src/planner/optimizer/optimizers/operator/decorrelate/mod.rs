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

#[allow(clippy::module_inception)]
mod decorrelate;
mod flatten_plan;
mod flatten_scalar;
mod subquery_decorrelator;

use std::collections::HashMap;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Symbol;
pub use subquery_decorrelator::FlattenInfo;
pub use subquery_decorrelator::SubqueryDecorrelatorOptimizer;
pub use subquery_decorrelator::UnnestResult;

use crate::optimizer::ir::SExpr;

type FlattenPlanResult = (SExpr, DerivedColumnScope);

#[derive(Clone, Default)]
pub(crate) struct DerivedColumnScope {
    inherited: HashMap<Symbol, Symbol>,
    local: HashMap<Symbol, Symbol>,
}

impl DerivedColumnScope {
    fn child_scope_for_filter(&self, filter_derived_columns: Self) -> Self {
        let mut inherited = self.snapshot();
        inherited.extend(
            filter_derived_columns
                .inherited
                .into_iter()
                .chain(filter_derived_columns.local),
        );
        Self {
            inherited,
            local: HashMap::new(),
        }
    }

    fn absorb_child_scope(&mut self, child: &Self) {
        for (old, new) in child.snapshot() {
            if self.resolve(old) == Some(new) {
                continue;
            }
            if !self.local.contains_key(&old) {
                self.local.insert(old, new);
            }
        }
    }

    fn record(&mut self, old: Symbol, new: Symbol) {
        self.local.insert(old, new);
    }

    pub(crate) fn resolve(&self, old: Symbol) -> Option<Symbol> {
        self.local
            .get(&old)
            .copied()
            .or_else(|| self.inherited.get(&old).copied())
    }

    pub(crate) fn resolve_or_self(&self, old: Symbol) -> Symbol {
        self.resolve(old).unwrap_or(old)
    }

    pub(crate) fn must_resolve(&self, old: Symbol) -> Result<Symbol> {
        self.resolve(old)
            .ok_or_else(|| ErrorCode::Internal(format!("Missing derived column {old}")))
    }

    pub(crate) fn visible_symbols(&self) -> Vec<Symbol> {
        self.snapshot().into_values().collect()
    }

    fn snapshot(&self) -> HashMap<Symbol, Symbol> {
        let mut visible = self.inherited.clone();
        visible.extend(self.local.iter().map(|(&old, &new)| (old, new)));
        visible
    }
}
