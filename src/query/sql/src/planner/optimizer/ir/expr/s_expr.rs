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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::OnceLock;

use databend_common_exception::Result;
use educe::Educe;

use super::ScanRequiredColumns;
use super::visitor::ExprVisitor;
use super::visitor::VisitAction;
use crate::IndexType;
use crate::Symbol;
use crate::optimizer::ir::Expr;
use crate::optimizer::ir::ExprKind;
use crate::optimizer::ir::StatInfo;
use crate::optimizer::ir::property::RelExpr;
use crate::optimizer::ir::property::RelationalProperty;
use crate::optimizer::optimizers::rule::AppliedRules;
use crate::optimizer::optimizers::rule::RuleID;
use crate::plans::RelOperator;

/// Logical expression, with stage-specific state on a shared recursive tree.
pub type SExpr = Expr<Logical>;

pub struct Logical;

#[derive(Educe)]
#[educe(PartialEq, Eq, Hash, Clone)]
pub struct LogicalState {
    pub(crate) original_group: Option<IndexType>,
    /// Shared, lazily populated caches; excluded from expression identity.
    #[educe(Hash(ignore), PartialEq(ignore))]
    pub(crate) rel_prop: Arc<OnceLock<Arc<RelationalProperty>>>,
    #[educe(Hash(ignore), PartialEq(ignore))]
    pub(crate) stat_info: Arc<OnceLock<Arc<StatInfo>>>,
    pub(crate) applied_rules: AppliedRules,
}

impl ExprKind for Logical {
    type Operator = RelOperator;
    type State = LogicalState;
    const NAME: &'static str = "SExpr";

    fn rewritten_state(state: &Self::State) -> Self::State {
        Self::State {
            original_group: None,
            rel_prop: Default::default(),
            stat_info: Default::default(),
            applied_rules: state.applied_rules.clone(),
        }
    }

    fn rel_expr(expr: &Expr<Self>) -> RelExpr<'_> {
        RelExpr::with_s_expr(expr)
    }
    fn relational_cache(state: &Self::State) -> &Arc<OnceLock<Arc<RelationalProperty>>> {
        &state.rel_prop
    }
    fn statistics_cache(state: &Self::State) -> &Arc<OnceLock<Arc<StatInfo>>> {
        &state.stat_info
    }
    fn validate_types(expr: &Expr<Self>, metadata: &crate::MetadataRef) -> Result<()> {
        super::type_validator::validate_logical_types(expr, metadata)
    }
    fn validate_column_scope(
        expr: &Expr<Self>,
        metadata: &crate::MetadataRef,
        ambient: &crate::ColumnSet,
    ) -> Result<()> {
        super::column_scope_validator::validate_logical_scope(expr, metadata, ambient)
    }
}

// Preserve diagnostic output rather than exposing the implementation's state wrapper.
impl std::fmt::Debug for Expr<Logical> {
    #[recursive::recursive]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SExpr")
            .field("plan", &self.plan)
            .field("children", &self.children)
            .field("original_group", &self.state.original_group)
            .field("rel_prop", &self.state.rel_prop)
            .field("stat_info", &self.state.stat_info)
            .field("applied_rules", &self.state.applied_rules)
            .finish()
    }
}

impl SExpr {
    pub fn add_column_index_to_scans(&self, table_index: IndexType, column_index: Symbol) -> Self {
        let required_columns = BTreeMap::from([(table_index, ScanRequiredColumns {
            columns: BTreeSet::from([column_index]),
            inverted_index: None,
            vector_index: None,
        })]);
        self.add_column_indexes_to_scans(&required_columns)
    }

    pub fn add_column_indexes_to_scans(
        &self,
        required_columns: &BTreeMap<IndexType, ScanRequiredColumns>,
    ) -> Self {
        struct Visitor<'a> {
            required_columns: &'a BTreeMap<IndexType, ScanRequiredColumns>,
        }
        impl ExprVisitor for Visitor<'_> {
            fn visit(&mut self, expr: &SExpr) -> Result<VisitAction> {
                if let RelOperator::Scan(scan) = expr.plan() {
                    if let Some(required) = self.required_columns.get(&scan.table_index) {
                        let mut scan = scan.clone();
                        scan.columns.extend(required.columns.iter().copied());
                        if required.inverted_index.is_some() {
                            scan.inverted_index = required.inverted_index.clone();
                        }
                        if required.vector_index.is_some() {
                            scan.vector_index = required.vector_index.clone();
                        }
                        return Ok(VisitAction::Replace(expr.replace_plan(scan)));
                    }
                    return Ok(VisitAction::SkipChildren);
                }
                Ok(VisitAction::Continue)
            }
        }
        match self.accept(&mut Visitor { required_columns }) {
            Ok(Some(expr)) => expr,
            _ => self.clone(),
        }
    }

    pub fn create(
        plan: impl Into<Arc<RelOperator>>,
        children: Vec<Arc<SExpr>>,
        original_group: Option<IndexType>,
        rel_prop: Option<Arc<RelationalProperty>>,
        stat_info: Option<Arc<StatInfo>>,
    ) -> Self {
        SExpr {
            plan: plan.into(),
            children,
            state: LogicalState {
                original_group,
                rel_prop: Arc::new(match rel_prop {
                    Some(rel_prop) => OnceLock::from(rel_prop),
                    None => OnceLock::new(),
                }),
                stat_info: Arc::new(match stat_info {
                    Some(stat_info) => OnceLock::from(stat_info),
                    None => OnceLock::new(),
                }),
                applied_rules: AppliedRules::default(),
            },
        }
    }

    pub fn create_unary(plan: impl Into<Arc<RelOperator>>, child: impl Into<Arc<SExpr>>) -> Self {
        Self::create(plan.into(), vec![child.into()], None, None, None)
    }

    pub fn create_binary(
        plan: impl Into<Arc<RelOperator>>,
        left_child: impl Into<Arc<SExpr>>,
        right_child: impl Into<Arc<SExpr>>,
    ) -> Self {
        Self::create(
            plan,
            vec![left_child.into(), right_child.into()],
            None,
            None,
            None,
        )
    }

    pub fn create_leaf(plan: impl Into<Arc<RelOperator>>) -> Self {
        Self::create(plan, vec![], None, None, None)
    }

    pub fn build_unary(self, plan: impl Into<Arc<RelOperator>>) -> Self {
        Self::create(plan, vec![self.into()], None, None, None)
    }

    pub fn ref_build_unary(self: &Arc<SExpr>, plan: impl Into<Arc<RelOperator>>) -> Self {
        Self::create(plan, vec![self.clone()], None, None, None)
    }

    pub fn original_group(&self) -> Option<IndexType> {
        self.state.original_group
    }

    /// Record the applied rule id in current SExpr
    pub(crate) fn set_applied_rule(&mut self, rule_id: &RuleID) {
        self.state.applied_rules.set(rule_id, true);
    }

    /// Check if a rule is applied for current SExpr
    pub(crate) fn applied_rule(&self, rule_id: &RuleID) -> bool {
        self.state.applied_rules.get(rule_id)
    }

    // The method will clear the applied rules of current SExpr and its children.
    #[recursive::recursive]
    pub fn clear_applied_rules(&mut self) {
        self.state.applied_rules.clear();
        let children = self
            .children()
            .map(|child| {
                let mut child = child.clone();
                child.clear_applied_rules();
                Arc::new(child)
            })
            .collect::<Vec<_>>();
        self.children = children;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plans::Scan;

    #[test]
    fn binding_scan_updates_preserve_input_and_invalidate_cache() -> Result<()> {
        let input = SExpr::create(
            Scan {
                table_index: 7,
                ..Default::default()
            },
            vec![],
            Some(7),
            None,
            None,
        );
        input.derive_relational_prop()?;
        let column = Symbol::new(3);
        let updated = input.add_column_index_to_scans(7, column);
        assert!(updated.plan().as_scan().unwrap().columns.contains(&column));
        assert!(input.plan().as_scan().unwrap().columns.is_empty());
        assert_eq!(input.original_group(), Some(7));
        assert_eq!(updated.original_group(), None);
        assert!(updated.state.rel_prop.get().is_none());
        assert_eq!(input.add_column_index_to_scans(8, column), input);
        Ok(())
    }
}
