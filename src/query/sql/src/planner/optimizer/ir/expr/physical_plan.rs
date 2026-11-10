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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use super::PExpr;
use super::SExpr;
use crate::optimizer::ir::implementation::FragmentInput;
use crate::optimizer::ir::implementation::PhysicalFragment;
use crate::optimizer::ir::implementation::implement_operator;

/// Select the explicit default implementation without a cost search. Fallback,
/// skip-list and direct physical planning share the same candidate definitions
/// as Memo planning, but never enumerate combinations of child alternatives.
pub struct DefaultPhysicalPlanner;

impl DefaultPhysicalPlanner {
    #[recursive::recursive]
    pub fn plan(expr: &SExpr) -> Result<PExpr> {
        let implementations = implement_operator(expr.plan(), expr.arity());
        let children = expr
            .children()
            .map(|child| Self::plan(child).map(Arc::new))
            .collect::<Result<Vec<_>>>()?;
        let mut root = Self::build_fragment(implementations.default(), &children)?;
        // Only the root is equivalent to the logical expression. Its semantic
        // caches may be shared; local intermediates and physical rule history
        // must not inherit the logical expression's state.
        root.state.original_group = expr.original_group();
        root.state.rel_prop = expr.state.rel_prop.clone();
        root.state.stat_info = expr.state.stat_info.clone();
        Ok(root)
    }

    #[recursive::recursive]
    fn build_fragment(fragment: &PhysicalFragment, inputs: &[Arc<PExpr>]) -> Result<PExpr> {
        let children = fragment
            .inputs
            .iter()
            .map(|input| match input {
                FragmentInput::Child(index) => inputs.get(*index).cloned().ok_or_else(|| {
                    ErrorCode::Internal(format!("Invalid implementation input slot: {index}"))
                }),
                FragmentInput::Local(local) => Self::build_fragment(local, inputs).map(Arc::new),
            })
            .collect::<Result<_>>()?;
        Ok(PExpr::create(
            fragment.operator.clone(),
            children,
            None,
            None,
            None,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::OnceLock;

    use super::*;
    use crate::optimizer::ir::RelExpr;
    use crate::optimizer::ir::StatContext;
    use crate::optimizer::ir::implementation::PhysicalImplementations;
    use crate::optimizer::optimizers::rule::RuleID;
    use crate::plans::Aggregate;
    use crate::plans::AggregateMode;
    use crate::plans::DummyTableScan;
    use crate::plans::EvalScalar;
    use crate::plans::Limit;
    use crate::plans::PhysicalRelOperator;

    #[test]
    fn default_planning_shares_only_root_semantic_caches() -> Result<()> {
        // Local cache invariant; SQL integration tests cover real plan shapes.
        let mut logical = SExpr::create(DummyTableScan::new(), vec![], Some(7), None, None);
        logical.set_applied_rule(&RuleID::EliminateEvalScalar);
        logical.derive_relational_prop()?;
        RelExpr::with_s_expr(&logical)
            .derive_cardinality(&StatContext::without_settings(Default::default()))?;
        let physical = DefaultPhysicalPlanner::plan(&logical)?;
        assert_eq!(physical.original_group(), Some(7));
        assert!(!physical.applied_rule(&RuleID::EliminateEvalScalar));
        assert!(Arc::ptr_eq(
            &logical.state.rel_prop,
            &physical.state.rel_prop
        ));
        assert!(Arc::ptr_eq(
            &logical.state.stat_info,
            &physical.state.stat_info
        ));
        // Cache contents do not affect physical identity.
        let fresh = PExpr::create(DummyTableScan::new(), vec![], Some(7), None, None);
        assert_eq!(physical, fresh);
        for replaced in [
            physical.replace_plan(physical.plan.clone()),
            physical.replace_children([]),
        ] {
            assert_eq!(replaced.original_group(), None);
            assert!(OnceLock::get(&replaced.state.rel_prop).is_none());
            assert!(OnceLock::get(&replaced.state.stat_info).is_none());
        }
        Ok(())
    }

    #[test]
    fn aggregate_default_builds_partial_final_without_mutating_logical_plan() -> Result<()> {
        let logical = SExpr::create_unary(
            Aggregate::default(),
            SExpr::create_leaf(DummyTableScan::new()),
        );
        let before = logical.clone();
        let physical = DefaultPhysicalPlanner::plan(&logical)?;
        assert_eq!(logical, before);
        assert_eq!(
            physical.plan().as_aggregate().unwrap().mode,
            AggregateMode::Final
        );
        let partial = physical.unary_child();
        assert_eq!(
            partial.plan().as_aggregate().unwrap().mode,
            AggregateMode::Partial
        );
        assert!(partial.unary_child().plan().is_dummy_table_scan());
        assert!(!Arc::ptr_eq(
            &logical.state.rel_prop,
            &partial.state.rel_prop
        ));
        assert!(!Arc::ptr_eq(
            &logical.state.stat_info,
            &partial.state.stat_info
        ));
        Ok(())
    }

    #[test]
    fn default_selection_is_explicit_and_input_slots_are_preserved() -> Result<()> {
        // Both shapes are identity operators over the input. They are test-only
        // candidates, not new production implementation strategies.
        let implementations = PhysicalImplementations::new(
            PhysicalFragment::new(
                PhysicalRelOperator::Limit(Limit {
                    before_exchange: false,
                    limit: None,
                    offset: 0,
                    lazy_columns: Default::default(),
                }),
                vec![FragmentInput::Child(0)],
            ),
            vec![PhysicalFragment::new(
                PhysicalRelOperator::EvalScalar(EvalScalar { items: vec![] }),
                vec![FragmentInput::Child(0)],
            )],
        );
        let child = Arc::new(PExpr::create_leaf(DummyTableScan::new()));
        let selected = DefaultPhysicalPlanner::build_fragment(
            implementations.default(),
            std::slice::from_ref(&child),
        )?;
        assert!(selected.plan().is_limit());
        assert!(Arc::ptr_eq(&selected.children[0], &child));
        assert_eq!(implementations.iter().count(), 2);
        assert!(DefaultPhysicalPlanner::build_fragment(implementations.default(), &[]).is_err());
        Ok(())
    }
}
