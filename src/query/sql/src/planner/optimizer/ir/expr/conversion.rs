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

use super::PExpr;
use super::p_expr::PhysicalState;
use crate::optimizer::ir::SExpr;

impl From<SExpr> for PExpr {
    #[recursive::recursive]
    fn from(expr: SExpr) -> Self {
        Self {
            plan: expr.plan,
            children: expr
                .children
                .into_iter()
                .map(|child| Arc::new(Self::from(Arc::unwrap_or_clone(child))))
                .collect(),
            state: PhysicalState {
                original_group: expr.state.original_group,
                rel_prop: expr.state.rel_prop,
                stat_info: expr.state.stat_info,
                applied_rules: expr.state.applied_rules,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hash;
    use std::hash::Hasher;
    use std::sync::OnceLock;

    use super::*;
    use crate::optimizer::ir::RelExpr;
    use crate::optimizer::ir::StatContext;
    use crate::optimizer::optimizers::rule::RuleID;
    use crate::plans::DummyTableScan;

    #[test]
    fn conversion_preserves_identity_and_replacement_invalidates_caches()
    -> databend_common_exception::Result<()> {
        // Local node/cache invariant; SQL-driven integration tests cover real trees.
        let mut logical = SExpr::create(DummyTableScan::new(), vec![], Some(7), None, None);
        logical.set_applied_rule(&RuleID::EliminateEvalScalar);
        logical.derive_relational_prop()?;
        RelExpr::with_s_expr(&logical).derive_cardinality(&StatContext::default())?;
        let physical = PExpr::from(logical.clone());
        assert_eq!(physical.original_group(), Some(7));
        assert!(physical.applied_rule(&RuleID::EliminateEvalScalar));
        assert!(Arc::ptr_eq(
            &logical.state.rel_prop,
            &physical.state.rel_prop
        ));
        assert!(Arc::ptr_eq(
            &logical.state.stat_info,
            &physical.state.stat_info
        ));
        fn hash(value: &impl Hash) -> u64 {
            let mut hash = DefaultHasher::new();
            value.hash(&mut hash);
            hash.finish()
        }
        assert_eq!(hash(&logical), hash(&physical));
        let logical_clone = logical.clone();
        let physical_clone = physical.clone();
        assert!(Arc::ptr_eq(
            &logical.state.rel_prop,
            &logical_clone.state.rel_prop
        ));
        assert!(Arc::ptr_eq(
            &physical.state.stat_info,
            &physical_clone.state.stat_info
        ));
        assert_eq!(logical, logical_clone);
        assert_eq!(physical, physical_clone);
        // Cache contents are not identity, but group and rule state still are.
        let fresh = PExpr::create(DummyTableScan::new(), vec![], Some(7), None, None);
        let mut same_identity = fresh.clone();
        same_identity.set_applied_rule(&RuleID::EliminateEvalScalar);
        assert_eq!(physical, same_identity);
        assert_eq!(hash(&physical), hash(&same_identity));
        assert_ne!(physical, fresh);
        let mut different_group = PExpr::create(DummyTableScan::new(), vec![], Some(8), None, None);
        different_group.set_applied_rule(&RuleID::EliminateEvalScalar);
        assert_ne!(physical, different_group);
        for replaced in [
            logical.replace_plan(logical.plan.clone()),
            logical.replace_children([]),
        ] {
            assert_eq!(replaced.original_group(), None);
            assert!(OnceLock::get(&replaced.state.rel_prop).is_none());
            assert!(OnceLock::get(&replaced.state.stat_info).is_none());
            assert!(replaced.applied_rule(&RuleID::EliminateEvalScalar));
            assert!(!Arc::ptr_eq(
                &logical.state.rel_prop,
                &replaced.state.rel_prop
            ));
        }
        for replaced in [
            physical.replace_plan(physical.plan.clone()),
            physical.replace_children([]),
        ] {
            assert_eq!(replaced.original_group(), None);
            assert!(OnceLock::get(&replaced.state.rel_prop).is_none());
            assert!(OnceLock::get(&replaced.state.stat_info).is_none());
            assert!(replaced.applied_rule(&RuleID::EliminateEvalScalar));
            assert!(!Arc::ptr_eq(
                &physical.state.rel_prop,
                &replaced.state.rel_prop
            ));
        }
        Ok(())
    }
}
