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

use std::clone::Share;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::IndexType;
use crate::optimizer::ir::Group;
use crate::optimizer::ir::GroupState;
use crate::optimizer::ir::StatContext;
use crate::optimizer::ir::expr::ImplementationState;
use crate::optimizer::ir::expr::MExpr;
use crate::optimizer::ir::expr::SExpr;
use crate::optimizer::ir::implementation::FragmentInput;
use crate::optimizer::ir::implementation::PhysicalFragment;
use crate::optimizer::ir::implementation::PhysicalImplementations;
use crate::optimizer::ir::implementation::implement_operator;
use crate::optimizer::ir::property::RelExpr;
use crate::optimizer::ir::property::RelationalProperty;
use crate::optimizer::ir::property::StatInfo;
use crate::optimizer::optimizers::rule::AppliedRules;
use crate::plans::MemoOperator;
use crate::plans::PhysicalRelOperator;

/// `Memo` is a search space which memoize possible plans of a query.
/// The plans inside `Memo` are organized with `Group`s.
/// Each `Group` is a set of logically equivalent relational expressions represented with `MExpr`.
#[derive(Clone)]
pub struct Memo {
    pub groups: Vec<Group>,
    pub root: Option<IndexType>,
    stat_context: StatContext,

    /// Hash table for detecting duplicated expressions.
    /// The entry is `(plan, children) -> (group_index, m_expr_index)`.
    pub m_expr_lookup_table: HashMap<(MemoOperator, Vec<IndexType>), (IndexType, IndexType)>,
}

impl Memo {
    pub fn new(stat_context: StatContext) -> Self {
        Memo {
            groups: vec![],
            root: None,
            stat_context,
            m_expr_lookup_table: HashMap::new(),
        }
    }

    pub fn root(&self) -> Option<&Group> {
        self.root.map(|index| &self.groups[index])
    }

    pub fn set_root(&mut self, group_index: IndexType) {
        self.root = Some(group_index);
    }

    pub fn set_group_state(&mut self, group_index: IndexType, state: GroupState) -> Result<()> {
        let group = self
            .groups
            .get_mut(group_index)
            .ok_or_else(|| ErrorCode::Internal(format!("Group index {} not found", group_index)))?;
        group.state = state;
        Ok(())
    }

    // Initialize memo with given expression
    pub fn init(&mut self, s_expr: SExpr) -> Result<()> {
        let root = self.insert(None, s_expr)?;
        self.set_root(root);

        Ok(())
    }

    #[recursive::recursive]
    pub fn insert(&mut self, target_group: Option<IndexType>, s_expr: SExpr) -> Result<IndexType> {
        let mut children_group = vec![];
        for child in s_expr.children() {
            // Insert children expressions recursively and collect their group indices
            let group = self.insert(None, child.clone())?;
            children_group.push(group);
        }

        let plan = MemoOperator::Logical(s_expr.plan.share());
        if let Some((group_index, _)) = self
            .m_expr_lookup_table
            .get(&(plan.share(), children_group.clone()))
        {
            // If the expression already exists, return the group index of the existing expression
            return Ok(*group_index);
        }

        if let Some(group_index) = s_expr.original_group() {
            // The expression is extracted by PatternExtractor, no need to reinsert.
            return Ok(group_index);
        }

        // Create new group if not specified
        let group_index = match target_group {
            Some(index) => index,
            _ => {
                let rel_expr = RelExpr::with_s_expr(&s_expr);
                let relational_prop = rel_expr.derive_relational_prop()?;
                let stat_info = rel_expr.derive_cardinality(&self.stat_context)?;
                self.add_group(relational_prop, stat_info)
            }
        };

        let m_expr = MExpr::new(
            group_index,
            self.group(group_index)?.num_exprs(),
            plan,
            children_group,
            s_expr.state.applied_rules,
        );
        self.insert_m_expr(group_index, m_expr)?;

        Ok(group_index)
    }

    /// Enumerate all implementations after logical exploration, before consulting
    /// cached best choices. Returns whether there were newly implemented logical
    /// expressions, so the scheduler can restart its physical-expression cursor.
    pub fn implement_group(&mut self, group_index: IndexType) -> Result<bool> {
        let group = self.group(group_index)?;
        if !group.state.explored() {
            return Err(ErrorCode::Internal("Cannot implement an unexplored group"));
        }
        // Snapshot only the inputs needed by generation: insertion mutates Memo.
        let pending = group
            .m_exprs
            .iter()
            .filter(|expr| expr.implementation == Some(ImplementationState::Pending))
            .filter_map(|expr| {
                expr.plan
                    .logical()
                    .map(|operator| (expr.index, operator.clone(), expr.children.clone()))
            })
            .collect::<Vec<_>>();
        let changed = !pending.is_empty();
        for (index, operator, children) in pending {
            let implementations = implement_operator(&operator, children.len());
            self.insert_implementations(group_index, &children, &implementations)?;
            self.group_mut(group_index)?.m_exprs[index].implementation =
                Some(ImplementationState::Complete);
        }
        Ok(changed)
    }

    fn insert_implementations(
        &mut self,
        group_index: IndexType,
        children: &[IndexType],
        implementations: &PhysicalImplementations,
    ) -> Result<()> {
        // A choice computed over an incomplete candidate set is not reusable.
        // Enforcer insertion is separate and must not discard in-flight choices.
        self.group_mut(group_index)?.best_props.clear();
        for fragment in implementations.iter() {
            let inputs = self.resolve_fragment_inputs(fragment, children)?;
            self.insert_physical(group_index, fragment.operator.clone(), inputs)?;
        }
        Ok(())
    }

    #[recursive::recursive]
    fn resolve_fragment_inputs(
        &mut self,
        fragment: &PhysicalFragment,
        children: &[IndexType],
    ) -> Result<Vec<IndexType>> {
        fragment
            .inputs
            .iter()
            .map(|input| match input {
                FragmentInput::Child(index) => children.get(*index).copied().ok_or_else(|| {
                    ErrorCode::Internal(format!("Invalid implementation input slot: {index}"))
                }),
                FragmentInput::Local(local) => {
                    let inputs = self.resolve_fragment_inputs(local, children)?;
                    self.insert_physical_intermediate(local.operator.clone(), inputs)
                }
            })
            .collect()
    }

    fn insert_physical_intermediate(
        &mut self,
        operator: impl Into<Arc<PhysicalRelOperator>>,
        children: Vec<IndexType>,
    ) -> Result<IndexType> {
        let plan = MemoOperator::Physical(operator.into());
        if let Some((group, _)) = self
            .m_expr_lookup_table
            .get(&(plan.share(), children.clone()))
        {
            return Ok(*group);
        }
        // Derive this node's semantics from its own inputs, never from the root
        // equivalence group. This view does not need an existing parent group.
        let expr = MExpr::new(
            self.groups.len(),
            0,
            plan,
            children,
            AppliedRules::default(),
        );
        let rel_expr = RelExpr::with_m_expr(&expr, self);
        let relational_prop = rel_expr.derive_relational_prop()?;
        let stat_info = rel_expr.derive_cardinality(&self.stat_context)?;
        let group = self.add_group(relational_prop, stat_info);
        self.set_group_state(group, GroupState::Explored)?;
        self.insert_m_expr(group, expr)?;
        Ok(group)
    }

    /// Insert an implementation root or enforcer into its target equivalence group.
    pub fn insert_physical(
        &mut self,
        group_index: IndexType,
        operator: impl Into<Arc<PhysicalRelOperator>>,
        children: Vec<IndexType>,
    ) -> Result<IndexType> {
        let plan = MemoOperator::Physical(operator.into());
        if let Some((group, _)) = self
            .m_expr_lookup_table
            .get(&(plan.share(), children.clone()))
        {
            if *group != group_index {
                return Err(ErrorCode::Internal(
                    "Physical root belongs to another equivalence group",
                ));
            }
            return Ok(*group);
        }
        let expr = MExpr::new(
            group_index,
            self.group(group_index)?.num_exprs(),
            plan,
            children,
            AppliedRules::default(),
        );
        self.insert_m_expr(group_index, expr)?;
        Ok(group_index)
    }

    pub fn group(&self, index: IndexType) -> Result<&Group> {
        self.groups
            .get(index)
            .ok_or_else(|| ErrorCode::Internal(format!("Group index {} not found", index)))
    }

    pub fn insert_m_expr(&mut self, group_index: IndexType, m_expr: MExpr) -> Result<()> {
        self.m_expr_lookup_table.insert(
            (m_expr.plan.clone(), m_expr.children.clone()),
            (m_expr.group_index, m_expr.index),
        );
        self.group_mut(group_index)?.insert(m_expr)
    }

    pub fn group_mut(&mut self, index: IndexType) -> Result<&mut Group> {
        self.groups
            .get_mut(index)
            .ok_or_else(|| ErrorCode::Internal(format!("Group index {} not found", index)))
    }

    fn add_group(
        &mut self,
        relational_prop: Arc<RelationalProperty>,
        stat_info: Arc<StatInfo>,
    ) -> IndexType {
        let group_index = self.groups.len();
        let group = Group::create(group_index, relational_prop, stat_info);
        self.groups.push(group);
        group_index
    }

    /// Get an estimate of the memory size of the memo.
    pub fn mem_size(&self) -> usize {
        // Include the owned operator behind the phase tag, not just its Arc.
        // As before, this estimates the fixed-size operator payloads only.
        self.m_expr_lookup_table
            .keys()
            .map(|(operator, _)| {
                std::mem::size_of::<MemoOperator>()
                    + match operator {
                        MemoOperator::Logical(_) => {
                            std::mem::size_of::<crate::plans::RelOperator>()
                        }
                        MemoOperator::Physical(_) => std::mem::size_of::<PhysicalRelOperator>(),
                    }
            })
            .sum()
    }

    /// Get the number of groups in the memo.
    pub fn num_groups(&self) -> usize {
        self.groups.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Symbol;
    use crate::optimizer::cost::Cost;
    use crate::optimizer::cost::CostContext;
    use crate::optimizer::ir::PhysicalProperty;
    use crate::optimizer::ir::RequiredProperty;
    use crate::plans::Aggregate;
    use crate::plans::DummyTableScan;
    use crate::plans::EvalScalar;
    use crate::plans::Join;
    use crate::plans::Limit;

    fn limit(rows: Option<usize>) -> Limit {
        Limit {
            before_exchange: false,
            limit: rows,
            offset: 0,
            lazy_columns: Default::default(),
        }
    }

    fn best_choice(group: IndexType, expr: IndexType, cost: f64) -> CostContext {
        CostContext {
            group_index: group,
            expr_index: expr,
            cost: Cost(cost),
            physical_prop: PhysicalProperty::default(),
            children_required_props: vec![],
        }
    }

    #[test]
    fn enumeration_completes_once_and_preserves_completed_choices() -> Result<()> {
        let mut memo = Memo::new(StatContext::without_settings(Default::default()));
        memo.init(SExpr::create_unary(
            Aggregate::default(),
            SExpr::create_leaf(DummyTableScan::new()),
        ))?;
        let root = memo.root.unwrap();
        assert!(memo.implement_group(root).is_err());
        memo.set_group_state(root, GroupState::Explored)?;
        assert!(memo.implement_group(root)?);
        let group_count = memo.num_groups();
        let group = memo.group(root)?;
        assert_eq!(group.num_exprs(), 2);
        assert!(group.m_exprs[0].implementation == Some(ImplementationState::Complete));
        assert!(group.m_exprs[1].implementation.is_none());
        let partial = memo.group(group.m_exprs[1].children[0])?;
        assert!(partial.state.explored());
        assert!(partial.m_exprs[0].implementation.is_none());
        assert!(!Arc::ptr_eq(&group.stat_info, &partial.stat_info));
        let required = RequiredProperty::default();
        memo.group_mut(root)?
            .update_best_cost(&required, best_choice(root, 1, 17.0));
        assert!(!memo.implement_group(root)?);
        assert_eq!(memo.num_groups(), group_count);
        assert_eq!(memo.group(root)?.num_exprs(), 2);
        assert_eq!(
            memo.group(root)?.best_prop(&required).unwrap().cost,
            Cost(17.0)
        );
        Ok(())
    }

    #[test]
    fn intermediate_derives_its_own_properties_and_statistics() -> Result<()> {
        let mut memo = Memo::new(StatContext::without_settings(Default::default()));
        let input = memo.add_group(
            Arc::new(RelationalProperty {
                output_columns: [Symbol::new(1)].into_iter().collect(),
                ..Default::default()
            }),
            Arc::new(StatInfo {
                cardinality: 50.0,
                ..Default::default()
            }),
        );
        let intermediate = memo
            .insert_physical_intermediate(PhysicalRelOperator::Limit(limit(Some(3))), vec![
                input,
            ])?;
        assert_eq!(memo.group(intermediate)?.stat_info.cardinality, 3.0);
        assert_eq!(memo.group(input)?.stat_info.cardinality, 50.0);
        assert_eq!(
            memo.group(intermediate)?.relational_prop.output_columns,
            memo.group(input)?.relational_prop.output_columns
        );
        assert_eq!(
            memo.insert_physical_intermediate(PhysicalRelOperator::Limit(limit(Some(3))), vec![
                input
            ])?,
            intermediate
        );
        Ok(())
    }

    #[test]
    fn memo_property_views_resolve_child_positions_not_parent_group() -> Result<()> {
        let mut memo = Memo::new(StatContext::without_settings(Default::default()));
        let mut groups = vec![];
        for (column, rows) in [(1, 10.0), (2, 20.0), (99, 777.0)] {
            groups.push(memo.add_group(
                Arc::new(RelationalProperty {
                    output_columns: [Symbol::new(column)].into_iter().collect(),
                    ..Default::default()
                }),
                Arc::new(StatInfo {
                    cardinality: rows,
                    ..Default::default()
                }),
            ));
        }
        let expr = MExpr::new(
            groups[2],
            0,
            MemoOperator::Physical(Arc::new(PhysicalRelOperator::Join(Join::default()))),
            vec![groups[0], groups[1]],
            AppliedRules::default(),
        );
        let child_physical_props = vec![PhysicalProperty::default(); 2];
        for view in [
            RelExpr::with_m_expr(&expr, &memo),
            RelExpr::with_opt_context(&expr, &memo, &child_physical_props),
        ] {
            for (index, group) in groups[..2].iter().enumerate() {
                assert!(Arc::ptr_eq(
                    &view.derive_relational_prop_child(index)?,
                    &memo.group(*group)?.relational_prop,
                ));
                assert!(Arc::ptr_eq(
                    &view.derive_cardinality_child(
                        index,
                        &StatContext::without_settings(Default::default()),
                    )?,
                    &memo.group(*group)?.stat_info,
                ));
            }
            assert!(view.derive_relational_prop_child(2).is_err());
            assert!(
                view.derive_cardinality_child(
                    2,
                    &StatContext::without_settings(Default::default())
                )
                .is_err()
            );
        }
        Ok(())
    }

    #[test]
    fn all_implementation_roots_enter_the_target_group_and_are_interned() -> Result<()> {
        // Local consumer contract, not rewrite-rule evidence. Both fragments
        // implement LIMIT ALL; production keeps its existing single strategy.
        let mut memo = Memo::new(StatContext::without_settings(Default::default()));
        memo.init(SExpr::create_unary(
            limit(None),
            SExpr::create_leaf(DummyTableScan::new()),
        ))?;
        let root = memo.root.unwrap();
        let input = memo.group(root)?.m_exprs[0].children[0];
        let implementations = PhysicalImplementations::new(
            PhysicalFragment::new(PhysicalRelOperator::Limit(limit(None)), vec![
                FragmentInput::Local(PhysicalFragment::new(
                    PhysicalRelOperator::EvalScalar(EvalScalar { items: vec![] }),
                    vec![FragmentInput::Child(0)],
                )),
            ]),
            vec![PhysicalFragment::new(
                PhysicalRelOperator::Limit(limit(None)),
                vec![FragmentInput::Child(0)],
            )],
        );
        let required = RequiredProperty::default();
        memo.group_mut(root)?
            .update_best_cost(&required, best_choice(root, 0, 0.0));
        memo.insert_implementations(root, &[input], &implementations)?;
        assert!(memo.group(root)?.best_prop(&required).is_none());
        let group_count = memo.num_groups();
        memo.insert_implementations(root, &[input], &implementations)?;
        assert_eq!(memo.num_groups(), group_count);
        let group = memo.group(root)?;
        assert_eq!(group.num_exprs(), 3);
        assert_eq!(group.m_exprs[1].group_index, root);
        assert_eq!(group.m_exprs[2].group_index, root);
        assert_eq!(group.m_exprs[2].children, vec![input]);
        let intermediate = memo.group(group.m_exprs[1].children[0])?;
        assert_eq!(intermediate.m_exprs[0].children, vec![input]);
        assert!(intermediate.state.explored());
        assert!(!Arc::ptr_eq(&group.stat_info, &intermediate.stat_info));
        Ok(())
    }
}
