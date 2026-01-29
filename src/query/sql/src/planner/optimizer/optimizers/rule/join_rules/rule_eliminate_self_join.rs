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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::Scalar;

use crate::ColumnBinding;
use crate::ColumnBindingBuilder;
use crate::ColumnEntry;
use crate::ColumnSet;
use crate::IndexType;
use crate::Metadata;
use crate::MetadataRef;
use crate::Visibility;
use crate::optimizer::OptimizerContext;
use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::recursive::RecursiveRuleOptimizer;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::ComparisonOp;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::plans::Join;
use crate::plans::JoinEquiCondition;
use crate::plans::JoinType;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::Scan;

#[derive(Clone, Debug)]
struct MultiJoin {
    relations: Vec<Arc<SExpr>>,
    equi_conditions: Vec<JoinEquiCondition>,
    non_equi_conditions: Vec<ScalarExpr>,
}

impl MultiJoin {
    fn build_equivalence_classes(&self) -> Result<UnionFind> {
        let mut uf = UnionFind::default();
        for cond in self.equi_conditions.iter() {
            if let (ScalarExpr::BoundColumnRef(l), ScalarExpr::BoundColumnRef(r)) =
                (&cond.left, &cond.right)
            {
                uf.union(l.column.index, r.column.index);
            }
        }
        Ok(uf)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct GroupKeyItemSignature {
    column_id: Option<u32>,
    column_position: Option<usize>,
    column_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct AggFuncSignature {
    func_name: String,
    distinct: bool,
    params: Vec<Scalar>,
    args: Vec<GroupKeyItemSignature>,
}

#[derive(Clone, Debug)]
struct Candidate {
    table_id: u64,
    group_key: GroupKeyItemSignature,
    group_key_index: IndexType,
    agg_func_indexes: Vec<(AggFuncSignature, IndexType)>,
    /// Scalar items introduced above the (Filter +) Aggregate chain.
    ///
    /// If the corresponding relation is eliminated, these derived columns must be
    /// recreated on top of the kept relation; otherwise parent operators may
    /// reference missing columns.
    extra_scalar_items: Vec<ScalarItem>,
    strict: bool,
    relation_idx: usize,
}

pub struct RuleEliminateSelfJoin {
    id: RuleID,
    matchers: Vec<Matcher>,
    opt_ctx: Arc<OptimizerContext>,
}

impl RuleEliminateSelfJoin {
    pub fn new(opt_ctx: Arc<OptimizerContext>) -> Self {
        Self {
            id: RuleID::EliminateSelfJoin,
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Join,
                children: vec![Matcher::Leaf, Matcher::Leaf],
            }],
            opt_ctx,
        }
    }

    fn core_candidate_matcher(&self) -> Matcher {
        Matcher::MatchOp {
            op_type: RelOp::Aggregate,
            children: vec![Matcher::MatchOp {
                op_type: RelOp::Aggregate,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::EvalScalar,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::Scan,
                        children: vec![],
                    }],
                }],
            }],
        }
    }

    fn extract_multi_join(&self, s_expr: &SExpr) -> Result<Option<MultiJoin>> {
        match s_expr.plan() {
            RelOperator::Join(join)
                if matches!(join.join_type, JoinType::Inner) && !join.has_null_equi_condition() =>
            {
                let mut relations = Vec::new();
                let mut equi_conditions = join.equi_conditions.clone();
                let mut non_equi_conditions = join.non_equi_conditions.clone();

                match self.extract_multi_join(s_expr.child(0)?)? {
                    Some(mut left) => {
                        relations.append(&mut left.relations);
                        equi_conditions.append(&mut left.equi_conditions);
                        non_equi_conditions.append(&mut left.non_equi_conditions);
                    }
                    None => {
                        relations.push(Arc::new(s_expr.child(0)?.clone()));
                    }
                }

                match self.extract_multi_join(s_expr.child(1)?)? {
                    Some(mut right) => {
                        relations.append(&mut right.relations);
                        equi_conditions.append(&mut right.equi_conditions);
                        non_equi_conditions.append(&mut right.non_equi_conditions);
                    }
                    None => {
                        relations.push(Arc::new(s_expr.child(1)?.clone()));
                    }
                }

                Ok(Some(MultiJoin {
                    relations,
                    equi_conditions,
                    non_equi_conditions,
                }))
            }
            _ => Ok(None),
        }
    }

    fn eliminate_multi_join(&self, multi_join: MultiJoin, original: &SExpr) -> Result<SExpr> {
        let mut eq_classes = multi_join.build_equivalence_classes()?;
        let metadata_ref: MetadataRef = self.opt_ctx.get_metadata();
        let metadata = metadata_ref.read();

        let mut candidates = Vec::new();
        for (relation_idx, relation) in multi_join.relations.iter().enumerate() {
            if let Some(candidate) = self.try_parse_candidate(relation, relation_idx, &metadata) {
                candidates.push(candidate);
            }
        }

        let mut groups: HashMap<(u64, GroupKeyItemSignature), Vec<Candidate>> = HashMap::new();
        for candidate in candidates.into_iter() {
            groups
                .entry((candidate.table_id, candidate.group_key.clone()))
                .or_default()
                .push(candidate);
        }

        let mut remove_relations: HashSet<usize> = HashSet::new();
        let mut removed_to_keep_column_mapping: HashMap<IndexType, IndexType> = HashMap::new();
        let mut extra_scalar_items: Vec<ScalarItem> = Vec::new();
        for (_key, group_candidates) in groups.iter() {
            let has_strict = group_candidates.iter().any(|c| c.strict);
            let has_loose = group_candidates.iter().any(|c| !c.strict);
            if !has_strict || !has_loose {
                continue;
            }

            let Some(keep) = group_candidates.iter().find(|c| c.strict) else {
                continue;
            };

            // If there is at least one strict candidate, loose candidates are redundant:
            // both sides are INNER JOIN on unique group keys (Aggregate), so the join acts
            // as a filter, and the strict side already filters stronger than the loose side.
            for c in group_candidates.iter().filter(|c| !c.strict) {
                let Some(mapping) = Self::build_removed_to_keep_index_mapping(c, keep) else {
                    continue;
                };

                if !eq_classes.same(c.group_key_index, keep.group_key_index) {
                    continue;
                }

                let Some(rewritten_extra_items) =
                    Self::rewrite_extra_scalar_items(&c.extra_scalar_items, &mapping, &metadata)
                else {
                    continue;
                };
                remove_relations.insert(c.relation_idx);
                removed_to_keep_column_mapping.extend(mapping);
                extra_scalar_items.extend(rewritten_extra_items);
            }
        }

        if remove_relations.is_empty() {
            return Ok(original.clone());
        }

        let Some(rewritten) = self.build_new_join_tree(
            &multi_join.relations,
            &multi_join.equi_conditions,
            &multi_join.non_equi_conditions,
            &remove_relations,
            &removed_to_keep_column_mapping,
            &metadata,
        )?
        else {
            return Ok(original.clone());
        };

        let rewritten =
            Self::project_columns(rewritten, &removed_to_keep_column_mapping, &metadata)?;
        let rewritten = Self::add_extra_scalar_items(rewritten, &extra_scalar_items, &metadata)?;

        Ok(rewritten)
    }

    fn rewrite_extra_scalar_items(
        items: &[ScalarItem],
        mapping: &HashMap<IndexType, IndexType>,
        metadata: &Metadata,
    ) -> Option<Vec<ScalarItem>> {
        if items.is_empty() {
            return Some(vec![]);
        }

        let mut available_columns: ColumnSet = mapping.values().copied().collect();

        let mut rewritten = Vec::with_capacity(items.len());
        for item in items.iter() {
            let mut scalar = item.scalar.clone();
            for (old, new) in mapping.iter() {
                if old == new {
                    continue;
                }
                let new_column = Self::make_column_binding(metadata, *new);
                if scalar.replace_column_binding(*old, &new_column).is_err() {
                    return None;
                }
            }

            // After rewriting, the expression should be evaluable solely from the kept side's
            // mapped columns. If not, eliminating this relation would drop required inputs.
            if !scalar.used_columns().is_subset(&available_columns) {
                return None;
            }
            rewritten.push(ScalarItem {
                scalar,
                index: item.index,
            });
            available_columns.insert(item.index);
        }
        Some(rewritten)
    }

    fn try_parse_candidate(
        &self,
        relation: &SExpr,
        relation_idx: usize,
        metadata: &Metadata,
    ) -> Option<Candidate> {
        let mut strict = false;
        let mut node = relation;
        let mut extra_scalar_items = Vec::new();

        if matches!(node.plan(), RelOperator::EvalScalar(_)) {
            let eval_scalar = node.plan().as_eval_scalar().unwrap();
            let child = node.unary_child();
            let child_output_columns = child.derive_relational_prop().ok()?.output_columns.clone();

            // Collect only those items that introduce new output column indices.
            let mut new_items = Vec::new();
            for item in eval_scalar.items.iter() {
                if !child_output_columns.contains(&item.index) {
                    new_items.push(item.clone());
                }
            }
            extra_scalar_items.extend(new_items);
            node = node.unary_child();
        }

        while matches!(node.plan(), RelOperator::Filter(_)) {
            strict = true;
            node = node.unary_child();
        }

        if !self.core_candidate_matcher().matches(node) {
            return None;
        }

        let final_agg = node.plan().as_aggregate().unwrap();

        if final_agg.group_items.len() != 1 {
            return None;
        }

        let group_key = Self::group_key_signature(&final_agg.group_items[0], metadata)?;
        let group_key_index = final_agg.group_items[0].index;

        let agg_func_indexes = Self::agg_func_indexes(&final_agg.aggregate_functions, metadata)?;

        let scan = node
            .unary_child()
            .unary_child()
            .unary_child()
            .plan()
            .as_scan()
            .unwrap();

        if scan
            .push_down_predicates
            .as_ref()
            .is_some_and(|v| !v.is_empty())
            || scan.limit.is_some()
            || scan.order_by.as_ref().is_some_and(|v| !v.is_empty())
            || scan.prewhere.is_some()
            || scan.agg_index.is_some()
            || scan.change_type.is_some()
            || scan.update_stream_columns
            || scan.inverted_index.is_some()
            || scan.vector_index.is_some()
            || scan.is_lazy_table
        {
            return None;
        }

        let table_id = self.table_id_from_scan(scan, metadata)?;

        Some(Candidate {
            table_id,
            group_key,
            group_key_index,
            agg_func_indexes,
            extra_scalar_items,
            strict,
            relation_idx,
        })
    }

    fn group_key_signature(
        item: &crate::plans::ScalarItem,
        metadata: &Metadata,
    ) -> Option<GroupKeyItemSignature> {
        let ScalarExpr::BoundColumnRef(col) = &item.scalar else {
            return None;
        };
        let ColumnEntry::BaseTableColumn(base_col) = metadata.column(col.column.index) else {
            return None;
        };
        Some(GroupKeyItemSignature {
            column_id: Some(base_col.column_id),
            column_position: base_col.column_position,
            column_name: base_col.column_name.clone(),
        })
    }

    fn agg_func_signature(item: &ScalarItem, metadata: &Metadata) -> Option<AggFuncSignature> {
        let ScalarExpr::AggregateFunction(agg) = &item.scalar else {
            return None;
        };

        let mut args = Vec::with_capacity(agg.args.len());
        for arg in agg.args.iter() {
            let ScalarExpr::BoundColumnRef(col) = arg else {
                return None;
            };
            let ColumnEntry::BaseTableColumn(base_col) = metadata.column(col.column.index) else {
                return None;
            };
            args.push(GroupKeyItemSignature {
                column_id: Some(base_col.column_id),
                column_position: base_col.column_position,
                column_name: base_col.column_name.clone(),
            });
        }

        Some(AggFuncSignature {
            func_name: agg.func_name.clone(),
            distinct: agg.distinct,
            params: agg.params.clone(),
            args,
        })
    }

    fn agg_func_indexes(
        agg_items: &[ScalarItem],
        metadata: &Metadata,
    ) -> Option<Vec<(AggFuncSignature, IndexType)>> {
        let mut result = Vec::with_capacity(agg_items.len());
        for item in agg_items.iter() {
            result.push((Self::agg_func_signature(item, metadata)?, item.index));
        }
        Some(result)
    }

    fn table_id_from_scan(&self, scan: &Scan, metadata: &Metadata) -> Option<u64> {
        let table_entry = metadata.table(scan.table_index);
        Some(table_entry.table().get_table_info().ident.table_id)
    }

    fn build_removed_to_keep_index_mapping(
        remove: &Candidate,
        keep: &Candidate,
    ) -> Option<HashMap<IndexType, IndexType>> {
        let mut mapping = HashMap::new();

        mapping.insert(remove.group_key_index, keep.group_key_index);

        let mut keep_agg_by_sig: HashMap<&AggFuncSignature, Vec<IndexType>> = HashMap::new();
        for (sig, idx) in keep.agg_func_indexes.iter() {
            keep_agg_by_sig.entry(sig).or_default().push(*idx);
        }
        let mut keep_agg_pos: HashMap<&AggFuncSignature, usize> = HashMap::new();

        for (sig, old_index) in remove.agg_func_indexes.iter() {
            let list = keep_agg_by_sig.get(sig)?;
            let pos = keep_agg_pos.entry(sig).or_insert(0);
            if *pos >= list.len() {
                return None;
            }
            mapping.insert(*old_index, list[*pos]);
            *pos += 1;
        }

        Some(mapping)
    }

    fn make_column_binding(metadata: &Metadata, index: IndexType) -> ColumnBinding {
        let entry = metadata.column(index);
        ColumnBindingBuilder::new(
            entry.name(),
            index,
            Box::new(entry.data_type()),
            Visibility::Visible,
        )
        .build()
    }

    fn make_bound_column_ref(metadata: &Metadata, index: IndexType) -> ScalarExpr {
        let binding = Self::make_column_binding(metadata, index);
        crate::plans::BoundColumnRef {
            span: None,
            column: binding,
        }
        .into()
    }

    fn project_columns(
        rewritten: SExpr,
        column_mapping: &HashMap<IndexType, IndexType>,
        metadata: &Metadata,
    ) -> Result<SExpr> {
        let output_columns = rewritten.derive_relational_prop()?.output_columns.clone();
        let mut items = Vec::new();
        let mut has_mapping = false;

        for col in output_columns.iter() {
            let scalar = Self::make_bound_column_ref(metadata, *col);

            for (old, new) in column_mapping.iter() {
                if new == col {
                    has_mapping = true;
                    items.push(ScalarItem {
                        scalar: scalar.clone(),
                        index: *old,
                    });
                }
            }

            items.push(ScalarItem {
                scalar,
                index: *col,
            });
        }

        if !has_mapping {
            return Ok(rewritten);
        }

        Ok(SExpr::create_unary(
            Arc::new(RelOperator::EvalScalar(crate::plans::EvalScalar { items })),
            Arc::new(rewritten),
        ))
    }

    fn add_extra_scalar_items(
        rewritten: SExpr,
        extra_scalar_items: &[ScalarItem],
        metadata: &Metadata,
    ) -> Result<SExpr> {
        if extra_scalar_items.is_empty() {
            return Ok(rewritten);
        }

        let output_columns = rewritten.derive_relational_prop()?.output_columns.clone();
        let mut items = Vec::with_capacity(output_columns.len() + extra_scalar_items.len());
        for col in output_columns.iter() {
            items.push(ScalarItem {
                scalar: Self::make_bound_column_ref(metadata, *col),
                index: *col,
            });
        }

        for item in extra_scalar_items.iter() {
            if output_columns.contains(&item.index) {
                continue;
            }
            items.push(item.clone());
        }

        Ok(SExpr::create_unary(
            Arc::new(RelOperator::EvalScalar(crate::plans::EvalScalar { items })),
            Arc::new(rewritten),
        ))
    }

    fn build_new_join_tree(
        &self,
        relations: &[Arc<SExpr>],
        equi_conditions: &[JoinEquiCondition],
        non_equi_conditions: &[ScalarExpr],
        remove_relations: &HashSet<usize>,
        mapping: &HashMap<IndexType, IndexType>,
        metadata: &Metadata,
    ) -> Result<Option<SExpr>> {
        let mut kept_relations: Vec<Arc<SExpr>> = relations
            .iter()
            .enumerate()
            .filter_map(|(idx, rel)| {
                if remove_relations.contains(&idx) {
                    None
                } else {
                    Some(rel.clone())
                }
            })
            .collect();

        if kept_relations.is_empty() {
            return Ok(None);
        }

        // Step 1: replace column bindings in join conditions.
        let mut equi_conditions = equi_conditions.to_vec();
        let mut non_equi_conditions = non_equi_conditions.to_vec();
        for (old, new) in mapping.iter() {
            if old == new {
                continue;
            }
            let new_column = Self::make_column_binding(metadata, *new);
            for cond in equi_conditions.iter_mut() {
                cond.left.replace_column_binding(*old, &new_column)?;
                cond.right.replace_column_binding(*old, &new_column)?;
            }
            for cond in non_equi_conditions.iter_mut() {
                cond.replace_column_binding(*old, &new_column)?;
            }
        }

        // Step 2: rebuild a join tree from remaining relations (structure only).
        let mut join_tree = kept_relations.remove(0);
        for rel in kept_relations.into_iter() {
            join_tree = Arc::new(SExpr::create_binary(
                Arc::new(RelOperator::Join(Join::default())),
                join_tree,
                rel,
            ));
        }

        // Step 3: put all conditions into a single Filter above the join tree.
        let mut predicates: Vec<ScalarExpr> = Vec::with_capacity(
            equi_conditions
                .len()
                .saturating_add(non_equi_conditions.len()),
        );
        for cond in equi_conditions.into_iter() {
            predicates.push(ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: String::from(ComparisonOp::Equal.to_func_name()),
                params: vec![],
                arguments: vec![cond.left, cond.right],
            }));
        }
        predicates.extend(non_equi_conditions);

        // Safety check: all predicates must be evaluable from the rebuilt tree.
        if !predicates.is_empty() {
            let output_columns = join_tree.derive_relational_prop()?.output_columns.clone();
            for pred in predicates.iter() {
                if !pred.used_columns().is_subset(&output_columns) {
                    return Ok(None);
                }
            }
        }

        let root = if predicates.is_empty() {
            join_tree.as_ref().clone()
        } else {
            SExpr::create_unary(
                Arc::new(RelOperator::Filter(Filter { predicates })),
                join_tree,
            )
        };

        let root = self.push_down_filter(root)?;
        Ok(Some(root))
    }

    fn push_down_filter(&self, root: SExpr) -> Result<SExpr> {
        static RULES: &[RuleID] = &[RuleID::PushDownFilterJoin, RuleID::EliminateFilter];
        let optimizer = RecursiveRuleOptimizer::new(self.opt_ctx.clone(), RULES);
        optimizer.optimize_sync(&root)
    }
}

#[derive(Default, Clone, Debug)]
struct UnionFind {
    parent: HashMap<IndexType, IndexType>,
}

impl UnionFind {
    fn find(&mut self, x: IndexType) -> IndexType {
        let parent = *self.parent.get(&x).unwrap_or(&x);
        if parent == x {
            self.parent.insert(x, x);
            return x;
        }
        let root = self.find(parent);
        self.parent.insert(x, root);
        root
    }

    fn union(&mut self, a: IndexType, b: IndexType) {
        let ra = self.find(a);
        let rb = self.find(b);
        if ra == rb {
            return;
        }
        self.parent.insert(rb, ra);
    }

    fn same(&mut self, a: IndexType, b: IndexType) -> bool {
        self.find(a) == self.find(b)
    }
}

impl Rule for RuleEliminateSelfJoin {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        if let Some(multi_join) = self.extract_multi_join(s_expr)? {
            let result = self.eliminate_multi_join(multi_join, s_expr)?;
            if result.ne(s_expr) {
                let mut result = result;
                result.set_applied_rule(&self.id);
                state.add_result(result);
            }
        }

        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}
