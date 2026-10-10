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

use databend_common_base::runtime::spawn;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use super::JoinRelation;
use super::RelationId;
use super::algorithm::HyperDp;
use super::algorithm::JoinEdgeRef;
use super::algorithm::JoinNode;
use super::algorithm::JoinOrderModel;
use crate::IndexType;
use crate::binder::JoinPredicate;
use crate::optimizer::Optimizer;
use crate::optimizer::OptimizerContext;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::optimizer::ir::SExprVisitor;
use crate::optimizer::ir::StatContext;
use crate::optimizer::ir::StatInfo;
use crate::optimizer::ir::Statistics;
use crate::optimizer::ir::VisitAction;
use crate::optimizer::optimizers::operator::JoinCondition;
use crate::optimizer::optimizers::operator::JoinFilters;
use crate::plans::Filter;
use crate::plans::Join;
use crate::plans::JoinEquiCondition;
use crate::plans::JoinType;
use crate::plans::MaterializedCTERef;
use crate::plans::RelOperator;

/// The join reorder algorithm follows the paper: Dynamic Programming Strikes Back
/// See the paper for more details.
pub struct DPhpyOptimizer {
    opt_ctx: Arc<OptimizerContext>,
    join_relations: Vec<JoinRelation>,
    // base table index -> index of join_relations
    table_index_map: HashMap<IndexType, RelationId>,
    // Detached filters retain their original input relation scope.
    filters: Vec<PositionedFilter>,
}

struct PositionedFilter {
    filter: Filter,
    // Original input scope limits which base relations may supply columns.
    // Opaque relations cannot be traversed by predicate placement.
    relations: HashSet<RelationId>,
    // Preserve ON residuals as join conditions, not standalone filters.
    is_join_condition: bool,
}

struct PlacedPredicate {
    predicate: crate::ScalarExpr,
    relations: HashSet<RelationId>,
    placement: PredicatePlacement,
}

impl PlacedPredicate {
    fn applies_at(
        &self,
        relations: &HashSet<RelationId>,
        children: &[&HashSet<RelationId>],
    ) -> bool {
        self.relations.is_subset(relations)
            && !children.iter().any(|child| self.relations.is_subset(child))
    }

    fn append_filter(&self, residual: &mut Vec<crate::ScalarExpr>) {
        // Scoped occurrences may represent independent volatile calls.
        if matches!(self.placement, PredicatePlacement::ScopedFilter)
            || !residual.contains(&self.predicate)
        {
            residual.push(self.predicate.clone());
        }
    }
}

#[derive(Clone, Copy)]
enum PredicatePlacement {
    // Constants, volatile expressions and unresolved references keep their scope.
    ScopedFilter,
    // Deterministic filters can be classified against the new join sides.
    MovableFilter,
    // Original ON residuals remain join conditions when placed on a join.
    JoinCondition,
}

#[derive(Clone)]
struct DPhypPlan {
    expr: SExpr,
    relations: HashSet<RelationId>,
}

struct DPhypJoinOrderModel<'a> {
    join_relations: &'a [JoinRelation],
    join_conditions: &'a [JoinEquiCondition],
    filters: &'a [PlacedPredicate],
    stat_context: &'a StatContext,
}

impl DPhypJoinOrderModel<'_> {
    /// A base relation is opaque: all applicable predicates become Filters
    /// above it, even if its root happens to be a Join.
    fn attach_base_filters(&self, expr: SExpr, relations: &HashSet<RelationId>) -> SExpr {
        let mut residual = vec![];
        for predicate in self.filters.iter().filter(|p| p.applies_at(relations, &[])) {
            predicate.append_filter(&mut residual);
        }
        JoinFilters::wrap(expr, residual)
    }

    /// Place existing predicates on a join candidate without inference.
    fn attach_join_filters(
        &self,
        mut expr: SExpr,
        relations: &HashSet<RelationId>,
        children: &[&HashSet<RelationId>; 2],
    ) -> Result<SExpr> {
        let predicates = self
            .filters
            .iter()
            .filter(|p| p.applies_at(relations, children));
        let RelOperator::Join(join) = expr.plan() else {
            unreachable!("binary DPhyp candidate must be a Join")
        };
        let mut updated_join = None;
        let mut properties = None;
        let mut residual = vec![];
        for positioned in predicates {
            let predicate = &positioned.predicate;
            if matches!(positioned.placement, PredicatePlacement::ScopedFilter) {
                positioned.append_filter(&mut residual);
                continue;
            }
            if properties.is_none() {
                let rel_expr = RelExpr::with_s_expr(&expr);
                properties = Some((
                    rel_expr.derive_relational_prop_child(0)?,
                    rel_expr.derive_relational_prop_child(1)?,
                ));
            }
            let (left_prop, right_prop) = properties.as_ref().unwrap();
            let classified = JoinPredicate::new(predicate, left_prop, right_prop);
            let condition = match &classified {
                JoinPredicate::Both {
                    left,
                    right,
                    is_equal_op: true,
                } if predicate.is_deterministic() => JoinCondition::Equi {
                    left,
                    right,
                    is_null_equal: false,
                },
                JoinPredicate::Both { .. } => JoinCondition::NonEqui(predicate),
                // prepare_predicates already proved that a movable predicate
                // is deterministic and supplied entirely by these relations.
                // Other need not be separable into left/right operands to be
                // evaluated as a residual on this inner/cross join.
                JoinPredicate::Other(_)
                    if matches!(positioned.placement, PredicatePlacement::MovableFilter) =>
                {
                    JoinCondition::NonEqui(predicate)
                }
                _ if matches!(positioned.placement, PredicatePlacement::JoinCondition) => {
                    JoinCondition::NonEqui(predicate)
                }
                _ => {
                    positioned.append_filter(&mut residual);
                    continue;
                }
            };
            let join = updated_join.get_or_insert_with(|| join.clone());
            condition.insert_into(join);
            if join.join_type == JoinType::Cross {
                join.join_type = JoinType::Inner;
            }
        }
        if let Some(join) = updated_join {
            expr = expr.replace_plan(RelOperator::Join(join));
        }
        Ok(JoinFilters::wrap(expr, residual))
    }

    fn join_s_expr(
        &self,
        left: &JoinNode<DPhypPlan>,
        right: &JoinNode<DPhypPlan>,
        edge_refs: &[JoinEdgeRef],
    ) -> SExpr {
        let left_expr = left.state().expr.clone();
        let right_expr = right.state().expr.clone();
        let mut conditions = Vec::with_capacity(edge_refs.len());

        for edge_ref in edge_refs {
            let mut condition = self.join_conditions[edge_ref.id].clone();
            if edge_ref.reversed {
                std::mem::swap(&mut condition.left, &mut condition.right);
            }
            conditions.push(condition);
        }

        let join_type = if edge_refs.is_empty() {
            JoinType::Cross
        } else {
            JoinType::Inner
        };
        let rel_op = RelOperator::Join(Join {
            equi_conditions: conditions,
            non_equi_conditions: vec![],
            join_type,
            marker_index: None,
            from_correlated_subquery: false,
            need_hold_hash_table: false,
            is_lateral: false,
            single_to_inner: None,
            build_side_cache_info: None,
            spatial_join: None,
        });

        SExpr::create(
            Arc::new(rel_op),
            vec![Arc::new(left_expr), Arc::new(right_expr)],
            None,
            None,
            None,
        )
    }
}

impl JoinOrderModel for DPhypJoinOrderModel<'_> {
    type NodeState = DPhypPlan;

    fn base_node(&self, relation: RelationId) -> Result<(f64, Self::NodeState)> {
        let relations = HashSet::from([relation]);
        // Never push into an opaque base's outer join, projection or LIMIT.
        let base = self.join_relations[relation].s_expr();
        let expr = self.attach_base_filters(base.clone(), &relations);
        let cardinality = if expr == base {
            self.join_relations[relation].cardinality(self.stat_context)?
        } else {
            RelExpr::with_s_expr(&expr)
                .derive_cardinality(self.stat_context)?
                .cardinality
        };
        Ok((cardinality, DPhypPlan { expr, relations }))
    }

    fn join_node(
        &self,
        left: &JoinNode<Self::NodeState>,
        right: &JoinNode<Self::NodeState>,
        edge_refs: &[JoinEdgeRef],
    ) -> Result<(f64, Self::NodeState)> {
        let relations = left
            .state()
            .relations
            .union(&right.state().relations)
            .copied()
            .collect();
        let expr =
            self.attach_join_filters(self.join_s_expr(left, right, edge_refs), &relations, &[
                &left.state().relations,
                &right.state().relations,
            ])?;
        let cardinality = RelExpr::with_s_expr(&expr)
            .derive_cardinality(self.stat_context)?
            .cardinality;
        Ok((cardinality, DPhypPlan { expr, relations }))
    }

    fn join_cost(
        &self,
        left: &JoinNode<Self::NodeState>,
        right: &JoinNode<Self::NodeState>,
        edge_refs: &[JoinEdgeRef],
        cardinality: f64,
    ) -> Result<f64> {
        if edge_refs.is_empty() {
            Ok(left.cardinality() * right.cardinality())
        } else {
            Ok(cardinality + left.cost() + right.cost())
        }
    }
}

impl DPhpyOptimizer {
    pub fn new(opt_ctx: Arc<OptimizerContext>) -> Self {
        Self {
            opt_ctx,
            join_relations: vec![],
            table_index_map: Default::default(),
            filters: vec![],
        }
    }

    /// Process children of a node in parallel
    async fn new_children(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        // Parallel process children: start a new dphyp for each child.
        let left_expr = s_expr.children[0].clone();
        let opt_ctx = self.opt_ctx.clone();
        let left_res = spawn(async move {
            let mut dphyp = DPhpyOptimizer::new(opt_ctx.clone());
            (
                dphyp.optimize_async(&left_expr).await,
                dphyp.table_index_map,
            )
        });

        let right_expr = s_expr.children[1].clone();
        let opt_ctx = self.opt_ctx.clone();
        let right_res = spawn(async move {
            let mut dphyp = DPhpyOptimizer::new(opt_ctx.clone());
            (
                dphyp.optimize_async(&right_expr).await,
                dphyp.table_index_map,
            )
        });

        let left_res = left_res
            .await
            .map_err(|e| ErrorCode::TokioError(format!("Cannot join tokio job, err: {:?}", e)))?;
        let right_res = right_res
            .await
            .map_err(|e| ErrorCode::TokioError(format!("Cannot join tokio job, err: {:?}", e)))?;

        let left_expr = left_res.0?;
        let right_expr = right_res.0?;

        // Merge `table_index_map` of left and right into current `table_index_map`.
        let relation_idx = self.join_relations.len();
        for table_index in left_res.1.keys() {
            self.table_index_map.insert(*table_index, relation_idx);
        }
        for table_index in right_res.1.keys() {
            self.table_index_map.insert(*table_index, relation_idx);
        }

        Ok(s_expr.replace_children([Arc::new(left_expr), Arc::new(right_expr)]))
    }

    /// Process a subquery expression
    async fn process_subquery(&mut self, s_expr: &SExpr) -> Result<(Arc<SExpr>, bool)> {
        let mut dphyp = DPhpyOptimizer::new(self.opt_ctx.clone());
        let new_s_expr = Arc::new(dphyp.optimize_async(s_expr).await?);

        // Merge `table_index_map` of subquery into current `table_index_map`.
        let relation_idx = self.join_relations.len();
        for table_index in dphyp.table_index_map.keys() {
            self.table_index_map.insert(*table_index, relation_idx);
        }

        self.join_relations.push(JoinRelation::new(&new_s_expr));

        Ok((new_s_expr, true))
    }

    /// Process a scan node
    fn process_scan_node(
        &mut self,
        s_expr: &SExpr,
        join_relation: Option<&SExpr>,
    ) -> Result<(Arc<SExpr>, bool)> {
        let join_relation = JoinRelation::new(join_relation.unwrap_or(s_expr));

        if let RelOperator::Scan(op) = s_expr.plan() {
            self.table_index_map
                .insert(op.table_index, self.join_relations.len());
        }

        self.join_relations.push(join_relation);
        Ok((Arc::new(s_expr.clone()), true))
    }

    /// Check if an operator represents a subquery
    fn is_subquery_operator(op: &RelOperator) -> bool {
        matches!(
            op,
            RelOperator::EvalScalar(_)
                | RelOperator::Aggregate(_)
                | RelOperator::Sort(_)
                | RelOperator::Limit(_)
                | RelOperator::TopN(_)
                | RelOperator::ProjectSet(_)
                | RelOperator::Window(_)
                | RelOperator::WindowGroup(_)
                | RelOperator::Udf(_)
        )
    }

    /// Process a join node
    async fn process_join_node(
        &mut self,
        s_expr: &SExpr,
        join_conditions: &mut Vec<JoinEquiCondition>,
    ) -> Result<(Arc<SExpr>, bool)> {
        let op = match s_expr.plan() {
            RelOperator::Join(op) => op,
            _ => unreachable!(),
        };

        // Preserve joins that cannot participate in this reorder region.
        if op.build_side_cache_info.is_some() || op.join_type.is_any_join() {
            self.collect_table_indexes(s_expr)?;
            self.join_relations.push(JoinRelation::new(s_expr));
            return Ok((Arc::new(s_expr.clone()), true));
        }

        // Check if it's an inner join
        let mut is_inner_join =
            matches!(op.join_type, JoinType::Inner) || op.join_type == JoinType::Cross;

        // Check if children are subqueries
        let left_op = s_expr.child(0)?.plan.as_ref();
        let right_op = s_expr.child(1)?.plan.as_ref();
        let left_is_subquery = Self::is_subquery_operator(left_op);
        let right_is_subquery = Self::is_subquery_operator(right_op);

        // Add join conditions
        for condition in op.equi_conditions.iter() {
            let left_used_tables = condition.left.used_tables()?;
            let right_used_tables = condition.right.used_tables()?;

            if left_used_tables.is_empty() || right_used_tables.is_empty() {
                is_inner_join = false;
                break;
            }
        }

        if !is_inner_join {
            // For non-inner joins, process children in parallel
            let new_s_expr = self.new_children(s_expr).await?;
            self.join_relations.push(JoinRelation::new(&new_s_expr));
            return Ok((Arc::new(new_s_expr), true));
        }

        join_conditions.extend(op.equi_conditions.iter().cloned());
        let relation_start = self.join_relations.len();

        // Process left and right children
        let left_res = self
            .get_base_relations(
                s_expr.child(0)?,
                join_conditions,
                true,
                None,
                left_is_subquery,
            )
            .await?;

        let right_res = self
            .get_base_relations(
                s_expr.child(1)?,
                join_conditions,
                true,
                None,
                right_is_subquery,
            )
            .await?;

        if !op.non_equi_conditions.is_empty() {
            self.filters.push(PositionedFilter {
                filter: Filter {
                    predicates: op.non_equi_conditions.clone(),
                },
                relations: (relation_start..self.join_relations.len()).collect(),
                is_join_condition: true,
            });
        }
        let new_s_expr = Arc::new(s_expr.replace_children([left_res.0, right_res.0]));
        Ok((new_s_expr, left_res.1 && right_res.1))
    }

    async fn process_sequence_node(&mut self, s_expr: &SExpr) -> Result<(Arc<SExpr>, bool)> {
        let mut left_dphyp = DPhpyOptimizer::new(self.opt_ctx.clone());
        let left_expr = left_dphyp.optimize_async(s_expr.left_child()).await?;

        let mut cte_stats = HashMap::new();
        Self::collect_materialized_cte_stats(
            &left_expr,
            &mut cte_stats,
            self.opt_ctx.get_stat_context(),
        )?;
        let (right_child, _) =
            Self::sync_materialized_cte_ref_stats(s_expr.right_child(), &cte_stats)?;

        let mut right_dphyp = DPhpyOptimizer::new(self.opt_ctx.clone());
        let right_expr = right_dphyp.optimize_async(&right_child).await?;

        // Merge table_index_map from right child into current table_index_map
        let relation_idx = self.join_relations.len();
        for table_index in right_dphyp.table_index_map.keys() {
            self.table_index_map.insert(*table_index, relation_idx);
        }

        let new_s_expr = s_expr.replace_children([Arc::new(left_expr), Arc::new(right_expr)]);
        self.join_relations.push(JoinRelation::new(&new_s_expr));
        Ok((Arc::new(new_s_expr), true))
    }

    fn collect_materialized_cte_stats(
        s_expr: &SExpr,
        cte_stats: &mut HashMap<String, Arc<StatInfo>>,
        stat_context: &StatContext,
    ) -> Result<()> {
        struct StatsCollector<'a> {
            cte_stats: &'a mut HashMap<String, Arc<StatInfo>>,
            stat_context: &'a StatContext,
        }

        impl SExprVisitor for StatsCollector<'_> {
            fn visit(&mut self, expr: &SExpr) -> Result<VisitAction> {
                if let RelOperator::MaterializedCTE(cte) = expr.plan() {
                    let stat_info = RelExpr::with_s_expr(expr.unary_child())
                        .derive_cardinality(self.stat_context)?;
                    self.cte_stats.insert(cte.cte_name.clone(), stat_info);
                }

                Ok(VisitAction::Continue)
            }
        }

        s_expr
            .accept(&mut StatsCollector {
                cte_stats,
                stat_context,
            })
            .map(|_| ())
    }

    fn remap_materialized_cte_ref_stat_info(
        cte_ref: &MaterializedCTERef,
        producer_stat_info: &Arc<StatInfo>,
    ) -> Arc<StatInfo> {
        let producer_to_ref = cte_ref
            .column_mapping
            .iter()
            .map(|(ref_col, producer_col)| (*producer_col, *ref_col))
            .collect::<HashMap<_, _>>();
        let column_stats = producer_stat_info
            .statistics
            .column_stats
            .iter()
            .filter_map(|(producer_col, stat)| {
                producer_to_ref
                    .get(producer_col)
                    .map(|ref_col| (*ref_col, stat.clone()))
            })
            .collect();

        Arc::new(StatInfo {
            cardinality: producer_stat_info.cardinality,
            statistics: Statistics {
                precise_cardinality: producer_stat_info.statistics.precise_cardinality,
                column_stats,
                top_n: Default::default(),
                count_min_sketch: Default::default(),
            },
        })
    }

    fn sync_materialized_cte_ref_stats(
        s_expr: &SExpr,
        cte_stats: &HashMap<String, Arc<StatInfo>>,
    ) -> Result<(SExpr, bool)> {
        struct StatsSyncer<'a> {
            cte_stats: &'a HashMap<String, Arc<StatInfo>>,
            changed: bool,
        }

        impl SExprVisitor for StatsSyncer<'_> {
            fn visit(&mut self, expr: &SExpr) -> Result<VisitAction> {
                if let RelOperator::MaterializedCTERef(cte_ref) = expr.plan()
                    && let Some(producer_stat_info) = self.cte_stats.get(&cte_ref.cte_name)
                {
                    let mut new_cte_ref = cte_ref.clone();
                    new_cte_ref.stat_info =
                        Some(DPhpyOptimizer::remap_materialized_cte_ref_stat_info(
                            cte_ref,
                            producer_stat_info,
                        ));
                    self.changed = true;
                    return Ok(VisitAction::Replace(expr.replace_plan(Arc::new(
                        RelOperator::MaterializedCTERef(new_cte_ref),
                    ))));
                }

                Ok(VisitAction::Continue)
            }
        }

        let mut visitor = StatsSyncer {
            cte_stats,
            changed: false,
        };
        let result = s_expr
            .accept(&mut visitor)?
            .unwrap_or_else(|| s_expr.clone());
        Ok((result, visitor.changed))
    }

    fn sync_materialized_cte_ref_stats_in_sequences(
        s_expr: &SExpr,
        stat_context: &StatContext,
    ) -> Result<(SExpr, bool)> {
        struct InSequencesStatsSyncer<'a> {
            changed: bool,
            stat_context: &'a StatContext,
        }

        impl SExprVisitor for InSequencesStatsSyncer<'_> {
            fn visit(&mut self, expr: &SExpr) -> Result<VisitAction> {
                if !matches!(expr.plan(), RelOperator::Sequence(_)) {
                    return Ok(VisitAction::Continue);
                }

                let (left_expr, left_changed) =
                    DPhpyOptimizer::sync_materialized_cte_ref_stats_in_sequences(
                        expr.left_child(),
                        self.stat_context,
                    )?;

                let mut cte_stats = HashMap::new();
                DPhpyOptimizer::collect_materialized_cte_stats(
                    &left_expr,
                    &mut cte_stats,
                    self.stat_context,
                )?;
                let (right_expr, right_synced) = DPhpyOptimizer::sync_materialized_cte_ref_stats(
                    expr.right_child(),
                    &cte_stats,
                )?;
                let (right_expr, right_changed) =
                    DPhpyOptimizer::sync_materialized_cte_ref_stats_in_sequences(
                        &right_expr,
                        self.stat_context,
                    )?;

                let changed = left_changed || right_synced || right_changed;
                self.changed |= changed;

                if changed {
                    Ok(VisitAction::Replace(expr.replace_children([
                        Arc::new(left_expr),
                        Arc::new(right_expr),
                    ])))
                } else {
                    Ok(VisitAction::SkipChildren)
                }
            }
        }

        let mut visitor = InSequencesStatsSyncer {
            changed: false,
            stat_context,
        };
        let result = s_expr
            .accept(&mut visitor)?
            .unwrap_or_else(|| s_expr.clone());
        Ok((result, visitor.changed))
    }

    fn process_cte_consumer_node(
        &mut self,
        s_expr: &SExpr,
        join_relation: Option<&SExpr>,
    ) -> Result<(Arc<SExpr>, bool)> {
        let cte_consumer = match s_expr.plan() {
            RelOperator::MaterializedCTERef(consumer) => consumer,
            _ => unreachable!(),
        };

        let join_relation = JoinRelation::new(join_relation.unwrap_or(s_expr));

        // Map table indexes before adding to join_relations
        self.collect_table_indexes(&cte_consumer.def)?;

        self.join_relations.push(join_relation);
        Ok((Arc::new(s_expr.clone()), true))
    }

    fn collect_table_indexes(&mut self, s_expr: &SExpr) -> Result<()> {
        struct TableIndexCollector<'a> {
            relation_idx: RelationId,
            table_index_map: &'a mut HashMap<IndexType, RelationId>,
        }

        impl SExprVisitor for TableIndexCollector<'_> {
            fn visit(&mut self, expr: &SExpr) -> Result<VisitAction> {
                match expr.plan() {
                    RelOperator::Scan(scan) => {
                        self.table_index_map
                            .insert(scan.table_index, self.relation_idx);
                    }
                    RelOperator::MaterializedCTERef(cte_consumer) => {
                        cte_consumer.def.accept(self)?;
                    }
                    _ => {}
                }

                Ok(VisitAction::Continue)
            }
        }

        let mut collector = TableIndexCollector {
            relation_idx: self.join_relations.len(),
            table_index_map: &mut self.table_index_map,
        };
        s_expr.accept(&mut collector)?;
        Ok(())
    }

    /// Process a unary operator node
    async fn process_unary_node(
        &mut self,
        s_expr: &SExpr,
        join_conditions: &mut Vec<JoinEquiCondition>,
        join_child: bool,
        join_relation: Option<&SExpr>,
    ) -> Result<(Arc<SExpr>, bool)> {
        let relation_start = self.join_relations.len();
        let (child, optimized) = if join_child {
            self.get_base_relations(
                s_expr.unary_child(),
                join_conditions,
                true,
                join_relation.or(Some(s_expr)),
                false,
            )
            .await?
        } else {
            self.get_base_relations(s_expr.unary_child(), join_conditions, false, None, false)
                .await?
        };

        if join_child {
            if let RelOperator::Filter(filter) = s_expr.plan() {
                let relations = &mut self.join_relations[relation_start..];
                if relations.len() == 1 {
                    // Restore this occurrence above the optimized child, not
                    // by scalar equality: equal filters at different positions
                    // must not cause one occurrence to remove another.
                    relations[0] = JoinRelation::new(&s_expr.replace_children([child.clone()]));
                } else {
                    self.filters.push(PositionedFilter {
                        filter: filter.clone(),
                        relations: (relation_start..self.join_relations.len()).collect(),
                        is_join_condition: false,
                    });
                }
            }
        }
        Ok((s_expr.replace_children([child]).into(), optimized))
    }

    /// Process a union all node
    async fn process_union_all_node(&mut self, s_expr: &SExpr) -> Result<(Arc<SExpr>, bool)> {
        let new_s_expr = self.new_children(s_expr).await?;
        self.join_relations.push(JoinRelation::new(&new_s_expr));
        Ok((Arc::new(new_s_expr), true))
    }

    /// Traverse the s_expr and get all base relations and join conditions
    #[async_recursion::async_recursion(# [recursive::recursive])]
    async fn get_base_relations(
        &mut self,
        s_expr: &SExpr,
        join_conditions: &mut Vec<JoinEquiCondition>,
        join_child: bool,
        join_relation: Option<&SExpr>,
        is_subquery: bool,
    ) -> Result<(Arc<SExpr>, bool)> {
        if is_subquery || (join_child && Self::is_subquery_operator(s_expr.plan())) {
            return self.process_subquery(s_expr).await;
        }

        match s_expr.plan() {
            RelOperator::Scan(_) => self.process_scan_node(s_expr, join_relation),

            RelOperator::Join(_) => self.process_join_node(s_expr, join_conditions).await,

            RelOperator::Sequence(_) => self.process_sequence_node(s_expr).await,
            RelOperator::MaterializedCTERef(_) => {
                self.process_cte_consumer_node(s_expr, join_relation)
            }

            RelOperator::ProjectSet(_)
            | RelOperator::Aggregate(_)
            | RelOperator::Sort(_)
            | RelOperator::Limit(_)
            | RelOperator::TopN(_)
            | RelOperator::EvalScalar(_)
            | RelOperator::Window(_)
            | RelOperator::WindowGroup(_)
            | RelOperator::Udf(_)
            | RelOperator::Filter(_)
            | RelOperator::MaterializedCTE(_) => {
                self.process_unary_node(s_expr, join_conditions, join_child, join_relation)
                    .await
            }

            RelOperator::UnionAll(_) => self.process_union_all_node(s_expr).await,

            RelOperator::Exchange(_) => {
                unreachable!()
            }

            RelOperator::DummyTableScan(_)
            | RelOperator::ConstantTableScan(_)
            | RelOperator::ExpressionScan(_)
            | RelOperator::CacheScan(_)
            | RelOperator::AsyncFunction(_)
            | RelOperator::RecursiveCteScan(_)
            | RelOperator::Mutation(_)
            | RelOperator::MutationSource(_)
            | RelOperator::CompactBlock(_) => Ok((Arc::new(s_expr.clone()), true)),
        }
    }

    /// The input plan tree has been optimized by heuristic optimizer
    /// So filters have pushed down join and cross join has been converted to inner join as possible as we can
    /// The output plan will have optimal join order theoretically
    pub async fn optimize_async(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        if !self.opt_ctx.get_enable_dphyp() || !self.opt_ctx.get_enable_join_reorder() {
            let (s_expr, _) = Self::sync_materialized_cte_ref_stats_in_sequences(
                s_expr,
                self.opt_ctx.get_stat_context(),
            )?;
            return Ok(s_expr);
        }

        // Firstly, we need to extract all join conditions and base tables
        let mut join_conditions = vec![];
        let (s_expr, optimized) = self
            .get_base_relations(s_expr, &mut join_conditions, false, None, false)
            .await?;

        if !optimized {
            self.opt_ctx.set_flag("dphyp_optimized", false);
            return Ok(Arc::unwrap_or_clone(s_expr));
        }

        if self.join_relations.len() == 1 || join_conditions.is_empty() {
            self.opt_ctx.set_flag("dphyp_optimized", true);
            return Ok(Arc::unwrap_or_clone(s_expr));
        }

        let placed_predicates = self.prepare_predicates()?;
        let model = DPhypJoinOrderModel {
            join_relations: &self.join_relations,
            join_conditions: &join_conditions,
            filters: &placed_predicates,
            stat_context: self.opt_ctx.get_stat_context(),
        };
        let mut hyper_dp = HyperDp::new(self.join_relations.len(), &model);

        if !self.build_join_order_edges(&mut hyper_dp, &join_conditions)? {
            self.opt_ctx.set_flag("dphyp_optimized", false);
            return Ok(Arc::unwrap_or_clone(s_expr));
        }

        if let Some(join_expr) = hyper_dp.find_best_order()? {
            let new_s_expr = Self::replace_join_expr(&join_expr.expr, &s_expr)?;
            self.opt_ctx.set_flag("dphyp_optimized", true);
            Ok(new_s_expr)
        } else {
            // Maybe exist cross join, which make graph disconnected
            self.opt_ctx.set_flag("dphyp_optimized", false);
            Ok(Arc::unwrap_or_clone(s_expr))
        }
    }

    /// Narrow each predicate's scope using columns available from opaque base
    /// outputs. Missing/outer columns, constants and non-deterministic scalars
    /// retain the collected scope. No expression rewriting is performed here.
    fn prepare_predicates(&self) -> Result<Vec<PlacedPredicate>> {
        let outputs = self
            .join_relations
            .iter()
            .map(|relation| {
                RelExpr::with_s_expr(&relation.s_expr())
                    .derive_relational_prop()
                    .map(|prop| prop.output_columns.clone())
            })
            .collect::<Result<Vec<_>>>()?;
        let mut result = vec![];
        for filter in &self.filters {
            for predicate in &filter.filter.predicates {
                let columns = predicate.used_columns();
                let mut required = HashSet::new();
                let mut movable = predicate.is_deterministic() && !columns.is_empty();
                if movable {
                    for column in &columns {
                        let providers = filter
                            .relations
                            .iter()
                            .copied()
                            .filter(|relation| outputs[*relation].contains(column))
                            .collect::<Vec<_>>();
                        if providers.len() != 1 {
                            movable = false;
                            break;
                        }
                        required.insert(providers[0]);
                    }
                }
                result.push(PlacedPredicate {
                    predicate: predicate.clone(),
                    relations: if movable {
                        required
                    } else {
                        filter.relations.clone()
                    },
                    placement: if filter.is_join_condition {
                        PredicatePlacement::JoinCondition
                    } else if movable {
                        PredicatePlacement::MovableFilter
                    } else {
                        PredicatePlacement::ScopedFilter
                    },
                });
            }
        }
        Ok(result)
    }

    fn build_join_order_edges(
        &self,
        hyper_dp: &mut HyperDp<'_, DPhypJoinOrderModel<'_>>,
        join_conditions: &[JoinEquiCondition],
    ) -> Result<bool> {
        for (edge_id, condition) in join_conditions.iter().enumerate() {
            let mut left_relation_set = HashSet::new();
            let mut right_relation_set = HashSet::new();

            let left_used_tables = condition.left.used_tables()?;
            for table in left_used_tables.iter() {
                left_relation_set.insert(self.table_index_map[table]);
            }

            let right_used_tables = condition.right.used_tables()?;
            for table in right_used_tables.iter() {
                right_relation_set.insert(self.table_index_map[table]);
            }

            if left_relation_set.is_empty() || right_relation_set.is_empty() {
                return Ok(false);
            }
            if left_relation_set.is_disjoint(&right_relation_set) {
                hyper_dp.add_edge(&left_relation_set, &right_relation_set, edge_id)?;
            }
        }

        Ok(true)
    }

    /// Replace the join expression in the plan tree.
    fn replace_join_expr(join_expr: &SExpr, s_expr: &SExpr) -> Result<SExpr> {
        struct JoinExprReplacer<'a> {
            join_expr: &'a SExpr,
            replaced: bool,
        }

        impl SExprVisitor for JoinExprReplacer<'_> {
            fn visit(&mut self, expr: &SExpr) -> Result<VisitAction> {
                if self.replaced {
                    return Ok(VisitAction::SkipChildren);
                }

                if matches!(expr.plan(), RelOperator::Join(_)) {
                    self.replaced = true;
                    return Ok(VisitAction::Replace(self.join_expr.clone()));
                }

                Ok(VisitAction::Continue)
            }
        }

        let result = s_expr.accept(&mut JoinExprReplacer {
            join_expr,
            replaced: false,
        })?;
        result.ok_or_else(|| {
            ErrorCode::Internal(
                "DPhyp replaced a join, but the expression tree was not rebuilt".to_string(),
            )
        })
    }
}

#[async_trait::async_trait]
impl Optimizer for DPhpyOptimizer {
    fn name(&self) -> String {
        "DPhpyOptimizer".to_string()
    }

    async fn optimize(&mut self, s_expr: SExpr) -> Result<SExpr> {
        self.optimize_async(&s_expr).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use databend_common_expression::Scalar;

    use super::*;
    use crate::plans::ConstantExpr;
    use crate::plans::DummyTableScan;
    use crate::plans::MaterializedCTE;
    use crate::plans::MaterializedCTERef;
    use crate::plans::Sequence;

    fn bool_constant(value: bool) -> crate::ScalarExpr {
        ConstantExpr {
            span: None,
            value: Scalar::Boolean(value),
        }
        .into()
    }

    #[test]
    fn test_filter_attaches_only_when_original_scope_is_complete() {
        let filters = [PlacedPredicate {
            // No referenced columns: placement must come from the collected
            // input scope, not from the scalar's used_columns/used_tables.
            predicate: bool_constant(false),
            relations: HashSet::from([0, 1]),
            placement: PredicatePlacement::ScopedFilter,
        }];
        let model = DPhypJoinOrderModel {
            join_relations: &[],
            join_conditions: &[],
            filters: &filters,
            stat_context: &StatContext::default(),
        };
        let expr = SExpr::create_leaf(DummyTableScan::new());
        let a = HashSet::from([0]);
        let b = HashSet::from([1]);
        let ab = HashSet::from([0, 1]);
        let bc = HashSet::from([1, 2]);
        let abc = HashSet::from([0, 1, 2]);
        let c = HashSet::from([2]);

        assert_eq!(model.attach_base_filters(expr.clone(), &a), expr);
        let expr = SExpr::create_binary(Join::default(), Arc::new(expr.clone()), Arc::new(expr));
        assert_eq!(
            model
                .attach_join_filters(expr.clone(), &bc, &[&b, &c])
                .unwrap(),
            expr
        );
        let attached = model
            .attach_join_filters(expr.clone(), &ab, &[&a, &b])
            .unwrap();
        assert!(matches!(attached.plan(), RelOperator::Filter(_)));
        assert_eq!(attached.unary_child(), &expr);
        // The old a/b subtree need not exist: use its first covering ancestor.
        assert!(matches!(
            model
                .attach_join_filters(expr.clone(), &abc, &[&a, &bc])
                .unwrap()
                .plan(),
            RelOperator::Filter(_)
        ));
        // A child already covered the scope: never attach the filter twice.
        assert_eq!(
            model
                .attach_join_filters(expr.clone(), &abc, &[&ab, &c])
                .unwrap(),
            expr
        );
    }

    #[test]
    fn test_sync_materialized_cte_ref_updates_consumer_stats() {
        let old_def = SExpr::create_leaf(DummyTableScan::new());
        let new_def = SExpr::create_unary(
            Filter {
                predicates: vec![bool_constant(false)],
            },
            Arc::new(old_def.clone()),
        );

        let producer = SExpr::create_unary(
            MaterializedCTE::new("cte".to_string(), None),
            Arc::new(new_def),
        );
        let consumer = SExpr::create_leaf(RelOperator::MaterializedCTERef(MaterializedCTERef {
            cte_name: "cte".to_string(),
            output_columns: vec![],
            def: old_def.clone(),
            column_mapping: HashMap::new(),
            stat_info: None,
        }));
        let query = SExpr::create_unary(
            Filter {
                predicates: vec![bool_constant(true)],
            },
            Arc::new(consumer),
        );

        let mut cte_stats = HashMap::new();
        DPhpyOptimizer::collect_materialized_cte_stats(
            &producer,
            &mut cte_stats,
            &StatContext::default(),
        )
        .unwrap();

        let (optimized, changed) =
            DPhpyOptimizer::sync_materialized_cte_ref_stats(&query, &cte_stats).unwrap();

        assert!(changed);
        let consumer = optimized.child(0).unwrap();
        let RelOperator::MaterializedCTERef(cte_ref) = consumer.plan() else {
            panic!("expected materialized cte ref");
        };

        assert_eq!(cte_ref.def, old_def);
        assert_eq!(cte_ref.stat_info.as_ref().unwrap().cardinality, 0.0);
    }

    #[test]
    fn test_sync_materialized_cte_ref_in_sequences_without_join_reorder() {
        let old_def = SExpr::create_leaf(DummyTableScan::new());
        let new_def = SExpr::create_unary(
            Filter {
                predicates: vec![bool_constant(false)],
            },
            Arc::new(old_def.clone()),
        );

        let producer = SExpr::create_unary(
            MaterializedCTE::new("cte".to_string(), None),
            Arc::new(new_def),
        );
        let consumer = SExpr::create_leaf(RelOperator::MaterializedCTERef(MaterializedCTERef {
            cte_name: "cte".to_string(),
            output_columns: vec![],
            def: old_def.clone(),
            column_mapping: HashMap::new(),
            stat_info: None,
        }));
        let root = SExpr::create_binary(Sequence, Arc::new(producer), Arc::new(consumer));

        let (optimized, changed) = DPhpyOptimizer::sync_materialized_cte_ref_stats_in_sequences(
            &root,
            &StatContext::default(),
        )
        .unwrap();

        assert!(changed);
        let consumer = optimized.child(1).unwrap();
        let RelOperator::MaterializedCTERef(cte_ref) = consumer.plan() else {
            panic!("expected materialized cte ref");
        };

        assert_eq!(cte_ref.def, old_def);
        assert_eq!(cte_ref.stat_info.as_ref().unwrap().cardinality, 0.0);
    }

    #[test]
    fn test_sync_materialized_cte_ref_keeps_unmatched_consumer_stats() {
        let old_def = SExpr::create_leaf(DummyTableScan::new());
        let producer = SExpr::create_unary(
            MaterializedCTE::new("cte".to_string(), None),
            Arc::new(old_def.clone()),
        );
        let consumer = SExpr::create_leaf(RelOperator::MaterializedCTERef(MaterializedCTERef {
            cte_name: "other_cte".to_string(),
            output_columns: vec![],
            def: old_def.clone(),
            column_mapping: HashMap::new(),
            stat_info: None,
        }));

        let mut cte_stats = HashMap::new();
        DPhpyOptimizer::collect_materialized_cte_stats(
            &producer,
            &mut cte_stats,
            &StatContext::default(),
        )
        .unwrap();

        let (optimized, changed) =
            DPhpyOptimizer::sync_materialized_cte_ref_stats(&consumer, &cte_stats).unwrap();

        assert!(!changed);
        let RelOperator::MaterializedCTERef(cte_ref) = optimized.plan() else {
            panic!("expected materialized cte ref");
        };

        assert_eq!(cte_ref.def, old_def);
        assert!(cte_ref.stat_info.is_none());
    }
}
