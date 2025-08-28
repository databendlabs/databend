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
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::hyper_dp::util::intersect;
use crate::optimizer::optimizers::hyper_dp::util::union;
use crate::optimizer::optimizers::hyper_dp::JoinNode;
use crate::optimizer::optimizers::hyper_dp::JoinRelation;
use crate::optimizer::optimizers::hyper_dp::QueryGraph;
use crate::optimizer::optimizers::hyper_dp::RelationSetTree;
use crate::optimizer::optimizers::rule::RuleFactory;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::optimizer::Optimizer;
use crate::optimizer::OptimizerContext;
use crate::plans::Filter;
use crate::plans::JoinType;
use crate::plans::RelOperator;
use crate::IndexType;
use crate::MetadataRef;
use crate::ScalarExpr;

const EMIT_THRESHOLD: usize = 10000;
const RELATION_THRESHOLD: usize = 10;

/// The join reorder algorithm follows the paper: Dynamic Programming Strikes Back
/// See the paper for more details.
pub struct DPhpyOptimizer {
    opt_ctx: Arc<OptimizerContext>,
    join_relations: Vec<JoinRelation>,
    // base table index -> index of join_relations
    table_index_map: HashMap<IndexType, IndexType>,
    dp_table: HashMap<Vec<IndexType>, JoinNode>,
    query_graph: QueryGraph,
    relation_set_tree: RelationSetTree,
    // non-equi conditions
    filters: HashSet<Filter>,
    // The number of times emit_csg_cmp is called
    emit_count: usize,
}

impl DPhpyOptimizer {
    pub fn new(opt_ctx: Arc<OptimizerContext>) -> Self {
        Self {
            opt_ctx,
            join_relations: vec![],
            table_index_map: Default::default(),
            dp_table: Default::default(),
            query_graph: QueryGraph::new(),
            relation_set_tree: Default::default(),
            filters: HashSet::new(),
            emit_count: 0,
        }
    }

    fn table_ctx(&self) -> Arc<dyn TableContext> {
        self.opt_ctx.get_table_ctx()
    }

    fn metadata(&self) -> MetadataRef {
        self.opt_ctx.get_metadata()
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
        let relation_idx = self.join_relations.len() as IndexType;
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
        let relation_idx = self.join_relations.len() as IndexType;
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
        let join_relation = if let Some(relation) = join_relation {
            // Check if relation contains filter, if exists, check if the filter in `filters`
            // If exists, remove it from `filters`
            self.check_filter(relation);
            JoinRelation::new(relation)
        } else {
            JoinRelation::new(s_expr)
        };

        if let RelOperator::Scan(op) = s_expr.plan() {
            self.table_index_map
                .insert(op.table_index, self.join_relations.len() as IndexType);
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
                | RelOperator::ProjectSet(_)
                | RelOperator::Window(_)
                | RelOperator::Udf(_)
        )
    }

    /// Process a join node
    async fn process_join_node(
        &mut self,
        s_expr: &SExpr,
        join_conditions: &mut Vec<(ScalarExpr, ScalarExpr)>,
    ) -> Result<(Arc<SExpr>, bool)> {
        let op = match s_expr.plan() {
            RelOperator::Join(op) => op,
            _ => unreachable!(),
        };

        // Skip if build side cache info is present
        if op.build_side_cache_info.is_some() {
            return Ok((Arc::new(s_expr.clone()), true));
        }

        // Check if it's an inner join
        let mut is_inner_join =
            matches!(op.join_type, JoinType::Inner) || matches!(op.join_type, JoinType::Cross);

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

            join_conditions.push((condition.left.clone(), condition.right.clone()));
        }

        // Add non-equi conditions to filters
        if !op.non_equi_conditions.is_empty() {
            let filter = Filter {
                predicates: op.non_equi_conditions.clone(),
            };
            self.filters.insert(filter);
        }

        if !is_inner_join {
            // For non-inner joins, process children in parallel
            let new_s_expr = self.new_children(s_expr).await?;
            self.join_relations.push(JoinRelation::new(&new_s_expr));
            return Ok((Arc::new(new_s_expr), true));
        }

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

        let new_s_expr: Arc<SExpr> = Arc::new(s_expr.replace_children([left_res.0, right_res.0]));
        Ok((new_s_expr, left_res.1 && right_res.1))
    }

    async fn process_sequence_node(&mut self, s_expr: &SExpr) -> Result<(Arc<SExpr>, bool)> {
        let mut left_dphyp = DPhpyOptimizer::new(self.opt_ctx.clone());
        let left_expr = left_dphyp.optimize_async(s_expr.child(0)?).await?;

        let mut right_dphyp = DPhpyOptimizer::new(self.opt_ctx.clone());
        let right_expr = right_dphyp.optimize_async(s_expr.child(1)?).await?;

        // Merge table_index_map from right child into current table_index_map
        let relation_idx = self.join_relations.len() as IndexType;
        for table_index in right_dphyp.table_index_map.keys() {
            self.table_index_map.insert(*table_index, relation_idx);
        }

        let new_s_expr = s_expr.replace_children([Arc::new(left_expr), Arc::new(right_expr)]);
        self.join_relations.push(JoinRelation::new(&new_s_expr));
        Ok((Arc::new(new_s_expr), true))
    }

    async fn process_cte_consumer_node(
        &mut self,
        s_expr: &SExpr,
        join_relation: Option<&SExpr>,
    ) -> Result<(Arc<SExpr>, bool)> {
        let cte_consumer = match s_expr.plan() {
            RelOperator::MaterializedCTERef(consumer) => consumer,
            _ => unreachable!(),
        };

        let join_relation = if let Some(relation) = join_relation {
            // Check if relation contains filter, if exists, check if the filter in `filters`
            // If exists, remove it from `filters`
            self.check_filter(relation);
            JoinRelation::new(relation)
        } else {
            JoinRelation::new(s_expr)
        };

        // Map table indexes before adding to join_relations
        let relation_idx = self.join_relations.len() as IndexType;
        let table_indexes = Self::get_table_indexes(&cte_consumer.def);
        for table_index in table_indexes {
            self.table_index_map.insert(table_index, relation_idx);
        }

        self.join_relations.push(join_relation);
        Ok((Arc::new(s_expr.clone()), true))
    }

    fn get_table_indexes(s_expr: &SExpr) -> Vec<IndexType> {
        let mut table_indexes = Vec::new();
        Self::collect_table_indexes_recursive(s_expr, &mut table_indexes);
        table_indexes
    }

    fn collect_table_indexes_recursive(s_expr: &SExpr, table_indexes: &mut Vec<IndexType>) {
        if let RelOperator::Scan(scan) = s_expr.plan() {
            table_indexes.push(scan.table_index);
        }

        if let RelOperator::MaterializedCTERef(cte_consumer) = s_expr.plan() {
            Self::collect_table_indexes_recursive(&cte_consumer.def, table_indexes);
        }

        for child in s_expr.children() {
            Self::collect_table_indexes_recursive(child, table_indexes);
        }
    }

    /// Process a unary operator node
    async fn process_unary_node(
        &mut self,
        s_expr: &SExpr,
        join_conditions: &mut Vec<(ScalarExpr, ScalarExpr)>,
        join_child: bool,
        join_relation: Option<&SExpr>,
    ) -> Result<(Arc<SExpr>, bool)> {
        // If plan is filter, save it
        if let RelOperator::Filter(op) = s_expr.plan.as_ref() {
            if join_child {
                self.filters.insert(op.clone());
            }
        }

        let (child, optimized) = if join_child {
            self.get_base_relations(
                s_expr.child(0)?,
                join_conditions,
                true,
                join_relation.or(Some(s_expr)),
                false,
            )
            .await?
        } else {
            self.get_base_relations(s_expr.child(0)?, join_conditions, false, None, false)
                .await?
        };

        let new_s_expr = Arc::new(s_expr.replace_children([child]));
        Ok((new_s_expr, optimized))
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
        join_conditions: &mut Vec<(ScalarExpr, ScalarExpr)>,
        join_child: bool,
        join_relation: Option<&SExpr>,
        is_subquery: bool,
    ) -> Result<(Arc<SExpr>, bool)> {
        if is_subquery {
            return self.process_subquery(s_expr).await;
        }

        match s_expr.plan() {
            RelOperator::Scan(_) => self.process_scan_node(s_expr, join_relation),

            RelOperator::Join(_) => self.process_join_node(s_expr, join_conditions).await,

            RelOperator::Sequence(_) => self.process_sequence_node(s_expr).await,
            RelOperator::MaterializedCTERef(_) => {
                self.process_cte_consumer_node(s_expr, join_relation).await
            }

            RelOperator::ProjectSet(_)
            | RelOperator::Aggregate(_)
            | RelOperator::Sort(_)
            | RelOperator::Limit(_)
            | RelOperator::EvalScalar(_)
            | RelOperator::Window(_)
            | RelOperator::Udf(_)
            | RelOperator::Filter(_)
            | RelOperator::SecureFilter(_)
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

    /// Initialize the DP table with single relation plans
    async fn initialize_dp_table(&mut self) -> Result<()> {
        for (idx, relation) in self.join_relations.iter().enumerate() {
            // Get nodes in `relation_set_tree`
            let nodes = self.relation_set_tree.get_relation_set_by_index(idx)?;
            let cardinality = relation
                .cardinality(self.table_ctx().clone(), self.metadata().clone())
                .await?;

            let join = JoinNode {
                join_type: JoinType::Inner,
                leaves: Arc::new(nodes.clone()),
                children: Arc::new(vec![]),
                join_conditions: Arc::new(vec![]),
                cost: 0.0,
                cardinality: Some(cardinality),
                s_expr: None,
            };

            self.dp_table.insert(nodes, join);
        }
        Ok(())
    }

    /// The input plan tree has been optimized by heuristic optimizer
    /// So filters have pushed down join and cross join has been converted to inner join as possible as we can
    /// The output plan will have optimal join order theoretically
    pub async fn optimize_async(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        if !self.opt_ctx.get_enable_dphyp() || !self.opt_ctx.get_enable_join_reorder() {
            return Ok(s_expr.clone());
        }

        // Firstly, we need to extract all join conditions and base tables
        // `join_condition` is pair, left is left_condition, right is right_condition
        let mut join_conditions = vec![];
        let (s_expr, optimized) = self
            .get_base_relations(s_expr, &mut join_conditions, false, None, false)
            .await?;

        if !optimized {
            self.opt_ctx.set_flag("dphyp_optimized", false);
            return Ok(s_expr.as_ref().clone());
        }

        if self.join_relations.len() == 1 || join_conditions.is_empty() {
            self.opt_ctx.set_flag("dphyp_optimized", true);
            return Ok(s_expr.as_ref().clone());
        }

        // Second, use `join_conditions` to create edges in `query_graph`
        if !self.build_query_graph(&join_conditions)? {
            self.opt_ctx.set_flag("dphyp_optimized", false);
            return Ok(s_expr.as_ref().clone());
        }

        // Sort neighbors for deterministic behavior
        for (_, neighbors) in self.query_graph.cached_neighbors.iter_mut() {
            neighbors.sort();
        }

        // Perform join reordering
        self.join_reorder().await?;

        // Get all join relations in `relation_set_tree`
        let all_relations = self
            .relation_set_tree
            .get_relation_set(&(0..self.join_relations.len()).collect())?;

        if let Some(final_plan) = self.dp_table.get(&all_relations) {
            let (s_expr, optimized) = self.generate_final_plan(final_plan, &s_expr)?;
            self.opt_ctx.set_flag("dphyp_optimized", optimized);
            Ok(s_expr.as_ref().clone())
        } else {
            // Maybe exist cross join, which make graph disconnected
            self.opt_ctx.set_flag("dphyp_optimized", false);
            Ok(s_expr.as_ref().clone())
        }
    }

    /// Build the query graph from join conditions
    fn build_query_graph(&mut self, join_conditions: &[(ScalarExpr, ScalarExpr)]) -> Result<bool> {
        for (left_condition, right_condition) in join_conditions.iter() {
            // Find the corresponding join relations in `join_conditions`
            let mut left_relation_set = HashSet::new();
            let mut right_relation_set = HashSet::new();

            let left_used_tables = left_condition.used_tables()?;
            for table in left_used_tables.iter() {
                left_relation_set.insert(self.table_index_map[table]);
            }

            let right_used_tables = right_condition.used_tables()?;
            for table in right_used_tables.iter() {
                right_relation_set.insert(self.table_index_map[table]);
            }

            if !left_relation_set.is_empty() && !right_relation_set.is_empty() {
                // Use `left_relation_set` and `right_relation_set` to initial RelationSetTree
                let left_relation_set = self
                    .relation_set_tree
                    .get_relation_set(&left_relation_set)?;
                let right_relation_set = self
                    .relation_set_tree
                    .get_relation_set(&right_relation_set)?;

                // Try to create edges if `left_relation_set` and `right_relation_set` are not disjoint
                if !intersect(&left_relation_set, &right_relation_set) {
                    self.query_graph.create_edges(
                        &left_relation_set,
                        &right_relation_set,
                        (left_condition.clone(), right_condition.clone()),
                    )?;
                    self.query_graph.create_edges(
                        &right_relation_set,
                        &left_relation_set,
                        (right_condition.clone(), left_condition.clone()),
                    )?;
                }
            } else {
                // If one of `left_relation_set` and `right_relation_set` is empty, we need to forbid dphyp algo
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Process a node as a starting point for enumeration
    async fn process_node_as_start(&mut self, idx: usize) -> Result<bool> {
        // Get node from `relation_set_tree`
        let node = self.relation_set_tree.get_relation_set_by_index(idx)?;

        // Emit node as subgraph
        if !self.emit_csg(&node).await? {
            return Ok(false);
        }

        // Create forbidden node set
        // Forbid node idx will less than current idx
        let forbidden_nodes = (0..idx).collect();

        // Enlarge the subgraph recursively
        if !self.enumerate_csg_rec(&node, &forbidden_nodes).await? {
            return Ok(false);
        }

        Ok(true)
    }

    /// Join reorder by dynamic programming algorithm.
    async fn join_reorder_by_dphyp(&mut self) -> Result<bool> {
        // Choose all nodes as enumeration start node once (desc order)
        for idx in (0..self.join_relations.len()).rev() {
            if !self.process_node_as_start(idx).await? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Find the minimum cost pair of relations
    async fn find_minimum_cost_pair(
        &mut self,
        join_relations: &[Vec<IndexType>],
    ) -> Result<(f64, usize, usize, Vec<IndexType>)> {
        let mut min_cost = f64::INFINITY;
        let mut left_idx = 0;
        let mut right_idx = 0;
        let mut new_relations = vec![];

        for i in 0..join_relations.len() {
            let left_relation = &join_relations[i];

            for (j, right_relation) in join_relations.iter().enumerate().skip(i + 1) {
                // Check if left_relation set and right_relation set are connected, if connected, get join conditions.
                let join_conditions = self
                    .query_graph
                    .is_connected(left_relation, right_relation)?;

                if !join_conditions.is_empty() {
                    // If left_relation set and right_relation set are connected, emit csg-cmp-pair and keep the
                    // minimum cost pair in `dp_table`.
                    let cost = self
                        .emit_csg_cmp(left_relation, right_relation, join_conditions)
                        .await?;

                    // Update the minimum cost pair.
                    if cost < min_cost {
                        min_cost = cost;
                        left_idx = i;
                        right_idx = j;
                        // Update the new relation set in this iteration.
                        new_relations = union(left_relation, right_relation);
                    }
                }
            }
        }

        Ok((min_cost, left_idx, right_idx, new_relations))
    }

    /// Handle the case when no connected relations exist
    async fn handle_disconnected_relations(
        &mut self,
        join_relations: &[Vec<IndexType>],
    ) -> Result<(usize, usize, Vec<IndexType>)> {
        // Pick the pair of relation sets with the minimum cost and connect them with a cross product.
        let mut lowest_cost = Vec::with_capacity(2);
        let mut lowest_index = Vec::with_capacity(2);

        for (i, relation) in join_relations.iter().enumerate().take(2) {
            let mut join_node = self.dp_table.get(relation).unwrap().clone();
            let cardinality = join_node.cardinality(&self.join_relations).await?;
            lowest_cost.push(cardinality);
            lowest_index.push(i);
        }

        if lowest_cost[1] < lowest_cost[0] {
            lowest_cost.swap(0, 1);
            lowest_index.swap(0, 1);
        }

        // Update the minimum cost relation set pair.
        for (i, relation) in join_relations.iter().enumerate().skip(2) {
            let mut join_node = self.dp_table.get(relation).unwrap().clone();
            let cardinality = join_node.cardinality(&self.join_relations).await?;

            if cardinality < lowest_cost[0] {
                lowest_cost[1] = lowest_cost[0];
                lowest_index[1] = lowest_index[0];
                lowest_cost[0] = cardinality;
                lowest_index[0] = i;
            } else if cardinality < lowest_cost[1] {
                lowest_cost[1] = cardinality;
                lowest_index[1] = i;
            }
        }

        // Update the minimum cost index pair.
        let left_idx = lowest_index[0];
        let right_idx = lowest_index[1];

        // Store the new relation set in this iteration.
        let new_relations = union(&join_relations[left_idx], &join_relations[right_idx]);

        // Emit csg-cmp-pair and insert the cross product into `dp_table`.
        self.emit_csg_cmp(
            &join_relations[left_idx],
            &join_relations[right_idx],
            vec![],
        )
        .await?;

        Ok((left_idx, right_idx, new_relations))
    }

    /// Join reorder by greedy algorithm.
    async fn join_reorder_by_greedy(&mut self) -> Result<bool> {
        // The Greedy Operator Ordering starts with a single relation and iteratively adds the relation that minimizes the cost of the join.
        // the algorithm terminates when all relations have been added, the cost of a join is the sum of the cardinalities of the node involved
        // in the tree, the algorithm is not guaranteed to find the optimal join tree, it is guaranteed to find it in polynomial time.
        // Create join relations list, all relations have been inserted into dp_table in func `join_reorder`.
        let mut join_relations = (0..self.join_relations.len())
            .map(|idx| self.relation_set_tree.get_relation_set_by_index(idx))
            .collect::<Result<Vec<_>>>()?;

        // When all relations have been added, the algorithm terminates.
        while join_relations.len() > 1 {
            // Find the minimum cost pair of relations
            let (min_cost, mut left_idx, mut right_idx, mut new_relations) =
                self.find_minimum_cost_pair(&join_relations).await?;

            // The minimum cost has no updates, which means there are no connected relation set pairs.
            if min_cost == f64::INFINITY {
                let (left, right, new_rel) =
                    self.handle_disconnected_relations(&join_relations).await?;
                left_idx = left;
                right_idx = right;
                new_relations = new_rel;
            }

            if left_idx > right_idx {
                std::mem::swap(&mut left_idx, &mut right_idx);
            }

            // Update join_relations by removing the two relations and adding the new one
            join_relations.remove(right_idx);
            join_relations.remove(left_idx);
            join_relations.push(new_relations);
        }

        Ok(true)
    }

    /// Adaptive Optimization:
    /// If the if the query graph is simple enough, it uses dynamic programming to construct the optimal join tree,
    /// if that is not possible within the given optimization budget, it switches to a greedy approach.
    async fn join_reorder(&mut self) -> Result<()> {
        // Initial `dp_table` with plan for single relation
        self.initialize_dp_table().await?;

        // First, try to use dynamic programming to find the optimal join order.
        if !self.join_reorder_by_dphyp().await? {
            // When DPhpy takes too much time during join ordering, it is necessary to exit the dynamic programming algorithm
            // and switch to a greedy algorithm to minimizes the overall query time.
            self.join_reorder_by_greedy().await?;
        }

        Ok(())
    }

    /// EmitCsg will take a non-empty subset of hyper_graph's nodes(V) which contains a connected subgraph.
    /// Then it will possibly generate a connected complement which will combine `nodes` to be a csg-cmp-pair.
    async fn emit_csg(&mut self, nodes: &[IndexType]) -> Result<bool> {
        if nodes.len() == self.join_relations.len() {
            return Ok(true);
        }

        let mut forbidden_nodes: HashSet<IndexType> = (0..(nodes[0])).collect();
        forbidden_nodes.extend(nodes);

        // Get neighbors of `nodes`
        let neighbors = self.query_graph.neighbors(nodes, &forbidden_nodes)?;
        if neighbors.is_empty() {
            return Ok(true);
        }

        // Traverse all neighbors by desc
        for neighbor in neighbors.iter().rev() {
            let neighbor_relations = self
                .relation_set_tree
                .get_relation_set_by_index(*neighbor)?;

            // Check if neighbor is connected with `nodes`
            let join_conditions = self.query_graph.is_connected(nodes, &neighbor_relations)?;
            if !join_conditions.is_empty()
                && !self
                    .try_emit_csg_cmp(nodes, &neighbor_relations, join_conditions)
                    .await?
            {
                return Ok(false);
            }

            if !self
                .enumerate_cmp_rec(nodes, &neighbor_relations, &forbidden_nodes)
                .await?
            {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Check if we should stop optimization due to exceeding emit threshold
    async fn try_emit_csg_cmp(
        &mut self,
        left: &[IndexType],
        right: &[IndexType],
        join_conditions: Vec<(ScalarExpr, ScalarExpr)>,
    ) -> Result<bool> {
        self.emit_count += 1;
        // If `emit_count` is greater than `EMIT_THRESHOLD`, we need to stop dynamic programming,
        // otherwise it will take too much time
        match self.emit_count > EMIT_THRESHOLD {
            false => {
                self.emit_csg_cmp(left, right, join_conditions).await?;
                Ok(true)
            }
            true => Ok(false),
        }
    }

    /// Create a join node from left and right relations
    async fn create_join_node(
        &self,
        left: &[IndexType],
        right: &[IndexType],
        join_conditions: Vec<(ScalarExpr, ScalarExpr)>,
        left_cardinality: f64,
        right_cardinality: f64,
        left_join: JoinNode,
        right_join: JoinNode,
    ) -> Result<JoinNode> {
        let parent_set = union(left, right);

        if !join_conditions.is_empty() {
            let mut join_node = JoinNode {
                join_type: JoinType::Inner,
                leaves: Arc::new(parent_set.clone()),
                children: if left_cardinality < right_cardinality {
                    Arc::new(vec![right_join, left_join])
                } else {
                    Arc::new(vec![left_join, right_join])
                },
                cost: 0.0,
                join_conditions: Arc::new(join_conditions),
                cardinality: None,
                s_expr: None,
            };

            // Calculate cost for inner join
            let cost = join_node.cardinality(&self.join_relations).await?
                + join_node.children[0].cost
                + join_node.children[1].cost;

            let mut result = join_node;
            result.set_cost(cost);
            Ok(result)
        } else {
            // Create cross join
            let join_node = JoinNode {
                join_type: JoinType::Cross,
                leaves: Arc::new(parent_set.clone()),
                children: if left_cardinality < right_cardinality {
                    Arc::new(vec![right_join, left_join])
                } else {
                    Arc::new(vec![left_join, right_join])
                },
                cost: left_cardinality * right_cardinality,
                join_conditions: Arc::new(vec![]),
                cardinality: None,
                s_expr: None,
            };

            Ok(join_node)
        }
    }

    /// EmitCsgCmp will join the optimal plan from left and right
    async fn emit_csg_cmp(
        &mut self,
        left: &[IndexType],
        right: &[IndexType],
        mut join_conditions: Vec<(ScalarExpr, ScalarExpr)>,
    ) -> Result<f64> {
        debug_assert!(self.dp_table.contains_key(left));
        debug_assert!(self.dp_table.contains_key(right));

        let parent_set = union(left, right);
        let mut left_join = self.dp_table.get(left).unwrap().clone();
        let mut right_join = self.dp_table.get(right).unwrap().clone();

        let left_cardinality = left_join.cardinality(&self.join_relations).await?;
        let right_cardinality = right_join.cardinality(&self.join_relations).await?;

        // Swap join conditions if left cardinality is smaller
        if left_cardinality < right_cardinality {
            for join_condition in join_conditions.iter_mut() {
                std::mem::swap(&mut join_condition.0, &mut join_condition.1);
            }
        }

        // Create join node
        let join_node = self
            .create_join_node(
                left,
                right,
                join_conditions,
                left_cardinality,
                right_cardinality,
                left_join,
                right_join,
            )
            .await?;

        // Check if we already have a better plan
        let parent_node = self.dp_table.get(&parent_set);
        let cost = if parent_node.is_none() {
            join_node.cost
        } else {
            let parent_node = parent_node.unwrap();
            if parent_node.cost < join_node.cost {
                parent_node.cost
            } else {
                join_node.cost
            }
        };

        // Update `dp_table` if we found a better plan
        if parent_node.is_none() || parent_node.unwrap().cost > join_node.cost {
            self.dp_table.insert(parent_set, join_node);
        }

        Ok(cost)
    }

    /// The second parameter is a set which is connected and must be extended until a valid csg-cmp-pair is reached.
    /// Therefore, it considers the neighborhood of right.
    #[async_recursion::async_recursion(# [recursive::recursive])]
    async fn enumerate_cmp_rec(
        &mut self,
        left: &[IndexType],
        right: &[IndexType],
        forbidden_nodes: &HashSet<IndexType>,
    ) -> Result<bool> {
        let neighbor_set = self.query_graph.neighbors(right, forbidden_nodes)?;
        if neighbor_set.is_empty() {
            return Ok(true);
        }

        let mut merged_sets = Vec::new();
        for neighbor in neighbor_set.iter() {
            let neighbor_relations = self
                .relation_set_tree
                .get_relation_set_by_index(*neighbor)?;

            // Merge `right` with `neighbor_relations`
            let merged_relation_set = union(right, &neighbor_relations);
            let join_conditions = self.query_graph.is_connected(left, &merged_relation_set)?;

            if merged_relation_set.len() > right.len()
                && self.dp_table.contains_key(&merged_relation_set)
                && !join_conditions.is_empty()
                && !self
                    .try_emit_csg_cmp(left, &merged_relation_set, join_conditions)
                    .await?
            {
                return Ok(false);
            }

            merged_sets.push(merged_relation_set);
        }

        // Continue to enumerate cmp
        let mut new_forbidden_nodes = forbidden_nodes.clone();
        for (idx, neighbor) in neighbor_set.iter().enumerate() {
            new_forbidden_nodes.insert(*neighbor);
            if !self
                .enumerate_cmp_rec(left, &merged_sets[idx], &new_forbidden_nodes)
                .await?
            {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// EnumerateCsgRec will extend the given `nodes`.
    /// It'll consider each non-empty, proper subset of the neighborhood of nodes that are not forbidden.
    #[async_recursion::async_recursion(# [recursive::recursive])]
    async fn enumerate_csg_rec(
        &mut self,
        nodes: &[IndexType],
        forbidden_nodes: &HashSet<IndexType>,
    ) -> Result<bool> {
        let mut neighbors = self.query_graph.neighbors(nodes, forbidden_nodes)?;
        if neighbors.is_empty() {
            return Ok(true);
        }

        // Limit neighbors to consider for large relation sets
        if self.join_relations.len() >= RELATION_THRESHOLD {
            neighbors = neighbors
                .iter()
                .take(nodes.len())
                .copied()
                .collect::<Vec<IndexType>>();
        }

        let mut merged_sets = Vec::new();
        for neighbor in neighbors.iter() {
            let neighbor_relations = self
                .relation_set_tree
                .get_relation_set_by_index(*neighbor)?;

            let merged_relation_set = union(nodes, &neighbor_relations);
            if self.dp_table.contains_key(&merged_relation_set)
                && merged_relation_set.len() > nodes.len()
                && !self.emit_csg(&merged_relation_set).await?
            {
                return Ok(false);
            }

            merged_sets.push(merged_relation_set);
        }

        // Continue to enumerate csg
        let mut new_forbidden_nodes = forbidden_nodes.clone();
        for (idx, neighbor) in neighbors.iter().enumerate() {
            // Only clone for small relation sets to improve performance
            if self.join_relations.len() < RELATION_THRESHOLD {
                new_forbidden_nodes = forbidden_nodes.clone();
            }

            new_forbidden_nodes.insert(*neighbor);
            if !self
                .enumerate_csg_rec(&merged_sets[idx], &new_forbidden_nodes)
                .await?
            {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Apply filters to the optimized plan
    fn apply_filters(&self, s_expr: &SExpr) -> Result<SExpr> {
        if self.filters.is_empty() {
            return Ok(s_expr.clone());
        }

        // Add filters to `s_expr`, then push down filters if possible
        let mut predicates = vec![];
        for filter in self.filters.iter() {
            predicates.extend(filter.clone().predicates.iter().cloned())
        }

        let mut new_s_expr = SExpr::create_unary(
            Arc::new(RelOperator::Filter(Filter { predicates })),
            Arc::new(s_expr.clone()),
        );

        // Push down filters
        new_s_expr = self.push_down_filter(&new_s_expr)?;

        // Remove empty filter
        if let RelOperator::Filter(filter) = new_s_expr.plan.as_ref() {
            if filter.predicates.is_empty() {
                new_s_expr = new_s_expr.child(0)?.clone();
            }
        }

        Ok(new_s_expr)
    }

    /// Map join order in `JoinNode` to `SExpr`
    fn generate_final_plan(
        &self,
        final_plan: &JoinNode,
        s_expr: &SExpr,
    ) -> Result<(Arc<SExpr>, bool)> {
        // Convert `final_plan` to `SExpr`
        let join_expr = final_plan.s_expr(&self.join_relations);
        // Find first join node in `s_expr`, then replace it with `join_expr`
        let new_s_expr = self.replace_join_expr(&join_expr, s_expr)?;
        Ok((Arc::new(new_s_expr), true))
    }

    /// Replace the join expression in the plan tree
    #[allow(clippy::only_used_in_recursion)]
    #[recursive::recursive]
    fn replace_join_expr(&self, join_expr: &SExpr, s_expr: &SExpr) -> Result<SExpr> {
        match s_expr.plan.as_ref() {
            RelOperator::Join(_) => {
                let mut new_s_expr = s_expr.clone();
                new_s_expr.plan = join_expr.plan.clone();
                new_s_expr.children = join_expr.children.clone();

                // Apply filters if needed
                self.apply_filters(&new_s_expr)
            }
            _ => {
                let child_expr = self.replace_join_expr(join_expr, &s_expr.children[0])?;
                let mut new_s_expr = s_expr.clone();
                new_s_expr.children = vec![Arc::new(child_expr)];
                Ok(new_s_expr)
            }
        }
    }

    /// Check if a filter exists in the expression and remove it from filters set
    fn check_filter(&mut self, expr: &SExpr) {
        if let RelOperator::Filter(filter) = expr.plan.as_ref() {
            if self.filters.contains(filter) {
                self.filters.remove(filter);
            }
        }
        for child in expr.children() {
            self.check_filter(child);
        }
    }

    /// Push down filters to lower levels in the plan tree
    fn push_down_filter(&self, s_expr: &SExpr) -> Result<SExpr> {
        // Recursively optimize children first
        let mut optimized_children = Vec::with_capacity(s_expr.arity());
        for expr in s_expr.children() {
            optimized_children.push(Arc::new(self.push_down_filter(expr)?));
        }

        let optimized_expr = s_expr.replace_children(optimized_children);

        // Apply filter push down rule
        let result = self.apply_rule(&optimized_expr)?;
        Ok(result)
    }

    /// Apply a specific optimization rule to the expression
    fn apply_rule(&self, s_expr: &SExpr) -> Result<SExpr> {
        let mut s_expr = s_expr.clone();
        let rule = RuleFactory::create_rule(RuleID::PushDownFilterJoin, self.opt_ctx.clone())?;
        let mut state = TransformResult::new();

        if rule
            .matchers()
            .iter()
            .any(|matcher| matcher.matches(&s_expr))
            && !s_expr.applied_rule(&rule.id())
        {
            s_expr.set_applied_rule(&rule.id());
            rule.apply(&s_expr, &mut state)?;

            if !state.results().is_empty() {
                // Recursive optimize the result
                let result = &state.results()[0];
                let optimized_result = self.push_down_filter(result)?;
                return Ok(optimized_result);
            }
        }

        Ok(s_expr)
    }
}

#[async_trait::async_trait]
impl Optimizer for DPhpyOptimizer {
    fn name(&self) -> String {
        "DPhpyOptimizer".to_string()
    }

    async fn optimize(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        self.optimize_async(s_expr).await
    }
}
