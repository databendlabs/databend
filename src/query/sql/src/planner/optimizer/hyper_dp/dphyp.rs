// Copyright 2022 Datafuse Labs.
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

use common_exception::Result;

use crate::optimizer::hyper_dp::join_node::JoinNode;
use crate::optimizer::hyper_dp::join_relation::JoinRelation;
use crate::optimizer::hyper_dp::join_relation::RelationSetTree;
use crate::optimizer::hyper_dp::query_graph::QueryGraph;
use crate::optimizer::hyper_dp::util::intersect;
use crate::optimizer::hyper_dp::util::union;
use crate::optimizer::SExpr;
use crate::plans::RelOperator;
use crate::IndexType;
use crate::MetadataRef;
use crate::ScalarExpr;

static COST_FACTOR_COMPUTE_PER_ROW: f64 = 1.0;
static COST_FACTOR_HASH_TABLE_PER_ROW: f64 = 10.0;

// The join reorder algorithm follows the paper: Dynamic Programming Strikes Back
// See the paper for more details.
// Current, we only support inner join
pub struct DPhpy {
    metadata: MetadataRef,
    join_relations: Vec<JoinRelation>,
    // base table index -> index of join_relations
    table_index_map: HashMap<IndexType, IndexType>,
    dp_table: HashMap<Vec<IndexType>, JoinNode>,
    query_graph: QueryGraph,
    relation_set_tree: RelationSetTree,
}

impl DPhpy {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            metadata,
            join_relations: vec![],
            table_index_map: Default::default(),
            dp_table: Default::default(),
            query_graph: QueryGraph::new(),
            relation_set_tree: Default::default(),
        }
    }

    // Traverse the s_expr and get all base relations and join conditions
    fn get_base_relations(
        &mut self,
        s_expr: &SExpr,
        join_conditions: &mut Vec<(ScalarExpr, ScalarExpr)>,
        join_child: bool,
        join_relation: Option<SExpr>,
    ) -> Result<bool> {
        // Start to traverse and stop when meet join
        if s_expr.is_pattern() {
            return Ok(false);
        }

        match &s_expr.plan {
            RelOperator::Scan(op) => {
                let join_relation = if let Some(relation) = join_relation {
                    JoinRelation::new(&relation)
                } else {
                    JoinRelation::new(&s_expr)
                };
                self.table_index_map
                    .insert(op.table_index, self.join_relations.len() as IndexType);
                self.join_relations.push(join_relation);
                Ok(true)
            }
            RelOperator::Join(op) => {
                // Add join conditions
                for condition_pair in op.left_conditions.iter().zip(op.right_conditions.iter()) {
                    join_conditions.push((condition_pair.0.clone(), condition_pair.1.clone()));
                }
                let left_res =
                    self.get_base_relations(&s_expr.children()[0], join_conditions, true, None)?;
                let right_res =
                    self.get_base_relations(&s_expr.children()[1], join_conditions, true, None)?;
                Ok(left_res && right_res)
            }

            RelOperator::ProjectSet(_)
            | RelOperator::EvalScalar(_)
            | RelOperator::Filter(_)
            | RelOperator::Aggregate(_)
            | RelOperator::Sort(_)
            | RelOperator::Limit(_) => {
                if join_child {
                    self.get_base_relations(
                        &s_expr.children()[0],
                        join_conditions,
                        true,
                        Some(s_expr.clone()),
                    )
                } else {
                    self.get_base_relations(&s_expr.children()[0], join_conditions, false, None)
                }
            }
            RelOperator::Exchange(_) | RelOperator::Pattern(_) => unreachable!(),
            RelOperator::Window(_)
            | RelOperator::UnionAll(_)
            | RelOperator::DummyTableScan(_)
            | RelOperator::RuntimeFilterSource(_) => Ok(false),
        }
    }

    // The input plan tree has been optimized by heuristic optimizer
    // So filters have pushed down join and cross join has been converted to inner join as possible as we can
    // The output plan will have optimal join order theoretically
    pub fn optimize(&mut self, s_expr: SExpr) -> Result<(SExpr, bool)> {
        // Firstly, we need to extract all join conditions and base tables
        // `join_condition` is pair, left is left_condition, right is right_condition
        let mut join_conditions = vec![];
        let res = self.get_base_relations(&s_expr, &mut join_conditions, false, None)?;
        if !res {
            return Ok((s_expr, false));
        }
        if self.join_relations.len() == 1 {
            return Ok((s_expr, true));
        }

        // Second, use `join_conditions` to create edges in `query_graph`
        for (left_condition, right_condition) in join_conditions.iter() {
            // Find the corresponding join relations in `join_conditions`
            let mut left_relation_set = HashSet::new();
            let mut right_relation_set = HashSet::new();
            let left_used_tables = left_condition.used_tables(self.metadata.clone())?;
            for table in left_used_tables.iter() {
                if let Some(idx) = self.table_index_map.get(table) {
                    left_relation_set.insert(*idx);
                }
            }
            let right_used_tables = right_condition.used_tables(self.metadata.clone())?;
            for table in right_used_tables.iter() {
                if let Some(idx) = self.table_index_map.get(table) {
                    right_relation_set.insert(*idx);
                }
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
                    self.query_graph
                        .create_edges(&left_relation_set, &right_relation_set)?;
                    self.query_graph
                        .create_edges(&right_relation_set, &left_relation_set)?;
                }
                continue;
            }
        }
        dbg!(&self.query_graph);
        let optimized = self.solve()?;
        // Get all join relations in `relation_set_tree`
        let all_relations = self
            .relation_set_tree
            .get_relation_set(&(0..self.join_relations.len()).collect())?;
        if optimized {
            if let Some(final_plan) = self.dp_table.get(&all_relations) {
                dbg!(final_plan);
                todo!("Convert final plan to SExpr")
            } else {
                Ok((s_expr.clone(), false))
            }
        } else {
            Ok((s_expr.clone(), false))
        }
    }

    // This method will run dynamic programming algorithm to find the optimal join order
    fn solve(&mut self) -> Result<bool> {
        // Initial `dp_table` with plan for single relation
        for (idx, relation) in self.join_relations.iter().enumerate() {
            // Get nodes  in `relation_set_tree`
            let nodes = self.relation_set_tree.get_relation_set_by_index(idx)?;
            let join = JoinNode {
                leaves: nodes.clone(),
                children: vec![],
                cost: relation.cost()?,
            };
            let _ = self.dp_table.insert(nodes, join);
        }

        // Choose all nodes as enumeration start node once (desc order)
        for idx in (0..self.join_relations.len()).rev() {
            // Get node from `relation_set_tree`
            let node = self.relation_set_tree.get_relation_set_by_index(idx)?;
            // Emit node as subgraph
            if !self.emit_csg(&node)? {
                return Ok(false);
            }

            // Create forbidden node set
            // Forbid node idx will less than current idx
            let forbidden_nodes = (0..idx).collect();
            // Enlarge the subgraph recursively
            if !self.enumerate_csg_rec(&node, &forbidden_nodes)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    // EmitCsg will take a non-empty subset of hyper_graph's nodes(V) which contains a connected subgraph.
    // Then it will possibly generate a connected complement which will combine `nodes` to be a csg-cmp-pair.
    fn emit_csg(&mut self, nodes: &[IndexType]) -> Result<bool> {
        if nodes.len() == self.join_relations.len() {
            return Ok(true);
        }
        let mut forbidden_nodes: HashSet<IndexType> = (0..(nodes[0])).collect();
        forbidden_nodes.extend(nodes);

        // Get neighbors of `nodes`
        let mut neighbors = self.query_graph.neighbors(nodes, &forbidden_nodes)?;
        if neighbors.is_empty() {
            return Ok(true);
        }
        neighbors.sort();
        // Traverse all neighbors by desc
        for neighbor in neighbors.iter().rev() {
            let neighbor_relations = self
                .relation_set_tree
                .get_relation_set_by_index(*neighbor)?;
            // Check if neighbor is connected with `nodes`
            if self.query_graph.is_connected(nodes, &neighbor_relations)?
                && !self.emit_csg_cmp(nodes, &neighbor_relations)?
            {
                return Ok(false);
            }
            if !self.enumerate_cmp_rec(nodes, &neighbor_relations, &forbidden_nodes)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    // EnumerateCsgRec will extend the given `nodes`.
    // It'll consider each non-empty, proper subset of the neighborhood of nodes that are not forbidden.
    fn enumerate_csg_rec(
        &mut self,
        nodes: &[IndexType],
        forbidden_nodes: &HashSet<IndexType>,
    ) -> Result<bool> {
        let neighbors = self.query_graph.neighbors(nodes, forbidden_nodes)?;
        if neighbors.is_empty() {
            return Ok(true);
        }
        let mut merged_sets = Vec::new();
        for neighbor in neighbors.iter() {
            let neighbor_relations = self
                .relation_set_tree
                .get_relation_set_by_index(*neighbor)?;
            let merged_relation_set = union(nodes, &neighbor_relations);
            if self.dp_table.contains_key(&merged_relation_set)
                && !self.emit_csg(&merged_relation_set)?
            {
                return Ok(false);
            }
            merged_sets.push(merged_relation_set);
        }

        let mut new_forbidden_nodes;
        for (idx, neighbor) in neighbors.iter().enumerate() {
            new_forbidden_nodes = forbidden_nodes.clone();
            new_forbidden_nodes.insert(neighbor.clone());
            if !self.enumerate_csg_rec(&merged_sets[idx], &new_forbidden_nodes)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    // EmitCsgCmp will join the optimal plan from left and right
    fn emit_csg_cmp(&mut self, left: &[IndexType], right: &[IndexType]) -> Result<bool> {
        debug_assert!(self.dp_table.contains_key(left));
        debug_assert!(self.dp_table.contains_key(right));
        let mut left_join = self.dp_table.get(left).unwrap();
        let mut right_join = self.dp_table.get(right).unwrap();
        let parent_set = union(left, right);

        if left_join.cost < right_join.cost {
            // swap left_join and right_join
            std::mem::swap(&mut left_join, &mut right_join);
        }
        let parent_node = self.dp_table.get(&parent_set);
        // Todo: consider cross join? aka. there is no join condition
        let cost = left_join.cost * COST_FACTOR_COMPUTE_PER_ROW
            + right_join.cost * COST_FACTOR_HASH_TABLE_PER_ROW;

        let join_node = JoinNode::new(
            parent_set.clone(),
            vec![left_join.clone(), right_join.clone()],
            cost,
        );

        if parent_set.len() == self.join_relations.len() {
            dbg!(&parent_set, &join_node.cost);
        }

        if parent_node.is_none() || parent_node.unwrap().cost > join_node.cost {
            // Update `dp_table`
            self.dp_table.insert(parent_set, join_node);
        }
        Ok(true)
    }

    // The second parameter is a set which is connected and must be extended until a valid csg-cmp-pair is reached.
    // Therefore, it considers the neighborhood of right.
    fn enumerate_cmp_rec(
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
            if self.dp_table.contains_key(&merged_relation_set)
                && self.query_graph.is_connected(left, &merged_relation_set)?
                && !self.emit_csg_cmp(left, &merged_relation_set)?
            {
                return Ok(false);
            }
            merged_sets.push(merged_relation_set);
        }
        // Continue to enumerate cmp
        let mut new_forbidden_nodes;
        for (idx, neighbor) in neighbor_set.iter().enumerate() {
            new_forbidden_nodes = forbidden_nodes.clone();
            new_forbidden_nodes.insert(neighbor.clone());
            if !self.enumerate_cmp_rec(left, &merged_sets[idx], &new_forbidden_nodes)? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}
