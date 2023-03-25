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
use crate::optimizer::hyper_dp::join_relation::JoinRelationSet;
use crate::optimizer::hyper_dp::join_relation::RelationSetTree;
use crate::optimizer::hyper_dp::query_graph::QueryGraph;
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
    dp_table: HashMap<JoinRelationSet, JoinNode>,
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

    // Traverse the s_expr and get all base tables and join conditions
    fn get_base_tables(
        &mut self,
        s_expr: &SExpr,
        join_conditions: &mut Vec<(ScalarExpr, ScalarExpr)>,
        parent: Option<SExpr>,
    ) -> Result<bool> {
        // Start to traverse and stop when meet join
        let mut s_expr = s_expr.clone();
        while s_expr.children().len() == 1 && !s_expr.is_pattern() {
            // Update `s_expr` to its child
            s_expr = s_expr.children()[0].clone();
        }

        match &s_expr.plan {
            RelOperator::Scan(op) => {
                debug_assert!(parent.is_some());
                let join_relation = JoinRelation::new(&s_expr, &parent.unwrap());
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
                let left_res = self.get_base_tables(
                    &s_expr.children()[0],
                    join_conditions,
                    Some(s_expr.clone()),
                )?;
                let right_res = self.get_base_tables(
                    &s_expr.children()[1],
                    join_conditions,
                    Some(s_expr.clone()),
                )?;
                Ok(left_res && right_res)
            }
            _ => {
                // Currently, we don't consider set operators
                Ok(false)
            }
        }
    }

    // The input plan tree has been optimized by heuristic optimizer
    // So filters have pushed down join and cross join has been converted to inner join as possible as we can
    // The output plan will have optimal join order theoretically
    pub fn optimize(&mut self, s_expr: SExpr) -> Result<(SExpr, bool)> {
        // Firstly, we need to extract all join conditions and base tables
        let mut join_conditions = vec![];
        let res = self.get_base_tables(&s_expr, &mut join_conditions, None)?;
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
                if left_relation_set.is_disjoint(&right_relation_set) {
                    self.query_graph
                        .create_edges(&left_relation_set, &right_relation_set)?;
                    self.query_graph
                        .create_edges(&right_relation_set, &left_relation_set)?;
                }
                continue;
            }
        }

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
            // Get node (aka relation_set) in `relation_set_tree`
            let nodes = self.relation_set_tree.get_relation_set_by_index(idx)?;
            let join = JoinNode {
                leaves: nodes.clone(),
                cost: relation.cost()?,
                ..Default::default()
            };
            let _ = self.dp_table.insert(nodes, join);
        }

        for idx in self.join_relations.len()..0 {
            // Get node from `relation_set_tree`
            let node = self.relation_set_tree.get_relation_set_by_index(idx)?;
            if !self.emit_csg(&node)? {
                return Ok(false);
            }

            // Create forbidden node set
            let forbidden_nodes = (0..idx).collect();
            if !self.enumerate_csg_rec(&node, &forbidden_nodes)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    // EmitCsg will take a non-empty subset of hyper_graph's nodes(V) which contains a connected subgraph.
    // Then it will possibly generate a connected complement which will combine `nodes` to be a csg-cmp-pair.
    fn emit_csg(&mut self, nodes: &JoinRelationSet) -> Result<bool> {
        let mut forbidden_nodes: HashSet<IndexType> = (0..(nodes.relations()[0])).collect();
        forbidden_nodes.extend(nodes.relations());

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
    // It'll consider each non-empty, proper subset of the neighborhood of nodes.
    fn enumerate_csg_rec(
        &mut self,
        nodes: &JoinRelationSet,
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
            let merged_relation_set = nodes.merge_relation_set(&neighbor_relations);
            if self.dp_table.contains_key(&merged_relation_set)
                && !self.emit_csg(&merged_relation_set)?
            {
                return Ok(false);
            }
            merged_sets.push(merged_relation_set);
        }

        let mut new_forbidden_nodes;
        for neighbor in neighbors.iter() {
            new_forbidden_nodes = forbidden_nodes.clone();
            new_forbidden_nodes.insert(*neighbor);
            if !self.enumerate_csg_rec(&merged_sets[*neighbor], &new_forbidden_nodes)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    // EmitCsgCmp will join the optimal plan from left and right
    fn emit_csg_cmp(&mut self, left: &JoinRelationSet, right: &JoinRelationSet) -> Result<bool> {
        debug_assert!(self.dp_table.contains_key(left));
        debug_assert!(self.dp_table.contains_key(right));
        let mut left_join = self.dp_table.get(left).unwrap();
        let mut right_join = self.dp_table.get(right).unwrap();

        let parent_set = left.merge_relation_set(right);

        if left_join.cost < right_join.cost {
            // swap left_join and right_join
            std::mem::swap(&mut left_join, &mut right_join);
        }
        let cost;
        let parent_node = self.dp_table.get(&parent_set);
        // Todo: consider cross join? aka. there is no join condition
        if let Some(plan) = parent_node {
            cost = plan.cost;
        } else {
            cost = left_join.cost * COST_FACTOR_COMPUTE_PER_ROW
                + right_join.cost * COST_FACTOR_HASH_TABLE_PER_ROW;
        }

        let join_node = JoinNode::new(
            parent_set.clone(),
            left_join.clone(),
            right_join.clone(),
            cost,
        );

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
        left: &JoinRelationSet,
        right: &JoinRelationSet,
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
            let merged_relation_set = right.merge_relation_set(&neighbor_relations);
            if self.dp_table.contains_key(&merged_relation_set)
                && self.query_graph.is_connected(left, &merged_relation_set)?
                && !self.emit_csg_cmp(left, &merged_relation_set)?
            {
                return Ok(false);
            }
            merged_sets.push(merged_relation_set);
        }
        // Continue to enumerate cmp
        let mut new_forbidden_nodes = forbidden_nodes.clone();
        for neighbor in neighbor_set.iter() {
            new_forbidden_nodes.insert(*neighbor);
            if !self.enumerate_cmp_rec(left, &merged_sets[*neighbor], &new_forbidden_nodes)? {
                return Ok(false);
            }
        }
        Ok(true)
    }
}
