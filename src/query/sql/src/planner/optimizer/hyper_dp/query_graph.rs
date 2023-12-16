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

use databend_common_exception::Result;

use crate::optimizer::hyper_dp::util::is_subset;
use crate::IndexType;
use crate::ScalarExpr;

#[derive(Clone, Debug)]
struct NeighborInfo {
    neighbors: Vec<IndexType>,
    join_conditions: Vec<(ScalarExpr, ScalarExpr)>,
}

#[derive(Clone, Debug)]
struct QueryEdge {
    neighbors: Vec<NeighborInfo>,
    children: HashMap<IndexType, QueryEdge>,
}

impl QueryEdge {
    fn new() -> Self {
        Self {
            neighbors: vec![],
            children: Default::default(),
        }
    }
}

// The struct defines `QueryGraph` which contains node and edge
// Node represents a base relation
#[derive(Clone, Debug)]
pub struct QueryGraph {
    root_edge: QueryEdge,
    // cache neighbors
    pub cached_neighbors: HashMap<Vec<IndexType>, Vec<IndexType>>,
}

impl QueryGraph {
    pub fn new() -> Self {
        Self {
            root_edge: QueryEdge::new(),
            cached_neighbors: Default::default(),
        }
    }

    // Check if `nodes` is connected to `neighbor`
    pub fn is_connected(
        &self,
        nodes: &[IndexType],
        neighbor: &[IndexType],
    ) -> Result<Vec<(ScalarExpr, ScalarExpr)>> {
        let nodes_size = nodes.len();
        let mut join_conditions = vec![];
        for i in 0..nodes_size {
            let mut edge = &self.root_edge;
            for node in nodes.iter().take(nodes_size).skip(i) {
                if let Some(child) = edge.children.get(node) {
                    edge = child;
                } else {
                    break;
                }
                for neighbor_info in edge.neighbors.iter() {
                    if is_subset(&neighbor_info.neighbors, neighbor) {
                        join_conditions.extend(neighbor_info.join_conditions.clone());
                    }
                }
            }
        }
        Ok(join_conditions)
    }

    // Get all neighbors of `nodes` which are not in `forbidden_nodes`
    pub fn neighbors(
        &mut self,
        nodes: &[IndexType],
        forbidden_nodes: &HashSet<IndexType>,
    ) -> Result<Vec<IndexType>> {
        if let Some(neighbor) = self.cached_neighbors.get(nodes) {
            let mut neighbors = neighbor.clone();
            // `retain` will not change the original order, there is no need to sort.
            neighbors.retain(|node| !forbidden_nodes.contains(node));
            return Ok(neighbors);
        }
        let mut cached_neighbors = vec![];
        let mut neighbors = vec![];
        let mut visit = HashSet::new();
        // Find neighbors for nodes that aren't in `forbidden_nodes`
        let nodes_size = nodes.len();
        for i in 0..nodes_size {
            let mut edge = &self.root_edge;
            for node in nodes.iter().take(nodes_size).skip(i) {
                if let Some(child) = edge.children.get(node) {
                    edge = child;
                } else {
                    break;
                }
                for neighbor_info in edge.neighbors.iter() {
                    let min_neighbor = neighbor_info.neighbors[0];
                    if !visit.contains(&min_neighbor) {
                        visit.insert(min_neighbor);
                        cached_neighbors.push(min_neighbor);
                        if !forbidden_nodes.contains(&min_neighbor)
                            && !nodes.contains(&min_neighbor)
                        {
                            neighbors.push(min_neighbor);
                        }
                    }
                }
            }
        }
        cached_neighbors.sort();
        neighbors.sort();
        self.cached_neighbors
            .insert(nodes.to_vec(), cached_neighbors);
        Ok(neighbors)
    }

    // create edges for relation set
    fn create_edges_for_relation_set(
        &mut self,
        relation_set: &[IndexType],
    ) -> Result<&mut QueryEdge> {
        let mut edge = &mut self.root_edge;
        for relation in relation_set.iter() {
            if !edge.children.contains_key(relation) {
                edge.children.insert(*relation, QueryEdge::new());
            }
            edge = edge.children.get_mut(relation).unwrap();
        }
        Ok(edge)
    }

    // Create edges for left set and right set
    pub fn create_edges(
        &mut self,
        left_set: &[IndexType],
        right_set: &[IndexType],
        join_condition: (ScalarExpr, ScalarExpr),
    ) -> Result<()> {
        let left_edge = self.create_edges_for_relation_set(left_set)?;
        for neighbor_info in left_edge.neighbors.iter_mut() {
            if neighbor_info.neighbors.eq(right_set) {
                neighbor_info.join_conditions.push(join_condition);
                return Ok(());
            }
        }

        let neighbor_info = NeighborInfo {
            neighbors: right_set.to_vec(),
            join_conditions: vec![join_condition],
        };
        left_edge.neighbors.push(neighbor_info);
        self.cached_neighbors
            .entry(left_set.to_vec())
            .and_modify(|val| val.push(right_set[0]))
            .or_insert(vec![right_set[0]]);
        Ok(())
    }
}
