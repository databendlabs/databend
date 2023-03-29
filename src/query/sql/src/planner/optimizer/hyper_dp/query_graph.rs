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

use crate::optimizer::hyper_dp::util::is_subset;
use crate::IndexType;

#[derive(Clone, Debug)]
struct NeighborInfo {
    neighbors: Vec<IndexType>,
    // filters:
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
}

impl QueryGraph {
    pub fn new() -> Self {
        Self {
            root_edge: QueryEdge::new(),
        }
    }

    // Check if `nodes` is connected to `neighbor`
    pub fn is_connected(&self, nodes: &[IndexType], neighbor: &[IndexType]) -> Result<bool> {
        let mut edge = &self.root_edge;
        for node in nodes.iter() {
            if !edge.children.contains_key(node) {
                continue;
            }
            edge = edge.children.get(node).unwrap();
            for neighbor_info in edge.neighbors.iter() {
                if is_subset(&neighbor_info.neighbors, neighbor) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    // Get all neighbors of `nodes` which are not in `forbidden_nodes`
    pub fn neighbors(
        &self,
        nodes: &[IndexType],
        forbidden_nodes: &HashSet<IndexType>,
    ) -> Result<Vec<IndexType>> {
        let mut neighbors = vec![];
        // Find neighbors for nodes that aren't in `forbidden_nodes`
        for node in nodes.iter() {
            let mut edge = &self.root_edge;
            if !edge.children.contains_key(node) {
                continue;
            }
            edge = edge.children.get(node).unwrap();
            for neighbor_info in edge.neighbors.iter() {
                if !forbidden_nodes.contains(&neighbor_info.neighbors[0]) {
                    neighbors.push(neighbor_info.neighbors[0]);
                }
            }
        }
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
    pub fn create_edges(&mut self, left_set: &[IndexType], right_set: &[IndexType]) -> Result<()> {
        let left_edge = self.create_edges_for_relation_set(left_set)?;
        for neighbor_info in left_edge.neighbors.iter() {
            if neighbor_info.neighbors.eq(right_set) {
                // Todo: add filter info to `neighbor_info`? Maybe used in emit_csg
                return Ok(());
            }
        }

        let neighbor_info = NeighborInfo {
            neighbors: right_set.to_vec(),
        };
        left_edge.neighbors.push(neighbor_info);
        Ok(())
    }
}
