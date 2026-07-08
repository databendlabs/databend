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

use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;

use databend_common_exception::Result;

use super::RelationId;
use super::RelationSet;
use super::RelationSetTree;

const RELATION_THRESHOLD: usize = 10;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct JoinEdgeRef {
    pub id: usize,
    pub reversed: bool,
}

impl JoinEdgeRef {
    fn reversed(&self) -> Self {
        Self {
            id: self.id,
            reversed: !self.reversed,
        }
    }
}

pub struct JoinNode<S> {
    children: JoinNodeChildren,
    cost: f64,
    cardinality: f64,
    state: S,
}

impl<S> JoinNode<S> {
    pub fn cost(&self) -> f64 {
        self.cost
    }

    pub fn cardinality(&self) -> f64 {
        self.cardinality
    }

    pub fn state(&self) -> &S {
        &self.state
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct NodeId(usize);

enum JoinNodeChildren {
    Leaf,
    ArenaJoin { left: NodeId, right: NodeId },
}

struct JoinArena<S> {
    nodes: Vec<Option<JoinNode<S>>>,
    free_list: Vec<NodeId>,
}

impl<S> Default for JoinArena<S> {
    fn default() -> Self {
        Self {
            nodes: vec![],
            free_list: vec![],
        }
    }
}

impl<S> JoinArena<S> {
    fn push(&mut self, node: JoinNode<S>) -> NodeId {
        let id = if let Some(id) = self.free_list.pop() {
            self.nodes[id.0] = Some(node);
            id
        } else {
            let id = NodeId(self.nodes.len());
            self.nodes.push(Some(node));
            id
        };
        id
    }

    fn node(&self, id: NodeId) -> &JoinNode<S> {
        self.nodes[id.0].as_ref().expect("join node should be live")
    }

    fn collect_garbage(&mut self, roots: impl Iterator<Item = NodeId>) {
        let mut live = HashSet::new();
        let mut stack = roots.collect::<Vec<_>>();
        while let Some(id) = stack.pop() {
            if !live.insert(id) {
                continue;
            }
            if let Some(node) = self.nodes[id.0].as_ref() {
                if let JoinNodeChildren::ArenaJoin { left, right } = &node.children {
                    stack.push(*left);
                    stack.push(*right);
                }
            }
        }

        for idx in 0..self.nodes.len() {
            let id = NodeId(idx);
            if self.nodes[idx].is_some() && !live.contains(&id) {
                self.free_node(id);
            }
        }
    }

    fn free_node(&mut self, id: NodeId) {
        self.free_list.push(id);
    }
}

pub trait JoinOrderModel {
    type NodeState: Clone;

    fn base_node(&self, relation: RelationId) -> Result<(f64, Self::NodeState)>;

    fn join_node(
        &self,
        left: &JoinNode<Self::NodeState>,
        right: &JoinNode<Self::NodeState>,
        edge_refs: &[JoinEdgeRef],
    ) -> Result<(f64, Self::NodeState)>;

    fn join_cost(
        &self,
        left: &JoinNode<Self::NodeState>,
        right: &JoinNode<Self::NodeState>,
        edge_refs: &[JoinEdgeRef],
        cardinality: f64,
    ) -> Result<f64>;

    fn should_flip_join_inputs(
        &self,
        left: &JoinNode<Self::NodeState>,
        right: &JoinNode<Self::NodeState>,
    ) -> bool {
        left.cardinality() < right.cardinality()
    }
}

#[derive(Clone, Debug)]
struct NeighborInfo {
    neighbors: RelationSet,
    edge_refs: Vec<JoinEdgeRef>,
}

#[derive(Clone, Debug, Default)]
struct GraphEdge {
    neighbors: Vec<NeighborInfo>,
    children: HashMap<RelationId, GraphEdge>,
}

#[derive(Clone, Debug, Default)]
struct JoinGraph {
    root_edge: GraphEdge,
    cached_neighbors: HashMap<RelationSet, Vec<RelationId>>,
}

impl JoinGraph {
    fn sort_cached_neighbors(&mut self) {
        for neighbors in self.cached_neighbors.values_mut() {
            neighbors.sort();
        }
    }

    fn is_connected(
        &self,
        nodes: &[RelationId],
        neighbor: &[RelationId],
    ) -> Result<Vec<JoinEdgeRef>> {
        let nodes_size = nodes.len();
        let mut edge_refs = vec![];
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
                        edge_refs.extend(neighbor_info.edge_refs.clone());
                    }
                }
            }
        }
        Ok(edge_refs)
    }

    fn neighbors(
        &mut self,
        nodes: &[RelationId],
        forbidden_nodes: &HashSet<RelationId>,
    ) -> Result<Vec<RelationId>> {
        if let Some(neighbor) = self.cached_neighbors.get(nodes) {
            let mut neighbors = neighbor.clone();
            neighbors.retain(|node| !forbidden_nodes.contains(node));
            return Ok(neighbors);
        }

        let mut cached_neighbors = vec![];
        let mut neighbors = vec![];
        let mut visit = HashSet::new();
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
            .insert(nodes.to_vec().into_boxed_slice(), cached_neighbors);
        Ok(neighbors)
    }

    fn create_edges_for_relation_set(&mut self, relation_set: &[RelationId]) -> &mut GraphEdge {
        let mut edge = &mut self.root_edge;
        for relation in relation_set.iter() {
            if !edge.children.contains_key(relation) {
                edge.children.insert(*relation, GraphEdge::default());
            }
            edge = edge.children.get_mut(relation).unwrap();
        }
        edge
    }

    fn create_edges(
        &mut self,
        left_set: &[RelationId],
        right_set: &[RelationId],
        edge_ref: JoinEdgeRef,
    ) {
        let left_edge = self.create_edges_for_relation_set(left_set);
        for neighbor_info in left_edge.neighbors.iter_mut() {
            if neighbor_info.neighbors.as_ref() == right_set {
                neighbor_info.edge_refs.push(edge_ref);
                return;
            }
        }

        left_edge.neighbors.push(NeighborInfo {
            neighbors: right_set.to_vec().into_boxed_slice(),
            edge_refs: vec![edge_ref],
        });
        self.cached_neighbors
            .entry(left_set.to_vec().into_boxed_slice())
            .and_modify(|val| val.push(right_set[0]))
            .or_insert(vec![right_set[0]]);
    }
}

pub struct HyperDp<'a, M: JoinOrderModel> {
    relation_count: usize,
    model: &'a M,
    graph: JoinGraph,
    relation_set_tree: RelationSetTree,
    arena: JoinArena<M::NodeState>,
    dp_table: HashMap<RelationSet, NodeId>,
    emit_count: usize,
}

impl<'a, M: JoinOrderModel> HyperDp<'a, M> {
    pub fn new(relation_count: usize, model: &'a M) -> Self {
        Self {
            relation_count,
            model,
            graph: Default::default(),
            relation_set_tree: Default::default(),
            arena: JoinArena::default(),
            dp_table: Default::default(),
            emit_count: 0,
        }
    }

    pub fn add_edge(
        &mut self,
        left_relation_set: &HashSet<RelationId>,
        right_relation_set: &HashSet<RelationId>,
        edge_id: usize,
    ) -> Result<()> {
        let left_relation_set = self.relation_set_tree.get_relation_set(left_relation_set)?;
        let right_relation_set = self
            .relation_set_tree
            .get_relation_set(right_relation_set)?;

        self.graph
            .create_edges(&left_relation_set, &right_relation_set, JoinEdgeRef {
                id: edge_id,
                reversed: false,
            });
        self.graph
            .create_edges(&right_relation_set, &left_relation_set, JoinEdgeRef {
                id: edge_id,
                reversed: true,
            });

        Ok(())
    }

    pub fn find_best_order(mut self) -> Result<Option<M::NodeState>> {
        self.graph.sort_cached_neighbors();
        self.join_reorder()?;

        let all_relations = self
            .relation_set_tree
            .get_relation_set(&(0..self.relation_count).collect())?;

        Ok(self.dp_table.remove(&all_relations).map(|id| {
            self.arena.nodes[id.0]
                .take()
                .expect("join node should be live")
                .state
        }))
    }

    fn initialize_dp_table(&mut self) -> Result<()> {
        for idx in 0..self.relation_count {
            let nodes = self.relation_set_tree.get_relation_set_by_index(idx)?;
            let (cardinality, state) = self.model.base_node(idx)?;
            let id = self.arena.push(JoinNode {
                children: JoinNodeChildren::Leaf,
                cost: 0.0,
                cardinality,
                state,
            });
            self.dp_table.insert(nodes, id);
        }
        Ok(())
    }

    fn process_node_as_start(&mut self, idx: usize) -> Result<bool> {
        let node = self.relation_set_tree.get_relation_set_by_index(idx)?;

        if !self.emit_csg(&node)? {
            return Ok(false);
        }

        let forbidden_nodes = (0..idx).collect();
        if !self.enumerate_csg_rec(&node, &forbidden_nodes)? {
            return Ok(false);
        }

        Ok(true)
    }

    fn join_reorder_by_dphyp(&mut self) -> Result<bool> {
        for idx in (0..self.relation_count).rev() {
            if !self.process_node_as_start(idx)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn find_minimum_cost_pair(
        &mut self,
        join_relations: &[RelationSet],
    ) -> Result<(f64, usize, usize, RelationSet)> {
        let mut min_cost = f64::INFINITY;
        let mut left_idx = 0;
        let mut right_idx = 0;
        let mut new_relations = RelationSet::default();

        for i in 0..join_relations.len() {
            let left_relation = &join_relations[i];

            for (j, right_relation) in join_relations.iter().enumerate().skip(i + 1) {
                let edge_refs = self.graph.is_connected(left_relation, right_relation)?;

                if !edge_refs.is_empty() {
                    let cost = self.emit_csg_cmp(left_relation, right_relation, edge_refs)?;

                    if cost < min_cost {
                        min_cost = cost;
                        left_idx = i;
                        right_idx = j;
                        new_relations = union(left_relation, right_relation);
                    }
                }
            }
        }

        Ok((min_cost, left_idx, right_idx, new_relations))
    }

    fn handle_disconnected_relations(
        &mut self,
        join_relations: &[RelationSet],
    ) -> Result<(usize, usize, RelationSet)> {
        let mut lowest_cost = Vec::with_capacity(2);
        let mut lowest_index = Vec::with_capacity(2);

        for (i, relation) in join_relations.iter().enumerate().take(2) {
            let id = self.dp_table.get(relation).unwrap();
            lowest_cost.push(self.arena.node(*id).cardinality);
            lowest_index.push(i);
        }

        if lowest_cost[1] < lowest_cost[0] {
            lowest_cost.swap(0, 1);
            lowest_index.swap(0, 1);
        }

        for (i, relation) in join_relations.iter().enumerate().skip(2) {
            let id = self.dp_table.get(relation).unwrap();
            let cardinality = self.arena.node(*id).cardinality;

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

        let left_idx = lowest_index[0];
        let right_idx = lowest_index[1];
        let new_relations = union(&join_relations[left_idx], &join_relations[right_idx]);

        self.emit_csg_cmp(
            &join_relations[left_idx],
            &join_relations[right_idx],
            vec![],
        )?;

        Ok((left_idx, right_idx, new_relations))
    }

    fn join_reorder_by_greedy(&mut self) -> Result<bool> {
        let mut join_relations = (0..self.relation_count)
            .map(|idx| self.relation_set_tree.get_relation_set_by_index(idx))
            .collect::<Result<Vec<_>>>()?;

        while join_relations.len() > 1 {
            let (min_cost, mut left_idx, mut right_idx, mut new_relations) =
                self.find_minimum_cost_pair(&join_relations)?;

            if min_cost == f64::INFINITY {
                let (left, right, new_rel) = self.handle_disconnected_relations(&join_relations)?;
                left_idx = left;
                right_idx = right;
                new_relations = new_rel;
            }

            if left_idx > right_idx {
                std::mem::swap(&mut left_idx, &mut right_idx);
            }

            join_relations.remove(right_idx);
            join_relations.remove(left_idx);
            join_relations.push(new_relations);
        }

        Ok(true)
    }

    fn join_reorder(&mut self) -> Result<()> {
        self.initialize_dp_table()?;

        if !self.join_reorder_by_dphyp()? {
            self.join_reorder_by_greedy()?;
        }

        Ok(())
    }

    fn emit_csg(&mut self, nodes: &[RelationId]) -> Result<bool> {
        if nodes.len() == self.relation_count {
            return Ok(true);
        }

        let mut forbidden_nodes: HashSet<RelationId> = (0..(nodes[0])).collect();
        forbidden_nodes.extend(nodes);

        let neighbors = self.graph.neighbors(nodes, &forbidden_nodes)?;
        if neighbors.is_empty() {
            return Ok(true);
        }

        for neighbor in neighbors.iter().rev() {
            let neighbor_relations = self
                .relation_set_tree
                .get_relation_set_by_index(*neighbor)?;

            let edge_refs = self.graph.is_connected(nodes, &neighbor_relations)?;
            if !edge_refs.is_empty()
                && !self.try_emit_csg_cmp(nodes, &neighbor_relations, edge_refs)?
            {
                return Ok(false);
            }

            if !self.enumerate_cmp_rec(nodes, &neighbor_relations, &forbidden_nodes)? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn try_emit_csg_cmp(
        &mut self,
        left: &[RelationId],
        right: &[RelationId],
        edge_refs: Vec<JoinEdgeRef>,
    ) -> Result<bool> {
        const EMIT_THRESHOLD: usize = 10000;

        self.emit_count += 1;
        match self.emit_count > EMIT_THRESHOLD {
            false => {
                self.emit_csg_cmp(left, right, edge_refs)?;
                Ok(true)
            }
            true => Ok(false),
        }
    }

    fn create_join_node(
        &mut self,
        edge_refs: Vec<JoinEdgeRef>,
        left_id: NodeId,
        right_id: NodeId,
    ) -> Result<NodeId> {
        let flip_inputs = {
            let left_tree = self.arena.node(left_id);
            let right_tree = self.arena.node(right_id);
            self.model.should_flip_join_inputs(left_tree, right_tree)
        };

        let (left_id, right_id, edge_refs) = if flip_inputs {
            (
                right_id,
                left_id,
                edge_refs
                    .into_iter()
                    .map(|edge_ref| edge_ref.reversed())
                    .collect(),
            )
        } else {
            (left_id, right_id, edge_refs)
        };

        let (cardinality, state, cost) = {
            let left_tree = self.arena.node(left_id);
            let right_tree = self.arena.node(right_id);
            let (cardinality, state) = self.model.join_node(left_tree, right_tree, &edge_refs)?;
            let cost = self
                .model
                .join_cost(left_tree, right_tree, &edge_refs, cardinality)?;
            (cardinality, state, cost)
        };

        Ok(self.arena.push(JoinNode {
            children: JoinNodeChildren::ArenaJoin {
                left: left_id,
                right: right_id,
            },
            cost,
            cardinality,
            state,
        }))
    }

    fn emit_csg_cmp(
        &mut self,
        left: &[RelationId],
        right: &[RelationId],
        edge_refs: Vec<JoinEdgeRef>,
    ) -> Result<f64> {
        debug_assert!(self.dp_table.contains_key(left));
        debug_assert!(self.dp_table.contains_key(right));

        let parent_set = union(left, right);
        let left_id = *self.dp_table.get(left).unwrap();
        let right_id = *self.dp_table.get(right).unwrap();

        let join_id = self.create_join_node(edge_refs, left_id, right_id)?;
        let join_node = self.arena.node(join_id);
        let join_cost = join_node.cost;

        let parent_id = self.dp_table.get(&parent_set).copied();
        let parent_cost = parent_id.map(|id| self.arena.node(id).cost);
        let cost = if let Some(parent_cost) = parent_cost {
            parent_cost.min(join_cost)
        } else {
            join_cost
        };

        if parent_cost.is_none() || parent_cost.unwrap() > join_cost {
            let should_collect_garbage = parent_id.is_some();
            self.dp_table.insert(parent_set, join_id);
            if should_collect_garbage {
                self.arena.collect_garbage(self.dp_table.values().copied());
            }
        } else {
            self.arena.free_node(join_id);
        }

        Ok(cost)
    }

    #[recursive::recursive]
    fn enumerate_cmp_rec(
        &mut self,
        left: &[RelationId],
        right: &[RelationId],
        forbidden_nodes: &HashSet<RelationId>,
    ) -> Result<bool> {
        let neighbor_set = self.graph.neighbors(right, forbidden_nodes)?;
        if neighbor_set.is_empty() {
            return Ok(true);
        }

        let mut merged_sets = Vec::new();
        for neighbor in neighbor_set.iter() {
            let neighbor_relations = self
                .relation_set_tree
                .get_relation_set_by_index(*neighbor)?;

            let merged_relation_set = union(right, &neighbor_relations);
            let edge_refs = self.graph.is_connected(left, &merged_relation_set)?;

            if merged_relation_set.len() > right.len()
                && self.dp_table.contains_key(&merged_relation_set)
                && !edge_refs.is_empty()
                && !self.try_emit_csg_cmp(left, &merged_relation_set, edge_refs)?
            {
                return Ok(false);
            }

            merged_sets.push(merged_relation_set);
        }

        let mut new_forbidden_nodes = forbidden_nodes.clone();
        for (idx, neighbor) in neighbor_set.iter().enumerate() {
            new_forbidden_nodes.insert(*neighbor);
            if !self.enumerate_cmp_rec(left, &merged_sets[idx], &new_forbidden_nodes)? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    #[recursive::recursive]
    fn enumerate_csg_rec(
        &mut self,
        nodes: &[RelationId],
        forbidden_nodes: &HashSet<RelationId>,
    ) -> Result<bool> {
        let mut neighbors = self.graph.neighbors(nodes, forbidden_nodes)?;
        if neighbors.is_empty() {
            return Ok(true);
        }

        if self.relation_count >= RELATION_THRESHOLD {
            neighbors = neighbors
                .iter()
                .take(nodes.len())
                .copied()
                .collect::<Vec<RelationId>>();
        }

        let mut merged_sets = Vec::new();
        for neighbor in neighbors.iter() {
            let neighbor_relations = self
                .relation_set_tree
                .get_relation_set_by_index(*neighbor)?;

            let merged_relation_set = union(nodes, &neighbor_relations);
            if self.dp_table.contains_key(&merged_relation_set)
                && merged_relation_set.len() > nodes.len()
                && !self.emit_csg(&merged_relation_set)?
            {
                return Ok(false);
            }

            merged_sets.push(merged_relation_set);
        }

        let mut new_forbidden_nodes = forbidden_nodes.clone();
        for (idx, neighbor) in neighbors.iter().enumerate() {
            if self.relation_count < RELATION_THRESHOLD {
                new_forbidden_nodes = forbidden_nodes.clone();
            }

            new_forbidden_nodes.insert(*neighbor);
            if !self.enumerate_csg_rec(&merged_sets[idx], &new_forbidden_nodes)? {
                return Ok(false);
            }
        }

        Ok(true)
    }
}

// Union two nodes vector
fn union(left: &[RelationId], right: &[RelationId]) -> RelationSet {
    let mut result: Vec<usize> = Vec::with_capacity(left.len() + right.len());
    let mut i = 0;
    let mut j = 0;
    while i < left.len() && j < right.len() {
        match left[i].cmp(&right[j]) {
            Ordering::Equal => {
                result.push(left[i]);
                i += 1;
                j += 1;
            }
            Ordering::Less => {
                result.push(left[i]);
                i += 1;
            }
            Ordering::Greater => {
                result.push(right[j]);
                j += 1;
            }
        }
    }
    if i == left.len() {
        result.extend(right[j..].iter());
    } else {
        result.extend(left[i..].iter());
    }
    result.into_boxed_slice()
}

fn is_subset<T: PartialEq>(v1: &[T], v2: &[T]) -> bool {
    v1.iter().all(|x| v2.contains(x))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use databend_common_exception::Result;

    use super::*;

    struct TestJoinOrderModel {
        base_cardinalities: Vec<f64>,
        join_cardinalities: HashMap<RelationSet, f64>,
    }

    #[derive(Clone, Debug)]
    struct TestJoinState {
        leaves: RelationSet,
        cardinality: f64,
        root_edge_refs: Vec<JoinEdgeRef>,
        leaf_order: Vec<RelationSet>,
        joins: Vec<(RelationSet, Vec<JoinEdgeRef>)>,
    }

    impl TestJoinState {
        fn leaf(relation: RelationId, cardinality: f64) -> Self {
            Self {
                leaves: vec![relation].into_boxed_slice(),
                cardinality,
                root_edge_refs: vec![],
                leaf_order: vec![vec![relation].into_boxed_slice()],
                joins: vec![],
            }
        }
    }

    impl TestJoinOrderModel {
        fn new(base_cardinalities: Vec<f64>) -> Self {
            Self {
                base_cardinalities,
                join_cardinalities: Default::default(),
            }
        }

        fn with_join_cardinality(mut self, relations: &[RelationId], cardinality: f64) -> Self {
            let mut key = relations.to_vec();
            key.sort();
            self.join_cardinalities
                .insert(key.into_boxed_slice(), cardinality);
            self
        }
    }

    impl JoinOrderModel for TestJoinOrderModel {
        type NodeState = TestJoinState;

        fn base_node(&self, relation: RelationId) -> Result<(f64, Self::NodeState)> {
            let cardinality = self.base_cardinalities[relation];
            Ok((cardinality, TestJoinState::leaf(relation, cardinality)))
        }

        fn join_node(
            &self,
            left: &JoinNode<Self::NodeState>,
            right: &JoinNode<Self::NodeState>,
            edge_refs: &[JoinEdgeRef],
        ) -> Result<(f64, Self::NodeState)> {
            let left_state = left.state();
            let right_state = right.state();
            let key = union(&left_state.leaves, &right_state.leaves);
            let cardinality = if let Some(cardinality) = self.join_cardinalities.get(&key) {
                *cardinality
            } else if edge_refs.is_empty() {
                left.cardinality() * right.cardinality()
            } else {
                (left.cardinality() * right.cardinality()).max(1.0)
            };

            let mut leaf_order = left_state.leaf_order.clone();
            leaf_order.extend(right_state.leaf_order.clone());
            let mut joins = left_state.joins.clone();
            joins.extend(right_state.joins.clone());
            joins.push((key.clone(), edge_refs.to_vec()));

            Ok((cardinality, TestJoinState {
                leaves: key,
                cardinality,
                root_edge_refs: edge_refs.to_vec(),
                leaf_order,
                joins,
            }))
        }

        fn join_cost(
            &self,
            left: &JoinNode<Self::NodeState>,
            right: &JoinNode<Self::NodeState>,
            _edge_refs: &[JoinEdgeRef],
            cardinality: f64,
        ) -> Result<f64> {
            Ok(cardinality + left.cost() + right.cost())
        }
    }

    fn relation_set(relations: &[RelationId]) -> HashSet<RelationId> {
        relations.iter().copied().collect()
    }

    #[test]
    fn test_algorithm_chooses_lowest_cost_join_tree_without_plan_types() -> Result<()> {
        let model = TestJoinOrderModel::new(vec![100.0, 100.0, 100.0])
            .with_join_cardinality(&[0, 1], 1000.0)
            .with_join_cardinality(&[1, 2], 1.0)
            .with_join_cardinality(&[0, 1, 2], 10.0);
        let mut hyper_dp = HyperDp::new(3, &model);

        hyper_dp.add_edge(&relation_set(&[0]), &relation_set(&[1]), 0)?;
        hyper_dp.add_edge(&relation_set(&[1]), &relation_set(&[2]), 1)?;

        let best = hyper_dp.find_best_order()?.expect("expected join state");

        assert_eq!(best.leaves.as_ref(), &[0, 1, 2]);
        assert_eq!(best.cardinality, 10.0);
        assert!(
            best.joins
                .iter()
                .any(|(leaves, edge_refs)| leaves.as_ref() == [1, 2] && edge_refs[0].id == 1)
        );

        Ok(())
    }

    #[test]
    fn test_algorithm_records_edge_orientation_for_reversed_graph_edge() -> Result<()> {
        let model =
            TestJoinOrderModel::new(vec![10.0, 1000.0]).with_join_cardinality(&[0, 1], 100.0);
        let mut hyper_dp = HyperDp::new(2, &model);

        hyper_dp.add_edge(&relation_set(&[0]), &relation_set(&[1]), 0)?;

        let best = hyper_dp.find_best_order()?.expect("expected join state");

        assert_eq!(best.root_edge_refs, vec![JoinEdgeRef {
            id: 0,
            reversed: true,
        }]);

        assert_eq!(best.leaf_order, vec![
            vec![1].into_boxed_slice(),
            vec![0].into_boxed_slice()
        ]);

        Ok(())
    }
}
