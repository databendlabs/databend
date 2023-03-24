use std::collections::HashSet;

use common_exception::Result;

use crate::optimizer::hyper_dp::join_relation::JoinRelationSet;
use crate::IndexType;

struct NeighborInfo {
    neighbors: JoinRelationSet,
    // filters:
}

struct QueryEdge {
    neighbors: Vec<NeighborInfo>,
}

impl QueryEdge {
    fn new() -> Self {
        Self { neighbors: vec![] }
    }
}

// The struct defines `QueryGraph` which contains node and edge
// Node represents a base relation
pub struct QueryGraph {
    root_edge: QueryEdge,
}

impl QueryGraph {
    pub fn new() -> Self {
        Self {
            root_edge: QueryEdge::new(),
        }
    }

    // Get all neighbors of `nodes` which are not in `forbidden_nodes`
    pub fn neighbors(
        &self,
        nodes: &JoinRelationSet,
        forbidden_nodes: HashSet<IndexType>,
    ) -> Result<Vec<IndexType>> {
        todo!()
    }

    // Create edges for left set and right set
    pub fn create_edges(
        &mut self,
        left_set: &JoinRelationSet,
        right_set: &JoinRelationSet,
    ) -> Result<()> {
        todo!()
    }
}
