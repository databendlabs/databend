use std::collections::HashSet;

use common_exception::Result;

use crate::optimizer::hyper_dp::join_relation::JoinRelationSet;
use crate::IndexType;

struct QueryEdge {}

impl QueryEdge {
    fn new() -> Self {
        Self {}
    }
}

// The struct defines `QueryGraph` which contains node and edge
// Node represents a base relation
pub struct QueryGraph {
    edge: QueryEdge,
}

impl QueryGraph {
    pub fn new() -> Self {
        Self {
            edge: QueryEdge::new(),
        }
    }

    // Get all neighbors of `nodes` which are not in `forbidden_nodes`
    pub fn neighbors(
        nodes: &JoinRelationSet,
        forbidden_nodes: HashSet<IndexType>,
    ) -> Result<Vec<IndexType>> {
        todo!()
    }
}
