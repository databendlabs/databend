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
}
