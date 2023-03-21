use std::collections::HashMap;

use common_exception::Result;

use crate::optimizer::hyper_dp::join_relation::JoinRelation;
use crate::optimizer::hyper_dp::query_graph::QueryGraph;
use crate::optimizer::SExpr;
use crate::plans::RelOperator;
use crate::IndexType;

// The join reorder algorithm follows the paper: Dynamic Programming Strikes Back
// See the paper for more details.
// Current, we only support inner join
pub struct DPhpy {
    join_relations: Vec<JoinRelation>,
    // base table index -> index of join_relations
    table_index_map: HashMap<IndexType, IndexType>,

    query_graph: QueryGraph,
}

impl DPhpy {
    fn new() -> Self {
        Self {
            join_relations: vec![],
            table_index_map: Default::default(),
            query_graph: QueryGraph::new(),
        }
    }

    // The input plan tree has been optimized by heuristic optimizer
    // So filters have pushed down join and cross join has been converted to inner join as possible as we can
    // The output plan will have optimal join order theoretically
    fn optimize(&self, s_expr: SExpr) -> Result<SExpr> {
        todo!()
    }
}
