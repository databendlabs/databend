use std::collections::HashMap;
use std::collections::HashSet;

use common_exception::Result;

use crate::optimizer::hyper_dp::join_relation::JoinRelation;
use crate::optimizer::hyper_dp::join_relation::JoinRelationSet;
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
    pub fn new() -> Self {
        Self {
            join_relations: vec![],
            table_index_map: Default::default(),
            query_graph: QueryGraph::new(),
        }
    }

    // The input plan tree has been optimized by heuristic optimizer
    // So filters have pushed down join and cross join has been converted to inner join as possible as we can
    // The output plan will have optimal join order theoretically
    pub fn optimize(&self, s_expr: SExpr) -> Result<SExpr> {
        let optimized = self.solve()?;
        todo!()
    }

    // This method will run dynamic programming algorithm to find the optimal join order
    fn solve(&self) -> Result<bool> {
        todo!()
    }

    // EmitCsg will take a non-empty subset of hyper_graph's nodes(V) which contains a connected subgraph.
    // Then it will possibly generate a connected complement which will combine `nodes` to be a csg-cmp-pair.
    fn emit_csg(&self, nodes: &JoinRelationSet) -> Result<bool> {
        todo!()
    }

    // EnumerateCsgRec will extend the given `nodes`.
    // It'll consider each non-empty, proper subset of the neighborhood of nodes.
    fn enumerate_csg_rec(
        &self,
        nodes: &JoinRelationSet,
        forbidden_nodes: &HashSet<IndexType>,
    ) -> Result<bool> {
        todo!()
    }

    // EmitCsgCmp will join the optimal plan from left and right
    fn emit_csg_cmp(left: &JoinRelationSet, right: &JoinRelationSet) -> Result<bool> {
        todo!()
    }

    // The second parameter is a set which is connected and must be extended until a valid csg-cmp-pair is reached.
    // Therefore, it considers the neighborhood of right.
    fn enumerate_cmp_rec(
        left: &JoinRelationSet,
        right: &JoinRelationSet,
        forbidden_nodes: &HashSet<IndexType>,
    ) -> Result<bool> {
        todo!()
    }
}
