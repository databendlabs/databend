use std::sync::Arc;
use petgraph::stable_graph::StableGraph;
use common_exception::Result;
use common_planners::PlanNode;
use crate::sessions::QueryContext;

struct QueryFragmentNode {
    executor_port: u16,
    executor_address: String,

    execute_plan: PlanNode,
}

struct QueryFragmentEdge {}

pub struct QueryFragmentDAG {
    ctx: Arc<QueryContext>,
    graph: StableGraph<QueryFragmentNode, QueryFragmentEdge>,
}

impl QueryFragmentDAG {
    pub fn create(ctx: Arc<QueryContext>) -> QueryFragmentDAG {
        QueryFragmentDAG { ctx, graph: StableGraph::new() }
    }

    pub fn add_merge_node(&mut self, node: &PlanNode) -> Result<()> {
        unimplemented!()
    }

    pub fn add_shuffle_node(&mut self, node: &PlanNode) -> Result<()> {
        unimplemented!()
    }

    pub fn add_exchange_node(&mut self, node: &PlanNode) -> Result<()> {
        unimplemented!()
    }

    pub fn add_broadcast_node(&mut self, node: &PlanNode) -> Result<()> {
        let cluster = self.ctx.get_cluster();
        for executor_info in cluster.get_nodes() {
            self.graph.add_node(QueryFragmentNode {
                executor_port: 0,
                executor_address: executor_info.flight_address.to_owned(),
                execute_plan: node.clone(),
            });
        }

        Ok(())
    }
}
