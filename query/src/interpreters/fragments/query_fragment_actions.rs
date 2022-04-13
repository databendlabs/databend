use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use common_ast::parser::rule::expr::query;
use common_exception::{ErrorCode, Result};
use common_meta_types::NodeInfo;
use common_planners::PlanNode;
use crate::api::{DataExchange, ExecutorPacket, FragmentPacket, PublisherPacket};
use crate::clusters::Cluster;
use crate::interpreters::fragments::partition_state::PartitionState;
use crate::sessions::QueryContext;

// Query plan fragment with executor name
pub struct QueryFragmentAction {
    pub node: PlanNode,
    pub executor: String,
}

impl QueryFragmentAction {
    pub fn create(executor: String, node: PlanNode) -> QueryFragmentAction {
        QueryFragmentAction { node, executor }
    }
}

#[derive(Debug)]
pub struct QueryFragmentActions {
    pub exchange_actions: bool,
    pub fragment_id: String,
    pub data_exchange: Option<DataExchange>,
    fragment_actions: Vec<QueryFragmentAction>,
}

impl QueryFragmentActions {
    pub fn create(force_exchange: bool) -> QueryFragmentActions {
        QueryFragmentActions { exchange_actions: force_exchange, fragment_id: "".to_string(), data_exchange: None, fragment_actions: vec![] }
    }

    pub fn get_actions(&self) -> &[QueryFragmentAction] {
        &self.fragment_actions
    }

    pub fn add_action(&mut self, action: QueryFragmentAction) {
        self.fragment_actions.push(action)
    }

    pub fn set_exchange(&mut self, exchange: DataExchange) {
        self.data_exchange = Some(exchange);
    }
}

pub struct QueryFragmentsActions {
    ctx: Arc<QueryContext>,
    fragments_actions: Vec<QueryFragmentActions>,
}

impl QueryFragmentsActions {
    pub fn create(ctx: Arc<QueryContext>) -> QueryFragmentsActions {
        QueryFragmentsActions { ctx, fragments_actions: Vec::new() }
    }

    pub fn get_executors(&self) -> Vec<String> {
        let cluster = self.ctx.get_cluster();
        let cluster_nodes = cluster.get_nodes();

        cluster_nodes.iter().map(|node| &node.id)
            .cloned().collect()
    }

    pub fn get_local_executor(&self) -> String {
        self.ctx.get_cluster().local_id()
    }

    pub fn get_root_actions(&self) -> Result<&QueryFragmentActions> {
        match self.fragments_actions.last() {
            None => Err(ErrorCode::LogicalError("Logical error, call get_root_actions in empty QueryFragmentsActions")),
            Some(entity) => Ok(entity)
        }
    }

    pub fn add_fragment_actions(&mut self, actions: QueryFragmentActions) -> Result<()> {
        self.fragments_actions.push(actions);
        Ok(())
    }

    pub fn update_root_fragment_actions(&mut self, actions: QueryFragmentActions) -> Result<()> {
        if self.fragments_actions.is_empty() {
            return Err(ErrorCode::LogicalError(
                "Logical error, cannot update last element for empty actions."
            ));
        }

        *self.fragments_actions.last_mut().unwrap() = actions;
        Ok(())
    }

    pub fn prepare_packets(&self, ctx: Arc<QueryContext>) -> Result<Vec<ExecutorPacket>> {
        let fragments_packets = self.get_executors_fragments();
        let mut executors_packets = Vec::with_capacity(fragments_packets.len());

        let nodes_info = Self::nodes_info(&ctx);

        for (executor, fragments) in fragments_packets.into_iter() {
            let query_id = ctx.get_id();
            let executors_info = nodes_info.clone();
            let packet = ExecutorPacket::create(query_id, executor, fragments, executors_info);
            executors_packets.push(packet);
        }

        Ok(executors_packets)
    }

    pub fn prepare_publisher(&self, ctx: Arc<QueryContext>) -> Result<Vec<PublisherPacket>> {
        let nodes_info = Self::nodes_info(&ctx);
        let mut publisher_packets = Vec::with_capacity(nodes_info.len());

        for (node_id, node_info) in &nodes_info {
            publisher_packets.push(PublisherPacket::create(
                ctx.get_id(),
                node_id.to_owned(),
                nodes_info.clone(),
            ));
        }

        Ok(publisher_packets)
    }

    fn nodes_info(ctx: &Arc<QueryContext>) -> HashMap<String, Arc<NodeInfo>> {
        let nodes = ctx.get_cluster().get_nodes();
        let mut nodes_info = HashMap::with_capacity(nodes.len());

        for node in nodes {
            nodes_info.insert(node.id.to_owned(), node.clone());
        }

        nodes_info
    }

    fn get_executors_fragments(&self) -> HashMap<String, Vec<FragmentPacket>> {
        let mut fragments_packets = HashMap::new();
        for fragment_actions in &self.fragments_actions {
            for fragment_action in &fragment_actions.fragment_actions {
                let fragment_packet = FragmentPacket::create(
                    fragment_actions.fragment_id.to_owned(),
                    fragment_action.node.clone(),
                    fragment_actions.data_exchange.clone().unwrap(),
                );

                match fragments_packets.entry(fragment_action.executor.to_owned()) {
                    Entry::Vacant(entry) => {
                        entry.insert(vec![fragment_packet]);
                    }
                    Entry::Occupied(mut entry) => {
                        entry.get_mut().push(fragment_packet);
                    }
                }
            }
        }

        fragments_packets
    }
}

impl Debug for QueryFragmentsActions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryFragmentsActions")
            .field("actions", &self.fragments_actions)
            .finish()
    }
}

impl Debug for QueryFragmentAction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryFragmentAction")
            .field("node", &self.node.name())
            .finish()
    }
}
