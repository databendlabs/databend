use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::NodeInfo;
use common_planners::PlanNode;

use crate::api::DataExchange;
use crate::api::ExecutePacket;
use crate::api::ExecutorPacket;
use crate::api::FragmentPacket;
use crate::api::PrepareChannel;
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
    pub fragment_id: usize,
    pub exchange_actions: bool,
    pub data_exchange: Option<DataExchange>,
    fragment_actions: Vec<QueryFragmentAction>,
}

impl QueryFragmentActions {
    pub fn create(exchange_actions: bool, fragment_id: usize) -> QueryFragmentActions {
        QueryFragmentActions {
            exchange_actions,
            fragment_id,
            data_exchange: None,
            fragment_actions: vec![],
        }
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

    pub fn get_schema(&self) -> Result<DataSchemaRef> {
        let mut actions_schema = Vec::with_capacity(self.fragment_actions.len());
        for fragment_action in &self.fragment_actions {
            actions_schema.push(fragment_action.node.schema());
        }

        if actions_schema.is_empty() {
            return Err(ErrorCode::DataStructMissMatch(
                "Schema miss match in fragment actions.",
            ));
        }

        for action_schema in &actions_schema {
            if action_schema != &actions_schema[0] {
                return Err(ErrorCode::DataStructMissMatch(
                    "Schema miss match in fragment actions.",
                ));
            }
        }

        Ok(actions_schema[0].clone())
    }
}

pub struct QueryFragmentsActions {
    ctx: Arc<QueryContext>,
    fragments_actions: Vec<QueryFragmentActions>,
}

impl QueryFragmentsActions {
    pub fn create(ctx: Arc<QueryContext>) -> QueryFragmentsActions {
        QueryFragmentsActions {
            ctx,
            fragments_actions: Vec::new(),
        }
    }

    pub fn get_executors(&self) -> Vec<String> {
        let cluster = self.ctx.get_cluster();
        let cluster_nodes = cluster.get_nodes();

        cluster_nodes.iter().map(|node| &node.id).cloned().collect()
    }

    pub fn get_local_executor(&self) -> String {
        self.ctx.get_cluster().local_id()
    }

    pub fn get_root_actions(&self) -> Result<&QueryFragmentActions> {
        match self.fragments_actions.last() {
            None => Err(ErrorCode::LogicalError(
                "Logical error, call get_root_actions in empty QueryFragmentsActions",
            )),
            Some(entity) => Ok(entity),
        }
    }

    pub fn add_fragment_actions(&mut self, actions: QueryFragmentActions) -> Result<()> {
        self.fragments_actions.push(actions);
        Ok(())
    }

    pub fn update_root_fragment_actions(&mut self, actions: QueryFragmentActions) -> Result<()> {
        if self.fragments_actions.is_empty() {
            return Err(ErrorCode::LogicalError(
                "Logical error, cannot update last element for empty actions.",
            ));
        }

        *self.fragments_actions.last_mut().unwrap() = actions;
        Ok(())
    }

    pub fn prepare_packets(&self, ctx: Arc<QueryContext>) -> Result<Vec<ExecutorPacket>> {
        let fragments_packets = self.get_executors_fragments();
        let mut executors_packets = Vec::with_capacity(fragments_packets.len());

        let nodes_info = Self::nodes_info(&ctx);
        let source_2_fragments = self.get_source_2_fragments();

        let cluster = ctx.get_cluster();
        for (executor, fragments) in fragments_packets.into_iter() {
            let query_id = ctx.get_id();
            let executors_info = nodes_info.clone();

            let source_2_fragments = match source_2_fragments.get(&executor) {
                None => HashMap::new(),
                Some(source_2_fragments) => source_2_fragments.clone(),
            };

            executors_packets.push(ExecutorPacket::create(
                query_id,
                executor,
                fragments,
                executors_info,
                source_2_fragments,
                cluster.local_id(),
            ));
        }

        Ok(executors_packets)
    }

    pub fn prepare_channel(&self, ctx: Arc<QueryContext>) -> Result<Vec<PrepareChannel>> {
        let nodes_info = Self::nodes_info(&ctx);
        let mut prepare_channel = Vec::with_capacity(nodes_info.len());
        let connections_info = self.get_target_2_fragments();

        let cluster = ctx.get_cluster();
        for node_id in nodes_info.keys() {
            let mut target_nodes_info = HashMap::new();
            let mut target_2_fragments = HashMap::new();

            if let Some(target_connections_info) = connections_info.get(node_id) {
                for (target, fragments) in target_connections_info {
                    target_2_fragments.insert(target.clone(), fragments.clone());
                    target_nodes_info.insert(target.clone(), nodes_info[target].clone());
                }
            }

            prepare_channel.push(PrepareChannel::create(
                ctx.get_id(),
                node_id.to_owned(),
                cluster.local_id(),
                nodes_info.clone(),
                target_nodes_info,
                target_2_fragments,
            ));
        }

        Ok(prepare_channel)
    }

    // map(source, map(target, vec(fragment_id)))
    fn get_target_2_fragments(&self) -> HashMap<String, HashMap<String, Vec<usize>>> {
        let mut target_2_fragments = HashMap::new();
        for fragment_actions in &self.fragments_actions {
            if let Some(exchange) = &fragment_actions.data_exchange {
                let fragment_id = fragment_actions.fragment_id;
                let destinations = exchange.get_destinations();

                for fragment_action in &fragment_actions.fragment_actions {
                    let source = fragment_action.executor.to_string();

                    for destination in &destinations {
                        let target = destination.clone();
                        match target_2_fragments.entry(source.clone()) {
                            Entry::Vacant(v) => {
                                let target_2_fragments = v.insert(HashMap::new());
                                target_2_fragments.insert(target, vec![fragment_id]);
                            }
                            Entry::Occupied(mut v) => match v.get_mut().entry(target) {
                                Entry::Vacant(v) => {
                                    v.insert(vec![fragment_id]);
                                }
                                Entry::Occupied(mut v) => {
                                    v.get_mut().push(fragment_id);
                                }
                            },
                        };
                    }
                }
            }
        }
        target_2_fragments
    }

    // map(target, map(source, vec(fragment_id)))
    fn get_source_2_fragments(&self) -> HashMap<String, HashMap<String, Vec<usize>>> {
        let mut source_2_fragments = HashMap::new();
        for fragment_actions in &self.fragments_actions {
            if let Some(exchange) = &fragment_actions.data_exchange {
                let fragment_id = fragment_actions.fragment_id;
                let destinations = exchange.get_destinations();

                for fragment_action in &fragment_actions.fragment_actions {
                    let source = fragment_action.executor.to_string();

                    for destination in &destinations {
                        let target = destination.clone();
                        match source_2_fragments.entry(target) {
                            Entry::Vacant(v) => {
                                let target_2_fragments = v.insert(HashMap::new());
                                target_2_fragments.insert(source.clone(), vec![fragment_id]);
                            }
                            Entry::Occupied(mut v) => match v.get_mut().entry(source.clone()) {
                                Entry::Vacant(v) => {
                                    v.insert(vec![fragment_id]);
                                }
                                Entry::Occupied(mut v) => {
                                    v.get_mut().push(fragment_id);
                                }
                            },
                        };
                    }
                }
            }
        }
        source_2_fragments
    }

    pub fn execute_packets(&self, ctx: Arc<QueryContext>) -> Result<Vec<ExecutePacket>> {
        let nodes_info = Self::nodes_info(&ctx);
        let mut execute_packets = Vec::with_capacity(nodes_info.len());

        for node_id in nodes_info.keys() {
            execute_packets.push(ExecutePacket::create(
                ctx.get_id(),
                node_id.to_owned(),
                nodes_info.clone(),
            ));
        }

        Ok(execute_packets)
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
                    fragment_actions.fragment_id,
                    fragment_action.node.clone(),
                    fragment_actions.data_exchange.clone(),
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
            .field("node", &self.node)
            .field("executor", &self.executor)
            .finish()
    }
}
