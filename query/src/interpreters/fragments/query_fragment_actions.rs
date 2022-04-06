use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use common_exception::{ErrorCode, Result};
use common_planners::PlanNode;
use crate::api::{ExecutorPacket, FragmentPacket};
use crate::interpreters::fragments::partition_state::PartitionState;
use crate::sessions::QueryContext;

// Query plan fragment with executor name
#[derive(Debug)]
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
    fragment_actions: Vec<QueryFragmentAction>,
}

impl QueryFragmentActions {
    pub fn create(force_exchange: bool) -> QueryFragmentActions {
        QueryFragmentActions { exchange_actions: force_exchange, fragment_id: "".to_string(), fragment_actions: vec![] }
    }

    pub fn get_actions(&self) -> &[QueryFragmentAction] {
        &self.fragment_actions
    }

    pub fn add_action(&mut self, action: QueryFragmentAction) {
        self.fragment_actions.push(action)
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

    pub fn to_packets(self) -> Result<Vec<ExecutorPacket>> {
        let mut fragments_packets = HashMap::new();
        for fragment_actions in self.fragments_actions {
            for fragment_action in fragment_actions.fragment_actions {
                let fragment_packet = FragmentPacket::create(fragment_action.node.clone());

                // TODO: require node info
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

        let mut executors_packets = Vec::with_capacity(fragments_packets.len());

        for (executor, actions) in fragments_packets.into_iter() {
            executors_packets.push(ExecutorPacket::create(executor, actions));
        }

        Ok(executors_packets)
    }
}

impl Debug for QueryFragmentsActions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryFragmentsActions")
            .field("actions", &self.fragments_actions)
            .finish()
    }
}
