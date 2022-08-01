// Copyright 2022 Datafuse Labs.
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
use crate::api::ExecutePartialQueryPacket;
use crate::api::FragmentPayload;
use crate::api::FragmentPlanPacket;
use crate::api::InitNodesChannelPacket;
use crate::api::QueryFragmentsPlanPacket;
use crate::clusters::ClusterHelper;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::executor::PhysicalPlan;

// Query plan fragment with executor name
#[derive(Debug)]
pub struct QueryFragmentAction {
    pub payload: FragmentPayload,
    pub executor: String,
}

impl QueryFragmentAction {
    pub fn create(executor: String, node: PlanNode) -> QueryFragmentAction {
        QueryFragmentAction {
            payload: FragmentPayload::PlanV1(node),
            executor,
        }
    }

    pub fn create_v2(executor: String, plan: PhysicalPlan) -> QueryFragmentAction {
        QueryFragmentAction {
            payload: FragmentPayload::PlanV2(plan),
            executor,
        }
    }
}

#[derive(Debug)]
pub struct QueryFragmentActions {
    pub fragment_id: usize,
    pub exchange_actions: bool,
    pub data_exchange: Option<DataExchange>,
    pub fragment_actions: Vec<QueryFragmentAction>,
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
            actions_schema.push(fragment_action.payload.schema()?);
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
    pub fragments_actions: Vec<QueryFragmentActions>,
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

    pub fn pop_root_actions(&mut self) -> Option<QueryFragmentActions> {
        self.fragments_actions.pop()
    }

    pub fn add_fragment_actions(&mut self, actions: QueryFragmentActions) -> Result<()> {
        self.fragments_actions.push(actions);
        Ok(())
    }

    pub fn add_fragments_actions(&mut self, actions: QueryFragmentsActions) -> Result<()> {
        for fragment_actions in actions.fragments_actions.into_iter() {
            self.fragments_actions.push(fragment_actions);
        }

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

    pub fn get_query_fragments_plan_packets(&self) -> Result<Vec<QueryFragmentsPlanPacket>> {
        let fragments_packets = self.get_executors_fragments();
        let mut query_fragments_plan_packets = Vec::with_capacity(fragments_packets.len());

        let nodes_info = Self::nodes_info(&self.ctx);
        let target_source_fragments = self.get_target_source_fragments();

        let cluster = self.ctx.get_cluster();
        for (executor, fragments) in fragments_packets.into_iter() {
            let query_id = self.ctx.get_id();
            let executors_info = nodes_info.clone();

            let source_2_fragments = match target_source_fragments.get(&executor) {
                None => HashMap::new(),
                Some(source_2_fragments) => source_2_fragments.clone(),
            };

            query_fragments_plan_packets.push(QueryFragmentsPlanPacket::create(
                query_id,
                executor,
                fragments,
                executors_info,
                source_2_fragments,
                cluster.local_id(),
            ));
        }

        Ok(query_fragments_plan_packets)
    }

    pub fn get_init_nodes_channel_packets(&self) -> Result<Vec<InitNodesChannelPacket>> {
        let nodes_info = Self::nodes_info(&self.ctx);
        let mut init_nodes_channel_packets = Vec::with_capacity(nodes_info.len());
        let connections_info = self.get_source_target_fragments();

        let cluster = self.ctx.get_cluster();
        for node_id in nodes_info.keys() {
            let mut target_nodes_info = HashMap::new();
            let mut target_2_fragments = HashMap::new();

            if let Some(target_connections_info) = connections_info.get(node_id) {
                for (target, fragments) in target_connections_info {
                    if !nodes_info.contains_key(target) {
                        return Err(ErrorCode::ClusterUnknownNode(format!(
                            "Not found node {} in cluster. cluster nodes: {:?}",
                            target,
                            nodes_info.keys().cloned().collect::<Vec<_>>()
                        )));
                    }

                    target_2_fragments.insert(target.clone(), fragments.clone());
                    target_nodes_info.insert(target.clone(), nodes_info[target].clone());
                }
            }

            init_nodes_channel_packets.push(InitNodesChannelPacket::create(
                self.ctx.get_id(),
                node_id.to_owned(),
                cluster.local_id(),
                nodes_info.clone(),
                target_nodes_info,
                target_2_fragments,
            ));
        }

        Ok(init_nodes_channel_packets)
    }

    pub fn get_execute_partial_query_packets(&self) -> Result<Vec<ExecutePartialQueryPacket>> {
        let nodes_info = Self::nodes_info(&self.ctx);
        let mut execute_partial_query_packets = Vec::with_capacity(nodes_info.len());

        for node_id in nodes_info.keys() {
            execute_partial_query_packets.push(ExecutePartialQueryPacket::create(
                self.ctx.get_id(),
                node_id.to_owned(),
                nodes_info.clone(),
            ));
        }

        Ok(execute_partial_query_packets)
    }

    /// map(source, map(target, vec(fragment_id)))
    fn get_source_target_fragments(&self) -> HashMap<String, HashMap<String, Vec<usize>>> {
        let mut source_target_fragments = HashMap::new();
        for fragment_actions in &self.fragments_actions {
            if let Some(exchange) = &fragment_actions.data_exchange {
                let fragment_id = fragment_actions.fragment_id;
                let destinations = exchange.get_destinations();

                for fragment_action in &fragment_actions.fragment_actions {
                    let source = fragment_action.executor.to_string();

                    for destination in &destinations {
                        let target = destination.clone();
                        match source_target_fragments.entry(source.clone()) {
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
        source_target_fragments
    }

    /// map(target, map(source, vec(fragment_id)))
    fn get_target_source_fragments(&self) -> HashMap<String, HashMap<String, Vec<usize>>> {
        let mut target_source_fragments = HashMap::new();
        for fragment_actions in &self.fragments_actions {
            if let Some(exchange) = &fragment_actions.data_exchange {
                let fragment_id = fragment_actions.fragment_id;
                let destinations = exchange.get_destinations();

                for fragment_action in &fragment_actions.fragment_actions {
                    let source = fragment_action.executor.to_string();

                    for destination in &destinations {
                        let target = destination.clone();
                        match target_source_fragments.entry(target) {
                            Entry::Vacant(v) => {
                                let source_2_fragments = v.insert(HashMap::new());
                                source_2_fragments.insert(source.clone(), vec![fragment_id]);
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
        target_source_fragments
    }

    fn nodes_info(ctx: &Arc<QueryContext>) -> HashMap<String, Arc<NodeInfo>> {
        let nodes = ctx.get_cluster().get_nodes();
        let mut nodes_info = HashMap::with_capacity(nodes.len());

        for node in nodes {
            nodes_info.insert(node.id.to_owned(), node.clone());
        }

        nodes_info
    }

    fn get_executors_fragments(&self) -> HashMap<String, Vec<FragmentPlanPacket>> {
        let mut fragments_packets = HashMap::new();
        for fragment_actions in &self.fragments_actions {
            for fragment_action in &fragment_actions.fragment_actions {
                let fragment_packet = FragmentPlanPacket::create(
                    fragment_actions.fragment_id,
                    fragment_action.payload.clone(),
                    fragment_actions.data_exchange.clone(),
                );

                match fragments_packets.entry(fragment_action.executor.clone()) {
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
