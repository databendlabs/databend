// Copyright 2021 Datafuse Labs
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

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataSchemaRef;
use common_meta_types::NodeInfo;
use itertools::Itertools;

use crate::api::ConnectionInfo;
use crate::api::DataExchange;
use crate::api::ExecutePartialQueryPacket;
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
    pub physical_plan: PhysicalPlan,
    pub executor: String,
}

impl QueryFragmentAction {
    pub fn create(executor: String, physical_plan: PhysicalPlan) -> QueryFragmentAction {
        QueryFragmentAction {
            physical_plan,
            executor,
        }
    }
}

#[derive(Debug)]
pub struct QueryFragmentActions {
    pub fragment_id: usize,
    pub data_exchange: Option<DataExchange>,
    pub fragment_actions: Vec<QueryFragmentAction>,
}

impl QueryFragmentActions {
    pub fn create(fragment_id: usize) -> QueryFragmentActions {
        QueryFragmentActions {
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
            actions_schema.push(fragment_action.physical_plan.output_schema()?);
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
    enable_profiling: bool,
    pub fragments_actions: Vec<QueryFragmentActions>,
}

impl QueryFragmentsActions {
    pub fn create(ctx: Arc<QueryContext>, enable_profiling: bool) -> QueryFragmentsActions {
        QueryFragmentsActions {
            ctx,
            enable_profiling,
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
        self.fragments_actions.last().ok_or(ErrorCode::Internal(
            "Logical error, call get_root_actions in empty QueryFragmentsActions",
        ))
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
            return Err(ErrorCode::Internal(
                "Logical error, cannot update last element for empty actions.",
            ));
        }

        *self.fragments_actions.last_mut().unwrap() = actions;
        Ok(())
    }

    pub fn get_query_fragments_plan_packets(
        &self,
    ) -> Result<(QueryFragmentsPlanPacket, Vec<QueryFragmentsPlanPacket>)> {
        let nodes_info = Self::nodes_info(&self.ctx);

        let mut fragments_packets = self.get_executors_fragments();
        let mut query_fragments_plan_packets = Vec::with_capacity(fragments_packets.len());

        let cluster = self.ctx.get_cluster();
        let changed_settings = self.ctx.get_changed_settings();
        let local_query_fragments_plan_packet = QueryFragmentsPlanPacket::create(
            self.ctx.get_id(),
            cluster.local_id.clone(),
            fragments_packets.remove(&cluster.local_id).unwrap(),
            nodes_info.clone(),
            changed_settings.clone(),
            cluster.local_id(),
            self.enable_profiling,
        );

        for (executor, fragments) in fragments_packets.into_iter() {
            let query_id = self.ctx.get_id();
            let executors_info = nodes_info.clone();

            query_fragments_plan_packets.push(QueryFragmentsPlanPacket::create(
                query_id,
                executor,
                fragments,
                executors_info,
                changed_settings.clone(),
                cluster.local_id(),
                self.enable_profiling,
            ));
        }

        Ok((
            local_query_fragments_plan_packet,
            query_fragments_plan_packets,
        ))
    }

    pub fn get_init_nodes_channel_packets(&self) -> Result<Vec<InitNodesChannelPacket>> {
        let nodes_info = Self::nodes_info(&self.ctx);
        let local_id = self.ctx.get_cluster().local_id.clone();
        let connections_info = self.fragments_connections();
        let statistics_connections = self.statistics_connections();

        let mut init_nodes_channel_packets = Vec::with_capacity(connections_info.len());

        for (executor, fragments_connections) in &connections_info {
            if !nodes_info.contains_key(executor) {
                return Err(ErrorCode::NotFoundClusterNode(format!(
                    "Not found node {} in cluster. cluster nodes: {:?}",
                    executor,
                    nodes_info.keys().cloned().collect::<Vec<_>>()
                )));
            }

            let executor_node_info = &nodes_info[executor];
            let mut connections_info = Vec::with_capacity(fragments_connections.len());

            for (source, fragments) in fragments_connections {
                if !nodes_info.contains_key(source) {
                    return Err(ErrorCode::NotFoundClusterNode(format!(
                        "Not found node {} in cluster. cluster nodes: {:?}",
                        source,
                        nodes_info.keys().cloned().collect::<Vec<_>>()
                    )));
                }

                connections_info.push(ConnectionInfo::create(
                    nodes_info[source].clone(),
                    fragments.iter().cloned().unique().collect::<Vec<_>>(),
                ));
            }

            init_nodes_channel_packets.push(InitNodesChannelPacket::create(
                self.ctx.get_id(),
                executor_node_info.clone(),
                connections_info,
                match executor_node_info.id == local_id {
                    true => statistics_connections.clone(),
                    false => vec![],
                },
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

    /// unique map(target, map(source, vec(fragment_id)))
    fn fragments_connections(&self) -> HashMap<String, HashMap<String, Vec<usize>>> {
        let mut target_source_fragments = HashMap::<String, HashMap<String, Vec<usize>>>::new();

        for fragment_actions in &self.fragments_actions {
            if let Some(exchange) = &fragment_actions.data_exchange {
                let fragment_id = fragment_actions.fragment_id;
                let destinations = exchange.get_destinations();

                for fragment_action in &fragment_actions.fragment_actions {
                    let source = fragment_action.executor.to_string();

                    for destination in &destinations {
                        if &source == destination {
                            continue;
                        }

                        if target_source_fragments.contains_key(destination) {
                            let source_fragments = target_source_fragments
                                .get_mut(destination)
                                .expect("Target fragments expect source");

                            if source_fragments.contains_key(&source) {
                                source_fragments
                                    .get_mut(&source)
                                    .expect("Source target fragments expect destination")
                                    .push(fragment_id);

                                continue;
                            }
                        }

                        if target_source_fragments.contains_key(destination) {
                            let source_fragments = target_source_fragments
                                .get_mut(destination)
                                .expect("Target fragments expect source");

                            source_fragments.insert(source.clone(), vec![fragment_id]);
                            continue;
                        }

                        let mut target_fragments = HashMap::new();
                        target_fragments.insert(source.clone(), vec![fragment_id]);
                        target_source_fragments.insert(destination.clone(), target_fragments);
                    }
                }
            }
        }

        target_source_fragments
    }

    fn statistics_connections(&self) -> Vec<ConnectionInfo> {
        let local_id = self.ctx.get_cluster().local_id.clone();
        let nodes_info = Self::nodes_info(&self.ctx);
        let mut target_source_connections = Vec::with_capacity(nodes_info.len());

        for (id, node_info) in nodes_info {
            if local_id == id {
                continue;
            }

            target_source_connections.push(ConnectionInfo::create(node_info, vec![]));
        }

        target_source_connections
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
                    fragment_action.physical_plan.clone(),
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
