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

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use databend_common_base::runtime::QueryPerf;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_meta_types::NodeInfo;

use crate::clusters::ClusterHelper;
use crate::physical_plans::ExchangeSink;
use crate::physical_plans::PhysicalPlan;
use crate::physical_plans::PhysicalPlanCast;
use crate::servers::flight::v1::exchange::DataExchange;
use crate::servers::flight::v1::packets::DataflowDiagramBuilder;
use crate::servers::flight::v1::packets::QueryEnv;
use crate::servers::flight::v1::packets::QueryFragment;
use crate::servers::flight::v1::packets::QueryFragments;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

// Query plan fragment with executor name
#[derive(Debug)]
pub struct QueryFragmentAction {
    pub executor: String,
    pub physical_plan: PhysicalPlan,
}

impl QueryFragmentAction {
    pub fn create(executor: String, physical_plan: PhysicalPlan) -> QueryFragmentAction {
        QueryFragmentAction {
            executor,
            physical_plan,
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

    pub fn get_root_fragment_ids(&self) -> Result<Vec<usize>> {
        let mut fragment_ids = Vec::new();
        for fragment_actions in &self.fragments_actions {
            if fragment_actions.fragment_actions.is_empty() {
                return Err(ErrorCode::Internal(format!(
                    "Fragment actions is empty for fragment_id: {}",
                    fragment_actions.fragment_id
                )));
            }

            let plan = &fragment_actions.fragment_actions[0].physical_plan;
            if !ExchangeSink::check_physical_plan(plan) {
                fragment_ids.push(fragment_actions.fragment_id);
            }
        }

        Ok(fragment_ids)
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

    pub fn get_query_fragments(&self) -> Result<HashMap<String, QueryFragments>> {
        let mut query_fragments = HashMap::new();
        for (executor, fragments) in self.get_executors_fragments() {
            query_fragments.insert(executor, QueryFragments {
                query_id: self.ctx.get_id(),
                fragments,
            });
        }

        Ok(query_fragments)
    }

    pub fn get_query_env(&self) -> Result<QueryEnv> {
        let workload_group = self.ctx.get_current_session().get_current_workload_group();
        let mut builder = DataflowDiagramBuilder::create(self.ctx.get_cluster().nodes.clone());

        self.fragments_connections(&mut builder)?;
        self.statistics_connections(&mut builder)?;

        let perf_flag = QueryPerf::flag();

        Ok(QueryEnv {
            workload_group,
            query_id: self.ctx.get_id(),
            cluster: self.ctx.get_cluster(),
            settings: self.ctx.get_settings(),

            query_kind: self.ctx.get_query_kind(),
            dataflow_diagram: Arc::new(builder.build()),
            request_server_id: GlobalConfig::instance().query.node_id.clone(),
            create_rpc_clint_with_current_rt: self
                .ctx
                .get_settings()
                .get_create_query_flight_client_with_current_rt()?,
            perf_flag,
            user: self.ctx.get_current_user()?,
        })
    }

    pub fn prepared_query(&self) -> Result<HashMap<String, String>> {
        let nodes_info = Self::nodes_info(&self.ctx);
        let mut execute_partial_query_packets = HashMap::with_capacity(nodes_info.len());

        for node_id in nodes_info.keys() {
            execute_partial_query_packets.insert(node_id.to_string(), self.ctx.get_id());
        }

        Ok(execute_partial_query_packets)
    }

    /// unique map(target, map(source, vec(fragment_id)))
    fn fragments_connections(&self, builder: &mut DataflowDiagramBuilder) -> Result<()> {
        for fragment_actions in &self.fragments_actions {
            if let Some(exchange) = &fragment_actions.data_exchange {
                let destinations = exchange.get_destinations();

                for fragment_action in &fragment_actions.fragment_actions {
                    let source = fragment_action.executor.to_string();

                    for destination in &destinations {
                        for channel in exchange.get_channels(destination) {
                            builder.add_data_edge(&source, destination, &channel)?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn statistics_connections(&self, builder: &mut DataflowDiagramBuilder) -> Result<()> {
        let local_id = self.ctx.get_cluster().local_id.clone();

        for (_id, node_info) in Self::nodes_info(&self.ctx) {
            builder.add_statistics_edge(&node_info.id, &local_id)?;
        }

        Ok(())
    }

    fn nodes_info(ctx: &Arc<QueryContext>) -> HashMap<String, Arc<NodeInfo>> {
        let nodes = ctx.get_cluster().get_nodes();
        let mut nodes_info = HashMap::with_capacity(nodes.len());

        for node in nodes {
            nodes_info.insert(node.id.to_owned(), node.clone());
        }

        nodes_info
    }

    fn get_executors_fragments(&self) -> HashMap<String, Vec<QueryFragment>> {
        let mut fragments_packets = HashMap::new();
        for fragment_actions in &self.fragments_actions {
            for fragment_action in &fragment_actions.fragment_actions {
                let query_fragment = QueryFragment::create(
                    fragment_actions.fragment_id,
                    fragment_actions.data_exchange.clone(),
                    fragment_action.physical_plan.clone(),
                );

                match fragments_packets.entry(fragment_action.executor.clone()) {
                    Entry::Vacant(entry) => {
                        entry.insert(vec![query_fragment]);
                    }
                    Entry::Occupied(mut entry) => {
                        entry.get_mut().push(query_fragment);
                    }
                }
            }
        }

        fragments_packets
    }
}

impl Debug for QueryFragmentsActions {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("QueryFragmentsActions")
            .field("actions", &self.fragments_actions)
            .finish()
    }
}
