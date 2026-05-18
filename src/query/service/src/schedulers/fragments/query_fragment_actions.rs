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

use std::any::Any;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use databend_common_catalog::plan::DataSourceInfo;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_meta_client::types::NodeInfo;

use crate::clusters::ClusterHelper;
use crate::physical_plans::ConstantTableScan;
use crate::physical_plans::ExchangeSink;
use crate::physical_plans::PhysicalPlan;
use crate::physical_plans::PhysicalPlanCast;
use crate::physical_plans::PhysicalPlanVisitor;
use crate::physical_plans::TableScan;
use crate::servers::flight::v1::exchange::DataExchange;
use crate::servers::flight::v1::packets::DataflowDiagramBuilder;
use crate::servers::flight::v1::packets::QueryEnv;
use crate::servers::flight::v1::packets::QueryFragment;
use crate::servers::flight::v1::packets::QueryFragments;
use crate::sessions::QueryContext;
use crate::sessions::TableContextAuthorization;
use crate::sessions::TableContextCluster;
use crate::sessions::TableContextPerf;
use crate::sessions::TableContextQueryIdentity;
use crate::sessions::TableContextQueryInfo;
use crate::sessions::TableContextSettings;

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

    fn prune_empty_source_actions(&mut self, local_executor: &str) -> Result<()> {
        let states = self
            .fragment_actions
            .iter()
            .map(|action| detect_source_action_state(&action.physical_plan))
            .collect::<Result<Vec<_>>>()?;

        if !states
            .iter()
            .any(|state| !matches!(state, SourceActionState::NoSource))
        {
            return Ok(());
        }

        let has_non_empty_action = states
            .iter()
            .any(|state| matches!(state, SourceActionState::NonEmpty));
        let has_singleton_action = states
            .iter()
            .any(|state| matches!(state, SourceActionState::Singleton));

        self.fragment_actions = self
            .fragment_actions
            .drain(..)
            .zip(states)
            .filter_map(|(action, state)| {
                let keep = should_keep_source_action(
                    has_non_empty_action,
                    has_singleton_action,
                    state,
                    action.executor == local_executor,
                );

                keep.then_some(action)
            })
            .collect();

        Ok(())
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
        let mut actions = actions;
        actions.prune_empty_source_actions(&self.get_local_executor())?;
        self.fragments_actions.push(actions);
        Ok(())
    }

    pub fn add_fragments_actions(&mut self, actions: QueryFragmentsActions) -> Result<()> {
        for mut fragment_actions in actions.fragments_actions.into_iter() {
            fragment_actions.prune_empty_source_actions(&self.get_local_executor())?;
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
        let mut executors_fragments = self.get_executors_fragments()?;
        prune_query_fragments(&mut executors_fragments, &self.get_local_executor())?;

        let mut query_fragments = HashMap::new();
        for (executor, fragments) in executors_fragments {
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
            perf_config: self.ctx.get_perf_config(),
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
                let use_do_exchange = exchange.use_do_exchange();

                for fragment_action in &fragment_actions.fragment_actions {
                    let source = fragment_action.executor.to_string();

                    for destination in &destinations {
                        if use_do_exchange {
                            let channels = exchange.get_channels(destination);
                            builder.add_exchange_edge(
                                &source,
                                destination,
                                exchange.get_id(),
                                channels,
                            )?;
                        } else {
                            for channel in exchange.get_channels(destination) {
                                builder.add_data_edge(&source, destination, &channel)?;
                            }
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

    fn get_executors_fragments(&self) -> Result<HashMap<String, Vec<QueryFragment>>> {
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

        Ok(fragments_packets)
    }
}

impl Debug for QueryFragmentsActions {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("QueryFragmentsActions")
            .field("actions", &self.fragments_actions)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SourceActionState {
    NoSource,
    Empty,
    Singleton,
    NonEmpty,
}

fn should_keep_source_action(
    has_non_empty_action: bool,
    has_singleton_action: bool,
    state: SourceActionState,
    is_local_executor: bool,
) -> bool {
    match state {
        SourceActionState::NoSource | SourceActionState::NonEmpty => true,
        SourceActionState::Singleton => !has_non_empty_action && is_local_executor,
        SourceActionState::Empty => {
            !has_non_empty_action && !has_singleton_action && is_local_executor
        }
    }
}

fn is_singleton_system_one_source(scan: &TableScan) -> bool {
    matches!(
        &scan.source.source_info,
        DataSourceInfo::TableSource(table_info)
            if table_info.meta.engine == "SystemOne" && table_info.name == "one"
    )
}

fn detect_source_action_state(plan: &PhysicalPlan) -> Result<SourceActionState> {
    struct SourceActionVisitor {
        has_source: bool,
        has_singleton_source: bool,
        has_non_empty_source: bool,
    }

    impl SourceActionVisitor {
        fn create() -> Box<dyn PhysicalPlanVisitor> {
            Box::new(SourceActionVisitor {
                has_source: false,
                has_singleton_source: false,
                has_non_empty_source: false,
            })
        }
    }

    impl PhysicalPlanVisitor for SourceActionVisitor {
        fn as_any(&mut self) -> &mut dyn Any {
            self
        }

        fn visit(&mut self, plan: &PhysicalPlan) -> Result<()> {
            if let Some(scan) = TableScan::from_physical_plan(plan) {
                self.has_source = true;
                if is_singleton_system_one_source(scan) {
                    self.has_singleton_source = true;
                } else if !scan.source.parts.is_empty() {
                    self.has_non_empty_source = true;
                }
            } else if let Some(scan) = ConstantTableScan::from_physical_plan(plan) {
                self.has_source = true;
                if scan.num_rows > 0 {
                    self.has_non_empty_source = true;
                }
            }

            Ok(())
        }
    }

    let mut visitor = SourceActionVisitor::create();
    plan.visit(&mut visitor)?;

    let visitor = visitor
        .as_any()
        .downcast_mut::<SourceActionVisitor>()
        .ok_or_else(|| ErrorCode::Internal("Failed to downcast SourceActionVisitor"))?;

    Ok(
        match (
            visitor.has_source,
            visitor.has_non_empty_source,
            visitor.has_singleton_source,
        ) {
            (false, _, _) => SourceActionState::NoSource,
            (true, true, _) => SourceActionState::NonEmpty,
            (true, false, true) => SourceActionState::Singleton,
            (true, false, false) => SourceActionState::Empty,
        },
    )
}

fn prune_query_fragments(
    executors_fragments: &mut HashMap<String, Vec<QueryFragment>>,
    local_executor: &str,
) -> Result<()> {
    let mut fragment_states = HashMap::<usize, Vec<(String, SourceActionState)>>::new();

    for (executor, fragments) in executors_fragments.iter() {
        for fragment in fragments {
            fragment_states
                .entry(fragment.fragment_id)
                .or_default()
                .push((
                    executor.clone(),
                    detect_source_action_state(&fragment.physical_plan)?,
                ));
        }
    }

    let mut keep_lookup = HashMap::<(usize, String), bool>::new();
    for (fragment_id, states) in fragment_states {
        if !states
            .iter()
            .any(|(_, state)| !matches!(state, SourceActionState::NoSource))
        {
            continue;
        }

        let has_non_empty_action = states
            .iter()
            .any(|(_, state)| matches!(state, SourceActionState::NonEmpty));
        let has_singleton_action = states
            .iter()
            .any(|(_, state)| matches!(state, SourceActionState::Singleton));

        for (executor, state) in states {
            let keep = should_keep_source_action(
                has_non_empty_action,
                has_singleton_action,
                state,
                executor == local_executor,
            );
            keep_lookup.insert((fragment_id, executor), keep);
        }
    }

    executors_fragments.retain(|executor, fragments| {
        fragments.retain(|fragment| {
            keep_lookup
                .get(&(fragment.fragment_id, executor.clone()))
                .copied()
                .unwrap_or(true)
        });
        !fragments.is_empty()
    });

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::SourceActionState;
    use super::should_keep_source_action;

    #[test]
    fn test_prune_singleton_sources_to_local_executor() {
        assert!(should_keep_source_action(
            false,
            true,
            SourceActionState::Singleton,
            true
        ));
        assert!(!should_keep_source_action(
            false,
            true,
            SourceActionState::Singleton,
            false
        ));
        assert!(!should_keep_source_action(
            false,
            true,
            SourceActionState::Empty,
            true
        ));
    }

    #[test]
    fn test_keep_non_empty_source_actions_when_real_work_exists() {
        assert!(should_keep_source_action(
            true,
            false,
            SourceActionState::NonEmpty,
            false
        ));
        assert!(!should_keep_source_action(
            true,
            true,
            SourceActionState::Singleton,
            true
        ));
        assert!(!should_keep_source_action(
            true,
            true,
            SourceActionState::Empty,
            true
        ));
    }
}
