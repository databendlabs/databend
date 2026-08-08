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
use std::fmt::Debug;
use std::fmt::Formatter;
use std::ops::Deref;
use std::sync::Arc;

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::PerfConfig;
use databend_common_catalog::cluster_info::Cluster;
use databend_common_catalog::query_kind::QueryKind;
use databend_common_catalog::session_type::SessionType;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::UserInfo;
use databend_common_settings::Settings;
use databend_meta_client::types::NodeInfo;
use log::debug;
use log::warn;
use petgraph::Direction;
use petgraph::Graph;
use petgraph::dot::Dot;
use petgraph::graph::NodeIndex;
use petgraph::visit::EdgeRef;
use serde::Deserialize;
use serde::Serialize;

use crate::clusters::ClusterHelper;
use crate::clusters::FlightParams;
use crate::servers::flight::v1::actions::ABORT_QUERY_ENV;
use crate::servers::flight::v1::actions::INIT_QUERY_ENV;
use crate::servers::flight::v1::actions::PREPARE_QUERY_ENV;
use crate::sessions::QueryContext;
use crate::sessions::SessionManager;
use crate::sessions::TableContextCluster;
use crate::sessions::TableContextQueryIdentity;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Edge {
    Statistics,
    /// A remote exchange edge. Block mode multiplexes its channels over one logical stream;
    /// packet mode keeps one logical stream per channel for the existing DataPacket pipeline.
    ExchangeFragment {
        exchange_id: String,
        channels: Vec<String>,
        mode: ExchangeMode,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExchangeMode {
    /// Block exchanges route by tid and may batch multiple FlightData messages.
    Blocks,
    /// Merge and node-shuffle exchanges preserve their existing DataPacket stream.
    Packets,
}

#[derive(Serialize, Deserialize)]
pub struct DataflowDiagram {
    graph: Graph<Arc<NodeInfo>, Edge>,
}

impl Deref for DataflowDiagram {
    type Target = Graph<Arc<NodeInfo>, Edge>;

    fn deref(&self) -> &Self::Target {
        &self.graph
    }
}

impl Debug for DataflowDiagram {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", &Dot::new(&self.graph))
    }
}

pub struct DataflowDiagramBuilder {
    nodes: HashMap<String, NodeIndex>,
    graph: Graph<Arc<NodeInfo>, Edge>,
}

impl DataflowDiagramBuilder {
    pub fn create(nodes: Vec<Arc<NodeInfo>>) -> DataflowDiagramBuilder {
        let mut nodes_index = HashMap::with_capacity(nodes.len());
        let mut graph = Graph::with_capacity(nodes.len(), nodes.len() * 2);

        for node in nodes {
            let node_id = node.id.clone();
            let node_index = graph.add_node(node);
            nodes_index.insert(node_id, node_index);
        }

        DataflowDiagramBuilder {
            graph,
            nodes: nodes_index,
        }
    }

    pub fn add_exchange_edge(
        &mut self,
        source: &str,
        destination: &str,
        exchange_id: &str,
        channels: Vec<String>,
        mode: ExchangeMode,
    ) -> Result<()> {
        self.add_edge_inner(source, destination, Edge::ExchangeFragment {
            exchange_id: exchange_id.to_string(),
            channels,
            mode,
        })
    }

    fn add_edge_inner(&mut self, source: &str, destination: &str, edge: Edge) -> Result<()> {
        if source != destination {
            let source = self
                .nodes
                .get(source)
                .ok_or_else(|| ErrorCode::NotFoundClusterNode(format!("not found {}", source)))?;
            let destination = self.nodes.get(destination).ok_or_else(|| {
                ErrorCode::NotFoundClusterNode(format!("not found {}", destination))
            })?;

            self.graph.add_edge(*source, *destination, edge);
        }

        Ok(())
    }

    pub fn add_statistics_edge(&mut self, source: &str, destination: &str) -> Result<()> {
        if source != destination {
            // avoid local to local
            let source = self
                .nodes
                .get(source)
                .ok_or_else(|| ErrorCode::NotFoundClusterNode(""))?;
            let destination = self
                .nodes
                .get(destination)
                .ok_or_else(|| ErrorCode::NotFoundClusterNode(""))?;

            self.graph.add_edge(*source, *destination, Edge::Statistics);
        }

        Ok(())
    }

    pub fn build(self) -> DataflowDiagram {
        DataflowDiagram { graph: self.graph }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QueryEnv {
    pub query_id: String,
    pub exchange_session_id: String,
    pub cluster: Arc<Cluster>,
    pub settings: Arc<Settings>,
    pub query_kind: QueryKind,
    pub dataflow_diagram: Arc<DataflowDiagram>,
    pub request_server_id: String,
    pub workload_group: Option<String>,
    pub create_rpc_clint_with_current_rt: bool,
    #[serde(default)]
    pub perf_config: PerfConfig,
    pub user: UserInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryEnvAdmission {
    pub query_id: String,
    pub exchange_session_id: String,
    pub inbound_channels: HashMap<String, InboundChannelAdmission>,
    pub statistics_sources: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum InboundChannelAdmission {
    Blocks {
        num_threads: usize,
        source_ids: Vec<String>,
    },
    Packets {
        source_ids: Vec<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeSession {
    pub query_id: String,
    pub exchange_session_id: String,
}

struct QueryEnvAdmissionGuard {
    cluster: Arc<Cluster>,
    query_id: String,
    exchange_session_id: String,
    node_ids: Vec<String>,
    flight_params: FlightParams,
    armed: bool,
}

impl QueryEnvAdmissionGuard {
    fn create(
        cluster: Arc<Cluster>,
        query_id: String,
        exchange_session_id: String,
        node_ids: Vec<String>,
        flight_params: FlightParams,
    ) -> Self {
        Self {
            cluster,
            query_id,
            exchange_session_id,
            node_ids,
            flight_params,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for QueryEnvAdmissionGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }

        let cluster = self.cluster.clone();
        let query_id = self.query_id.clone();
        let exchange_session_id = self.exchange_session_id.clone();
        let node_ids = self.node_ids.clone();
        let flight_params = self.flight_params;

        GlobalIORuntime::instance().spawn(async move {
            for node_id in node_ids {
                let message = HashMap::from([(node_id.clone(), ExchangeSession {
                    query_id: query_id.clone(),
                    exchange_session_id: exchange_session_id.clone(),
                })]);
                if let Err(cause) = cluster
                    .do_action::<_, ()>(ABORT_QUERY_ENV, message, flight_params)
                    .await
                {
                    warn!(
                        "Failed to abort query env admission for {} on {}: {}",
                        query_id, node_id, cause
                    );
                }
            }
        });
    }
}

impl QueryEnv {
    fn admissions(&self) -> Result<HashMap<String, QueryEnvAdmission>> {
        let mut admissions = HashMap::with_capacity(self.dataflow_diagram.node_count());

        for index in self.dataflow_diagram.node_indices() {
            let mut inbound_channels: HashMap<String, InboundChannelAdmission> = HashMap::new();
            let mut statistics_sources = Vec::new();
            for edge in self
                .dataflow_diagram
                .edges_directed(index, Direction::Incoming)
            {
                let source_id = self.dataflow_diagram[edge.source()].id.clone();
                match edge.weight() {
                    Edge::Statistics => {
                        if !statistics_sources.contains(&source_id) {
                            statistics_sources.push(source_id);
                        }
                    }
                    Edge::ExchangeFragment {
                        exchange_id,
                        channels,
                        mode: ExchangeMode::Blocks,
                    } => match inbound_channels.entry(exchange_id.clone()) {
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            entry.insert(InboundChannelAdmission::Blocks {
                                num_threads: channels.len(),
                                source_ids: vec![source_id],
                            });
                        }
                        std::collections::hash_map::Entry::Occupied(mut entry) => {
                            let InboundChannelAdmission::Blocks {
                                num_threads,
                                source_ids,
                            } = entry.get_mut()
                            else {
                                return Err(ErrorCode::Internal(format!(
                                    "Conflicting do_exchange modes for exchange {}",
                                    exchange_id
                                )));
                            };
                            if *num_threads != channels.len() {
                                return Err(ErrorCode::Internal(format!(
                                    "Conflicting do_exchange admission for exchange {}: {} and {} channels",
                                    exchange_id,
                                    num_threads,
                                    channels.len()
                                )));
                            }
                            if !source_ids.contains(&source_id) {
                                source_ids.push(source_id);
                            }
                        }
                    },
                    Edge::ExchangeFragment {
                        channels,
                        mode: ExchangeMode::Packets,
                        ..
                    } => {
                        for channel_id in channels {
                            match inbound_channels.entry(channel_id.clone()) {
                                std::collections::hash_map::Entry::Vacant(entry) => {
                                    entry.insert(InboundChannelAdmission::Packets {
                                        source_ids: vec![source_id.clone()],
                                    });
                                }
                                std::collections::hash_map::Entry::Occupied(mut entry) => {
                                    let InboundChannelAdmission::Packets { source_ids } =
                                        entry.get_mut()
                                    else {
                                        return Err(ErrorCode::Internal(format!(
                                            "Conflicting do_exchange modes for channel {}",
                                            channel_id
                                        )));
                                    };
                                    if !source_ids.contains(&source_id) {
                                        source_ids.push(source_id.clone());
                                    }
                                }
                            }
                        }
                    }
                }
            }

            admissions.insert(self.dataflow_diagram[index].id.clone(), QueryEnvAdmission {
                query_id: self.query_id.clone(),
                exchange_session_id: self.exchange_session_id.clone(),
                inbound_channels,
                statistics_sources,
            });
        }

        Ok(admissions)
    }

    pub async fn init(&self, ctx: &Arc<QueryContext>, flight_params: FlightParams) -> Result<()> {
        debug!("Dataflow diagram {:?}", self.dataflow_diagram);

        let cluster = ctx.get_cluster();
        let admissions = self.admissions()?;
        let mut admission_guard = QueryEnvAdmissionGuard::create(
            cluster.clone(),
            self.query_id.clone(),
            self.exchange_session_id.clone(),
            admissions.keys().cloned().collect(),
            flight_params,
        );
        let _ = cluster
            .do_action::<_, ()>(PREPARE_QUERY_ENV, admissions, flight_params)
            .await?;

        let mut message = HashMap::with_capacity(self.dataflow_diagram.node_count());

        for node in self.dataflow_diagram.node_weights() {
            message.insert(node.id.clone(), self.clone());
        }

        let _ = cluster
            .do_action::<_, ()>(INIT_QUERY_ENV, message, flight_params)
            .await?;

        admission_guard.disarm();
        Ok(())
    }

    pub async fn create_query_ctx(&self) -> Result<Arc<QueryContext>> {
        let session_manager = SessionManager::instance();

        let session = session_manager.register_session(session_manager.create_with_settings(
            SessionType::FlightRPC,
            self.settings.clone(),
            Some(self.user.clone()),
        )?)?;

        if let Some(workload_group) = &self.workload_group {
            session.set_current_workload_group(workload_group.clone());
        }

        let query_ctx = session.create_query_context_with_cluster(
            Arc::new(Cluster {
                unassign: self.cluster.unassign,
                nodes: self.cluster.nodes.clone(),
                local_id: GlobalConfig::instance().query.node_id.clone(),
            }),
            GlobalConfig::version(),
        )?;

        query_ctx.update_init_query_id(self.query_id.clone());
        query_ctx.attach_query_str(self.query_kind, "".to_string());

        Ok(query_ctx)
    }
}
