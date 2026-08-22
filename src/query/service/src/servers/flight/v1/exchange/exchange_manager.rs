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

use std::cell::SyncUnsafeCell;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use arrow_flight::flight_service_client::FlightServiceClient;
use databend_common_base::JoinHandle;
use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::ExecutorStatsSnapshot;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::QueryPerf;
use databend_common_base::runtime::spawn_blocking;
use databend_common_cache::Cache;
use databend_common_cache::LruCache;
use databend_common_cache::MemSized;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_grpc::ConnectionFactory;
use databend_common_pipeline::core::ExecutionInfo;
use databend_common_pipeline::core::always_callback;
use databend_common_pipeline::core::basic_callback;
use databend_common_settings::FlightKeepAliveParams;
use fastrace::prelude::*;
use log::warn;
use parking_lot::Mutex;
use parking_lot::ReentrantMutex;
use petgraph::Direction;
use petgraph::prelude::EdgeRef;
use tokio::sync::oneshot;

use super::exchange_params::BroadcastExchangeParams;
use super::exchange_params::ExchangeParams;
use super::exchange_params::GlobalExchangeParams;
use super::exchange_params::MergeExchangeParams;
use super::exchange_params::ShuffleExchangeParams;
use super::exchange_sink::ExchangeSink;
use super::exchange_transform::ExchangeTransform;
use super::statistics_receiver::StatisticsReceiver;
use super::statistics_sender::StatisticsSender;
use crate::clusters::ClusterHelper;
use crate::clusters::FlightParams;
use crate::physical_plans::PhysicalPlan;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::PipelineBuilder;
use crate::pipelines::attach_runtime_filter_logger;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::executor::PlanNodeMemoryUsage;
use crate::schedulers::QueryFragmentsActions;
use crate::servers::flight::DoExchangeParams;
use crate::servers::flight::DoExchangeStream;
use crate::servers::flight::FlightClient;
use crate::servers::flight::FlightClientInfo;
use crate::servers::flight::FlightOperation;
use crate::servers::flight::FlightReceiver;
use crate::servers::flight::FlightSender;
use crate::servers::flight::keep_alive::build_keep_alive_config;
use crate::servers::flight::v1::actions::INIT_QUERY_FRAGMENTS;
use crate::servers::flight::v1::actions::START_PREPARED_QUERY;
use crate::servers::flight::v1::actions::init_query_fragments;
use crate::servers::flight::v1::exchange::DataExchange;
use crate::servers::flight::v1::exchange::DefaultExchangeInjector;
use crate::servers::flight::v1::exchange::ExchangeInjector;
use crate::servers::flight::v1::network::DoExchangeConnector;
use crate::servers::flight::v1::network::DoExchangeTransport;
use crate::servers::flight::v1::network::FlightReconnectPolicy;
use crate::servers::flight::v1::network::NetworkInboundAttachment;
use crate::servers::flight::v1::network::NetworkInboundChannelSet;
use crate::servers::flight::v1::network::NetworkInboundSource;
use crate::servers::flight::v1::network::PendingNetworkOutbound;
use crate::servers::flight::v1::packets::Edge;
use crate::servers::flight::v1::packets::ExchangeMode;
use crate::servers::flight::v1::packets::InboundChannelAdmission;
use crate::servers::flight::v1::packets::QueryEnv;
use crate::servers::flight::v1::packets::QueryEnvAdmission;
use crate::servers::flight::v1::packets::QueryFragment;
use crate::servers::flight::v1::packets::QueryFragments;
use crate::sessions::QueryContext;
use crate::sessions::TableContextCluster;
use crate::sessions::TableContextPerf;
use crate::sessions::TableContextQueryIdentity;
use crate::sessions::TableContextSettings;

enum PreparedOutbound {
    Packets {
        channel: String,
        sender: FlightSender,
    },
    Statistics {
        target: String,
        sender: FlightSender,
    },
    Blocks {
        exchange_id: String,
        target_id: String,
        outbound: PendingNetworkOutbound,
    },
}

const CLOSED_EXCHANGE_SESSION_CACHE_CAPACITY: usize = 100_000;

#[derive(Clone)]
enum ClosedExchangeSession {
    Completed,
    Failed(ErrorCode),
}

impl MemSized for ClosedExchangeSession {
    fn mem_bytes(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

fn create_do_exchange_connector(
    target_id: String,
    address: String,
    use_current_rt: bool,
    keep_alive: FlightKeepAliveParams,
    params: DoExchangeParams,
) -> DoExchangeConnector {
    Arc::new(move || {
        let target_id = target_id.clone();
        let address = address.clone();
        let params = params.clone();
        Box::pin(async move {
            let mut flight_client =
                create_flight_client(target_id, address, use_current_rt, keep_alive).await?;
            let (send_tx, send_rx) = async_channel::bounded(1);
            let response_stream = flight_client.do_exchange(send_rx, params).await?;
            Ok(DoExchangeTransport {
                send_tx,
                response_stream: Box::pin(response_stream),
            })
        })
    })
}

async fn open_do_exchange_outbound(
    target_id: String,
    address: String,
    use_current_rt: bool,
    keep_alive: FlightKeepAliveParams,
    params: DoExchangeParams,
    num_threads: usize,
    reconnect: FlightReconnectPolicy,
) -> Result<PendingNetworkOutbound> {
    let info = FlightClientInfo::new(params.source_id.clone(), target_id.clone());
    let connector =
        create_do_exchange_connector(target_id, address, use_current_rt, keep_alive, params);
    let transport = connector().await.map_err(|cause| {
        warn!(
            "do_exchange sender initial connection failed: client={}, service={}, error={}",
            info.local_node_id, info.remote_node_id, cause
        );
        cause
    })?;
    Ok(PendingNetworkOutbound::create(
        num_threads,
        transport,
        connector,
        reconnect,
        info,
    ))
}

async fn create_flight_client(
    remote_node_id: String,
    address: String,
    use_current_rt: bool,
    keep_alive: FlightKeepAliveParams,
) -> Result<FlightClient> {
    let config = GlobalConfig::instance();
    let info = FlightClientInfo::new(config.query.node_id.clone(), remote_node_id);
    let keep_alive_config = build_keep_alive_config(keep_alive);
    let task = async move {
        let channel = match config.tls_query_cli_enabled() {
            true => {
                ConnectionFactory::create_rpc_channel(
                    address.to_owned(),
                    None,
                    Some(config.query.to_grpc_tls_config()),
                    keep_alive_config,
                )
                .await
            }
            false => {
                ConnectionFactory::create_rpc_channel(
                    address.to_owned(),
                    None,
                    None,
                    keep_alive_config,
                )
                .await
            }
        }
        .map_err(|error| {
            info.add_error_context(ErrorCode::from(error), FlightOperation::Connect)
        })?;

        Ok(FlightClient::with_info(
            FlightServiceClient::new(channel),
            info,
        ))
    };
    if use_current_rt {
        task.await
    } else {
        GlobalIORuntime::instance()
            .spawn(task)
            .await
            .expect("create client future must be joined successfully")
    }
}

pub struct DataExchangeManager {
    // A query has at most one active exchange coordinator, so query_id is the lookup key used by
    // query-scoped APIs. query_id may be supplied by a client and reused after an execution leaves
    // the active map, so it does not uniquely identify one distributed execution. The separately
    // generated exchange_session_id is therefore a fencing token: after lookup, lifecycle-specific
    // RPCs must validate it so a delayed cleanup or reconnect from an old execution cannot attach
    // to or remove the current coordinator (the ABA case).
    //
    // TODO: Introduce a cluster-wide unique query_execution_id and use it as the coordinator and
    // terminal-state cache key. Once all exchange RPCs and callbacks carry that ID,
    // exchange_session_id and this secondary validation can be removed.
    queries_coordinator: ReentrantMutex<SyncUnsafeCell<HashMap<String, QueryCoordinator>>>,
    // Retaining terminal fencing tokens also prevents a delayed admission or reconnect from
    // resurrecting an exchange after its coordinator has been removed from the active map.
    closed_exchange_sessions: Mutex<LruCache<String, ClosedExchangeSession>>,
}

impl DataExchangeManager {
    pub fn init() -> Result<()> {
        GlobalInstance::set(Arc::new(DataExchangeManager {
            queries_coordinator: ReentrantMutex::new(SyncUnsafeCell::new(HashMap::new())),
            closed_exchange_sessions: Mutex::new(LruCache::with_items_capacity(
                CLOSED_EXCHANGE_SESSION_CACHE_CAPACITY,
            )),
        }));

        Ok(())
    }

    pub fn instance() -> Arc<DataExchangeManager> {
        GlobalInstance::get()
    }

    pub fn get_running_query_graph_dump(&self, query_id: &str) -> Result<String> {
        let running_executor = {
            let queries_coordinator_guard = self.queries_coordinator.lock();
            let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };
            let Some(coordinator) = queries_coordinator.get(query_id) else {
                return Ok(format!("Unknown query {}", query_id));
            };

            let Some(info) = &coordinator.info else {
                return Ok(format!("Unknown running query {}", query_id));
            };

            info.query_executor.clone()
        };

        Ok(match running_executor {
            None => format!("Unknown running query {}", query_id),
            Some(executor) => executor.get_inner().format_graph_nodes(),
        })
    }

    pub fn get_query_execution_stats(&self) -> Vec<(String, ExecutorStatsSnapshot)> {
        let mut executors = Vec::new();
        {
            let queries_coordinator_guard = self.queries_coordinator.lock();
            let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };
            for (query_id, query_coordinator) in queries_coordinator.iter() {
                if let Some(info) = &query_coordinator.info {
                    if let Some(executor) = info.query_executor.clone() {
                        executors.push((query_id, executor));
                    }
                }
            }
        }
        executors
            .into_iter()
            .map(|(query_id, executor)| {
                (
                    query_id.clone(),
                    executor.get_inner().get_query_execution_stats(),
                )
            })
            .collect()
    }

    pub fn get_queries_top_memory_plan_nodes(
        &self,
        limit: usize,
    ) -> Vec<(String, Vec<PlanNodeMemoryUsage>)> {
        let mut executors = Vec::new();
        {
            let queries_coordinator_guard = self.queries_coordinator.lock();
            let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };
            for (query_id, query_coordinator) in queries_coordinator.iter() {
                if let Some(info) = &query_coordinator.info {
                    if let Some(executor) = info.query_executor.clone() {
                        executors.push((query_id.clone(), executor));
                    }
                }
            }
        }

        executors
            .into_iter()
            .filter_map(|(query_id, executor)| {
                let plan_nodes = executor.get_inner().top_memory_plan_nodes(limit);
                if plan_nodes.is_empty() {
                    None
                } else {
                    Some((query_id, plan_nodes))
                }
            })
            .collect()
    }

    pub fn get_query_ctx(
        &self,
        query_id: &str,
        exchange_session_id: &str,
    ) -> Result<Arc<QueryContext>> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        if let Some(coordinator) = queries_coordinator.get_mut(query_id) {
            coordinator.validate_exchange_session(exchange_session_id)?;
            if let Some(coordinator) = &coordinator.info {
                return Ok(coordinator.query_ctx.clone());
            }
        }

        Err(ErrorCode::Internal(format!(
            "Query {} not found in cluster.",
            query_id
        )))
    }

    pub fn admit_query_env(&self, admission: &QueryEnvAdmission) -> Result<()> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };
        let reconnect_lease = Duration::from_secs(admission.reconnect_lease_secs);

        if self
            .closed_exchange_sessions
            .lock()
            .contains(&admission.exchange_session_id)
        {
            return Err(ErrorCode::ClosedQuery(format!(
                "Exchange session {} is closed",
                admission.exchange_session_id
            )));
        }

        let query_coordinator = match queries_coordinator.entry(admission.query_id.clone()) {
            Entry::Occupied(entry) => {
                let coordinator = entry.into_mut();
                coordinator.validate_exchange_session(&admission.exchange_session_id)?;
                coordinator
            }
            Entry::Vacant(entry) => entry.insert(QueryCoordinator::create(
                admission.query_id.clone(),
                admission.exchange_session_id.clone(),
                reconnect_lease,
            )),
        };

        for (channel_id, channel) in &admission.inbound_channels {
            match channel {
                InboundChannelAdmission::Blocks {
                    num_threads,
                    source_ids,
                } => query_coordinator.admit_block_sources(channel_id, *num_threads, source_ids)?,
                InboundChannelAdmission::Packets { source_ids } => {
                    query_coordinator.admit_packet_sources(channel_id, source_ids)?
                }
            }
        }
        query_coordinator.admit_statistics_sources(&admission.statistics_sources)?;

        Ok(())
    }

    pub fn abort_query_env(&self, query_id: &str, exchange_session_id: &str) {
        self.closed_exchange_sessions.lock().insert(
            exchange_session_id.to_string(),
            ClosedExchangeSession::Failed(ErrorCode::AbortedQuery(format!(
                "Exchange session {} was aborted during initialization",
                exchange_session_id
            ))),
        );
        let query_coordinator = {
            let queries_coordinator_guard = self.queries_coordinator.lock();
            let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

            match queries_coordinator.get(query_id) {
                Some(query_coordinator)
                    if query_coordinator.exchange_session_id == exchange_session_id =>
                {
                    let query_coordinator = queries_coordinator.remove(query_id);
                    query_coordinator
                }
                _ => None,
            }
        };

        if let Some(mut query_coordinator) = query_coordinator {
            query_coordinator.shutdown_query(None);
            query_coordinator.on_finished();
        }
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn init_query_env(
        &self,
        env: &QueryEnv,
        ctx: Option<Arc<QueryContext>>,
    ) -> Result<()> {
        if env.perf_config.is_perf_active() {
            if let Some(ctx) = ctx.as_ref() {
                let mut perf_config = env.perf_config.clone();
                // The coordinator already starts the profiler manually in ExplainPerfInterpreter.
                // Suppress it here to avoid starting a second profiler on the same process.
                if GlobalConfig::instance().query.node_id == env.request_server_id {
                    perf_config.profiler_enabled = false;
                }
                ctx.set_perf_config(perf_config);
            }
        }

        let config = GlobalConfig::instance();
        let with_cur_rt = env.create_rpc_clint_with_current_rt;
        let settings = match ctx {
            Some(ref ctx) => ctx.get_settings(),
            None => env.settings.clone(),
        };
        let keep_alive = settings.get_flight_keep_alive_params()?;
        let reconnect = FlightReconnectPolicy::from_settings(&settings)?;

        let mut statistics_senders = HashMap::new();
        let mut packet_senders = HashMap::<String, Vec<FlightSender>>::new();

        for index in env.dataflow_diagram.node_indices() {
            if env.dataflow_diagram[index].id == config.query.node_id {
                let mut outbound_futures: Vec<
                    std::pin::Pin<
                        Box<dyn std::future::Future<Output = Result<PreparedOutbound>> + Send>,
                    >,
                > = vec![];

                // Every producer opens do_exchange to its consumer. Admission has already
                // created the logical receiver, so these connections can only attach or retry.
                let outgoing_edges = env
                    .dataflow_diagram
                    .edges_directed(index, Direction::Outgoing);

                for edge in outgoing_edges {
                    let target = env.dataflow_diagram[edge.target()].clone();
                    let edge = edge.weight().clone();

                    let target_id = target.id.clone();
                    let local_node_id = config.query.node_id.clone();
                    let query_id = env.query_id.clone();
                    let exchange_session_id = env.exchange_session_id.clone();

                    match edge {
                        Edge::Statistics => {
                            let address = target.flight_address.clone();
                            let exchange_target = target_id.clone();
                            outbound_futures.push(Box::pin(async move {
                                let params = DoExchangeParams {
                                    query_id,
                                    exchange_session_id,
                                    source_id: local_node_id,
                                    stream: DoExchangeStream::Statistics,
                                };
                                let outbound = open_do_exchange_outbound(
                                    target_id,
                                    address,
                                    with_cur_rt,
                                    keep_alive,
                                    params,
                                    1,
                                    reconnect,
                                )
                                .await?;
                                Ok::<PreparedOutbound, ErrorCode>(PreparedOutbound::Statistics {
                                    target: exchange_target,
                                    sender: FlightSender::from_pending_outbound(
                                        outbound,
                                        &GlobalIORuntime::instance(),
                                    ),
                                })
                            }));
                        }
                        Edge::ExchangeFragment {
                            exchange_id,
                            channels,
                            mode: ExchangeMode::Blocks,
                        } => {
                            let address = target.flight_address.clone();
                            let num_threads = channels.len();
                            warn!(
                                "do_exchange: node={} -> target={}, exchange_id={}, num_threads={}",
                                config.query.node_id, target_id, exchange_id, num_threads
                            );
                            outbound_futures.push(Box::pin(async move {
                                let params = DoExchangeParams {
                                    query_id,
                                    exchange_session_id,
                                    source_id: local_node_id,
                                    stream: DoExchangeStream::Blocks {
                                        exchange_id: exchange_id.clone(),
                                    },
                                };
                                Ok::<PreparedOutbound, ErrorCode>(PreparedOutbound::Blocks {
                                    target_id: target_id.clone(),
                                    exchange_id,
                                    outbound: open_do_exchange_outbound(
                                        target_id,
                                        address,
                                        with_cur_rt,
                                        keep_alive,
                                        params,
                                        num_threads,
                                        reconnect,
                                    )
                                    .await?,
                                })
                            }));
                        }
                        Edge::ExchangeFragment {
                            channels,
                            mode: ExchangeMode::Packets,
                            ..
                        } => {
                            for channel in channels {
                                let target_id = target_id.clone();
                                let address = target.flight_address.clone();
                                let query_id = query_id.clone();
                                let exchange_session_id = exchange_session_id.clone();
                                let local_node_id = local_node_id.clone();
                                outbound_futures.push(Box::pin(async move {
                                    let params = DoExchangeParams {
                                        query_id,
                                        exchange_session_id,
                                        source_id: local_node_id,
                                        stream: DoExchangeStream::Packets {
                                            channel_id: channel.clone(),
                                        },
                                    };
                                    let outbound = open_do_exchange_outbound(
                                        target_id,
                                        address,
                                        with_cur_rt,
                                        keep_alive,
                                        params,
                                        1,
                                        reconnect,
                                    )
                                    .await?;
                                    Ok::<PreparedOutbound, ErrorCode>(PreparedOutbound::Packets {
                                        channel,
                                        sender: FlightSender::from_pending_outbound(
                                            outbound,
                                            &GlobalIORuntime::instance(),
                                        ),
                                    })
                                }));
                            }
                        }
                    }
                }

                let prepared_outbounds = futures::future::try_join_all(outbound_futures).await?;

                let mut block_outbounds =
                    HashMap::<String, HashMap<String, PendingNetworkOutbound>>::new();

                for prepared_outbound in prepared_outbounds {
                    match prepared_outbound {
                        PreparedOutbound::Packets { channel, sender } => {
                            match packet_senders.entry(channel) {
                                Entry::Occupied(mut v) => v.get_mut().push(sender),
                                Entry::Vacant(v) => {
                                    v.insert(vec![sender]);
                                }
                            }
                        }
                        PreparedOutbound::Statistics { target, sender } => {
                            statistics_senders.insert(target, sender);
                        }
                        PreparedOutbound::Blocks {
                            exchange_id,
                            outbound,
                            target_id,
                        } => {
                            match block_outbounds.entry(exchange_id) {
                                Entry::Occupied(mut v) => {
                                    v.get_mut().insert(target_id, outbound);
                                }
                                Entry::Vacant(v) => {
                                    v.insert(HashMap::from([(target_id, outbound)]));
                                }
                            };
                        }
                    };
                }

                let mut query_info = Self::create_info(ctx)?;

                if let Some(query_info) = query_info.as_mut() {
                    let query_id = env.query_id.clone();
                    let exchange_session_id = env.exchange_session_id.clone();
                    query_info.remove_leak_query_worker =
                        Some(GlobalIORuntime::instance().spawn(async move {
                            let _ = tokio::time::sleep(Duration::from_secs(180)).await;
                            DataExchangeManager::instance()
                                .remove_if_leak_query(query_id, exchange_session_id);
                        }));
                }

                let queries_coordinator_guard = self.queries_coordinator.lock();
                let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

                let query_coordinator =
                    queries_coordinator.get_mut(&env.query_id).ok_or_else(|| {
                        ErrorCode::ClosedQuery(format!(
                            "Query {} was closed during init",
                            env.query_id
                        ))
                    })?;
                query_coordinator.validate_exchange_session(&env.exchange_session_id)?;
                query_coordinator.install_outbound_state(
                    query_info,
                    GlobalConfig::instance().query.node_id == env.request_server_id,
                    packet_senders,
                    block_outbounds,
                    statistics_senders,
                );

                return Ok(());
            }
        }

        // do nothing
        Ok(())
    }

    fn remove_if_leak_query(&self, query_id: String, exchange_session_id: String) {
        let is_leaked = {
            let queries_coordinator_guard = self.queries_coordinator.lock();
            let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

            match queries_coordinator.get(&query_id) {
                Some(may_leak_query)
                    if may_leak_query.exchange_session_id == exchange_session_id =>
                {
                    may_leak_query
                        .info
                        .as_ref()
                        .is_some_and(|info| !info.started.load(Ordering::SeqCst))
                }
                _ => false,
            }
        };

        if is_leaked {
            warn!(
                "Query {} cannot start command while in 180 seconds",
                query_id
            );
            self.on_finished_exchange(
                &query_id,
                &exchange_session_id,
                Some(ErrorCode::Internal(format!(
                    "Query {} cannot start command while in 180 seconds",
                    query_id
                ))),
            );
        }
    }

    fn create_info(query_ctx: Option<Arc<QueryContext>>) -> Result<Option<QueryInfo>> {
        match query_ctx {
            None => Ok(None),
            Some(query_ctx) => {
                let query_id = query_ctx.get_id();

                Ok(Some(QueryInfo {
                    query_ctx,
                    query_executor: None,
                    query_id: query_id.clone(),
                    started: AtomicBool::new(false),
                    current_executor: GlobalConfig::instance().query.node_id.clone(),
                    remove_leak_query_worker: None,
                }))
            }
        }
    }

    pub fn set_ctx(
        &self,
        query_id: &str,
        exchange_session_id: &str,
        ctx: Arc<QueryContext>,
    ) -> Result<()> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };
        match queries_coordinator.get_mut(query_id) {
            None => Err(ErrorCode::Internal(format!(
                "Query {} not found in cluster.",
                query_id
            ))),
            Some(coordinator) => {
                coordinator.validate_exchange_session(exchange_session_id)?;
                if let Some(info) = coordinator.info.as_mut() {
                    info.query_ctx = ctx;
                    return Ok(());
                }

                coordinator.info = Self::create_info(Some(ctx))?;
                Ok(())
            }
        }
    }

    // Execute query in background
    #[fastrace::trace]
    pub fn execute_partial_query(&self, query_id: &str, exchange_session_id: &str) -> Result<()> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.get_mut(query_id) {
            None => Err(ErrorCode::Internal(format!(
                "Query {} not found in cluster.",
                query_id
            ))),
            Some(coordinator) => {
                coordinator.validate_exchange_session(exchange_session_id)?;
                coordinator.execute_pipeline()
            }
        }
    }

    // Create a pipeline based on query plan
    #[fastrace::trace]
    pub fn init_query_fragments_plan(&self, fragments: &QueryFragments) -> Result<()> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.get_mut(&fragments.query_id) {
            None => Err(ErrorCode::Internal(format!(
                "Query {} not found in cluster.",
                fragments.query_id
            ))),
            Some(query_coordinator) => {
                query_coordinator.validate_exchange_session(&fragments.exchange_session_id)?;
                query_coordinator.prepare_pipeline(fragments)
            }
        }
    }

    /// Handle a do_exchange request from a remote node.
    ///
    /// Resolves the admitted logical source shared by all reconnects of this stream.
    #[fastrace::trace]
    pub fn handle_do_exchange(
        &self,
        query_id: &str,
        exchange_session_id: &str,
        source_id: &str,
        stream: &DoExchangeStream,
    ) -> Result<Option<NetworkInboundAttachment>> {
        let result = (|| {
            let queries_coordinator_guard = self.queries_coordinator.lock();
            let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

            let Some(query_coordinator) = queries_coordinator.get(query_id) else {
                return match self
                    .closed_exchange_sessions
                    .lock()
                    .get(exchange_session_id)
                    .cloned()
                {
                    Some(ClosedExchangeSession::Completed) => Ok(None),
                    Some(ClosedExchangeSession::Failed(cause)) => Err(cause),
                    None => Err(ErrorCode::ClosedQuery(format!(
                        "Cannot attach do_exchange to closed query {}",
                        query_id
                    ))),
                };
            };
            query_coordinator.validate_exchange_session(exchange_session_id)?;
            let source = query_coordinator.get_inbound_source(stream, source_id)?;
            source.attach(
                GlobalIORuntime::instance(),
                ErrorCode::CannotConnectNode(format!(
                    "do_exchange source {} for stream {:?} did not reconnect before its lease expired",
                    source_id, stream
                )),
            )
        })();

        if let Err(cause) = &result {
            warn!(
                "do_exchange receiver rejected connection: query_id={}, exchange_session_id={}, source_id={}, stream={:?}, error={}",
                query_id, exchange_session_id, source_id, stream, cause
            );
        }
        result
    }

    /// Get the NetworkInboundReceivers for a given query and channel.
    ///
    /// Returns one `Arc<NetworkInboundReceiver>` per tid, for building
    /// `ThreadChannelReader` processors in the pipeline.
    pub fn get_exchange_channel_set(
        &self,
        query_id: &str,
        channel_id: &str,
    ) -> Result<Arc<NetworkInboundChannelSet>> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.get(query_id) {
            None => Err(ErrorCode::Internal(format!(
                "Query {} not found in cluster.",
                query_id
            ))),
            Some(coordinator) => match coordinator.inbound_channel_sets.get(channel_id) {
                None => Err(ErrorCode::Internal(format!(
                    "NetworkInboundChannelSet not found for channel {}",
                    channel_id
                ))),
                Some(channel_set) => Ok(channel_set.clone()),
            },
        }
    }

    /// Return the inbound channels for a ping-pong exchange, creating them when
    /// the exchange has only local edges and therefore no do_exchange request.
    pub fn get_or_create_exchange_channel_set(
        &self,
        query_id: &str,
        channel_id: &str,
        num_threads: usize,
    ) -> Result<Arc<NetworkInboundChannelSet>> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.get_mut(query_id) {
            None => Err(ErrorCode::Internal(format!(
                "Query {} not found in cluster.",
                query_id
            ))),
            Some(coordinator) => {
                coordinator.get_or_create_inbound_channel_set(channel_id, num_threads)
            }
        }
    }

    /// Takes the block-stream outbounds prepared for one exchange channel.
    pub(crate) fn take_block_outbounds(
        &self,
        query_id: &str,
        channel_id: &str,
    ) -> Result<HashMap<String, PendingNetworkOutbound>> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.get_mut(query_id) {
            None => Err(ErrorCode::Internal(format!(
                "Query {} not found in cluster.",
                query_id
            ))),
            Some(coordinator) => Ok(coordinator
                .block_outbounds
                .remove(channel_id)
                .unwrap_or_default()),
        }
    }

    pub fn shutdown_query(&self, query_id: &str, cause: Option<ErrorCode>) {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        if let Some(query_coordinator) = queries_coordinator.get_mut(query_id) {
            query_coordinator.shutdown_query(cause);
        }
    }

    pub fn shutdown_exchange(
        &self,
        query_id: &str,
        exchange_session_id: &str,
        cause: Option<ErrorCode>,
    ) {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        if let Some(query_coordinator) = queries_coordinator.get_mut(query_id) {
            if query_coordinator
                .validate_exchange_session(exchange_session_id)
                .is_ok()
            {
                query_coordinator.shutdown_query(cause);
            }
        }
    }

    #[fastrace::trace]
    pub fn on_finished_query(&self, query_id: &str, cause: Option<ErrorCode>) {
        self.finish_query(query_id, None, cause);
    }

    #[fastrace::trace]
    pub fn on_finished_exchange(
        &self,
        query_id: &str,
        exchange_session_id: &str,
        cause: Option<ErrorCode>,
    ) {
        self.finish_query(query_id, Some(exchange_session_id), cause);
    }

    fn finish_query(
        &self,
        query_id: &str,
        expected_exchange_session_id: Option<&str>,
        cause: Option<ErrorCode>,
    ) {
        let lock_start = Instant::now();
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let lock_wait = lock_start.elapsed();
        if lock_wait > Duration::from_secs(1) {
            warn!(
                "Waited {:?} to acquire queries_coordinator lock in on_finished_query, query_id={}",
                lock_wait, query_id
            );
        }

        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        let matches_session = queries_coordinator
            .get(query_id)
            .is_some_and(|coordinator| {
                expected_exchange_session_id
                    .is_none_or(|expected| coordinator.exchange_session_id == expected)
            });

        if matches_session {
            let mut query_coordinator = queries_coordinator
                .remove(query_id)
                .expect("query coordinator must exist after session check");
            let terminal = match &cause {
                None => ClosedExchangeSession::Completed,
                Some(cause) => ClosedExchangeSession::Failed(cause.clone()),
            };
            self.closed_exchange_sessions
                .lock()
                .insert(query_coordinator.exchange_session_id.clone(), terminal);

            // Drop mutex guard to avoid deadlock during shutdown,
            drop(queries_coordinator_guard);

            query_coordinator.shutdown_query(cause);
            query_coordinator.on_finished();
        }
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn commit_actions(
        &self,
        ctx: Arc<QueryContext>,
        actions: QueryFragmentsActions,
    ) -> Result<PipelineBuildResult> {
        let settings = ctx.get_settings();
        let flight_params = FlightParams {
            timeout: settings.get_flight_client_timeout()?,
            retry_times: settings.get_flight_max_retry_times()?,
            retry_interval: settings.get_flight_retry_interval()?,
            keep_alive: settings.get_flight_keep_alive_params()?,
        };
        let mut root_fragment_ids = actions.get_root_fragment_ids()?;
        let exchange_session_id = actions.get_exchange_session_id().to_string();
        let conf = GlobalConfig::instance();

        // Initialize query env between cluster nodes
        let query_env = actions.get_query_env()?;
        query_env.init(&ctx, flight_params).await?;

        // Submit distributed tasks to all nodes.
        let cluster = ctx.get_cluster();
        let mut query_fragments = actions.get_query_fragments()?;

        let local_fragments = query_fragments.remove(&conf.query.node_id);

        let _: HashMap<String, ()> = cluster
            .do_action(INIT_QUERY_FRAGMENTS, query_fragments, flight_params)
            .await?;

        self.set_ctx(&ctx.get_id(), &exchange_session_id, ctx.clone())?;
        if let Some(query_fragments) = local_fragments {
            init_query_fragments(query_fragments).await?;
        }

        // Get local pipeline of local task
        let main_fragment_id = root_fragment_ids.pop().unwrap();
        let build_res = self.get_root_pipeline(
            ctx,
            &exchange_session_id,
            main_fragment_id,
            root_fragment_ids,
        )?;

        let prepared_query = actions.prepared_query()?;
        let _: HashMap<String, ()> = cluster
            .do_action(START_PREPARED_QUERY, prepared_query, flight_params)
            .await?;

        Ok(build_res)
    }

    fn get_root_pipeline(
        &self,
        ctx: Arc<QueryContext>,
        exchange_session_id: &str,
        main_fragment_id: usize,
        fragment_ids: Vec<usize>,
    ) -> Result<PipelineBuildResult> {
        let query_id = ctx.get_id();

        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.get_mut(&query_id) {
            None => Err(ErrorCode::Internal("Query not exists.")),
            Some(query_coordinator) => {
                query_coordinator.validate_exchange_session(exchange_session_id)?;
                let exchange_session_id = query_coordinator.exchange_session_id.clone();
                if !query_coordinator.packet_senders.is_empty() {
                    unreachable!(
                        "query_coordinator.packet_senders is not empty: {:?}",
                        query_coordinator.packet_senders.keys().collect::<Vec<_>>()
                    );
                }

                if !query_coordinator.packet_receivers.is_empty() {
                    unreachable!(
                        "query_coordinator.packet_receivers is not empty: {:?}",
                        query_coordinator
                            .packet_receivers
                            .keys()
                            .collect::<Vec<_>>()
                    );
                }

                let injector = DefaultExchangeInjector::create();
                let mut build_res = query_coordinator.subscribe_fragment(
                    &ctx,
                    main_fragment_id,
                    injector.clone(),
                )?;

                for fragment_id in fragment_ids {
                    let sub_build_res = query_coordinator.subscribe_fragment(
                        &ctx,
                        fragment_id,
                        injector.clone(),
                    )?;
                    build_res
                        .sources_pipelines
                        .push(sub_build_res.main_pipeline);
                    build_res
                        .sources_pipelines
                        .extend(sub_build_res.sources_pipelines);
                }

                let receivers = std::mem::take(&mut query_coordinator.statistics_receivers);
                let statistics_receiver = StatisticsReceiver::spawn_receiver(&ctx, receivers)?;

                let statistics_receiver: Mutex<StatisticsReceiver> =
                    Mutex::new(statistics_receiver);

                // Interrupting the execution of finished callback if network error
                build_res.main_pipeline.set_on_finished(basic_callback(
                    move |info: &ExecutionInfo| {
                        let query_id = ctx.get_id();
                        let mut statistics_receiver = statistics_receiver.lock();

                        statistics_receiver.shutdown(info.res.is_err());
                        // Keep the exchange session admitted while normal statistics are
                        // drained: a physical stream may still need to reconnect and send EOS.
                        let shutdown_result = statistics_receiver.wait_shutdown();
                        ctx.get_exchange_manager().on_finished_exchange(
                            &query_id,
                            &exchange_session_id,
                            info.res.clone().err(),
                        );
                        shutdown_result
                    },
                ));

                // Return if it‘s an error returned by another query node
                build_res
                    .main_pipeline
                    .set_on_finished(move |info: &ExecutionInfo| match &info.res {
                        Ok(_) => Ok(()),
                        Err(error_code) => Err(error_code.clone()),
                    });

                Ok(build_res)
            }
        }
    }

    pub fn get_flight_sender(
        &self,
        params: &ExchangeParams,
    ) -> Result<Vec<(String, FlightSender)>> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.get_mut(&params.get_query_id()) {
            None => Err(ErrorCode::Internal("Query not exists.")),
            Some(coordinator) => params.take_flight_sender(&mut coordinator.packet_senders),
        }
    }

    pub fn get_flight_receiver(&self, params: &ExchangeParams) -> Result<Vec<FlightReceiver>> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.get_mut(&params.get_query_id()) {
            None => Err(ErrorCode::Internal("Query not exists.")),
            Some(coordinator) => params.take_flight_receiver(&mut coordinator.packet_receivers),
        }
    }

    pub fn get_fragment_source(
        &self,
        query_id: &str,
        fragment_id: usize,
        injector: Arc<dyn ExchangeInjector>,
    ) -> Result<PipelineBuildResult> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.get_mut(query_id) {
            None => Err(ErrorCode::Internal("Query not exists.")),
            Some(query_coordinator) => {
                let query_ctx = query_coordinator
                    .info
                    .as_ref()
                    .expect("QueryInfo is none")
                    .query_ctx
                    .clone();

                query_coordinator.subscribe_fragment(&query_ctx, fragment_id, injector)
            }
        }
    }
}

struct QueryInfo {
    query_id: String,
    started: AtomicBool,
    current_executor: String,
    query_ctx: Arc<QueryContext>,
    remove_leak_query_worker: Option<JoinHandle<()>>,
    query_executor: Option<Arc<PipelineCompleteExecutor>>,
}

/// One admitted logical source. Every physical reconnect resolves to the same key.
#[derive(Eq, Hash, PartialEq)]
struct InboundSourceKey {
    stream: DoExchangeStream,
    source_id: String,
}

impl InboundSourceKey {
    fn new(stream: DoExchangeStream, source_id: &str) -> Self {
        Self {
            stream,
            source_id: source_id.to_string(),
        }
    }
}

pub(crate) struct QueryCoordinator {
    query_id: String,
    exchange_session_id: String,
    reconnect_lease: Duration,
    info: Option<QueryInfo>,
    fragments_coordinator: HashMap<usize, Box<FragmentCoordinator>>,
    /// True when this node is the request server (coordinator) for the query.
    /// The coordinator starts the profiler manually in ExplainPerfInterpreter,
    /// so execute_pipeline() must not start a second one.
    is_request_server: bool,

    statistics_senders: HashMap<String, FlightSender>,
    statistics_receivers: HashMap<String, FlightReceiver>,
    packet_senders: HashMap<String, Vec<FlightSender>>,
    packet_receivers: HashMap<String, Vec<FlightReceiver>>,
    inbound_channel_sets: HashMap<String, Arc<NetworkInboundChannelSet>>,
    inbound_sources: HashMap<InboundSourceKey, Arc<NetworkInboundSource>>,
    block_outbounds: HashMap<String, HashMap<String, PendingNetworkOutbound>>,
}

impl QueryCoordinator {
    pub fn create(
        query_id: String,
        exchange_session_id: String,
        reconnect_lease: Duration,
    ) -> QueryCoordinator {
        QueryCoordinator {
            query_id,
            exchange_session_id,
            reconnect_lease,
            info: None,
            is_request_server: false,
            packet_senders: HashMap::new(),
            packet_receivers: HashMap::new(),
            statistics_senders: HashMap::new(),
            statistics_receivers: HashMap::new(),
            fragments_coordinator: HashMap::new(),
            inbound_channel_sets: HashMap::new(),
            inbound_sources: HashMap::new(),
            block_outbounds: HashMap::new(),
        }
    }

    fn validate_exchange_session(&self, exchange_session_id: &str) -> Result<()> {
        if self.exchange_session_id == exchange_session_id {
            return Ok(());
        }

        Err(ErrorCode::ClosedQuery(format!(
            "Exchange session {} does not belong to the active query",
            exchange_session_id
        )))
    }

    fn install_outbound_state(
        &mut self,
        mut info: Option<QueryInfo>,
        is_request_server: bool,
        packet_senders: HashMap<String, Vec<FlightSender>>,
        block_outbounds: HashMap<String, HashMap<String, PendingNetworkOutbound>>,
        statistics_senders: HashMap<String, FlightSender>,
    ) {
        if self
            .info
            .as_ref()
            .is_some_and(|info| info.started.load(Ordering::Acquire))
        {
            if let Some(worker) = info
                .as_mut()
                .and_then(|info| info.remove_leak_query_worker.take())
            {
                worker.abort();
            }
            return;
        }
        if let Some(worker) = self
            .info
            .as_mut()
            .and_then(|info| info.remove_leak_query_worker.take())
        {
            worker.abort();
        }
        self.info = info;
        self.is_request_server = is_request_server;
        self.packet_senders = packet_senders;
        self.block_outbounds = block_outbounds;
        self.statistics_senders = statistics_senders;
    }

    fn admit_block_sources(
        &mut self,
        exchange_id: &str,
        num_threads: usize,
        source_ids: &[String],
    ) -> Result<()> {
        let channel_set = self.get_or_create_inbound_channel_set(exchange_id, num_threads)?;
        for source_id in source_ids {
            let stream = DoExchangeStream::Blocks {
                exchange_id: exchange_id.to_string(),
            };
            let source_label = self.inbound_source_label(source_id, &stream);
            self.inbound_sources
                .entry(InboundSourceKey::new(stream, source_id))
                .or_insert_with(|| {
                    Arc::new(NetworkInboundSource::new(
                        &channel_set,
                        20 * 1024 * 1024,
                        self.reconnect_lease,
                        source_label,
                    ))
                });
        }
        Ok(())
    }

    fn admit_packet_sources(&mut self, channel_id: &str, source_ids: &[String]) -> Result<()> {
        for source_id in source_ids {
            let stream = DoExchangeStream::Packets {
                channel_id: channel_id.to_string(),
            };
            let source_label = self.inbound_source_label(source_id, &stream);
            let key = InboundSourceKey::new(stream, source_id);
            let Entry::Vacant(entry) = self.inbound_sources.entry(key) else {
                continue;
            };

            let (source, receiver) =
                NetworkInboundSource::new_packets(8, self.reconnect_lease, source_label);
            entry.insert(Arc::new(source));
            self.packet_receivers
                .entry(channel_id.to_string())
                .or_default()
                .push(FlightReceiver::create(receiver));
        }
        Ok(())
    }

    fn admit_statistics_sources(&mut self, source_ids: &[String]) -> Result<()> {
        for source_id in source_ids {
            let stream = DoExchangeStream::Statistics;
            let source_label = self.inbound_source_label(source_id, &stream);
            let key = InboundSourceKey::new(stream, source_id);
            let Entry::Vacant(entry) = self.inbound_sources.entry(key) else {
                continue;
            };

            let (source, receiver) =
                NetworkInboundSource::new_packets(8, self.reconnect_lease, source_label);
            entry.insert(Arc::new(source));
            if self
                .statistics_receivers
                .insert(source_id.clone(), FlightReceiver::create(receiver))
                .is_some()
            {
                return Err(ErrorCode::Internal(
                    "statistics exchange source was admitted twice",
                ));
            }
        }
        Ok(())
    }

    fn inbound_source_label(&self, source_id: &str, stream: &DoExchangeStream) -> String {
        format!(
            "query_id={}, exchange_session_id={}, source_id={}, stream={:?}",
            self.query_id, self.exchange_session_id, source_id, stream
        )
    }

    fn get_inbound_source(
        &self,
        stream: &DoExchangeStream,
        source_id: &str,
    ) -> Result<Arc<NetworkInboundSource>> {
        let key = InboundSourceKey::new(stream.clone(), source_id);
        self.inbound_sources
            .get(&key)
            .ok_or_else(|| {
                ErrorCode::ClosedQuery(format!(
                    "do_exchange source {} for stream {:?} was not admitted",
                    source_id, stream
                ))
            })
            .cloned()
    }

    fn get_or_create_inbound_channel_set(
        &mut self,
        channel_id: &str,
        num_threads: usize,
    ) -> Result<Arc<NetworkInboundChannelSet>> {
        let channel_set = self
            .inbound_channel_sets
            .entry(channel_id.to_string())
            .or_insert_with(|| Arc::new(NetworkInboundChannelSet::new(num_threads)))
            .clone();
        if channel_set.channels.len() != num_threads {
            return Err(ErrorCode::Internal(format!(
                "NetworkInboundChannelSet {} has {} channels, expected {}",
                channel_id,
                channel_set.channels.len(),
                num_threads
            )));
        }
        Ok(channel_set)
    }

    pub fn prepare_pipeline(&mut self, fragments: &QueryFragments) -> Result<()> {
        let query_info = self.info.as_ref().expect("expect query info");
        let query_context = query_info.query_ctx.clone();

        for fragment in &fragments.fragments {
            self.fragments_coordinator.insert(
                fragment.fragment_id.to_owned(),
                FragmentCoordinator::create(fragment),
            );
        }

        for fragment in &fragments.fragments {
            let fragment_id = fragment.fragment_id;
            if let Some(coordinator) = self.fragments_coordinator.get_mut(&fragment_id) {
                coordinator.prepare_pipeline(query_context.clone())?;
            }
        }

        Ok(())
    }

    pub fn subscribe_fragment(
        &mut self,
        ctx: &Arc<QueryContext>,
        fragment_id: usize,
        injector: Arc<dyn ExchangeInjector>,
    ) -> Result<PipelineBuildResult> {
        // Merge pipelines if exist locally pipeline
        if let Some(mut fragment_coordinator) = self.fragments_coordinator.remove(&fragment_id) {
            let info = self.info.as_ref().expect("QueryInfo is none");
            fragment_coordinator.prepare_pipeline(ctx.clone())?;

            if fragment_coordinator.pipeline_build_res.is_none() {
                return Err(ErrorCode::Internal(
                    "Pipeline is none, maybe query fragment circular dependency.",
                ));
            }

            if fragment_coordinator.data_exchange.is_none() {
                // When the root fragment and the data has been send to the coordination node,
                // we do not need to wait for the data of other nodes.
                return Ok(fragment_coordinator.pipeline_build_res.unwrap());
            }

            let exchange_params = fragment_coordinator
                .create_exchange_params(
                    info,
                    fragment_coordinator
                        .pipeline_build_res
                        .as_ref()
                        .map(|x| x.exchange_injector.clone())
                        .ok_or_else(|| {
                            ErrorCode::Internal("Pipeline build result is none, It's a bug")
                        })?,
                )?
                .unwrap();
            let mut build_res = fragment_coordinator.pipeline_build_res.unwrap();

            // Add exchange data transform.

            ExchangeTransform::via(
                ctx,
                &exchange_params,
                &mut build_res.main_pipeline,
                injector,
            )?;

            return Ok(build_res);
        }
        Err(ErrorCode::Unimplemented("ExchangeSource is unimplemented"))
    }

    pub fn shutdown_query(&mut self, cause: Option<ErrorCode>) {
        if let Some(query_info) = &mut self.info {
            if let Some(query_executor) = &query_info.query_executor {
                query_executor.finish(cause);
            }

            if let Some(worker) = query_info.remove_leak_query_worker.take() {
                worker.abort();
            }
        }
    }

    pub fn on_finished(self) {
        // Do something when query finished.
    }

    pub fn execute_pipeline(&mut self) -> Result<()> {
        let info = self.info.as_mut().expect("Query info is None");

        let perf_guard = {
            let pc = info.query_ctx.get_perf_config();
            if pc.profiler_enabled && !self.is_request_server {
                Some(QueryPerf::start(pc.frequency)?)
            } else {
                None
            }
        };

        if !info.started.swap(true, Ordering::SeqCst) {
            if let Some(leak_worker) = info.remove_leak_query_worker.take() {
                leak_worker.abort();
            }
        }

        if self.fragments_coordinator.is_empty() {
            // Empty fragments if it is a request server, because the pipelines may have been linked.
            return Ok(());
        }

        let max_threads = info.query_ctx.get_settings().get_max_threads()?;
        let mut pipelines = Vec::with_capacity(self.fragments_coordinator.len());

        let mut params = Vec::with_capacity(self.fragments_coordinator.len());
        for coordinator in self.fragments_coordinator.values() {
            params.push(
                coordinator.create_exchange_params(
                    info,
                    coordinator
                        .pipeline_build_res
                        .as_ref()
                        .map(|x| x.exchange_injector.clone())
                        .ok_or_else(|| {
                            ErrorCode::Internal("Pipeline build result is none, It's a bug")
                        })?,
                )?,
            );
        }

        for ((_, coordinator), params) in self.fragments_coordinator.iter_mut().zip(params) {
            if let Some(mut build_res) = coordinator.pipeline_build_res.take() {
                build_res.set_max_threads(max_threads as usize);

                if build_res.main_pipeline.is_pulling_pipeline()? {
                    let Some(params) = params else {
                        return Err(ErrorCode::Internal(
                            "pipeline is pulling pipeline, but exchange params is none",
                        ));
                    };
                    // Add exchange data publisher.
                    ExchangeSink::via(&info.query_ctx, &params, &mut build_res.main_pipeline)?;
                } else if build_res.main_pipeline.is_complete_pipeline()? && params.is_some() {
                    return Err(ErrorCode::Internal(
                        "pipeline is complete pipeline, but exchange params is some",
                    ));
                };

                if !build_res.main_pipeline.is_complete_pipeline()? {
                    return Err(ErrorCode::Internal("Logical error, It's a bug"));
                }

                pipelines.push(build_res.main_pipeline);
                pipelines.extend(build_res.sources_pipelines.into_iter());
            }
        }

        let (finished_profiling_tx, finished_profiling_rx) = oneshot::channel();
        if let Some(p) = pipelines.first_mut() {
            p.set_on_finished(always_callback(move |info: &ExecutionInfo| {
                let profiling = info.profiling.clone();
                let _ = finished_profiling_tx.send(profiling);
                Ok(())
            }));
        };

        let settings = ExecutorSettings::try_create(info.query_ctx.clone())?;
        let executor = PipelineCompleteExecutor::from_pipelines(pipelines, settings)?;

        assert!(self.packet_senders.is_empty() && self.packet_receivers.is_empty());
        let info_mut = self.info.as_mut().expect("Query info is None");
        info_mut.query_executor = Some(executor.clone());

        let query_id = info_mut.query_id.clone();
        let exchange_session_id = self.exchange_session_id.clone();
        let query_ctx = info_mut.query_ctx.clone();
        query_ctx.set_executor(executor.get_inner())?;
        let request_server_senders = std::mem::take(&mut self.statistics_senders);

        if request_server_senders.len() != 1 {
            return Err(ErrorCode::Internal(
                "Request server must less than 1 if is not request server.",
            ));
        }

        let ctx = query_ctx.clone();
        let (_, request_server_sender) = request_server_senders.into_iter().next().unwrap();
        let mut statistics_sender = StatisticsSender::spawn(
            &query_id,
            &exchange_session_id,
            ctx,
            request_server_sender,
            executor.get_inner(),
            perf_guard,
            finished_profiling_rx,
        );

        let span = if let Some(parent) = SpanContext::current_local_parent() {
            Span::root("Distributed-Executor", parent)
        } else {
            Span::noop()
        };
        GlobalIORuntime::instance().spawn_named(
            async move {
                let error = executor.execute().await.err();
                statistics_sender.shutdown(error.clone()).await;
                let exchange_manager = query_ctx.get_exchange_manager();
                if let Err(cause) = spawn_blocking(move || {
                    exchange_manager.on_finished_exchange(&query_id, &exchange_session_id, error);
                })
                .await
                {
                    warn!("on_finished_query cleanup task failed: {:?}", cause);
                }
            }
            .in_span(span),
            "Distributed-Executor",
        );

        Ok(())
    }
}

struct FragmentCoordinator {
    initialized: bool,
    fragment_id: usize,
    physical_plan: PhysicalPlan,
    data_exchange: Option<DataExchange>,
    pipeline_build_res: Option<PipelineBuildResult>,
}

impl FragmentCoordinator {
    pub fn create(packet: &QueryFragment) -> Box<FragmentCoordinator> {
        Box::new(FragmentCoordinator {
            initialized: false,
            physical_plan: packet.physical_plan.clone(),
            fragment_id: packet.fragment_id,
            data_exchange: packet.data_exchange.clone(),
            pipeline_build_res: None,
        })
    }

    pub fn create_exchange_params(
        &self,
        info: &QueryInfo,
        exchange_injector: Arc<dyn ExchangeInjector>,
    ) -> Result<Option<ExchangeParams>> {
        let Some(data_exchange) = &self.data_exchange else {
            return Ok(None);
        };
        match data_exchange {
            DataExchange::Merge(exchange) => {
                Ok(Some(ExchangeParams::MergeExchange(MergeExchangeParams {
                    exchange_injector: exchange_injector.clone(),
                    schema: self.physical_plan.output_schema()?,
                    fragment_id: self.fragment_id,
                    query_id: info.query_id.to_string(),
                    destination_id: exchange.destination_id.clone(),
                    allow_adjust_parallelism: exchange.allow_adjust_parallelism,
                    ignore_exchange: exchange.ignore_exchange,
                    channel_id: exchange.channel_id.clone(),
                })))
            }
            DataExchange::Broadcast(exchange) => Ok(Some(ExchangeParams::BroadcastExchange(
                BroadcastExchangeParams {
                    query_id: info.query_id.to_string(),
                    executor_id: info.current_executor.to_string(),
                    schema: self.physical_plan.output_schema()?,
                    exchange_id: exchange.id.clone(),
                    destination_channels: exchange.destination_channels.to_owned(),
                },
            ))),
            DataExchange::NodeToNodeExchange(exchange) => Ok(Some(
                ExchangeParams::NodeShuffleExchange(ShuffleExchangeParams {
                    exchange_injector: exchange_injector.clone(),
                    schema: self.physical_plan.output_schema()?,
                    fragment_id: self.fragment_id,
                    query_id: info.query_id.to_string(),
                    executor_id: info.current_executor.to_string(),
                    destination_ids: exchange.destination_ids.to_owned(),
                    destination_channels: exchange.destination_channels.clone(),
                    shuffle_scatter: exchange_injector
                        .flight_scatter(&info.query_ctx, data_exchange)?,
                    allow_adjust_parallelism: exchange.allow_adjust_parallelism,
                }),
            )),
            DataExchange::GlobalShuffleExchange(exchange) => Ok(Some(
                ExchangeParams::GlobalShuffleExchange(GlobalExchangeParams {
                    query_id: info.query_id.to_string(),
                    executor_id: info.current_executor.to_string(),
                    schema: self.physical_plan.output_schema()?,
                    exchange_id: exchange.id.clone(),
                    shuffle_keys: exchange.shuffle_keys.clone(),
                    destination_channels: exchange.destination_channels.clone(),
                }),
            )),
        }
    }

    pub fn prepare_pipeline(&mut self, ctx: Arc<QueryContext>) -> Result<()> {
        if !self.initialized {
            self.initialized = true;

            let pipeline_ctx = QueryContext::create_from(ctx.as_ref());

            unsafe {
                pipeline_ctx
                    .get_settings()
                    .unchecked_apply_changes(ctx.get_settings().changes());

                drop(ctx);
            }

            let pipeline_builder = PipelineBuilder::create(
                pipeline_ctx.get_function_context()?,
                pipeline_ctx.get_settings(),
                pipeline_ctx.clone(),
            );

            let mut res = pipeline_builder.finalize(&self.physical_plan)?;
            attach_runtime_filter_logger(pipeline_ctx, &mut res.main_pipeline);

            self.pipeline_build_res = Some(res);
        }

        Ok(())
    }
}
