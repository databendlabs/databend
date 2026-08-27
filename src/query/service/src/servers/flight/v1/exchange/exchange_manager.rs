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

use arrow_flight::FlightData;
use arrow_flight::flight_service_client::FlightServiceClient;
use async_channel::Receiver;
use databend_common_base::JoinHandle;
use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::ExecutorStatsSnapshot;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::QueryPerf;
use databend_common_base::runtime::spawn_blocking;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_grpc::ConnectionFactory;
use databend_common_pipeline::core::ExecutionInfo;
use databend_common_pipeline::core::always_callback;
use databend_common_pipeline::core::basic_callback;
use databend_common_settings::FlightKeepAliveParams;
use fastrace::prelude::*;
use futures::StreamExt;
use log::warn;
use parking_lot::Mutex;
use parking_lot::ReentrantMutex;
use petgraph::Direction;
use petgraph::prelude::EdgeRef;
use tokio::sync::Semaphore;
use tokio::sync::oneshot;
use tonic::Status;

use super::exchange_params::BroadcastExchangeParams;
use super::exchange_params::ExchangeParams;
use super::exchange_params::GlobalExchangeParams;
use super::exchange_params::MergeExchangeParams;
use super::exchange_params::ShuffleExchangeParams;
use super::exchange_sink::ExchangeSink;
use super::exchange_transform::ExchangeTransform;
use super::packet_receiver::PacketReceiver;
use super::reliable_delivery::StatisticsDelivery;
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
use crate::servers::flight::FlightClient;
use crate::servers::flight::FlightOperation;
use crate::servers::flight::NewFlightStream;
use crate::servers::flight::add_flight_error_context;
use crate::servers::flight::keep_alive::build_keep_alive_config;
use crate::servers::flight::v1::actions::INIT_QUERY_FRAGMENTS;
use crate::servers::flight::v1::actions::START_PREPARED_QUERY;
use crate::servers::flight::v1::actions::init_query_fragments;
use crate::servers::flight::v1::exchange::DataExchange;
use crate::servers::flight::v1::exchange::DefaultExchangeInjector;
use crate::servers::flight::v1::exchange::ExchangeInjector;
use crate::servers::flight::v1::exchange::exchange_packet_receiver::ExchangePacketReceiverSet;
use crate::servers::flight::v1::exchange::exchange_packet_receiver::NetworkInboundSender;
use crate::servers::flight::v1::packets::Edge;
use crate::servers::flight::v1::packets::QueryEnv;
use crate::servers::flight::v1::packets::QueryFragment;
use crate::servers::flight::v1::packets::QueryFragments;
use crate::servers::flight::v1::transport::InboundDelivery;
use crate::servers::flight::v1::transport::OutboundStreamRef;
use crate::servers::flight::v1::transport::legacy::LegacyInbound;
use crate::servers::flight::v1::transport::legacy::LegacyOutbound;
use crate::servers::flight::v1::transport::legacy::PingPongExchange;
use crate::servers::flight::v1::transport::reliable::DoExchangeConnector;
use crate::servers::flight::v1::transport::reliable::DoExchangeTransport;
use crate::servers::flight::v1::transport::reliable::FlightReconnectPolicy;
use crate::servers::flight::v1::transport::reliable::PendingReliableOutbound;
use crate::servers::flight::v1::transport::reliable::ReliableInboundConnection;
use crate::servers::flight::v1::transport::reliable::ReliableInboundSource;
use crate::sessions::QueryContext;
use crate::sessions::TableContextCluster;
use crate::sessions::TableContextPerf;
use crate::sessions::TableContextQueryIdentity;
use crate::sessions::TableContextSettings;

/// Inbound queue quota per do_exchange connection.
// TODO: get max_bytes_per_connection from query settings
const MAX_INBOUND_BYTES_PER_CONNECTION: usize = 20 * 1024 * 1024;

/// Queued statistics packets allowed before the source blocks.
const STATISTICS_QUEUE_CAPACITY: usize = 8;

enum QueryExchange {
    Fragment {
        channel: String,
        exchange: LegacyInbound,
    },
    Statistics {
        source: String,
        exchange: LegacyInbound,
    },
    StatisticsSender {
        target: String,
        sender: OutboundStreamRef,
    },
    NewFlightFragmentOutbound {
        exchange_id: String,
        target_id: String,
        outbound: PendingReliableOutbound,
    },
    PingPong {
        exchange_id: String,
        target_id: String,
        exchange: PingPongExchange,
    },
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
            let mut client =
                create_flight_client(target_id, address, use_current_rt, keep_alive).await?;
            let (send_tx, send_rx) = async_channel::bounded(1);
            let response_stream = client.do_exchange(send_rx, params).await?;
            Ok(DoExchangeTransport {
                send_tx,
                response_stream: response_stream.boxed(),
            })
        })
    })
}

async fn create_flight_client(
    remote_node_id: String,
    address: String,
    use_current_rt: bool,
    keep_alive: FlightKeepAliveParams,
) -> Result<FlightClient> {
    let config = GlobalConfig::instance();
    let local_node_id = config.query.node_id.clone();
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
            add_flight_error_context(
                ErrorCode::from(error),
                FlightOperation::Connect,
                &local_node_id,
                &remote_node_id,
            )
        })?;

        Ok(FlightClient::new(
            FlightServiceClient::new(channel),
            local_node_id,
            remote_node_id,
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
    queries_coordinator: ReentrantMutex<SyncUnsafeCell<HashMap<String, QueryCoordinator>>>,
}

impl DataExchangeManager {
    pub fn init() -> Result<()> {
        GlobalInstance::set(Arc::new(DataExchangeManager {
            queries_coordinator: ReentrantMutex::new(SyncUnsafeCell::new(HashMap::new())),
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

    pub fn get_query_ctx(&self, query_id: &str) -> Result<Arc<QueryContext>> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        if let Some(coordinator) = queries_coordinator.get_mut(query_id) {
            if let Some(coordinator) = &coordinator.info {
                return Ok(coordinator.query_ctx.clone());
            }
        }

        Err(ErrorCode::Internal(format!(
            "Query {} not found in cluster.",
            query_id
        )))
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
        let new_flight = FlightReconnectPolicy::from_settings(&settings)?;
        log::info!(
            "Flight transport selected: query_id={}, mode={}",
            env.query_id,
            if new_flight.is_some() {
                "new flight"
            } else {
                "legacy flight"
            }
        );

        let mut request_exchanges = HashMap::new();
        let mut targets_exchanges = HashMap::<String, Vec<LegacyInbound>>::new();

        for index in env.dataflow_diagram.node_indices() {
            if env.dataflow_diagram[index].id == config.query.node_id {
                let mut flight_exchanges: Vec<
                    std::pin::Pin<
                        Box<dyn std::future::Future<Output = Result<QueryExchange>> + Send>,
                    >,
                > = vec![];

                let incoming_edges = env
                    .dataflow_diagram
                    .edges_directed(index, Direction::Incoming);

                for edge in incoming_edges {
                    let source = env.dataflow_diagram[edge.source()].clone();
                    let target = env.dataflow_diagram[edge.target()].clone();
                    let edge = edge.weight().clone();
                    let query_id = env.query_id.clone();
                    let address = source.flight_address.clone();
                    let source_id = source.id.clone();
                    let keep_alive_params = keep_alive;

                    match (new_flight, edge) {
                        (None, Edge::Fragment(channel)) | (None, Edge::Merge(channel)) => {
                            flight_exchanges.push(Box::pin(async move {
                                let mut flight_client = Self::create_client(
                                    &source_id,
                                    &address,
                                    with_cur_rt,
                                    keep_alive_params,
                                )
                                .await?;
                                Ok::<QueryExchange, ErrorCode>(QueryExchange::Fragment {
                                    channel: channel.clone(),
                                    exchange: flight_client.do_get(&query_id, &channel).await?,
                                })
                            }));
                        }
                        (None, Edge::Statistics) => {
                            flight_exchanges.push(Box::pin(async move {
                                let mut flight_client = Self::create_client(
                                    &source_id,
                                    &address,
                                    with_cur_rt,
                                    keep_alive_params,
                                )
                                .await?;
                                Ok::<QueryExchange, ErrorCode>(QueryExchange::Statistics {
                                    source: source_id,
                                    exchange: flight_client
                                        .request_server_exchange(&query_id, &target.id)
                                        .await?,
                                })
                            }));
                        }
                        _ => {}
                    }
                }

                let outgoing_edges = env
                    .dataflow_diagram
                    .edges_directed(index, Direction::Outgoing);

                for edge in outgoing_edges {
                    let target = env.dataflow_diagram[edge.target()].clone();
                    let edge = edge.weight().clone();

                    if let (Some(reconnect), Edge::Statistics) = (new_flight, &edge) {
                        let target_id = target.id.clone();
                        let query_id = env.query_id.clone();
                        let source_id = config.query.node_id.clone();
                        let address = target.flight_address.clone();
                        let keep_alive_params = keep_alive;
                        flight_exchanges.push(Box::pin(async move {
                            let params = DoExchangeParams::new_flight_statistics(
                                query_id,
                                source_id.clone(),
                                reconnect.receiver_lease_secs(),
                            );
                            let connector = create_do_exchange_connector(
                                target_id.clone(),
                                address,
                                with_cur_rt,
                                keep_alive_params,
                                params,
                            );
                            let outbound = PendingReliableOutbound::connect(
                                1,
                                connector,
                                reconnect,
                                source_id,
                                target_id.clone(),
                            )
                            .await?;
                            let slots = Arc::new(Semaphore::new(STATISTICS_QUEUE_CAPACITY));
                            let sender =
                                Arc::new(outbound.start(slots, None, &GlobalIORuntime::instance()))
                                    as OutboundStreamRef;
                            Ok::<QueryExchange, ErrorCode>(QueryExchange::StatisticsSender {
                                target: target_id,
                                sender,
                            })
                        }));
                        continue;
                    }

                    let (exchange_id, channels, stream) = match (new_flight, edge) {
                        (Some(_), Edge::Merge(channel)) => {
                            (channel.clone(), vec![channel], NewFlightStream::Merge)
                        }
                        (Some(_), Edge::Fragment(channel)) => {
                            (channel.clone(), vec![channel], NewFlightStream::Fragment)
                        }
                        (
                            _,
                            Edge::ExchangeFragment {
                                exchange_id,
                                channels,
                            },
                        ) => (exchange_id, channels, NewFlightStream::Exchange),
                        _ => continue,
                    };

                    let target_id = target.id.clone();
                    let local_node_id = config.query.node_id.clone();
                    let query_id = env.query_id.clone();
                    let address = target.flight_address.clone();
                    let keep_alive_params = keep_alive;
                    let num_threads = channels.len();
                    flight_exchanges.push(Box::pin(async move {
                        match new_flight {
                            None => {
                                let mut flight_client = create_flight_client(
                                    target_id.clone(),
                                    address,
                                    with_cur_rt,
                                    keep_alive_params,
                                )
                                .await?;
                                let (send_tx, send_rx) = async_channel::bounded(1);
                                let response_stream = flight_client
                                    .do_exchange(
                                        send_rx,
                                        DoExchangeParams::create(
                                            query_id,
                                            exchange_id.clone(),
                                            num_threads,
                                        ),
                                    )
                                    .await?;
                                Ok::<QueryExchange, ErrorCode>(QueryExchange::PingPong {
                                    target_id: target_id.clone(),
                                    exchange_id,
                                    exchange: PingPongExchange::from_parts(
                                        num_threads,
                                        send_tx,
                                        response_stream,
                                        local_node_id,
                                        target_id,
                                    ),
                                })
                            }
                            Some(reconnect) => {
                                let params = match stream {
                                    NewFlightStream::Merge => DoExchangeParams::new_flight_merge(
                                        query_id,
                                        exchange_id.clone(),
                                        local_node_id.clone(),
                                        reconnect.receiver_lease_secs(),
                                    ),
                                    NewFlightStream::Fragment => {
                                        DoExchangeParams::new_flight_fragment(
                                            query_id,
                                            exchange_id.clone(),
                                            local_node_id.clone(),
                                            num_threads,
                                            reconnect.receiver_lease_secs(),
                                        )
                                    }
                                    NewFlightStream::Exchange => {
                                        DoExchangeParams::new_flight_exchange(
                                            query_id,
                                            exchange_id.clone(),
                                            local_node_id.clone(),
                                            num_threads,
                                            reconnect.receiver_lease_secs(),
                                        )
                                    }
                                    NewFlightStream::Statistics => unreachable!(
                                        "statistics streams are installed before fragment streams"
                                    ),
                                };
                                let connector = create_do_exchange_connector(
                                    target_id.clone(),
                                    address,
                                    with_cur_rt,
                                    keep_alive_params,
                                    params,
                                );
                                let outbound = PendingReliableOutbound::connect(
                                    num_threads,
                                    connector,
                                    reconnect,
                                    local_node_id,
                                    target_id.clone(),
                                )
                                .await?;
                                Ok::<QueryExchange, ErrorCode>(
                                    QueryExchange::NewFlightFragmentOutbound {
                                        target_id,
                                        exchange_id,
                                        outbound,
                                    },
                                )
                            }
                        }
                    }));
                }

                let flight_exchanges = futures::future::try_join_all(flight_exchanges).await?;
                let mut new_flight_fragment_outbounds =
                    HashMap::<String, HashMap<String, PendingReliableOutbound>>::new();
                let mut ping_pong_exchanges =
                    HashMap::<String, HashMap<String, PingPongExchange>>::new();
                let mut statistics_senders = HashMap::<String, OutboundStreamRef>::new();

                for flight_exchange in flight_exchanges {
                    match flight_exchange {
                        QueryExchange::Fragment { channel, exchange } => {
                            targets_exchanges.entry(channel).or_default().push(exchange);
                        }
                        QueryExchange::Statistics { source, exchange } => {
                            request_exchanges.insert(source, exchange);
                        }
                        QueryExchange::StatisticsSender { target, sender } => {
                            statistics_senders.insert(target, sender);
                        }
                        QueryExchange::NewFlightFragmentOutbound {
                            exchange_id,
                            target_id,
                            outbound,
                        } => {
                            new_flight_fragment_outbounds
                                .entry(exchange_id)
                                .or_default()
                                .insert(target_id, outbound);
                        }
                        QueryExchange::PingPong {
                            exchange_id,
                            exchange,
                            target_id,
                        } => {
                            ping_pong_exchanges
                                .entry(exchange_id)
                                .or_default()
                                .insert(target_id, exchange);
                        }
                    }
                }

                let mut query_info = Self::create_info(ctx)?;
                if let Some(query_info) = query_info.as_mut() {
                    let query_id = env.query_id.clone();
                    query_info.remove_leak_query_worker =
                        Some(GlobalIORuntime::instance().spawn(async move {
                            tokio::time::sleep(Duration::from_secs(180)).await;
                            DataExchangeManager::instance().remove_if_leak_query(query_id);
                        }));
                }

                let queries_coordinator_guard = self.queries_coordinator.lock();
                let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };
                let query_coordinator = queries_coordinator
                    .entry(env.query_id.clone())
                    .or_insert_with(QueryCoordinator::create);
                query_coordinator.info = query_info;
                query_coordinator.is_request_server =
                    GlobalConfig::instance().query.node_id == env.request_server_id;
                query_coordinator.register_fragment_receivers(targets_exchanges)?;
                for (exchange_id, outbounds) in new_flight_fragment_outbounds {
                    for (target_id, outbound) in outbounds {
                        query_coordinator.register_new_flight_fragment_outbound(
                            exchange_id.clone(),
                            target_id,
                            outbound,
                        );
                    }
                }
                query_coordinator.register_ping_pong_exchanges(ping_pong_exchanges);
                query_coordinator.add_statistics_exchanges(request_exchanges)?;
                query_coordinator.add_statistics_senders(statistics_senders)?;

                return Ok(());
            }
        }

        // do nothing
        Ok(())
    }

    fn remove_if_leak_query(&self, query_id: String) {
        let leak_query_id = {
            let queries_coordinator_guard = self.queries_coordinator.lock();
            let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

            match queries_coordinator.get(&query_id) {
                None => None,
                Some(may_leak_query) => {
                    let info = may_leak_query.info.as_ref().expect("expect query info");
                    match info.started.load(Ordering::SeqCst) {
                        true => None,
                        false => Some(query_id),
                    }
                }
            }
        };

        if let Some(query_id) = leak_query_id {
            warn!(
                "Query {} cannot start command while in 180 seconds",
                query_id
            );
            self.on_finished_query(
                &query_id,
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

    #[async_backtrace::framed]
    pub async fn create_client(
        remote_node_id: &str,
        address: &str,
        use_current_rt: bool,
        keep_alive: FlightKeepAliveParams,
    ) -> Result<FlightClient> {
        let config = GlobalConfig::instance();
        let local_node_id = config.query.node_id.clone();
        let address = address.to_string();
        let remote_node_id = remote_node_id.to_string();
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
                add_flight_error_context(
                    ErrorCode::from(error),
                    FlightOperation::Connect,
                    &local_node_id,
                    &remote_node_id,
                )
            })?;

            Ok(FlightClient::new(
                FlightServiceClient::new(channel),
                local_node_id,
                remote_node_id,
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

    pub fn set_ctx(&self, query_id: &str, ctx: Arc<QueryContext>) -> Result<()> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };
        match queries_coordinator.get_mut(query_id) {
            None => Err(ErrorCode::Internal(format!(
                "Query {} not found in cluster.",
                query_id
            ))),
            Some(coordinator) => {
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
    pub fn execute_partial_query(&self, query_id: &str) -> Result<()> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.get_mut(query_id) {
            None => Err(ErrorCode::Internal(format!(
                "Query {} not found in cluster.",
                query_id
            ))),
            Some(coordinator) => coordinator.execute_pipeline(),
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
            Some(query_coordinator) => query_coordinator.prepare_pipeline(fragments),
        }
    }

    #[fastrace::trace]
    pub fn handle_statistics_exchange(
        &self,
        id: String,
        target: String,
    ) -> Result<Receiver<std::result::Result<FlightData, Status>>> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.entry(id) {
            Entry::Occupied(mut v) => v.get_mut().add_statistics_exchange(target),
            Entry::Vacant(v) => v
                .insert(QueryCoordinator::create())
                .add_statistics_exchange(target),
        }
    }

    #[fastrace::trace]
    pub fn handle_exchange_fragment(
        &self,
        query: String,
        channel_id: String,
    ) -> Result<Receiver<std::result::Result<FlightData, Status>>> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.entry(query) {
            Entry::Occupied(mut v) => v.get_mut().register_flight_channel_sender(channel_id),
            Entry::Vacant(v) => v
                .insert(QueryCoordinator::create())
                .register_flight_channel_sender(channel_id),
        }
    }

    /// Handle a do_exchange request from a remote node.
    ///
    /// Creates a `NetworkInboundSender` for this connection, bound to the
    /// `ExchangePacketReceiverSet` for the given channel_id. The caller (flight_service)
    /// uses the sender to push incoming FlightData into per-tid queues.
    #[fastrace::trace]
    pub fn handle_do_exchange(
        &self,
        query_id: &str,
        channel_id: &str,
        num_threads: usize,
    ) -> Result<NetworkInboundSender> {
        warn!(
            "handle_do_exchange: query_id={}, channel_id={}, num_threads={}",
            query_id, channel_id, num_threads
        );
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.entry(query_id.to_string()) {
            Entry::Occupied(mut v) => v.get_mut().create_inbound_sender(channel_id, num_threads),
            Entry::Vacant(v) => v
                .insert(QueryCoordinator::create())
                .create_inbound_sender(channel_id, num_threads),
        }
    }

    /// Admits one New Flight `do_exchange` connection, creating the query coordinator if this is
    /// the first stream to arrive for the query.
    #[fastrace::trace]
    pub fn handle_new_flight_do_exchange(
        &self,
        query_id: &str,
        channel_id: &str,
        source_id: &str,
        num_threads: usize,
        stream: NewFlightStream,
        receiver_lease: Duration,
    ) -> Result<ReliableInboundConnection> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        queries_coordinator
            .entry(query_id.to_string())
            .or_insert_with(QueryCoordinator::create)
            .open_new_flight_inbound_connection(
                channel_id,
                source_id,
                num_threads,
                stream,
                receiver_lease,
            )
    }

    /// Get the NetworkInboundReceivers for a given query and channel.
    ///
    /// Returns one `Arc<NetworkInboundReceiver>` per tid, for building
    /// `ThreadChannelReader` processors in the pipeline.
    pub fn get_exchange_channel_set(
        &self,
        query_id: &str,
        channel_id: &str,
    ) -> Result<Arc<ExchangePacketReceiverSet>> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.get(query_id) {
            None => Err(ErrorCode::Internal(format!(
                "Query {} not found in cluster.",
                query_id
            ))),
            Some(coordinator) => match coordinator.inbound_channel_sets.get(channel_id) {
                None => Err(ErrorCode::Internal(format!(
                    "ExchangePacketReceiverSet not found for channel {}",
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
    ) -> Result<Arc<ExchangePacketReceiverSet>> {
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

    pub fn take_new_flight_fragment_outbounds(
        &self,
        query_id: &str,
        exchange_id: &str,
    ) -> Result<HashMap<String, PendingReliableOutbound>> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.get_mut(query_id) {
            None => Err(ErrorCode::Internal(format!(
                "Query {} not found in cluster.",
                query_id
            ))),
            Some(coordinator) => Ok(coordinator
                .new_flight_fragment_outbounds
                .remove(exchange_id)
                .unwrap_or_default()),
        }
    }

    pub fn take_ping_pong_exchanges(
        &self,
        query_id: &str,
        channel_id: &str,
    ) -> Result<HashMap<String, PingPongExchange>> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.get_mut(query_id) {
            None => Err(ErrorCode::Internal(format!(
                "Query {} not found in cluster.",
                query_id
            ))),
            Some(coordinator) => Ok(coordinator
                .ping_pong_exchanges
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

    #[fastrace::trace]
    pub fn on_finished_query(&self, query_id: &str, cause: Option<ErrorCode>) {
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

        if let Some(mut query_coordinator) = queries_coordinator.remove(query_id) {
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

        self.set_ctx(&ctx.get_id(), ctx.clone())?;
        if let Some(query_fragments) = local_fragments {
            init_query_fragments(query_fragments).await?;
        }

        // Get local pipeline of local task
        let main_fragment_id = root_fragment_ids.pop().unwrap();
        let build_res = self.get_root_pipeline(ctx, main_fragment_id, root_fragment_ids)?;

        let prepared_query = actions.prepared_query()?;
        let _: HashMap<String, ()> = cluster
            .do_action(START_PREPARED_QUERY, prepared_query, flight_params)
            .await?;

        Ok(build_res)
    }

    fn get_root_pipeline(
        &self,
        ctx: Arc<QueryContext>,
        main_fragment_id: usize,
        fragment_ids: Vec<usize>,
    ) -> Result<PipelineBuildResult> {
        let query_id = ctx.get_id();

        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.get_mut(&query_id) {
            None => Err(ErrorCode::Internal("Query not exists.")),
            Some(query_coordinator) => {
                if !query_coordinator.fragment_outbounds.is_empty() {
                    unreachable!(
                        "query_coordinator.fragment_senders is not empty: {:?}",
                        query_coordinator
                            .fragment_outbounds
                            .keys()
                            .collect::<Vec<_>>()
                    );
                }

                if !query_coordinator.fragment_receivers.is_empty() {
                    unreachable!(
                        "query_coordinator.fragment_receivers is not empty: {:?}",
                        query_coordinator
                            .fragment_receivers
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
                        ctx.get_exchange_manager()
                            .on_finished_query(&query_id, info.res.clone().err());
                        statistics_receiver.wait_shutdown()
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

    pub fn take_fragment_outbound_streams(
        &self,
        params: &ExchangeParams,
    ) -> Result<Vec<(String, Option<OutboundStreamRef>)>> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.get_mut(&params.get_query_id()) {
            None => Err(ErrorCode::Internal("Query not exists.")),
            Some(coordinator) => params.take_outbound_streams(&mut coordinator.fragment_outbounds),
        }
    }

    pub(super) fn take_packet_receivers(
        &self,
        params: &ExchangeParams,
    ) -> Result<Vec<PacketReceiver>> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.get_mut(&params.get_query_id()) {
            None => Err(ErrorCode::Internal("Query not exists.")),
            Some(coordinator) => params.take_packet_receivers(&mut coordinator.fragment_receivers),
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

pub(crate) struct QueryCoordinator {
    info: Option<QueryInfo>,
    fragments_coordinator: HashMap<usize, Box<FragmentCoordinator>>,
    /// True when this node is the request server (coordinator) for the query.
    /// The coordinator starts the profiler manually in ExplainPerfInterpreter,
    /// so execute_pipeline() must not start a second one.
    is_request_server: bool,

    statistics_senders: HashMap<String, OutboundStreamRef>,
    statistics_receivers: HashMap<String, PacketReceiver>,
    fragment_outbounds: HashMap<String, Vec<OutboundStreamRef>>,
    fragment_receivers: HashMap<String, Vec<PacketReceiver>>,
    inbound_channel_sets: HashMap<String, Arc<ExchangePacketReceiverSet>>,
    /// Logical inbound streams, keyed by the channel id and source node that opened them.
    /// Statistics streams use an empty channel id, since there is one per source node.
    new_flight_inbound_sources: HashMap<(String, String), Arc<ReliableInboundSource>>,
    new_flight_fragment_outbounds: HashMap<String, HashMap<String, PendingReliableOutbound>>,
    ping_pong_exchanges: HashMap<String, HashMap<String, PingPongExchange>>,
}

impl QueryCoordinator {
    pub fn create() -> QueryCoordinator {
        QueryCoordinator {
            info: None,
            is_request_server: false,
            fragment_outbounds: HashMap::new(),
            fragment_receivers: HashMap::new(),
            statistics_senders: HashMap::new(),
            statistics_receivers: HashMap::new(),
            fragments_coordinator: HashMap::new(),
            inbound_channel_sets: HashMap::new(),
            new_flight_inbound_sources: HashMap::new(),
            new_flight_fragment_outbounds: HashMap::new(),
            ping_pong_exchanges: HashMap::new(),
        }
    }

    pub fn add_statistics_exchange(
        &mut self,
        target: String,
    ) -> Result<Receiver<std::result::Result<FlightData, Status>>> {
        let (tx, rx) = async_channel::bounded(8);
        match self
            .statistics_senders
            .insert(target, LegacyOutbound::create(tx))
        {
            None => Ok(rx),
            Some(_) => Err(ErrorCode::Internal(
                "statistics exchanges can only have one",
            )),
        }
    }

    pub fn add_statistics_exchanges(
        &mut self,
        exchanges: HashMap<String, LegacyInbound>,
    ) -> Result<()> {
        for (source, exchange) in exchanges {
            if self
                .statistics_receivers
                .insert(source, PacketReceiver::from_legacy(exchange))
                .is_some()
            {
                return Err(ErrorCode::Internal(
                    "Internal error, statistics exchange can only have one.",
                ));
            }
        }

        Ok(())
    }

    pub fn add_statistics_senders(
        &mut self,
        senders: HashMap<String, OutboundStreamRef>,
    ) -> Result<()> {
        for (target, sender) in senders {
            if self.statistics_senders.insert(target, sender).is_some() {
                return Err(ErrorCode::Internal(
                    "Internal error, statistics exchange can only have one.",
                ));
            }
        }
        Ok(())
    }

    pub fn register_flight_channel_sender(
        &mut self,
        channel_id: String,
    ) -> Result<Receiver<std::result::Result<FlightData, Status>>> {
        let (tx, rx) = async_channel::bounded(8);
        match self.fragment_outbounds.entry(channel_id) {
            Entry::Occupied(mut v) => {
                v.get_mut().push(LegacyOutbound::create(tx));
            }
            Entry::Vacant(v) => {
                v.insert(vec![LegacyOutbound::create(tx)]);
            }
        }

        Ok(rx)
    }

    pub fn register_fragment_receivers(
        &mut self,
        channels: HashMap<String, Vec<LegacyInbound>>,
    ) -> Result<()> {
        for (id, exchanges) in channels.into_iter() {
            match self.fragment_receivers.entry(id) {
                Entry::Occupied(mut v) => {
                    v.get_mut()
                        .extend(exchanges.into_iter().map(PacketReceiver::from_legacy));
                }
                Entry::Vacant(v) => {
                    v.insert(
                        exchanges
                            .into_iter()
                            .map(PacketReceiver::from_legacy)
                            .collect(),
                    );
                }
            }
        }

        Ok(())
    }

    fn register_new_flight_fragment_outbound(
        &mut self,
        exchange_id: String,
        target_id: String,
        outbound: PendingReliableOutbound,
    ) {
        self.new_flight_fragment_outbounds
            .entry(exchange_id)
            .or_default()
            .insert(target_id, outbound);
    }

    pub fn register_ping_pong_exchanges(
        &mut self,
        exchanges: HashMap<String, HashMap<String, PingPongExchange>>,
    ) {
        for (channel, pps) in exchanges {
            match self.ping_pong_exchanges.entry(channel) {
                Entry::Occupied(mut v) => {
                    v.get_mut().extend(pps);
                }
                Entry::Vacant(v) => {
                    v.insert(pps);
                }
            }
        }
    }

    /// Create a NetworkInboundSender for a new do_exchange connection.
    ///
    /// The `num_threads` value is provided by the coordinator via DoExchangeParams.
    fn create_inbound_sender(
        &mut self,
        channel_id: &str,
        num_threads: usize,
    ) -> Result<NetworkInboundSender> {
        let channel_set = self.get_or_create_inbound_channel_set(channel_id, num_threads)?;

        Ok(NetworkInboundSender::new(
            &channel_set,
            MAX_INBOUND_BYTES_PER_CONNECTION,
        ))
    }

    /// Opens (or re-attaches to) the logical inbound stream for one source node.
    ///
    /// A reconnecting source finds the existing stream and resumes it, so the delivery target is
    /// created only on first attach.
    fn open_new_flight_inbound_connection(
        &mut self,
        channel_id: &str,
        source_id: &str,
        num_threads: usize,
        stream: NewFlightStream,
        receiver_lease: Duration,
    ) -> Result<ReliableInboundConnection> {
        let key = (channel_id.to_string(), source_id.to_string());
        let source = match self.new_flight_inbound_sources.entry(key) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let delivery = match stream {
                    NewFlightStream::Fragment => {
                        if num_threads != 1 {
                            return Err(ErrorCode::Internal(format!(
                                "New Flight fragment stream {} has {} lanes, expected 1",
                                channel_id, num_threads
                            )));
                        }
                        let channel_set = Arc::new(ExchangePacketReceiverSet::new(1));
                        let receiver = channel_set.receivers[0].clone();
                        self.fragment_receivers
                            .entry(channel_id.to_string())
                            .or_default()
                            .push(PacketReceiver::from_inbound_queue(receiver));
                        Arc::new(NetworkInboundSender::new(
                            &channel_set,
                            MAX_INBOUND_BYTES_PER_CONNECTION,
                        )) as Arc<dyn InboundDelivery>
                    }
                    NewFlightStream::Exchange => {
                        let channel_set = Self::inbound_channel_set(
                            &mut self.inbound_channel_sets,
                            channel_id,
                            num_threads,
                        )?;
                        Arc::new(NetworkInboundSender::new(
                            &channel_set,
                            MAX_INBOUND_BYTES_PER_CONNECTION,
                        )) as Arc<dyn InboundDelivery>
                    }
                    NewFlightStream::Merge => {
                        if num_threads != 1 {
                            return Err(ErrorCode::Internal(format!(
                                "New Flight merge stream {} has {} lanes, expected 1",
                                channel_id, num_threads
                            )));
                        }
                        let channel_set = Arc::new(ExchangePacketReceiverSet::new(1));
                        let receiver = channel_set.receivers[0].clone();
                        self.fragment_receivers
                            .entry(channel_id.to_string())
                            .or_default()
                            .push(PacketReceiver::from_inbound_queue(receiver));
                        Arc::new(NetworkInboundSender::new(
                            &channel_set,
                            MAX_INBOUND_BYTES_PER_CONNECTION,
                        )) as Arc<dyn InboundDelivery>
                    }
                    NewFlightStream::Statistics => {
                        let (delivery, receiver) =
                            StatisticsDelivery::create(STATISTICS_QUEUE_CAPACITY);
                        if self
                            .statistics_receivers
                            .insert(
                                source_id.to_string(),
                                PacketReceiver::from_result_queue(receiver),
                            )
                            .is_some()
                        {
                            return Err(ErrorCode::Internal(
                                "statistics exchange source was admitted twice",
                            ));
                        }
                        delivery
                    }
                };

                entry
                    .insert(Arc::new(ReliableInboundSource::new(
                        delivery,
                        receiver_lease,
                        format!("channel_id={}, source_id={}", channel_id, source_id),
                    )))
                    .clone()
            }
        };

        Ok(source.connect(
            GlobalIORuntime::instance(),
            ErrorCode::CannotConnectNode(format!(
                "New Flight source {} for channel {} did not reconnect before its lease expired",
                source_id, channel_id
            )),
        ))
    }

    fn get_or_create_inbound_channel_set(
        &mut self,
        channel_id: &str,
        num_threads: usize,
    ) -> Result<Arc<ExchangePacketReceiverSet>> {
        Self::inbound_channel_set(&mut self.inbound_channel_sets, channel_id, num_threads)
    }

    /// Takes the map rather than `&mut self` so callers can hold a borrow on another field.
    fn inbound_channel_set(
        channel_sets: &mut HashMap<String, Arc<ExchangePacketReceiverSet>>,
        channel_id: &str,
        num_threads: usize,
    ) -> Result<Arc<ExchangePacketReceiverSet>> {
        let channel_set = channel_sets
            .entry(channel_id.to_string())
            .or_insert_with(|| Arc::new(ExchangePacketReceiverSet::new(num_threads)))
            .clone();
        if channel_set.receivers.len() != num_threads {
            return Err(ErrorCode::Internal(format!(
                "ExchangePacketReceiverSet {} has {} channels, expected {}",
                channel_id,
                channel_set.receivers.len(),
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
        if let Some(cause) = &cause {
            for source in self.new_flight_inbound_sources.values() {
                source.fail(cause.clone());
            }
        }
        self.new_flight_fragment_outbounds.clear();
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

        assert!(self.fragment_outbounds.is_empty() && self.fragment_receivers.is_empty());
        let info_mut = self.info.as_mut().expect("Query info is None");
        info_mut.query_executor = Some(executor.clone());

        let query_id = info_mut.query_id.clone();
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
                    exchange_manager.on_finished_query(&query_id, error);
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
