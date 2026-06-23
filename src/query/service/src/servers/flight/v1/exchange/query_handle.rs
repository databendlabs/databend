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
use std::sync::Arc;

use arrow_flight::FlightData;
use arrow_flight::flight_service_client::FlightServiceClient;
use async_channel::Receiver;
use databend_common_base::JoinHandle;
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
use log::warn;
use parking_lot::Mutex;
use petgraph::Direction;
use petgraph::prelude::EdgeRef;
use tokio::sync::oneshot;
use tonic::Status;

use super::DataExchange;
use super::DefaultExchangeInjector;
use super::ExchangeInjector;
use super::exchange_params::BroadcastExchangeParams;
use super::exchange_params::ExchangeParams;
use super::exchange_params::GlobalExchangeParams;
use super::exchange_params::MergeExchangeParams;
use super::exchange_params::ShuffleExchangeParams;
use super::exchange_sink::ExchangeSink;
use super::exchange_transform::ExchangeTransform;
use super::statistics_receiver::StatisticsReceiver;
use super::statistics_sender::StatisticsSender;
use crate::physical_plans::PhysicalPlan;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::PipelineBuilder;
use crate::pipelines::attach_runtime_filter_logger;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::servers::flight::DoExchangeParams;
use crate::servers::flight::FlightClient;
use crate::servers::flight::FlightExchange;
use crate::servers::flight::FlightReceiver;
use crate::servers::flight::FlightSender;
use crate::servers::flight::keep_alive::build_keep_alive_config;
use crate::servers::flight::v1::network::NetworkInboundChannelSet;
use crate::servers::flight::v1::network::NetworkInboundSender;
use crate::servers::flight::v1::network::PingPongExchange;
use crate::servers::flight::v1::packets::Edge;
use crate::servers::flight::v1::packets::QueryEnv;
use crate::servers::flight::v1::packets::QueryFragment;
use crate::servers::flight::v1::packets::QueryFragments;
use crate::sessions::QueryContext;
use crate::sessions::TableContextPerf;
use crate::sessions::TableContextQueryIdentity;
use crate::sessions::TableContextSettings;

enum QueryExchange {
    Fragment {
        channel: String,
        exchange: FlightExchange,
    },
    Statistics {
        source: String,
        exchange: FlightExchange,
    },
    PingPong {
        exchange_id: String,
        target_id: String,
        exchange: PingPongExchange,
    },
}

pub(crate) async fn create_flight_client(
    address: String,
    use_current_rt: bool,
    keep_alive: FlightKeepAliveParams,
) -> Result<FlightClient> {
    let config = GlobalConfig::instance();
    let keep_alive_config = build_keep_alive_config(keep_alive);
    let task = async move {
        match config.tls_query_cli_enabled() {
            true => Ok(FlightClient::new(FlightServiceClient::new(
                ConnectionFactory::create_rpc_channel(
                    address.to_owned(),
                    None,
                    Some(config.query.to_grpc_tls_config()),
                    keep_alive_config,
                )
                .await?,
            ))),
            false => Ok(FlightClient::new(FlightServiceClient::new(
                ConnectionFactory::create_rpc_channel(
                    address.to_owned(),
                    None,
                    None,
                    keep_alive_config,
                )
                .await?,
            ))),
        }
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

pub(crate) struct QueryHandle {
    pub(crate) query_id: String,
    pub(crate) exchanges: QueryExchangeRegistry,
    pub(crate) runtime: QueryRuntime,
}

pub(crate) struct QueryRuntime {
    state: Mutex<QueryRuntimeState>,
}

struct QueryRuntimeState {
    info: Option<QueryInfo>,
    fragments: HashMap<usize, Box<FragmentRuntime>>,
    is_request_server: bool,
    pending_shutdown: Option<Option<ErrorCode>>,
}

impl QueryRuntime {
    pub(crate) fn create() -> Self {
        QueryRuntime {
            state: Mutex::new(QueryRuntimeState {
                info: None,
                fragments: HashMap::new(),
                is_request_server: false,
                pending_shutdown: None,
            }),
        }
    }

    pub(crate) async fn init_env(
        &self,
        env: &QueryEnv,
        ctx: Option<Arc<QueryContext>>,
        exchanges: &QueryExchangeRegistry,
        remove_leak_query_worker: Option<JoinHandle<()>>,
    ) -> Result<()> {
        if env.perf_config.is_perf_active() {
            if let Some(ctx) = ctx.as_ref() {
                let mut perf_config = env.perf_config.clone();
                if GlobalConfig::instance().query.node_id == env.request_server_id {
                    perf_config.profiler_enabled = false;
                }
                ctx.set_perf_config(perf_config);
            }
        }

        let settings = match ctx.as_ref() {
            Some(ctx) => ctx.get_settings(),
            None => env.settings.clone(),
        };
        let keep_alive = settings.get_flight_keep_alive_params()?;
        let with_cur_rt = env.create_rpc_clint_with_current_rt;
        let config = GlobalConfig::instance();

        let mut request_exchanges = HashMap::new();
        let mut target_exchanges = HashMap::<String, Vec<FlightExchange>>::new();
        let mut ping_pong_exchanges = HashMap::<String, HashMap<String, PingPongExchange>>::new();

        for index in env.dataflow_diagram.node_indices() {
            if env.dataflow_diagram[index].id != config.query.node_id {
                continue;
            }

            let mut flight_exchanges: Vec<
                std::pin::Pin<Box<dyn std::future::Future<Output = Result<QueryExchange>> + Send>>,
            > = vec![];

            for edge in env
                .dataflow_diagram
                .edges_directed(index, Direction::Incoming)
            {
                let source = env.dataflow_diagram[edge.source()].clone();
                let target = env.dataflow_diagram[edge.target()].clone();
                let edge = edge.weight().clone();
                let query_id = env.query_id.clone();
                let address = source.flight_address.clone();
                let keep_alive_params = keep_alive;

                match edge {
                    Edge::Fragment(channel) => {
                        flight_exchanges.push(Box::pin(async move {
                            let mut flight_client =
                                create_flight_client(address, with_cur_rt, keep_alive_params)
                                    .await?;
                            Ok::<QueryExchange, ErrorCode>(QueryExchange::Fragment {
                                channel: channel.clone(),
                                exchange: flight_client.do_get(&query_id, &channel).await?,
                            })
                        }));
                    }
                    Edge::Statistics => {
                        flight_exchanges.push(Box::pin(async move {
                            let mut flight_client =
                                create_flight_client(address, with_cur_rt, keep_alive_params)
                                    .await?;
                            Ok::<QueryExchange, ErrorCode>(QueryExchange::Statistics {
                                source: source.id.clone(),
                                exchange: flight_client
                                    .request_server_exchange(&query_id, &target.id)
                                    .await?,
                            })
                        }));
                    }
                    Edge::ExchangeFragment { .. } => {}
                }
            }

            for edge in env
                .dataflow_diagram
                .edges_directed(index, Direction::Outgoing)
            {
                let target = env.dataflow_diagram[edge.target()].clone();
                let edge = edge.weight().clone();
                if let Edge::ExchangeFragment {
                    exchange_id,
                    channels,
                } = edge
                {
                    let target_id = target.id.clone();
                    let query_id = env.query_id.clone();
                    let address = target.flight_address.clone();
                    let keep_alive_params = keep_alive;
                    let num_threads = channels.len();

                    flight_exchanges.push(Box::pin(async move {
                        let (send_tx, response_stream) = {
                            let mut flight_client =
                                create_flight_client(address, with_cur_rt, keep_alive_params)
                                    .await?;
                            let (send_tx, send_rx) = async_channel::bounded(1);
                            let response_stream = flight_client
                                .do_exchange(send_rx, DoExchangeParams {
                                    query_id,
                                    num_threads,
                                    exchange_id: exchange_id.clone(),
                                })
                                .await
                                .map_err(|e| {
                                    ErrorCode::Internal(format!("PingPong connect failed: {}", e))
                                })?;
                            Ok::<_, ErrorCode>((send_tx, response_stream))
                        }?;

                        Ok::<QueryExchange, ErrorCode>(QueryExchange::PingPong {
                            target_id,
                            exchange_id,
                            exchange: PingPongExchange::from_parts(
                                num_threads,
                                send_tx,
                                response_stream,
                            ),
                        })
                    }));
                }
            }

            for flight_exchange in futures::future::try_join_all(flight_exchanges).await? {
                match flight_exchange {
                    QueryExchange::Fragment { channel, exchange } => {
                        target_exchanges.entry(channel).or_default().push(exchange);
                    }
                    QueryExchange::Statistics { source, exchange } => {
                        request_exchanges.insert(source, exchange);
                    }
                    QueryExchange::PingPong {
                        exchange_id,
                        target_id,
                        exchange,
                    } => {
                        ping_pong_exchanges
                            .entry(exchange_id)
                            .or_default()
                            .insert(target_id, exchange);
                    }
                }
            }

            exchanges.register_flight_channel_receivers(target_exchanges)?;
            exchanges.register_ping_pong_exchanges(ping_pong_exchanges);
            exchanges.add_statistics_exchanges(request_exchanges)?;

            let mut state = self.state.lock();
            state.info = Self::create_info(ctx, remove_leak_query_worker)?;
            state.is_request_server = config.query.node_id == env.request_server_id;
            return Ok(());
        }

        Ok(())
    }

    fn create_info(
        query_ctx: Option<Arc<QueryContext>>,
        remove_leak_query_worker: Option<JoinHandle<()>>,
    ) -> Result<Option<QueryInfo>> {
        match query_ctx {
            None => Ok(None),
            Some(query_ctx) => {
                let query_id = query_ctx.get_id();

                Ok(Some(QueryInfo {
                    query_ctx,
                    query_executor: None,
                    query_id: query_id.clone(),
                    started: false,
                    current_executor: GlobalConfig::instance().query.node_id.clone(),
                    remove_leak_query_worker,
                }))
            }
        }
    }

    pub(crate) fn set_ctx(
        &self,
        ctx: Arc<QueryContext>,
        remove_leak_query_worker: Option<JoinHandle<()>>,
    ) -> Result<()> {
        let mut state = self.state.lock();
        match state.info.as_mut() {
            Some(info) => {
                info.query_ctx = ctx;
                if info.remove_leak_query_worker.is_none() {
                    info.remove_leak_query_worker = remove_leak_query_worker;
                }
                Ok(())
            }
            None => {
                state.info = Self::create_info(Some(ctx), remove_leak_query_worker)?;
                Ok(())
            }
        }
    }

    pub(crate) fn query_ctx(&self) -> Result<Arc<QueryContext>> {
        self.state
            .lock()
            .info
            .as_ref()
            .map(|info| info.query_ctx.clone())
            .ok_or_else(|| ErrorCode::Internal("Query context is not initialized."))
    }

    pub(crate) fn running_executor(&self) -> Option<Arc<PipelineCompleteExecutor>> {
        self.state
            .lock()
            .info
            .as_ref()
            .and_then(|info| info.query_executor.clone())
    }

    pub(crate) fn is_started(&self) -> bool {
        self.state
            .lock()
            .info
            .as_ref()
            .is_some_and(|info| info.started)
    }

    pub(crate) fn prepare_fragments(&self, fragments: &QueryFragments) -> Result<()> {
        let fragment_ids = fragments
            .fragments
            .iter()
            .map(|fragment| fragment.fragment_id)
            .collect::<Vec<_>>();

        let query_context = {
            let mut state = self.state.lock();
            let info = state
                .info
                .as_mut()
                .ok_or_else(|| ErrorCode::Internal("Query context is not initialized."))?;
            let query_context = info.query_ctx.clone();

            if let Some(leak_worker) = info.remove_leak_query_worker.take() {
                leak_worker.abort();
            }

            for fragment in &fragments.fragments {
                state.fragments.insert(
                    fragment.fragment_id.to_owned(),
                    FragmentRuntime::create(fragment),
                );
            }

            query_context
        };

        for fragment_id in fragment_ids {
            let Some(mut fragment) = self.state.lock().fragments.remove(&fragment_id) else {
                continue;
            };

            // ExchangeSource pipeline build may recursively request another local fragment.
            // Keep QueryRuntime state unlocked while building to avoid self-deadlock.
            fragment.prepare_pipeline(query_context.clone())?;

            if self
                .state
                .lock()
                .fragments
                .insert(fragment_id, fragment)
                .is_some()
            {
                return Err(ErrorCode::Internal(format!(
                    "Duplicate query fragment {} while preparing fragments",
                    fragment_id
                )));
            }
        }

        Ok(())
    }

    pub(crate) fn root_pipeline(
        &self,
        ctx: Arc<QueryContext>,
        exchanges: &QueryExchangeRegistry,
        main_fragment_id: usize,
        fragment_ids: Vec<usize>,
    ) -> Result<PipelineBuildResult> {
        let injector = DefaultExchangeInjector::create();
        let mut build_res = self.subscribe_fragment(&ctx, main_fragment_id, injector.clone())?;

        for fragment_id in fragment_ids {
            let sub_build_res = self.subscribe_fragment(&ctx, fragment_id, injector.clone())?;
            build_res
                .sources_pipelines
                .push(sub_build_res.main_pipeline);
            build_res
                .sources_pipelines
                .extend(sub_build_res.sources_pipelines);
        }

        let statistics_receiver =
            StatisticsReceiver::spawn_receiver(&ctx, exchanges.take_statistics_exchanges())?;
        let statistics_receiver: Mutex<StatisticsReceiver> = Mutex::new(statistics_receiver);

        build_res
            .main_pipeline
            .set_on_finished(basic_callback(move |info: &ExecutionInfo| {
                let query_id = ctx.get_id();
                let mut statistics_receiver = statistics_receiver.lock();

                statistics_receiver.shutdown(info.res.is_err());
                ctx.get_exchange_manager()
                    .on_finished_query(&query_id, info.res.clone().err());
                statistics_receiver.wait_shutdown()
            }));

        build_res
            .main_pipeline
            .set_on_finished(move |info: &ExecutionInfo| match &info.res {
                Ok(_) => Ok(()),
                Err(error_code) => Err(error_code.clone()),
            });

        Ok(build_res)
    }

    pub(crate) fn subscribe_fragment(
        &self,
        ctx: &Arc<QueryContext>,
        fragment_id: usize,
        injector: Arc<dyn ExchangeInjector>,
    ) -> Result<PipelineBuildResult> {
        let (mut fragment, exchange_info) = {
            let mut state = self.state.lock();
            let Some(fragment) = state.fragments.remove(&fragment_id) else {
                return Err(ErrorCode::Unimplemented(
                    "ExchangeSource is unimplemented, maybe query fragment circular dependency.",
                ));
            };

            let exchange_info = state
                .info
                .as_ref()
                .map(ExchangeBuildInfo::from_query_info)
                .ok_or_else(|| ErrorCode::Internal("Query context is not initialized."))?;
            (fragment, exchange_info)
        };

        // ExchangeSource pipeline build may recursively call get_fragment_source().
        // The state lock must not be held across this call.
        fragment.prepare_pipeline(ctx.clone())?;

        if fragment.pipeline_build_res.is_none() {
            return Err(ErrorCode::Internal(
                "Pipeline is none, maybe query fragment circular dependency.",
            ));
        }

        if fragment.data_exchange.is_none() {
            return Ok(fragment.pipeline_build_res.unwrap());
        }

        let exchange_params = fragment
            .create_exchange_params(
                &exchange_info,
                fragment
                    .pipeline_build_res
                    .as_ref()
                    .map(|x| x.exchange_injector.clone())
                    .ok_or_else(|| {
                        ErrorCode::Internal("Pipeline build result is none, It's a bug")
                    })?,
            )?
            .unwrap();
        let mut build_res = fragment.pipeline_build_res.unwrap();

        ExchangeTransform::via(
            ctx,
            &exchange_params,
            &mut build_res.main_pipeline,
            injector,
        )?;

        Ok(build_res)
    }

    pub(crate) fn execute(&self, exchanges: &QueryExchangeRegistry) -> Result<()> {
        let (exchange_info, perf_guard, max_threads, mut fragments) = {
            let mut state = self.state.lock();
            let is_request_server = state.is_request_server;
            let (exchange_info, perf_guard, max_threads) = {
                let info = state
                    .info
                    .as_mut()
                    .ok_or_else(|| ErrorCode::Internal("Query info is None"))?;
                let pc = info.query_ctx.get_perf_config();
                let perf_guard = if pc.profiler_enabled && !is_request_server {
                    Some(QueryPerf::start(pc.frequency)?)
                } else {
                    None
                };

                if !info.started {
                    info.started = true;
                    if let Some(leak_worker) = info.remove_leak_query_worker.take() {
                        leak_worker.abort();
                    }
                }

                (
                    ExchangeBuildInfo::from_query_info(info),
                    perf_guard,
                    info.query_ctx.get_settings().get_max_threads()?,
                )
            };

            (
                exchange_info,
                perf_guard,
                max_threads,
                std::mem::take(&mut state.fragments),
            )
        };

        if fragments.is_empty() {
            return Ok(());
        }

        let mut pipelines = Vec::with_capacity(fragments.len());
        for fragment in fragments.values_mut() {
            let params = fragment.create_exchange_params(
                &exchange_info,
                fragment
                    .pipeline_build_res
                    .as_ref()
                    .map(|x| x.exchange_injector.clone())
                    .ok_or_else(|| {
                        ErrorCode::Internal("Pipeline build result is none, It's a bug")
                    })?,
            )?;

            if let Some(mut build_res) = fragment.pipeline_build_res.take() {
                build_res.set_max_threads(max_threads as usize);

                if build_res.main_pipeline.is_pulling_pipeline()? {
                    let Some(params) = params else {
                        return Err(ErrorCode::Internal(
                            "pipeline is pulling pipeline, but exchange params is none",
                        ));
                    };
                    ExchangeSink::via(
                        &exchange_info.query_ctx,
                        &params,
                        &mut build_res.main_pipeline,
                    )?;
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

        let settings = ExecutorSettings::try_create(exchange_info.query_ctx.clone())?;
        let executor = PipelineCompleteExecutor::from_pipelines(pipelines, settings)?;
        let query_id = exchange_info.query_id.clone();
        let query_ctx = exchange_info.query_ctx.clone();
        query_ctx.set_executor(executor.get_inner())?;
        self.publish_executor(executor.clone())?;

        let request_server_exchanges = exchanges.take_statistics_exchanges();
        if request_server_exchanges.len() != 1 {
            return Err(ErrorCode::Internal(
                "Request server must less than 1 if is not request server.",
            ));
        }

        let ctx = query_ctx.clone();
        let (_, request_server_exchange) = request_server_exchanges.into_iter().next().unwrap();
        let mut statistics_sender = StatisticsSender::spawn(
            &query_id,
            ctx,
            request_server_exchange,
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

    // Pipeline/executor construction runs outside the runtime lock. A shutdown
    // request may arrive during that window, so check the locked state again
    // after publishing the executor and apply any pending finish immediately.
    fn publish_executor(&self, executor: Arc<PipelineCompleteExecutor>) -> Result<()> {
        let pending_shutdown = {
            let mut state = self.state.lock();
            state
                .info
                .as_mut()
                .ok_or_else(|| ErrorCode::Internal("Query info is None"))?
                .query_executor = Some(executor.clone());
            state.pending_shutdown.clone()
        };

        if let Some(cause) = pending_shutdown {
            executor.finish(cause);
        }

        Ok(())
    }

    pub(crate) fn shutdown(&self, cause: Option<ErrorCode>) {
        let mut state = self.state.lock();
        if state.pending_shutdown.is_none() {
            state.pending_shutdown = Some(cause.clone());
        }

        if let Some(query_info) = &mut state.info {
            if let Some(query_executor) = &query_info.query_executor {
                query_executor.finish(cause);
            }

            if let Some(worker) = query_info.remove_leak_query_worker.take() {
                worker.abort();
            }
        }
    }
}

struct QueryInfo {
    query_id: String,
    started: bool,
    current_executor: String,
    query_ctx: Arc<QueryContext>,
    remove_leak_query_worker: Option<JoinHandle<()>>,
    query_executor: Option<Arc<PipelineCompleteExecutor>>,
}

struct ExchangeBuildInfo {
    query_id: String,
    current_executor: String,
    query_ctx: Arc<QueryContext>,
}

impl ExchangeBuildInfo {
    fn from_query_info(info: &QueryInfo) -> Self {
        ExchangeBuildInfo {
            query_id: info.query_id.clone(),
            current_executor: info.current_executor.clone(),
            query_ctx: info.query_ctx.clone(),
        }
    }
}

struct FragmentRuntime {
    initialized: bool,
    fragment_id: usize,
    physical_plan: PhysicalPlan,
    data_exchange: Option<DataExchange>,
    pipeline_build_res: Option<PipelineBuildResult>,
}

impl FragmentRuntime {
    fn create(packet: &QueryFragment) -> Box<FragmentRuntime> {
        Box::new(FragmentRuntime {
            initialized: false,
            physical_plan: packet.physical_plan.clone(),
            fragment_id: packet.fragment_id,
            data_exchange: packet.data_exchange.clone(),
            pipeline_build_res: None,
        })
    }

    fn create_exchange_params(
        &self,
        info: &ExchangeBuildInfo,
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

    fn prepare_pipeline(&mut self, ctx: Arc<QueryContext>) -> Result<()> {
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

pub(crate) struct QueryExchangeRegistry {
    state: Mutex<QueryExchangeState>,
}

struct QueryExchangeState {
    statistics_exchanges: HashMap<String, FlightExchange>,
    flight_data_senders: HashMap<String, Vec<FlightSender>>,
    flight_data_receivers: HashMap<String, Vec<FlightReceiver>>,
    inbound_channel_sets: HashMap<String, Arc<NetworkInboundChannelSet>>,
    ping_pong_exchanges: HashMap<String, HashMap<String, PingPongExchange>>,
}

impl QueryExchangeRegistry {
    pub(crate) fn create() -> Self {
        QueryExchangeRegistry {
            state: Mutex::new(QueryExchangeState {
                statistics_exchanges: HashMap::new(),
                flight_data_senders: HashMap::new(),
                flight_data_receivers: HashMap::new(),
                inbound_channel_sets: HashMap::new(),
                ping_pong_exchanges: HashMap::new(),
            }),
        }
    }

    pub(crate) fn add_statistics_exchange(
        &self,
        target: String,
    ) -> Result<Receiver<std::result::Result<FlightData, Status>>> {
        let (tx, rx) = async_channel::bounded(8);
        let mut state = self.state.lock();
        match state.statistics_exchanges.entry(target) {
            Entry::Occupied(_) => Err(ErrorCode::Internal(
                "statistics exchanges can only have one",
            )),
            Entry::Vacant(v) => {
                v.insert(FlightExchange::create_sender(tx));
                Ok(rx)
            }
        }
    }

    pub(crate) fn add_statistics_exchanges(
        &self,
        exchanges: HashMap<String, FlightExchange>,
    ) -> Result<()> {
        let mut state = self.state.lock();
        for (source, exchange) in exchanges {
            if state
                .statistics_exchanges
                .insert(source, exchange)
                .is_some()
            {
                return Err(ErrorCode::Internal(
                    "Internal error, statistics exchange can only have one.",
                ));
            }
        }

        Ok(())
    }

    pub(crate) fn take_statistics_exchanges(&self) -> HashMap<String, FlightExchange> {
        std::mem::take(&mut self.state.lock().statistics_exchanges)
    }

    pub(crate) fn register_flight_channel_sender(
        &self,
        channel_id: String,
    ) -> Result<Receiver<std::result::Result<FlightData, Status>>> {
        let (tx, rx) = async_channel::bounded(8);
        let mut state = self.state.lock();
        match state.flight_data_senders.entry(channel_id) {
            Entry::Occupied(mut v) => {
                v.get_mut()
                    .push(FlightExchange::create_sender(tx).convert_to_sender());
            }
            Entry::Vacant(v) => {
                v.insert(vec![FlightExchange::create_sender(tx).convert_to_sender()]);
            }
        }

        Ok(rx)
    }

    pub(crate) fn register_flight_channel_receivers(
        &self,
        channels: HashMap<String, Vec<FlightExchange>>,
    ) -> Result<()> {
        let mut state = self.state.lock();
        for (id, exchanges) in channels {
            match state.flight_data_receivers.entry(id) {
                Entry::Occupied(mut v) => {
                    v.get_mut().extend(
                        exchanges
                            .into_iter()
                            .map(FlightExchange::convert_to_receiver),
                    );
                }
                Entry::Vacant(v) => {
                    v.insert(
                        exchanges
                            .into_iter()
                            .map(FlightExchange::convert_to_receiver)
                            .collect(),
                    );
                }
            }
        }

        Ok(())
    }

    pub(crate) fn take_flight_sender(
        &self,
        params: &ExchangeParams,
    ) -> Result<Vec<(String, FlightSender)>> {
        params.take_flight_sender(&mut self.state.lock().flight_data_senders)
    }

    pub(crate) fn take_flight_receiver(
        &self,
        params: &ExchangeParams,
    ) -> Result<Vec<FlightReceiver>> {
        params.take_flight_receiver(&mut self.state.lock().flight_data_receivers)
    }

    pub(crate) fn register_ping_pong_exchanges(
        &self,
        exchanges: HashMap<String, HashMap<String, PingPongExchange>>,
    ) {
        let mut state = self.state.lock();
        for (channel, pps) in exchanges {
            match state.ping_pong_exchanges.entry(channel) {
                Entry::Occupied(mut v) => {
                    v.get_mut().extend(pps);
                }
                Entry::Vacant(v) => {
                    v.insert(pps);
                }
            }
        }
    }

    pub(crate) fn take_ping_pong_exchanges(
        &self,
        channel_id: &str,
    ) -> HashMap<String, PingPongExchange> {
        self.state
            .lock()
            .ping_pong_exchanges
            .remove(channel_id)
            .unwrap_or_default()
    }

    pub(crate) fn create_inbound_sender(
        &self,
        channel_id: &str,
        num_threads: usize,
    ) -> Result<NetworkInboundSender> {
        let mut state = self.state.lock();
        let channel_set = state
            .inbound_channel_sets
            .entry(channel_id.to_string())
            .or_insert_with(|| Arc::new(NetworkInboundChannelSet::new(num_threads)))
            .clone();

        Ok(NetworkInboundSender::new(&channel_set, 20 * 1024 * 1024))
    }

    pub(crate) fn get_exchange_channel_set(
        &self,
        channel_id: &str,
    ) -> Result<Arc<NetworkInboundChannelSet>> {
        self.state
            .lock()
            .inbound_channel_sets
            .get(channel_id)
            .cloned()
            .ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "NetworkInboundChannelSet not found for channel {}",
                    channel_id
                ))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::servers::flight::v1::exchange::exchange_manager::QueriesCoordinator;

    #[test]
    fn queries_coordinator_reuses_same_handle() -> Result<()> {
        let coordinator = QueriesCoordinator::create();

        let first = coordinator.create_query("query-1".to_string())?;
        let second = coordinator.create_query("query-1".to_string())?;

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(coordinator.handles().len(), 1);
        assert_eq!(coordinator.get_query("query-1")?.query_id, "query-1");

        Ok(())
    }

    #[test]
    fn queries_coordinator_remove_returns_owned_handle() -> Result<()> {
        let coordinator = QueriesCoordinator::create();

        let handle = coordinator.create_query("query-1".to_string())?;
        assert!(coordinator.remove_query("query-1").is_some());
        assert!(coordinator.get_query("query-1").is_err());
        assert_eq!(handle.query_id, "query-1");

        Ok(())
    }

    #[test]
    fn query_exchange_registry_rejects_duplicate_statistics_target() -> Result<()> {
        let handle = QueryHandle {
            query_id: "query-1".to_string(),
            exchanges: QueryExchangeRegistry::create(),
            runtime: QueryRuntime::create(),
        };
        let exchanges = &handle.exchanges;

        let _rx = exchanges.add_statistics_exchange("target-1".to_string())?;
        assert!(
            exchanges
                .add_statistics_exchange("target-1".to_string())
                .is_err()
        );

        let statistics_exchanges = exchanges.take_statistics_exchanges();
        assert_eq!(statistics_exchanges.len(), 1);
        assert!(statistics_exchanges.contains_key("target-1"));
        assert!(exchanges.take_statistics_exchanges().is_empty());

        Ok(())
    }

    #[test]
    fn query_exchange_registry_reuses_inbound_channel_set() -> Result<()> {
        let handle = QueryHandle {
            query_id: "query-1".to_string(),
            exchanges: QueryExchangeRegistry::create(),
            runtime: QueryRuntime::create(),
        };
        let exchanges = &handle.exchanges;

        let _first_sender = exchanges.create_inbound_sender("channel-1", 2)?;
        let first_set = exchanges.get_exchange_channel_set("channel-1")?;
        let _second_sender = exchanges.create_inbound_sender("channel-1", 2)?;
        let second_set = exchanges.get_exchange_channel_set("channel-1")?;

        assert!(Arc::ptr_eq(&first_set, &second_set));

        Ok(())
    }

    #[test]
    fn query_exchange_registry_exposes_channel_operations() -> Result<()> {
        use databend_common_expression::DataSchema;

        use super::super::exchange_params::GlobalExchangeParams;

        let handle = QueryHandle {
            query_id: "query-1".to_string(),
            exchanges: QueryExchangeRegistry::create(),
            runtime: QueryRuntime::create(),
        };
        let exchanges = &handle.exchanges;

        exchanges.add_statistics_exchanges(HashMap::new())?;
        let _rx = exchanges.register_flight_channel_sender("channel-1".to_string())?;
        exchanges.register_flight_channel_receivers(HashMap::new())?;
        exchanges.register_ping_pong_exchanges(HashMap::new());
        assert!(exchanges.take_ping_pong_exchanges("channel-1").is_empty());

        let exchange_params = ExchangeParams::GlobalShuffleExchange(GlobalExchangeParams {
            query_id: "query-1".to_string(),
            executor_id: "node-1".to_string(),
            schema: Arc::new(DataSchema::empty()),
            exchange_id: "exchange-1".to_string(),
            shuffle_keys: vec![],
            destination_channels: vec![],
        });

        assert!(exchanges.take_flight_sender(&exchange_params)?.is_empty());
        assert!(exchanges.take_flight_receiver(&exchange_params)?.is_empty());

        Ok(())
    }
}
