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

use std::cell::SyncUnsafeCell;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

use common_arrow::arrow_format::flight::service::flight_service_client::FlightServiceClient;
use common_base::base::GlobalIORuntime;
use common_base::base::Singleton;
use common_base::base::Thread;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_grpc::ConnectionFactory;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use parking_lot::ReentrantMutex;

use crate::api::rpc::exchange::exchange_params::ExchangeParams;
use crate::api::rpc::exchange::exchange_params::MergeExchangeParams;
use crate::api::rpc::exchange::exchange_params::ShuffleExchangeParams;
use crate::api::rpc::exchange::exchange_sink::ExchangeSink;
use crate::api::rpc::exchange::exchange_transform::ExchangeTransform;
use crate::api::rpc::exchange::statistics_receiver::StatisticsReceiver;
use crate::api::rpc::exchange::statistics_sender::StatisticsSender;
use crate::api::rpc::flight_client::FlightExchange;
use crate::api::rpc::flight_scatter_broadcast::BroadcastFlightScatter;
use crate::api::rpc::flight_scatter_hash::HashFlightScatter;
use crate::api::rpc::flight_scatter_hash_v2::HashFlightScatterV2;
use crate::api::rpc::packets::DataPacket;
use crate::api::rpc::Packet;
use crate::api::DataExchange;
use crate::api::FlightClient;
use crate::api::FragmentPayload;
use crate::api::FragmentPlanPacket;
use crate::api::InitNodesChannelPacket;
use crate::api::QueryFragmentsPlanPacket;
use crate::interpreters::QueryFragmentActions;
use crate::interpreters::QueryFragmentsActions;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::QueryPipelineBuilder;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::executor::PipelineBuilder as PipelineBuilderV2;
use crate::Config;

pub struct DataExchangeManager {
    config: Config,
    queries_coordinator: ReentrantMutex<SyncUnsafeCell<HashMap<String, QueryCoordinator>>>,
}

static DATA_EXCHANGE_MANAGER: OnceCell<Singleton<Arc<DataExchangeManager>>> = OnceCell::new();

impl DataExchangeManager {
    pub fn init(config: Config, v: Singleton<Arc<DataExchangeManager>>) -> Result<()> {
        v.init(Arc::new(DataExchangeManager {
            config,
            queries_coordinator: ReentrantMutex::new(SyncUnsafeCell::new(HashMap::new())),
        }))?;

        DATA_EXCHANGE_MANAGER.set(v).ok();
        Ok(())
    }

    pub fn instance() -> Arc<DataExchangeManager> {
        match DATA_EXCHANGE_MANAGER.get() {
            None => panic!("DataExchangeManager is not init"),
            Some(data_exchange_manager) => data_exchange_manager.get(),
        }
    }

    // Create connections for cluster all nodes. We will push data through this connection.
    pub async fn init_nodes_channel(&self, packet: &InitNodesChannelPacket) -> Result<()> {
        let mut request_exchanges = vec![];
        let mut targets_exchanges = HashMap::new();

        let source = &packet.executor.id;
        for connection_info in &packet.connections_info {
            if connection_info.create_request_channel {
                let query_id = &packet.query_id;
                let address = &connection_info.target.flight_address;
                let mut flight_client = Self::create_client(&self.config, address).await?;
                request_exchanges.push(flight_client.request_server_exchange(query_id).await?);
            }

            for fragment in &connection_info.fragments {
                let address = &connection_info.target.flight_address;
                let mut flight_client = Self::create_client(&self.config, address).await?;

                targets_exchanges.insert(
                    (connection_info.target.id.clone(), *fragment),
                    flight_client
                        .do_exchange(&packet.query_id, source, *fragment)
                        .await?,
                );
            }
        }

        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.entry(packet.query_id.clone()) {
            Entry::Occupied(mut v) => {
                let query_coordinator = v.get_mut();
                query_coordinator.add_fragment_exchanges(targets_exchanges)?;
                query_coordinator.add_statistics_exchange(request_exchanges)
            }
            Entry::Vacant(v) => {
                let query_coordinator = v.insert(QueryCoordinator::create());
                query_coordinator.add_fragment_exchanges(targets_exchanges)?;
                query_coordinator.add_statistics_exchange(request_exchanges)
            }
        }
    }

    pub async fn create_client(config: &Config, address: &str) -> Result<FlightClient> {
        match config.tls_query_cli_enabled() {
            true => Ok(FlightClient::new(FlightServiceClient::new(
                ConnectionFactory::create_rpc_channel(
                    address.to_owned(),
                    None,
                    Some(config.query.to_rpc_client_tls_config()),
                )
                .await?,
            ))),
            false => Ok(FlightClient::new(FlightServiceClient::new(
                ConnectionFactory::create_rpc_channel(address.to_owned(), None, None).await?,
            ))),
        }
    }

    // Execute query in background
    pub fn execute_partial_query(&self, query_id: &str) -> Result<()> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.get_mut(query_id) {
            None => Err(ErrorCode::LogicalError(format!(
                "Query {} not found in cluster.",
                query_id
            ))),
            Some(coordinator) => coordinator.execute_pipeline(),
        }
    }

    // Create a pipeline based on query plan
    pub fn init_query_fragments_plan(
        &self,
        ctx: &Arc<QueryContext>,
        packet: &QueryFragmentsPlanPacket,
    ) -> Result<()> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        // TODO: When the query is not executed for a long time after submission, we need to remove it
        match queries_coordinator.get_mut(&packet.query_id) {
            None => Err(ErrorCode::LogicalError(format!(
                "Query {} not found in cluster.",
                packet.query_id
            ))),
            Some(query_coordinator) => query_coordinator.prepare_pipeline(ctx, packet),
        }
    }

    pub fn handle_statistics_exchange(&self, id: String, exchange: FlightExchange) -> Result<()> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.entry(id) {
            Entry::Occupied(mut v) => v.get_mut().add_statistics_exchange(vec![exchange]),
            Entry::Vacant(v) => v
                .insert(QueryCoordinator::create())
                .add_statistics_exchange(vec![exchange]),
        }
    }

    pub fn handle_exchange_fragment(
        &self,
        query: String,
        source: String,
        fragment: usize,
        exchange: FlightExchange,
    ) -> Result<()> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.entry(query) {
            Entry::Occupied(mut v) => v
                .get_mut()
                .add_fragment_exchange(source, fragment, exchange),
            Entry::Vacant(v) => v
                .insert(QueryCoordinator::create())
                .add_fragment_exchange(source, fragment, exchange),
        }
    }

    pub fn shutdown_query(&self, query_id: &str) {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        if let Some(query_coordinator) = queries_coordinator.get_mut(query_id) {
            query_coordinator.shutdown_query();
        }
    }

    pub fn on_finished_query(&self, query_id: &str) {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        if let Some(query_coordinator) = queries_coordinator.remove(query_id) {
            // Drop mutex guard to avoid deadlock during shutdown,
            drop(queries_coordinator_guard);

            query_coordinator.on_finished();
        }
    }

    pub async fn commit_actions(
        &self,
        ctx: Arc<QueryContext>,
        actions: QueryFragmentsActions,
    ) -> Result<PipelineBuildResult> {
        let settings = ctx.get_settings();
        let timeout = settings.get_flight_client_timeout()?;
        let root_actions = actions.get_root_actions()?;

        // Initialize channels between cluster nodes
        actions
            .get_init_nodes_channel_packets()?
            .commit(&self.config, timeout)
            .await?;

        // Submit distributed tasks to all nodes.
        let (local_query_fragments_plan_packet, query_fragments_plan_packets) =
            actions.get_query_fragments_plan_packets()?;

        // Submit tasks to other nodes
        query_fragments_plan_packets
            .commit(&self.config, timeout)
            .await?;

        // Submit tasks to localhost
        self.init_query_fragments_plan(&ctx, &local_query_fragments_plan_packet)?;

        // Get local pipeline of local task
        let build_res = self.get_root_pipeline(ctx, root_actions)?;

        actions
            .get_execute_partial_query_packets()?
            .commit(&self.config, timeout)
            .await?;
        Ok(build_res)
    }

    fn get_root_pipeline(
        &self,
        ctx: Arc<QueryContext>,
        root_actions: &QueryFragmentActions,
    ) -> Result<PipelineBuildResult> {
        let query_id = ctx.get_id();
        let fragment_id = root_actions.fragment_id;

        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.get_mut(&query_id) {
            None => Err(ErrorCode::LogicalError("Query not exists.")),
            Some(query_coordinator) => {
                query_coordinator.fragment_exchanges.clear();
                let mut build_res = query_coordinator.subscribe_fragment(&ctx, fragment_id)?;

                let exchanges = std::mem::take(&mut query_coordinator.statistics_exchanges);
                let mut statistics_receiver = StatisticsReceiver::create(ctx.clone(), exchanges);
                statistics_receiver.start();

                let statistics_receiver: Mutex<StatisticsReceiver> =
                    Mutex::new(statistics_receiver);
                build_res.main_pipeline.set_on_finished(move |may_error| {
                    let query_id = ctx.get_id();
                    let mut statistics_receiver = statistics_receiver.lock();

                    if let Some(error) = may_error {
                        // TODO: try get recv error if network error.
                        statistics_receiver.shutdown();
                        ctx.get_exchange_manager().on_finished_query(&query_id);

                        if error.code() == common_exception::ABORT_QUERY {
                            statistics_receiver.take_receive_error()?;
                        }

                        return Err(error.clone());
                    }

                    ctx.get_exchange_manager().on_finished_query(&query_id);
                    statistics_receiver.wait_shutdown()
                });

                Ok(build_res)
            }
        }
    }

    pub fn get_flight_exchanges(&self, params: &ExchangeParams) -> Result<Vec<FlightExchange>> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &*queries_coordinator_guard.deref().get() };

        match queries_coordinator.get(&params.get_query_id()) {
            None => Err(ErrorCode::LogicalError("Query not exists.")),
            Some(coordinator) => coordinator.get_flight_exchanges(params),
        }
    }

    pub fn get_fragment_source(
        &self,
        query_id: &str,
        fragment_id: usize,
        _schema: DataSchemaRef,
    ) -> Result<PipelineBuildResult> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.get_mut(query_id) {
            None => Err(ErrorCode::LogicalError("Query not exists.")),
            Some(query_coordinator) => {
                let query_ctx = query_coordinator
                    .info
                    .as_ref()
                    .expect("QueryInfo is none")
                    .query_ctx
                    .clone();

                query_coordinator.subscribe_fragment(&query_ctx, fragment_id)
            }
        }
    }
}

struct QueryInfo {
    query_id: String,
    current_executor: String,
    query_ctx: Arc<QueryContext>,
    query_executor: Option<Arc<PipelineCompleteExecutor>>,
}

struct QueryCoordinator {
    info: Option<QueryInfo>,
    statistics_exchanges: Vec<FlightExchange>,
    fragment_exchanges: HashMap<(String, usize), FlightExchange>,
    fragments_coordinator: HashMap<usize, Box<FragmentCoordinator>>,
}

impl QueryCoordinator {
    pub fn create() -> QueryCoordinator {
        QueryCoordinator {
            info: None,
            statistics_exchanges: vec![],
            fragment_exchanges: HashMap::new(),
            fragments_coordinator: HashMap::new(),
        }
    }

    pub fn add_statistics_exchange(&mut self, exchanges: Vec<FlightExchange>) -> Result<()> {
        for exchange in exchanges.into_iter() {
            self.statistics_exchanges.push(exchange);
        }

        Ok(())
    }

    pub fn add_fragment_exchange(
        &mut self,
        target: String,
        fragment: usize,
        exchange: FlightExchange,
    ) -> Result<()> {
        self.fragment_exchanges.insert((target, fragment), exchange);
        Ok(())
    }

    pub fn add_fragment_exchanges(
        &mut self,
        exchanges: HashMap<(String, usize), FlightExchange>,
    ) -> Result<()> {
        for ((target, fragment), exchange) in exchanges.into_iter() {
            self.fragment_exchanges.insert((target, fragment), exchange);
        }

        Ok(())
    }

    pub fn get_flight_exchanges(&self, params: &ExchangeParams) -> Result<Vec<FlightExchange>> {
        match params {
            ExchangeParams::MergeExchange(params) => {
                let mut exchanges = vec![];
                for ((_target, fragment), exchange) in &self.fragment_exchanges {
                    if *fragment == params.fragment_id {
                        exchanges.push(exchange.clone());
                    }
                }

                Ok(exchanges)
            }
            ExchangeParams::ShuffleExchange(params) => {
                let mut exchanges = Vec::with_capacity(params.destination_ids.len());

                for destination in &params.destination_ids {
                    exchanges.push(match destination == &params.executor_id {
                        true => Ok(FlightExchange::Dummy),
                        false => match self
                            .fragment_exchanges
                            .get(&(destination.clone(), params.fragment_id))
                        {
                            None => Err(ErrorCode::UnknownFragmentExchange(format!(
                                "Unknown fragment exchange channel, {}, {}",
                                destination, params.fragment_id
                            ))),
                            Some(exchange_channel) => Ok(exchange_channel.clone()),
                        },
                    }?);
                }

                Ok(exchanges)
            }
        }
    }

    pub fn prepare_pipeline(
        &mut self,
        ctx: &Arc<QueryContext>,
        packet: &QueryFragmentsPlanPacket,
    ) -> Result<()> {
        self.info = Some(QueryInfo {
            query_ctx: ctx.clone(),
            query_id: packet.query_id.clone(),
            current_executor: packet.executor.clone(),
            query_executor: None,
        });

        for fragment in &packet.fragments {
            self.fragments_coordinator.insert(
                fragment.fragment_id.to_owned(),
                FragmentCoordinator::create(fragment),
            );
        }

        for fragment in &packet.fragments {
            let fragment_id = fragment.fragment_id;
            if let Some(coordinator) = self.fragments_coordinator.get_mut(&fragment_id) {
                coordinator.prepare_pipeline(ctx.clone())?;
            }
        }

        Ok(())
    }

    pub fn subscribe_fragment(
        &mut self,
        ctx: &Arc<QueryContext>,
        fragment_id: usize,
    ) -> Result<PipelineBuildResult> {
        // Merge pipelines if exist locally pipeline
        if let Some(mut fragment_coordinator) = self.fragments_coordinator.remove(&fragment_id) {
            let info = self.info.as_ref().expect("QueryInfo is none");
            fragment_coordinator.prepare_pipeline(ctx.clone())?;

            if fragment_coordinator.pipeline_build_res.is_none() {
                return Err(ErrorCode::LogicalError(
                    "Pipeline is none, maybe query fragment circular dependency.",
                ));
            }

            if fragment_coordinator.data_exchange.is_none() {
                // When the root fragment and the data has been send to the coordination node,
                // we do not need to wait for the data of other nodes.
                return Ok(fragment_coordinator.pipeline_build_res.unwrap());
            }

            let exchange_params = fragment_coordinator.create_exchange_params(ctx, info)?;
            let mut build_res = fragment_coordinator.pipeline_build_res.unwrap();

            let data_exchange = fragment_coordinator.data_exchange.as_ref().unwrap();

            if !data_exchange.from_multiple_nodes() {
                return Err(ErrorCode::UnImplement(
                    "Exchange source and no from multiple nodes is unimplemented.",
                ));
            }

            // Add exchange data transform.
            ExchangeTransform::via(ctx, &exchange_params, &mut build_res.main_pipeline)?;

            return Ok(build_res);
        }

        Err(ErrorCode::UnImplement("ExchangeSource is unimplemented"))
    }

    pub fn shutdown_query(&mut self) {
        if let Some(query_info) = &self.info {
            if let Some(query_executor) = &query_info.query_executor {
                if let Err(cause) = query_executor.finish() {
                    tracing::error!("Cannot shutdown query, because {:?}", cause);
                }
            }
        }
    }

    pub fn on_finished(self) {
        // Do something when query finished.
    }

    pub fn execute_pipeline(&mut self) -> Result<()> {
        if self.fragments_coordinator.is_empty() {
            // Empty fragments if it is a request server, because the pipelines may have been linked.
            return Ok(());
        }

        let info = self.info.as_ref().expect("Query info is None");

        let max_threads = info.query_ctx.get_settings().get_max_threads()?;
        let mut pipelines = Vec::with_capacity(self.fragments_coordinator.len());

        let mut params = Vec::with_capacity(self.fragments_coordinator.len());
        for coordinator in self.fragments_coordinator.values() {
            params.push(coordinator.create_exchange_params(&info.query_ctx, info)?);
        }

        for ((_, coordinator), params) in self.fragments_coordinator.iter_mut().zip(params) {
            if let Some(mut build_res) = coordinator.pipeline_build_res.take() {
                build_res.set_max_threads(max_threads as usize);

                if !build_res.main_pipeline.is_pulling_pipeline()? {
                    return Err(ErrorCode::LogicalError("Logical error, It's a bug"));
                }

                // Add exchange data publisher.
                ExchangeSink::via(&info.query_ctx, &params, &mut build_res.main_pipeline)?;

                if !build_res.main_pipeline.is_complete_pipeline()? {
                    return Err(ErrorCode::LogicalError("Logical error, It's a bug"));
                }

                pipelines.push(build_res.main_pipeline);
                pipelines.extend(build_res.sources_pipelines.into_iter());
            }
        }

        let async_runtime = GlobalIORuntime::instance();
        let query_need_abort = info.query_ctx.query_need_abort();
        let executor_settings = ExecutorSettings::try_create(&info.query_ctx.get_settings())?;

        let executor = PipelineCompleteExecutor::from_pipelines(
            async_runtime,
            query_need_abort,
            pipelines,
            executor_settings,
        )?;

        self.fragment_exchanges.clear();
        let info_mut = self.info.as_mut().expect("Query info is None");
        info_mut.query_executor = Some(executor.clone());

        let query_id = info_mut.query_id.clone();
        let query_ctx = info_mut.query_ctx.clone();
        let mut request_server_exchanges = std::mem::take(&mut self.statistics_exchanges);

        if request_server_exchanges.len() != 1 {
            return Err(ErrorCode::LogicalError(
                "Request server must less than 1 if is not request server.",
            ));
        }

        let ctx = query_ctx.clone();
        let request_server_exchange = request_server_exchanges[0].clone();
        let mut statistics_sender =
            StatisticsSender::create(&query_id, ctx, request_server_exchange);
        statistics_sender.start();

        Thread::named_spawn(Some(String::from("Distributed-Executor")), move || {
            if let Err(cause) = executor.execute() {
                let request_server_exchange = request_server_exchanges.remove(0);

                futures::executor::block_on(async move {
                    statistics_sender.shutdown();

                    if let Err(_cause) = request_server_exchange
                        .send(DataPacket::ErrorCode(cause))
                        .await
                    {
                        tracing::warn!(
                            "Cannot send message to request server when executor failure."
                        );
                    };

                    // Wait request server close channel.
                    // request_server_exchange.close_output();
                    // while let Ok(Some(_data)) = request_server_exchange.recv().await {}
                });

                return;
            }

            // TODO: destroy query resource when panic?
            // Destroy coordinator_server_exchange if executor is not failure.
            statistics_sender.shutdown();
            drop(request_server_exchanges);
            query_ctx
                .get_exchange_manager()
                .on_finished_query(&query_id);
        });

        Ok(())
    }
}

struct FragmentCoordinator {
    payload: FragmentPayload,
    initialized: bool,
    fragment_id: usize,
    data_exchange: Option<DataExchange>,
    pipeline_build_res: Option<PipelineBuildResult>,
}

impl FragmentCoordinator {
    pub fn create(packet: &FragmentPlanPacket) -> Box<FragmentCoordinator> {
        Box::new(FragmentCoordinator {
            initialized: false,
            payload: packet.payload.clone(),
            fragment_id: packet.fragment_id,
            data_exchange: packet.data_exchange.clone(),
            pipeline_build_res: None,
        })
    }

    pub fn create_exchange_params(
        &self,
        ctx: &Arc<QueryContext>,
        info: &QueryInfo,
    ) -> Result<ExchangeParams> {
        match &self.data_exchange {
            None => Err(ErrorCode::LogicalError("Cannot find data exchange.")),
            Some(DataExchange::Merge(exchange)) => {
                Ok(ExchangeParams::MergeExchange(MergeExchangeParams {
                    schema: self.payload.schema()?,
                    fragment_id: self.fragment_id,
                    query_id: info.query_id.to_string(),
                    destination_id: exchange.destination_id.clone(),
                }))
            }
            Some(DataExchange::Broadcast(exchange)) => {
                Ok(ExchangeParams::ShuffleExchange(ShuffleExchangeParams {
                    schema: self.payload.schema()?,
                    fragment_id: self.fragment_id,
                    query_id: info.query_id.to_string(),
                    executor_id: info.current_executor.to_string(),
                    destination_ids: exchange.destination_ids.to_owned(),
                    shuffle_scatter: Arc::new(Box::new(BroadcastFlightScatter::try_create(
                        exchange.destination_ids.len(),
                    )?)),
                }))
            }
            Some(DataExchange::ShuffleDataExchange(exchange)) => {
                Ok(ExchangeParams::ShuffleExchange(ShuffleExchangeParams {
                    schema: self.payload.schema()?,
                    fragment_id: self.fragment_id,
                    query_id: info.query_id.to_string(),
                    executor_id: info.current_executor.to_string(),
                    destination_ids: exchange.destination_ids.to_owned(),
                    shuffle_scatter: Arc::new(Box::new(HashFlightScatter::try_create(
                        ctx.clone(),
                        self.payload.schema()?,
                        Some(exchange.exchange_expression.clone()),
                        exchange.destination_ids.len(),
                    )?)),
                }))
            }
            Some(DataExchange::ShuffleDataExchangeV2(exchange)) => {
                Ok(ExchangeParams::ShuffleExchange(ShuffleExchangeParams {
                    schema: self.payload.schema()?,
                    fragment_id: self.fragment_id,
                    query_id: info.query_id.to_string(),
                    executor_id: info.current_executor.to_string(),
                    destination_ids: exchange.destination_ids.to_owned(),
                    shuffle_scatter: Arc::new(HashFlightScatterV2::try_create(
                        info.query_ctx.try_get_function_context()?,
                        exchange.shuffle_keys.clone(),
                        exchange.destination_ids.len(),
                    )?),
                }))
            }
        }
    }

    pub fn prepare_pipeline(&mut self, ctx: Arc<QueryContext>) -> Result<()> {
        if !self.initialized {
            self.initialized = true;

            match &self.payload {
                FragmentPayload::PlanV1(node) => {
                    let pipeline_builder = QueryPipelineBuilder::create(ctx);
                    self.pipeline_build_res = Some(pipeline_builder.finalize(node)?);
                }
                FragmentPayload::PlanV2(plan) => {
                    let pipeline_builder = PipelineBuilderV2::create(ctx);
                    self.pipeline_build_res = Some(pipeline_builder.finalize(plan)?);
                }
            };
        }

        Ok(())
    }
}
