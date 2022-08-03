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

use async_channel::Sender;
use common_arrow::arrow_format::flight::data::FlightData;
use common_base::base::tokio::task::JoinHandle;
use common_base::base::Runtime;
use common_base::base::Thread;
use common_base::base::TrySpawn;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::PlanNode;
use futures::StreamExt;
use parking_lot::Mutex;
use parking_lot::ReentrantMutex;
use tonic::Streaming;

use crate::api::rpc::exchange::exchange_channel::FragmentReceiver;
use crate::api::rpc::exchange::exchange_channel::FragmentSender;
use crate::api::rpc::exchange::exchange_channel_receiver::ExchangeReceiver;
use crate::api::rpc::exchange::exchange_channel_sender::ExchangeSender;
use crate::api::rpc::exchange::exchange_params::ExchangeParams;
use crate::api::rpc::exchange::exchange_params::MergeExchangeParams;
use crate::api::rpc::exchange::exchange_params::ShuffleExchangeParams;
use crate::api::rpc::exchange::exchange_sink::ExchangeSink;
use crate::api::rpc::exchange::exchange_source::ExchangeSource;
use crate::api::rpc::flight_scatter_broadcast::BroadcastFlightScatter;
use crate::api::rpc::flight_scatter_hash::HashFlightScatter;
use crate::api::rpc::flight_scatter_hash_v2::HashFlightScatterV2;
use crate::api::rpc::packets::DataPacket;
use crate::api::rpc::Packet;
use crate::api::DataExchange;
use crate::api::FragmentPayload;
use crate::api::FragmentPlanPacket;
use crate::api::InitNodesChannelPacket;
use crate::api::QueryFragmentsPlanPacket;
use crate::interpreters::QueryFragmentActions;
use crate::interpreters::QueryFragmentsActions;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::Pipe;
use crate::pipelines::Pipeline;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::QueryPipelineBuilder;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::executor::PhysicalPlan;
use crate::sql::executor::PipelineBuilder as PipelineBuilderV2;
use crate::Config;

pub struct DataExchangeManager {
    config: Config,
    queries_coordinator: ReentrantMutex<SyncUnsafeCell<HashMap<String, QueryCoordinator>>>,
}

impl DataExchangeManager {
    pub fn create(config: Config) -> Arc<DataExchangeManager> {
        Arc::new(DataExchangeManager {
            config,
            queries_coordinator: ReentrantMutex::new(SyncUnsafeCell::new(HashMap::new())),
        })
    }

    // Create connections for cluster all nodes. We will push data through this connection.
    pub async fn init_nodes_channel(&self, packet: &InitNodesChannelPacket) -> Result<()> {
        let mut exchange_senders = HashMap::with_capacity(packet.target_2_fragments.len());
        for target in packet.target_2_fragments.keys() {
            if target != &packet.executor {
                let config = self.config.clone();
                let exchange_sender = ExchangeSender::create(config, packet, target).await?;
                exchange_senders.insert(target.clone(), exchange_sender);
            }
        }

        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.get_mut(&packet.query_id) {
            None => Err(ErrorCode::LogicalError(format!(
                "Query {} not found in cluster.",
                packet.query_id
            ))),
            Some(coordinator) => coordinator.init_publisher(exchange_senders),
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
        match queries_coordinator.entry(packet.query_id.to_owned()) {
            Entry::Occupied(_) => Err(ErrorCode::LogicalError(format!(
                "Already exists query id {:?}",
                packet.query_id
            ))),
            Entry::Vacant(entry) => {
                let query_coordinator = QueryCoordinator::create(ctx, packet)?;
                let query_coordinator = entry.insert(query_coordinator);
                query_coordinator.prepare_pipeline()?;

                if packet.executor != packet.request_executor {
                    query_coordinator
                        .prepare_subscribes_channel(query_coordinator.ctx.clone(), packet)?;
                }

                Ok(())
            }
        }
    }

    // Receive data by cluster other nodes.
    pub async fn handle_do_put(
        &self,
        id: &str,
        source: &str,
        stream: Streaming<FlightData>,
    ) -> Result<JoinHandle<()>> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.get_mut(id) {
            None => Err(ErrorCode::LogicalError(format!(
                "Query {} not found in cluster.",
                id
            ))),
            Some(coordinator) => coordinator.receive_data(source, stream),
        }
    }

    pub fn on_finished_query(&self, query_id: &str, may_error: &Option<ErrorCode>) {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };
        if let Some(mut query_coordinator) = queries_coordinator.remove(query_id) {
            // Drop mutex guard to avoid deadlock during shutdown,
            drop(queries_coordinator_guard);

            query_coordinator.on_finished(may_error);
        }
    }

    pub fn shutdown_query(&self, query_id: &str, cause: Option<ErrorCode>) -> Result<()> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.get_mut(query_id) {
            None => Err(ErrorCode::LogicalError(format!(
                "Query {} not found in cluster.",
                query_id
            ))),
            Some(coordinator) => coordinator.shutdown(cause),
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

        // Submit distributed tasks to all nodes.
        let query_fragments_plan_packets = actions.get_query_fragments_plan_packets()?;
        query_fragments_plan_packets
            .commit(&self.config, timeout)
            .await?;

        // Get local pipeline of local task
        let mut build_res = self.get_root_pipeline(ctx.get_id(), root_actions)?;

        // Initialize channels between cluster nodes
        let init_nodes_channel_packets = actions.get_init_nodes_channel_packets()?;
        self.init_root_pipeline_channel(ctx.clone(), &query_fragments_plan_packets)?;
        init_nodes_channel_packets
            .commit(&self.config, timeout)
            .await?;

        QueryCoordinator::init_pipeline(&mut build_res)?;
        let execute_partial_query_packets = actions.get_execute_partial_query_packets()?;
        execute_partial_query_packets
            .commit(&self.config, timeout)
            .await?;

        Ok(build_res)
    }

    fn get_root_pipeline(
        &self,
        query_id: String,
        root_actions: &QueryFragmentActions,
    ) -> Result<PipelineBuildResult> {
        let schema = root_actions.get_schema()?;
        let fragment_id = root_actions.fragment_id;
        self.get_fragment_source(query_id, fragment_id, schema)
    }

    fn init_root_pipeline_channel(
        &self,
        ctx: Arc<QueryContext>,
        executor_packet: &[QueryFragmentsPlanPacket],
    ) -> Result<()> {
        for executor_packet in executor_packet {
            if executor_packet.executor == executor_packet.request_executor {
                let queries_coordinator_guard = self.queries_coordinator.lock();
                let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

                match queries_coordinator.entry(executor_packet.query_id.to_owned()) {
                    Entry::Vacant(_) => Err(ErrorCode::LogicalError(format!(
                        "Cannot find query id {:?}",
                        executor_packet.query_id
                    ))),
                    Entry::Occupied(mut entry) => entry
                        .get_mut()
                        .prepare_subscribes_channel(ctx.clone(), executor_packet),
                }?;
            }
        }

        Ok(())
    }

    pub fn get_fragment_sink(
        &self,
        query_id: &str,
        id: usize,
        endpoint: &str,
    ) -> Result<FragmentSender> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &*queries_coordinator_guard.deref().get() };

        match queries_coordinator.get(query_id) {
            None => Err(ErrorCode::LogicalError("Query not exists.")),
            Some(coordinator) => coordinator.get_fragment_sink(id, endpoint),
        }
    }

    pub fn get_fragment_source(
        &self,
        query_id: String,
        fragment_id: usize,
        schema: DataSchemaRef,
    ) -> Result<PipelineBuildResult> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.get_mut(&query_id) {
            None => Err(ErrorCode::LogicalError("Query not exists.")),
            Some(query_coordinator) => query_coordinator.subscribe_fragment(fragment_id, schema),
        }
    }
}

struct QueryCoordinator {
    ctx: Arc<QueryContext>,
    query_id: String,
    executor_id: String,
    runtime: Arc<Runtime>,
    request_server_tx: Option<Sender<DataPacket>>,
    publish_fragments: HashMap<(String, usize), FragmentSender>,
    subscribe_channel: HashMap<String, Sender<Result<DataPacket>>>,
    subscribe_fragments: HashMap<usize, FragmentReceiver>,
    fragments_coordinator: HashMap<usize, Box<FragmentCoordinator>>,

    shutdown_cause: Arc<Mutex<Option<ErrorCode>>>,
    executor: Option<Arc<PipelineCompleteExecutor>>,
    exchange_senders: Vec<Arc<ExchangeSender>>,
    exchange_receivers: Vec<Arc<ExchangeReceiver>>,
}

impl QueryCoordinator {
    pub fn create(
        ctx: &Arc<QueryContext>,
        executor: &QueryFragmentsPlanPacket,
    ) -> Result<QueryCoordinator> {
        let mut fragments_coordinator = HashMap::with_capacity(executor.fragments.len());

        for fragment in &executor.fragments {
            fragments_coordinator.insert(
                fragment.fragment_id.to_owned(),
                FragmentCoordinator::create(fragment),
            );
        }

        Ok(QueryCoordinator {
            ctx: ctx.clone(),
            fragments_coordinator,
            executor: None,
            exchange_senders: vec![],
            exchange_receivers: vec![],
            shutdown_cause: Arc::new(Mutex::new(None)),
            request_server_tx: None,
            publish_fragments: Default::default(),
            query_id: executor.query_id.to_owned(),
            subscribe_channel: Default::default(),
            subscribe_fragments: Default::default(),
            executor_id: executor.executor.to_owned(),
            runtime: Arc::new(Runtime::with_worker_threads(
                2,
                Some(String::from("Cluster-Pub-Sub")),
            )?),
        })
    }

    pub fn prepare_pipeline(&mut self) -> Result<()> {
        let fragments_id = self
            .fragments_coordinator
            .keys()
            .cloned()
            .collect::<Vec<_>>();

        for fragment_id in fragments_id {
            if let Some(coordinator) = self.fragments_coordinator.get_mut(&fragment_id) {
                match coordinator.payload.clone() {
                    FragmentPayload::PlanV1(plan) => {
                        coordinator.prepare_pipeline(&plan, &self.query_id, &self.ctx)?;
                    }
                    FragmentPayload::PlanV2(plan) => {
                        coordinator.prepare_pipeline_v2(&plan, &self.query_id, &self.ctx)?;
                    }
                }
            }
        }

        Ok(())
    }

    pub fn on_finished(&mut self, may_error: &Option<ErrorCode>) {
        if let Some(cause) = may_error {
            if let Some(request_server_tx) = self.request_server_tx.take() {
                let may_error = cause.clone();
                tracing::warn!("Failed to execute pipeline, cause: {}", may_error);
                futures::executor::block_on(async move {
                    if request_server_tx
                        .send(DataPacket::ErrorCode(may_error))
                        .await
                        .is_err()
                    {
                        tracing::warn!("Cannot send error code, request server channel is closed.");
                    }

                    drop(request_server_tx);
                });
            }
        }

        if let Some(cause) = self.shutdown_cause.lock().take() {
            if let Some(request_server_tx) = self.request_server_tx.take() {
                futures::executor::block_on(async move {
                    if request_server_tx
                        .send(DataPacket::ErrorCode(cause))
                        .await
                        .is_err()
                    {
                        tracing::warn!("Cannot send error code, request server channel is closed.");
                    }

                    drop(request_server_tx);
                });
            }
        }

        // close request server channel.
        drop(self.request_server_tx.take());

        for sender in &self.exchange_senders {
            sender.abort();
        }

        for sender in &self.exchange_senders {
            let sender = sender.clone();
            futures::executor::block_on(async move {
                if let Err(cause) = sender.join().await {
                    tracing::warn!("Sender join failure {:?}", cause);
                }
            });
        }

        for receiver in &self.exchange_receivers {
            let receiver = receiver.clone();
            futures::executor::block_on(async move {
                if let Err(cause) = receiver.join().await {
                    tracing::warn!("Receiver join failure {:?}", cause);
                }
            });
        }
    }

    pub fn shutdown(&mut self, cause: Option<ErrorCode>) -> Result<()> {
        {
            let mut shutdown_cause = self.shutdown_cause.lock();
            *shutdown_cause = cause;
        }

        if let Some(executor) = self.executor.clone() {
            executor.finish()?;
        }

        Ok(())
    }

    pub fn prepare_subscribes_channel(
        &mut self,
        ctx: Arc<QueryContext>,
        prepare: &QueryFragmentsPlanPacket,
    ) -> Result<()> {
        for (source, fragments_id) in &prepare.source_2_fragments {
            if source == &prepare.executor {
                continue;
            }

            let (tx, rx) = async_channel::bounded(1);

            let ctx = ctx.clone();
            let query_id = prepare.query_id.clone();
            let runtime = self.runtime.clone();
            let receivers_map = {
                let mut receivers_map = HashMap::with_capacity(fragments_id.len());
                for receive_fragment_id in fragments_id {
                    receivers_map.insert(
                        *receive_fragment_id,
                        &self.subscribe_fragments[receive_fragment_id],
                    );
                }

                receivers_map
            };

            let receiver = ExchangeReceiver::create(ctx, query_id, runtime, rx, receivers_map);

            receiver.listen()?;
            self.exchange_receivers.push(receiver);
            self.subscribe_channel.insert(source.to_string(), tx);
        }

        Ok(())
    }

    pub fn execute_pipeline(&mut self) -> Result<()> {
        if self.fragments_coordinator.is_empty() {
            // Empty fragments if it is a request server, because the pipelines may have been linked.
            return Ok(());
        }

        let max_threads = self.ctx.get_settings().get_max_threads()?;
        let mut pipelines = Vec::with_capacity(self.fragments_coordinator.len());

        let mut params = Vec::with_capacity(self.fragments_coordinator.len());
        for coordinator in self.fragments_coordinator.values() {
            params.push(coordinator.create_exchange_params(self)?);
        }

        for ((_, coordinator), params) in self.fragments_coordinator.iter_mut().zip(params) {
            if let Some(mut build_res) = coordinator.pipeline_build_res.take() {
                build_res.set_max_threads(max_threads as usize);

                if !build_res.main_pipeline.is_pulling_pipeline()? {
                    return Err(ErrorCode::LogicalError("Logical error, It's a bug"));
                }

                // Add exchange data publisher.
                ExchangeSink::publisher_sink(&self.ctx, &params, &mut build_res.main_pipeline)?;

                if !build_res.main_pipeline.is_complete_pipeline()? {
                    return Err(ErrorCode::LogicalError("Logical error, It's a bug"));
                }

                QueryCoordinator::init_pipeline(&mut build_res)?;
                pipelines.push(build_res.main_pipeline);
                pipelines.extend(build_res.sources_pipelines.into_iter());
            }
        }

        let async_runtime = self.ctx.get_storage_runtime();
        let query_need_abort = self.ctx.query_need_abort();

        let executor =
            PipelineCompleteExecutor::from_pipelines(async_runtime, query_need_abort, pipelines)?;
        self.executor = Some(executor.clone());

        Thread::named_spawn(Some(String::from("Distributed-Executor")), move || {
            executor.execute().ok();
        });

        Ok(())
    }

    pub fn init_publisher(&mut self, senders: HashMap<String, ExchangeSender>) -> Result<()> {
        for (target, mut exchange_sender) in senders.into_iter() {
            let runtime = self.runtime.clone();
            exchange_sender.connect_flight(&runtime)?;
            let exchange_sender = Arc::new(exchange_sender);

            let ctx = self.ctx.clone();
            let source = self.executor_id.clone();
            let (flight_tx, fragments_sender) = exchange_sender.listen(ctx, source, runtime)?;

            for (fragment_id, fragment_sender) in fragments_sender.into_iter() {
                self.publish_fragments
                    .insert((target.clone(), fragment_id), fragment_sender);
            }

            if exchange_sender.is_to_request_server() {
                self.request_server_tx = Some(flight_tx);
            }

            self.exchange_senders.push(exchange_sender);
        }

        for coordinator in self.fragments_coordinator.values_mut() {
            if let Some(build_res) = coordinator.pipeline_build_res.as_mut() {
                Self::init_pipeline(build_res)?;
            }
        }

        Ok(())
    }

    pub fn init_pipeline(build_res: &mut PipelineBuildResult) -> Result<()> {
        for pipe in &mut build_res.main_pipeline.pipes {
            if let Pipe::SimplePipe { processors, .. } = pipe {
                for processor in processors {
                    ExchangeSink::init(processor)?;
                }
            }
        }

        for source_pipeline in &mut build_res.sources_pipelines {
            for pipe in &mut source_pipeline.pipes {
                if let Pipe::SimplePipe { processors, .. } = pipe {
                    for processor in processors {
                        ExchangeSink::init(processor)?;
                    }
                }
            }
        }

        Ok(())
    }

    pub fn receive_data(
        &mut self,
        source: &str,
        mut stream: Streaming<FlightData>,
    ) -> Result<JoinHandle<()>> {
        if let Some(subscribe_channel) = self.subscribe_channel.remove(source) {
            let ctx = self.ctx.clone();
            let source = source.to_string();
            let target = self.executor_id.clone();
            let query_id = self.query_id.clone();

            return Ok(self.runtime.spawn(async move {
                'fragment_loop: while let Some(flight_data) = stream.next().await {
                    match flight_data {
                        Err(status) => {
                            let cause = Some(ErrorCode::from(status));
                            let exchange_manager = ctx.get_exchange_manager();
                            exchange_manager.shutdown_query(&query_id, cause).ok();
                            break 'fragment_loop;
                        }
                        Ok(flight_data) => {
                            let data_packet = match DataPacket::try_from(flight_data) {
                                Ok(data_packet) => data_packet,
                                Err(error_code) => DataPacket::ErrorCode(error_code),
                            };

                            if matches!(&data_packet, DataPacket::FinishQuery) {
                                let exchange_manager = ctx.get_exchange_manager();
                                exchange_manager.shutdown_query(&query_id, None).ok();
                                break 'fragment_loop;
                            }

                            if let Err(_cause) = subscribe_channel.send(Ok(data_packet)).await {
                                tracing::warn!(
                                    "Subscribe channel closed, source {}, target {}",
                                    source,
                                    target
                                );

                                break 'fragment_loop;
                            }
                        }
                    };
                }
            }));
        };

        Err(ErrorCode::LogicalError(
            "Cannot found fragment channel, It's a bug.",
        ))
    }

    pub fn get_fragment_sink(&self, id: usize, endpoint: &str) -> Result<FragmentSender> {
        match self.publish_fragments.get(&(endpoint.to_string(), id)) {
            None => Err(ErrorCode::LogicalError(format!(
                "Not found node {} in cluster",
                endpoint
            ))),
            Some(publisher) => Ok(publisher.clone()),
        }
    }

    pub fn subscribe_fragment(
        &mut self,
        fragment_id: usize,
        schema: DataSchemaRef,
    ) -> Result<PipelineBuildResult> {
        // TODO(Winter): we need to implement back pressure on the network side.
        let (tx, rx) = async_channel::unbounded();

        // Register subscriber for data exchange.
        self.subscribe_fragments
            .insert(fragment_id, FragmentReceiver::create_unrecorded(tx));

        // Merge pipelines if exist locally pipeline
        if let Some(mut fragment_coordinator) = self.fragments_coordinator.remove(&fragment_id) {
            match fragment_coordinator.payload.clone() {
                FragmentPayload::PlanV1(plan) => {
                    fragment_coordinator.prepare_pipeline(&plan, &self.query_id, &self.ctx)?;
                }
                FragmentPayload::PlanV2(plan) => {
                    fragment_coordinator.prepare_pipeline_v2(&plan, &self.query_id, &self.ctx)?;
                }
            }

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

            let exchange_params = fragment_coordinator.create_exchange_params(self)?;
            let mut build_res = fragment_coordinator.pipeline_build_res.unwrap();

            // Add exchange data publisher.
            ExchangeSink::via_exchange(&self.ctx, &exchange_params, &mut build_res.main_pipeline)?;

            let data_exchange = fragment_coordinator.data_exchange.as_ref().unwrap();

            // Add exchange data subscriber.
            if data_exchange.from_multiple_nodes() {
                ExchangeSource::via_exchange(rx, &exchange_params, &mut build_res.main_pipeline)?;
            }

            return Ok(build_res);
        }

        let mut pipeline = Pipeline::create();

        let settings = self.ctx.get_settings();
        let max_threads = settings.get_max_threads()? as usize;
        let max_threads = std::cmp::max(max_threads, 1);

        // Add exchange data subscriber.
        ExchangeSource::create_source(rx, schema, &mut pipeline, max_threads)?;
        Ok(PipelineBuildResult {
            main_pipeline: pipeline,
            sources_pipelines: vec![],
        })
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

    pub fn create_exchange_params(&self, query: &QueryCoordinator) -> Result<ExchangeParams> {
        match &self.data_exchange {
            None => Err(ErrorCode::LogicalError("Cannot find data exchange.")),
            Some(DataExchange::Merge(exchange)) => {
                Ok(ExchangeParams::MergeExchange(MergeExchangeParams {
                    schema: self.payload.schema()?,
                    fragment_id: self.fragment_id,
                    query_id: query.query_id.to_string(),
                    destination_id: exchange.destination_id.clone(),
                }))
            }
            Some(DataExchange::Broadcast(exchange)) => {
                Ok(ExchangeParams::ShuffleExchange(ShuffleExchangeParams {
                    schema: self.payload.schema()?,
                    fragment_id: self.fragment_id,
                    query_id: query.query_id.to_string(),
                    executor_id: query.executor_id.to_string(),
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
                    query_id: query.query_id.to_string(),
                    executor_id: query.executor_id.to_string(),
                    destination_ids: exchange.destination_ids.to_owned(),
                    shuffle_scatter: Arc::new(Box::new(HashFlightScatter::try_create(
                        query.ctx.clone(),
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
                    query_id: query.query_id.to_string(),
                    executor_id: query.executor_id.to_string(),
                    destination_ids: exchange.destination_ids.to_owned(),
                    shuffle_scatter: Arc::new(Box::new(HashFlightScatterV2::try_create(
                        query.ctx.try_get_function_context()?,
                        exchange.shuffle_keys.clone(),
                        exchange.destination_ids.len(),
                    )?)),
                }))
            }
        }
    }

    pub fn prepare_pipeline(
        &mut self,
        plan_node: &PlanNode,
        query_id: &str,
        ctx: &Arc<QueryContext>,
    ) -> Result<()> {
        if !self.initialized {
            self.initialized = true;
            let pipeline_builder = QueryPipelineBuilder::create(ctx.clone());
            let mut build_res = pipeline_builder.finalize(plan_node)?;

            let ctx = ctx.clone();
            let query_id = query_id.to_string();
            build_res.main_pipeline.set_on_finished(move |may_error| {
                let exchange_manager = ctx.get_exchange_manager();
                exchange_manager.on_finished_query(&query_id, may_error);
            });

            self.pipeline_build_res = Some(build_res);
        }

        Ok(())
    }

    pub fn prepare_pipeline_v2(
        &mut self,
        plan: &PhysicalPlan,
        query_id: &str,
        ctx: &Arc<QueryContext>,
    ) -> Result<()> {
        if !self.initialized {
            self.initialized = true;
            let pipeline_builder = PipelineBuilderV2::create(ctx.clone());
            let mut build_res = pipeline_builder.finalize(plan)?;

            let ctx = ctx.clone();
            let query_id = query_id.to_string();
            build_res.main_pipeline.set_on_finished(move |may_error| {
                let exchange_manager = ctx.get_exchange_manager();
                exchange_manager.on_finished_query(&query_id, may_error);
            });

            self.pipeline_build_res = Some(build_res);
        }

        Ok(())
    }
}
