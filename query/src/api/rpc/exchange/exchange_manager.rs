use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use common_arrow::arrow_format::flight::data::FlightData;
use common_arrow::arrow_format::flight::service::flight_service_client::FlightServiceClient;
use common_base::base::{Thread, tokio};
use crate::pipelines::new::processors::Processor;
use crate::pipelines::new::processors::processor::{Event, ProcessorPtr};
use common_exception::{ErrorCode, Result};
use common_base::infallible::{Mutex, ReentrantMutex, RwLock};
use crate::api::{DataExchange, ExecutorPacket, FlightAction, FlightClient, FragmentPacket, PublisherPacket};
use crate::interpreters::QueryFragmentsActions;
use crate::pipelines::new::executor::PipelineCompleteExecutor;
use crate::pipelines::new::{NewPipe, NewPipeline, QueryPipelineBuilder};
use crate::sessions::QueryContext;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_grpc::ConnectionFactory;
use common_meta_types::NodeInfo;
use common_planners::PlanNode;
use async_channel::Sender;
use async_channel::Receiver;
use futures::{Stream, StreamExt};
use tonic::Streaming;
use crate::api::rpc::exchange::exchange_params::{ShuffleExchangeParams, MergeExchangeParams, ExchangeParams};
use crate::api::rpc::exchange::exchange_publisher::ExchangePublisher;
use crate::api::rpc::exchange::exchange_subscriber::ExchangeSubscriber;
use crate::api::rpc::flight_actions::{PreparePipeline, PreparePublisher};
use crate::api::rpc::flight_scatter::FlightScatter;
use crate::api::rpc::flight_scatter_hash::HashFlightScatter;
use crate::api::rpc::packet::ExecutePacket;
use crate::Config;
use crate::pipelines::new::processors::port::InputPort;

pub struct DataExchangeManager {
    config: Config,
    queries_coordinator: ReentrantMutex<HashMap<String, QueryCoordinator>>,
}

impl DataExchangeManager {
    pub fn create(config: Config) -> Arc<DataExchangeManager> {
        Arc::new(DataExchangeManager { config, queries_coordinator: ReentrantMutex::new(HashMap::new()) })
    }

    pub fn handle_prepare_publisher(&self, prepare: &PreparePublisher) -> Result<()> {
        let mut queries_coordinator = self.queries_coordinator.lock();

        let publisher_packet = &prepare.publisher_packet;
        match queries_coordinator.get_mut(&publisher_packet.query_id) {
            None => Err(ErrorCode::LogicalError(format!("Query {} not found in cluster.", publisher_packet.query_id))),
            Some(coordinator) => coordinator.init_publisher(self.config.clone(), publisher_packet)
        }
    }

    pub fn handle_execute_pipeline(&self, query_id: &str) -> Result<()> {
        let mut queries_coordinator = self.queries_coordinator.lock();

        match queries_coordinator.get_mut(query_id) {
            None => Err(ErrorCode::LogicalError(format!("Query {} not found in cluster.", query_id))),
            Some(coordinator) => coordinator.execute_pipeline()
        }
    }

    pub async fn handle_do_put(&self, mut stream: Streaming<FlightData>) -> Result<()> {
        // TODO: send data
        while let Some(flight_data) = stream.next().await {
            println!("Receive flight data");
        }
        // self.get_fragment_source()
        Ok(())
    }

    pub fn handle_prepare_executor(&self, ctx: &Arc<QueryContext>, prepare: &PreparePipeline) -> Result<()> {
        let mut queries_coordinator = self.queries_coordinator.lock();

        let executor_packet = &prepare.executor_packet;
        // TODO: When the query is not executed for a long time after submission, we need to remove it
        match queries_coordinator.entry(executor_packet.query_id.to_owned()) {
            Entry::Occupied(_) => Err(ErrorCode::LogicalError(format!("Already exists query id {:?}", executor_packet.query_id))),
            Entry::Vacant(entry) => {
                let query_coordinator = QueryCoordinator::create(ctx, executor_packet);
                entry.insert(query_coordinator).prepare_pipeline()
            }
        }
    }

    async fn create_client(&self, address: &str) -> Result<FlightClient> {
        return match self.config.tls_query_cli_enabled() {
            true => Ok(FlightClient::new(FlightServiceClient::new(
                ConnectionFactory::create_rpc_channel(
                    address.to_owned(),
                    None,
                    Some(self.config.query.to_rpc_client_tls_config()),
                ).await?,
            ))),
            false => Ok(FlightClient::new(FlightServiceClient::new(
                ConnectionFactory::create_rpc_channel(
                    address.to_owned(),
                    None,
                    None,
                ).await?,
            ))),
        };
    }

    async fn prepare_pipeline(&self, packets: Vec<ExecutorPacket>, timeout: u64) -> Result<()> {
        for executor_packet in packets.into_iter() {
            if !executor_packet.executors_info.contains_key(&executor_packet.executor) {
                return Err(ErrorCode::LogicalError(format!(
                    "Not found {} node in cluster", &executor_packet.executor
                )));
            }

            let executor_info = &executor_packet.executors_info[&executor_packet.executor];
            let mut connection = self.create_client(&executor_info.flight_address).await?;
            let action = FlightAction::PreparePipeline(PreparePipeline { executor_packet });
            connection.execute_action(action, timeout).await?;
        }

        Ok(())
    }

    async fn prepare_publisher(&self, packets: Vec<PublisherPacket>, timeout: u64) -> Result<()> {
        for publisher_packet in packets.into_iter() {
            if !publisher_packet.data_endpoints.contains_key(&publisher_packet.executor) {
                return Err(ErrorCode::LogicalError(format!(
                    "Not found {} node in cluster", &publisher_packet.executor
                )));
            }

            let executor_info = &publisher_packet.data_endpoints[&publisher_packet.executor];
            let mut connection = self.create_client(&executor_info.flight_address).await?;
            let action = FlightAction::PreparePublisher(PreparePublisher { publisher_packet });
            connection.execute_action(action, timeout).await?;
        }

        Ok(())
    }

    async fn execute_pipeline(&self, packets: Vec<ExecutePacket>, timeout: u64) -> Result<()> {
        for execute_packet in packets.into_iter() {
            if !execute_packet.executors_info.contains_key(&execute_packet.executor) {
                return Err(ErrorCode::LogicalError(format!(
                    "Not found {} node in cluster", &execute_packet.executor
                )));
            }

            let executor_info = &execute_packet.executors_info[&execute_packet.executor];
            let mut connection = self.create_client(&executor_info.flight_address).await?;
            let action = FlightAction::ExecutePipeline(execute_packet.query_id);
            connection.execute_action(action, timeout).await?;
        }

        Ok(())
    }

    pub async fn submit_query_actions(&self, ctx: Arc<QueryContext>, actions: QueryFragmentsActions) -> Result<NewPipeline> {
        let settings = ctx.get_settings();
        let timeout = settings.get_flight_client_timeout()?;
        let root_actions = actions.get_root_actions()?;
        let root_fragment_id = root_actions.fragment_id.to_owned();

        // Submit distributed tasks to all nodes.
        self.prepare_pipeline(actions.prepare_packets(ctx.clone())?, timeout).await?;

        // Get local pipeline of local task
        let schema = root_actions.get_schema()?;
        let root_pipeline = self.build_root_pipeline(ctx.get_id(), root_fragment_id, schema)?;

        self.prepare_publisher(actions.prepare_publisher(ctx.clone())?, timeout).await?;
        self.execute_pipeline(actions.execute_packets(ctx.clone())?, timeout).await?;

        Ok(root_pipeline)
    }

    fn build_root_pipeline(&self, query_id: String, fragment_id: String, schema: DataSchemaRef) -> Result<NewPipeline> {
        let mut pipeline = NewPipeline::create();
        self.get_fragment_source(query_id, fragment_id, schema, &mut pipeline)?;
        Ok(pipeline)
    }

    pub fn get_fragment_sink(&self, query_id: &str, endpoint: &str) -> Result<Sender<FlightData>> {
        let mut queries_coordinator = self.queries_coordinator.lock();

        match queries_coordinator.get(query_id) {
            None => Err(ErrorCode::LogicalError("Query not exists.")),
            Some(coordinator) => coordinator.get_fragment_sink(endpoint),
        }
    }

    pub fn get_fragment_source(&self, query_id: String, fragment_id: String, schema: DataSchemaRef, pipeline: &mut NewPipeline) -> Result<()> {
        let mut queries_coordinator = self.queries_coordinator.lock();

        match queries_coordinator.get_mut(&query_id) {
            None => Err(ErrorCode::LogicalError("Query not exists.")),
            Some(query_coordinator) => query_coordinator.subscribe_fragment(fragment_id, schema, pipeline)
        }
    }
}

struct QueryCoordinator {
    ctx: Arc<QueryContext>,
    query_id: String,
    executor_id: String,
    publish_fragments: HashMap<String, Sender<FlightData>>,
    subscribe_fragments: HashMap<String, Sender<FlightData>>,
    fragments_coordinator: HashMap<String, FragmentCoordinator>,
}

impl QueryCoordinator {
    pub fn create(ctx: &Arc<QueryContext>, executor: &ExecutorPacket) -> QueryCoordinator {
        let mut fragments_coordinator = HashMap::with_capacity(executor.fragments.len());

        for fragment in &executor.fragments {
            fragments_coordinator.insert(
                fragment.fragment_id.to_owned(),
                FragmentCoordinator::create(&fragment),
            );
        }

        QueryCoordinator { query_id: executor.query_id.to_owned(), ctx: ctx.clone(), publish_fragments: Default::default(), subscribe_fragments: Default::default(), fragments_coordinator, executor_id: executor.executor.to_owned() }
    }

    pub fn prepare_pipeline(&mut self) -> Result<()> {
        let mut params = Vec::with_capacity(self.fragments_coordinator.len());
        for (_, coordinator) in &self.fragments_coordinator {
            params.push(coordinator.create_exchange_params(self)?);
        }

        for ((_, coordinator), params) in self.fragments_coordinator.iter_mut().zip(params) {
            coordinator.prepare_pipeline(&self.ctx, &params)?;
        }

        Ok(())
    }

    pub fn execute_pipeline(&mut self) -> Result<()> {
        let mut pipelines = Vec::with_capacity(self.fragments_coordinator.len());

        for (_, coordinator) in self.fragments_coordinator.iter_mut() {
            if let Some(pipeline) = coordinator.pipeline.take() {
                pipelines.push(pipeline);
            }
        }

        let async_runtime = self.ctx.get_storage_runtime();
        let complete_executor = PipelineCompleteExecutor::from_pipelines(async_runtime, pipelines)?;

        Thread::named_spawn(Some(String::from("Executor")), move || {
            complete_executor.execute().unwrap();
        });

        Ok(())
    }

    pub fn init_publisher(&mut self, config: Config, packet: &PublisherPacket) -> Result<()> {
        for (executor, info) in &packet.data_endpoints {
            if executor == &packet.request_server {
                // Send log, metric, trace, (progress?)
            }

            if executor != &packet.executor {
                match self.publish_fragments.entry(executor.to_string()) {
                    Entry::Occupied(_) => Err(ErrorCode::LogicalError("Publisher id already exists")),
                    Entry::Vacant(entry) => {
                        let (tx, rx) = async_channel::bounded(2);
                        Self::spawn_publish_worker(config.clone(), rx, info.clone());
                        entry.insert(tx);
                        Ok(())
                    }
                }?;
            }
        }

        Ok(())
    }

    async fn create_client(config: Config, address: &str) -> Result<FlightClient> {
        return match config.tls_query_cli_enabled() {
            true => Ok(FlightClient::new(FlightServiceClient::new(
                ConnectionFactory::create_rpc_channel(
                    address.to_owned(),
                    None,
                    Some(config.query.to_rpc_client_tls_config()),
                ).await?,
            ))),
            false => Ok(FlightClient::new(FlightServiceClient::new(
                ConnectionFactory::create_rpc_channel(
                    address.to_owned(),
                    None,
                    None,
                ).await?,
            ))),
        };
    }

    fn spawn_publish_worker(config: Config, rx: Receiver<FlightData>, info: Arc<NodeInfo>) {
        tokio::spawn(async move {
            let mut connection = Self::create_client(config, &info.flight_address).await.unwrap();
            let tx = connection.pushed_stream(rx).await.unwrap();

            // TODO: rx.finished if error.
            // TODO: we push log, metric and trace to request server if timeout.
            // while let Ok(flight_data) = rx.recv().await {
            //     if let Err(cause) = tx.send(flight_data).await {
            //         // TODO: remote cannot send, get remote exception or finished.
            //         continue;
            //     }
            // }
        });
    }

    pub fn get_fragment_sink(&self, endpoint: &str) -> Result<Sender<FlightData>> {
        match self.publish_fragments.get(endpoint) {
            None => Err(ErrorCode::LogicalError(format!("Not found node {} in cluster", endpoint))),
            Some(publisher) => Ok(publisher.clone())
        }
    }

    pub fn subscribe_fragment(&mut self, fragment_id: String, schema: DataSchemaRef, pipeline: &mut NewPipeline) -> Result<()> {
        let (tx, rx) = async_channel::bounded(1);

        // Register subscriber for data exchange.
        self.subscribe_fragments.insert(fragment_id.clone(), tx);

        // Merge pipelines if exist locally pipeline
        if let Some(mut fragment_coordinator) = self.fragments_coordinator.remove(&fragment_id) {
            let params = fragment_coordinator.create_exchange_params(self)?;
            fragment_coordinator.prepare_pipeline(&self.ctx, &params)?;

            if fragment_coordinator.pipeline.is_none() {
                return Err(ErrorCode::LogicalError("Pipeline is none, maybe query fragment circular dependency."));
            }

            let exchange_params = fragment_coordinator.create_exchange_params(self)?;
            *pipeline = fragment_coordinator.pipeline.unwrap();

            // Add exchange data publisher.
            ExchangePublisher::via_exchange(&self.ctx, &exchange_params, pipeline)?;

            // Add exchange data subscriber.
            return ExchangeSubscriber::via_exchange(rx, &exchange_params, pipeline);
        }

        // Add exchange data subscriber.
        ExchangeSubscriber::create_source(rx, schema, pipeline)
    }
}

struct FragmentCoordinator {
    fragment_id: String,
    node: PlanNode,
    initialized: bool,
    data_exchange: DataExchange,
    pipeline: Option<NewPipeline>,
    executor: Option<Arc<PipelineCompleteExecutor>>,
}

impl FragmentCoordinator {
    pub fn create(packet: &FragmentPacket) -> FragmentCoordinator {
        FragmentCoordinator {
            initialized: false,
            node: packet.node.clone(),
            fragment_id: packet.fragment_id.to_owned(),
            data_exchange: packet.data_exchange.clone(),
            pipeline: None,
            executor: None,
        }
    }

    pub fn create_exchange_params(&self, query: &QueryCoordinator) -> Result<ExchangeParams> {
        match &self.data_exchange {
            DataExchange::Merge(exchange) => Ok(ExchangeParams::MergeExchange(
                MergeExchangeParams {
                    schema: self.node.schema(),
                    query_id: query.query_id.to_string(),
                    fragment_id: self.fragment_id.to_owned(),
                    destination_id: exchange.destination_id.clone(),
                }
            )),
            DataExchange::HashDataExchange(exchange) => Ok(ExchangeParams::ShuffleExchange(
                ShuffleExchangeParams {
                    schema: self.node.schema(),
                    query_id: query.query_id.to_string(),
                    fragment_id: self.fragment_id.to_string(),
                    executor_id: query.executor_id.to_string(),
                    destination_ids: exchange.destination_ids.to_owned(),
                    shuffle_scatter: Arc::new(Box::new(HashFlightScatter::try_create(
                        query.ctx.clone(),
                        self.node.schema(),
                        Some(exchange.exchange_expression.clone()),
                        exchange.destination_ids.len(),
                    )?)),
                }
            )),
        }
    }

    pub fn prepare_pipeline(&mut self, ctx: &Arc<QueryContext>, exchange_params: &ExchangeParams) -> Result<()> {
        if self.initialized {
            return Ok(());
        }

        self.initialized = true;
        let query_pipeline_builder = QueryPipelineBuilder::create(ctx.clone());
        let mut fragment_pipeline = query_pipeline_builder.finalize(&self.node)?;

        if !fragment_pipeline.is_pulling_pipeline()? {
            return Err(ErrorCode::LogicalError("Logical error, It's a bug"));
        }

        // Add exchange data publisher.
        ExchangePublisher::publisher_sink(&ctx, &exchange_params, &mut fragment_pipeline)?;

        if !fragment_pipeline.is_complete_pipeline()? {
            return Err(ErrorCode::LogicalError("Logical error, It's a bug"));
        }

        self.pipeline = Some(fragment_pipeline);
        Ok(())
    }
}
