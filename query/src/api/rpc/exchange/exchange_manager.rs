use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use common_arrow::arrow_format::flight::data::FlightData;
use common_arrow::arrow_format::flight::service::flight_service_client::FlightServiceClient;
use crate::pipelines::new::processors::Processor;
use crate::pipelines::new::processors::processor::{Event, ProcessorPtr};
use common_exception::{ErrorCode, Result};
use common_infallible::{Mutex, ReentrantMutex, RwLock};
use crate::api::{DataExchange, ExecutorPacket, FlightAction, FlightClient, FragmentPacket};
use crate::interpreters::QueryFragmentsActions;
use crate::pipelines::new::executor::PipelineCompleteExecutor;
use crate::pipelines::new::{NewPipe, NewPipeline, QueryPipelineBuilder};
use crate::sessions::QueryContext;
use common_base::tokio;
use common_base::tokio::sync::mpsc::{Receiver, Sender};
use common_datablocks::DataBlock;
use common_grpc::ConnectionFactory;
use common_planners::PlanNode;
use crate::api::rpc::exchange::exchange_params::{HashExchangeParams, MergeExchangeParams, ExchangeParams};
use crate::api::rpc::exchange::exchange_publisher::ExchangePublisher;
use crate::api::rpc::exchange::exchange_subscriber::ExchangeSubscriber;
use crate::api::rpc::flight_actions::PrepareExecutor;
use crate::api::rpc::flight_scatter::FlightScatter;
use crate::api::rpc::flight_scatter_hash::HashFlightScatter;
use crate::configs::Config;
use crate::pipelines::new::processors::port::InputPort;

pub struct DataExchangeManager {
    config: Config,
    queries_coordinator: ReentrantMutex<HashMap<String, QueryCoordinator>>,
}

impl DataExchangeManager {
    pub fn create(config: Config) -> Arc<DataExchangeManager> {
        Arc::new(DataExchangeManager { config, queries_coordinator: ReentrantMutex::new(HashMap::new()) })
    }

    pub fn handle_prepare(&self, ctx: &Arc<QueryContext>, prepare: &PrepareExecutor) -> Result<()> {
        let mut queries_coordinator = self.queries_coordinator.lock();

        let executor_packet = &prepare.executor_packet;
        // TODO: When the query is not executed for a long time after submission, we need to remove it
        match queries_coordinator.entry(executor_packet.query_id.to_owned()) {
            Entry::Occupied(_) => Err(ErrorCode::LogicalError(format!("Already exists query id {:?}", executor_packet.query_id))),
            Entry::Vacant(entry) => {
                let query_coordinator = QueryCoordinator::create(ctx, executor_packet);
                entry.insert(query_coordinator).init()
            }
        }
    }

    async fn create_client(&self, address: &str) -> Result<FlightClient> {
        return match self.config.tls_query_cli_enabled() {
            true => Ok(FlightClient::new(FlightServiceClient::new(
                ConnectionFactory::create_rpc_channel(
                    address.to_owned(),
                    None,
                    Some(self.config.tls_query_client_conf()),
                )?,
            ))),
            false => Ok(FlightClient::new(FlightServiceClient::new(
                ConnectionFactory::create_rpc_channel(
                    address.to_owned(),
                    None,
                    None,
                )?,
            ))),
        };
    }

    async fn create_conn(&self, executor_packet: &ExecutorPacket) -> Result<FlightClient> {
        let executors_info = &executor_packet.executors_info;

        if !executors_info.contains_key(&executor_packet.executor) {
            return Err(ErrorCode::LogicalError(format!(
                "Not found {} node in cluster", &executor_packet.executor
            )));
        }

        let executor_info = &executors_info[&executor_packet.executor];
        self.create_client(&executor_info.flight_address).await
    }

    async fn prepare_executors(&self, packets: Vec<ExecutorPacket>, timeout: u64) -> Result<()> {
        for executor_packet in packets.into_iter() {
            let mut connection = self.create_conn(&executor_packet).await?;
            let action = FlightAction::PrepareExecutor(PrepareExecutor { executor_packet });
            connection.execute_action(action, timeout).await?;
        }

        Ok(())
    }

    // async fn execute_action(&self)

    pub async fn submit_query_actions(&self, ctx: Arc<QueryContext>, actions: QueryFragmentsActions) -> Result<NewPipeline> {
        let settings = ctx.get_settings();
        let timeout = settings.get_flight_client_timeout()?;
        let root_actions = actions.get_root_actions()?;
        let root_fragment_id = root_actions.fragment_id.to_owned();

        // Submit distributed tasks to all nodes.
        self.prepare_executors(actions.prepare_packets(ctx.clone())?, timeout).await?;

        // Get local pipeline of local task
        let root_pipeline = self.build_root_pipeline(ctx.get_id(), root_fragment_id)?;

        // TODO: prepare connections(sink)
        // TODO: execute distributed query
        unimplemented!()
        // Ok(root_pipeline)
    }

    fn build_root_pipeline(&self, query_id: String, fragment_id: String) -> Result<NewPipeline> {
        let mut pipeline = NewPipeline::create();
        self.get_fragment_source(query_id, fragment_id, &mut pipeline)?;
        Ok(pipeline)
    }

    pub fn get_fragment_sink(&self, query_id: &str, endpoint: &str) -> Result<Sender<FlightData>> {
        let mut queries_coordinator = self.queries_coordinator.lock();

        match queries_coordinator.get(query_id) {
            None => Err(ErrorCode::LogicalError("Query not exists.")),
            Some(coordinator) => coordinator.get_fragment_sink(endpoint),
        }
    }

    pub fn get_fragment_source(&self, query_id: String, fragment_id: String, pipeline: &mut NewPipeline) -> Result<()> {
        let mut queries_coordinator = self.queries_coordinator.lock();

        match queries_coordinator.get_mut(&query_id) {
            None => Err(ErrorCode::LogicalError("Query not exists.")),
            Some(query_coordinator) => query_coordinator.subscribe_fragment(fragment_id, pipeline)
        }
    }
}

struct QueryCoordinator {
    ctx: Arc<QueryContext>,
    query_id: String,
    executor_id: String,
    publish_fragments: HashMap<String, Sender<FlightData>>,
    subscribe_fragments: HashMap<String, Sender<Option<FlightData>>>,
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

    pub fn init(&mut self) -> Result<()> {
        for (_, coordinator) in self.fragments_coordinator.iter_mut() {
            coordinator.init(&self.ctx)?;
        }

        Ok(())
    }

    pub fn get_fragment_sink(&self, endpoint: &str) -> Result<Sender<FlightData>> {
        match self.publish_fragments.get(endpoint) {
            None => Err(ErrorCode::LogicalError(format!("Not found node {} in cluster", endpoint))),
            Some(publisher) => Ok(publisher.clone())
        }
    }

    pub fn subscribe_fragment(&mut self, fragment_id: String, pipeline: &mut NewPipeline) -> Result<()> {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);

        // Register subscriber for data exchange.
        self.subscribe_fragments.insert(fragment_id, tx);

        // Merge pipelines if exist locally pipeline
        if let Some(mut fragment_coordinator) = self.fragments_coordinator.remove(&fragment_id) {
            fragment_coordinator.init(&self.ctx)?;

            if fragment_coordinator.pipeline.is_none() {
                return Err(ErrorCode::LogicalError("Pipeline is none, maybe query fragment circular dependency."));
            }

            *pipeline = fragment_coordinator.pipeline.unwrap();
            let exchange_params = fragment_coordinator.create_exchange_params(self)?;

            // Add exchange data publisher.
            ExchangePublisher::via_exchange(&self.ctx, &exchange_params, pipeline);

            // Add exchange data subscriber.
            return ExchangeSubscriber::via_exchange(rx, &exchange_params, pipeline);
        }

        // Add exchange data subscriber.
        ExchangeSubscriber::create_source(rx, pipeline)
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
                    query_id: query.query_id.to_string(),
                    fragment_id: self.fragment_id.to_owned(),
                    destination_id: exchange.destination_id.clone(),
                }
            )),
            DataExchange::HashDataExchange(exchange) => Ok(ExchangeParams::HashExchange(
                HashExchangeParams {
                    schema: self.node.schema(),
                    query_id: query.query_id.to_string(),
                    fragment_id: self.fragment_id.to_string(),
                    executor_id: query.executor_id.to_string(),
                    destination_ids: exchange.destination_ids.to_owned(),
                    hash_scatter: HashFlightScatter::try_create(
                        self.node.schema(),
                        Some(exchange.exchange_expression.clone()),
                        exchange.destination_ids.len(),
                    )?,
                }
            )),
        }
    }

    pub fn init(&mut self, ctx: &Arc<QueryContext>) -> Result<()> {
        if self.initialized {
            return Ok(());
        }

        self.initialized = true;
        let query_pipeline_builder = QueryPipelineBuilder::create(ctx.clone());
        self.pipeline = Some(query_pipeline_builder.finalize(&self.node)?);
        // let pipeline = query_pipeline_builder.finalize(&self.node)?;
        //
        // if pipeline.is_pushing_pipeline()? || pipeline.is_complete_pipeline()? || !pipeline.is_pulling_pipeline()? {
        //     return Err(ErrorCode::LogicalError("Logical error, It's a bug"));
        // }
        //
        // // TODO: add exchange data sink
        // self.executor = Some(Arc::new(PipelineCompleteExecutor::try_create(async_runtime, pipeline)?));
        Ok(())
    }
}
