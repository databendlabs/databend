use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::atomic::Ordering::Acquire;
use std::time::Duration;
use common_arrow::arrow_format::flight::data::FlightData;
use common_arrow::arrow_format::flight::service::flight_service_client::FlightServiceClient;
use common_base::base::{Runtime, Thread, tokio, TrySpawn};
use crate::pipelines::new::processors::Processor;
use crate::pipelines::new::processors::processor::{Event, ProcessorPtr};
use common_exception::{ErrorCode, Result};
use common_base::infallible::{Mutex, ReentrantMutex, RwLock};
use crate::api::{DataExchange, ExecutorPacket, FlightAction, FlightClient, FragmentPacket, PrepareChannel};
use crate::interpreters::QueryFragmentsActions;
use crate::pipelines::new::executor::PipelineCompleteExecutor;
use crate::pipelines::new::{NewPipe, NewPipeline, QueryPipelineBuilder};
use crate::sessions::QueryContext;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_grpc::ConnectionFactory;
use common_meta_types::NodeInfo;
use common_planners::PlanNode;
use async_channel::{RecvError, Sender, TrySendError};
use async_channel::Receiver;
use futures::{Stream, StreamExt};
use tonic::{Status, Streaming};
use common_base::base::tokio::task::JoinHandle;
use crate::api::rpc::exchange::exchange_channel::{FragmentReceiver, FragmentSender};
use crate::api::rpc::exchange::exchange_params::{ExchangeParams, MergeExchangeParams, ShuffleExchangeParams};
use crate::api::rpc::exchange::exchange_publisher::ExchangePublisher;
use crate::api::rpc::exchange::exchange_subscriber::ExchangeSubscriber;
use crate::api::rpc::flight_actions::{PreparePipeline, PreparePublisher};
use crate::api::rpc::flight_scatter::FlightScatter;
use crate::api::rpc::flight_scatter_hash::HashFlightScatter;
use crate::api::rpc::packet::{DataPacket, ExecutePacket};
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

    // Create connections for cluster all nodes. We will push data through this connection.
    pub fn handle_prepare_publisher(&self, prepare: &PreparePublisher) -> Result<()> {
        let mut queries_coordinator = self.queries_coordinator.lock();

        let publisher_packet = &prepare.publisher_packet;
        match queries_coordinator.get_mut(&publisher_packet.query_id) {
            None => Err(ErrorCode::LogicalError(format!("Query {} not found in cluster.", publisher_packet.query_id))),
            Some(coordinator) => coordinator.init_publisher(self.config.clone(), publisher_packet)
        }
    }

    // Execute query in background
    pub fn handle_execute_pipeline(&self, query_id: &str) -> Result<()> {
        let mut queries_coordinator = self.queries_coordinator.lock();

        match queries_coordinator.get_mut(query_id) {
            None => Err(ErrorCode::LogicalError(format!("Query {} not found in cluster.", query_id))),
            Some(coordinator) => coordinator.execute_pipeline()
        }
    }

    // Receive data by cluster other nodes.
    pub async fn handle_do_put(&self, id: &str, stream: Streaming<FlightData>) -> Result<JoinHandle<Result<()>>> {
        let mut queries_coordinator = self.queries_coordinator.lock();

        match queries_coordinator.get_mut(id) {
            None => Err(ErrorCode::LogicalError(format!("Query {} not found in cluster.", id))),
            Some(coordinator) => Ok(coordinator.receive_data(stream))
        }
    }

    // Create a pipeline based on query plan
    pub fn handle_prepare_executor(&self, ctx: &Arc<QueryContext>, prepare: &PreparePipeline) -> Result<()> {
        let mut queries_coordinator = self.queries_coordinator.lock();

        let executor_packet = &prepare.executor_packet;
        // TODO: When the query is not executed for a long time after submission, we need to remove it
        match queries_coordinator.entry(executor_packet.query_id.to_owned()) {
            Entry::Occupied(_) => Err(ErrorCode::LogicalError(format!("Already exists query id {:?}", executor_packet.query_id))),
            Entry::Vacant(entry) => {
                let query_coordinator = QueryCoordinator::create(ctx, executor_packet)?;
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

    async fn prepare_publisher(&self, packets: Vec<PrepareChannel>, timeout: u64) -> Result<()> {
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
        let mut root_pipeline = self.build_root_pipeline(ctx.get_id(), root_fragment_id, schema)?;

        self.prepare_publisher(actions.prepare_publisher(ctx.clone())?, timeout).await?;

        QueryCoordinator::init_pipeline(&mut root_pipeline)?;
        self.execute_pipeline(actions.execute_packets(ctx.clone())?, timeout).await?;

        Ok(root_pipeline)
    }

    fn build_root_pipeline(&self, query_id: String, fragment_id: usize, schema: DataSchemaRef) -> Result<NewPipeline> {
        let mut pipeline = NewPipeline::create();
        self.get_fragment_source(query_id, fragment_id, schema, &mut pipeline)?;
        Ok(pipeline)
    }

    pub fn get_fragment_sink(&self, query_id: &str, id: usize, endpoint: &str) -> Result<FragmentSender> {
        let mut queries_coordinator = self.queries_coordinator.lock();

        match queries_coordinator.get(query_id) {
            None => Err(ErrorCode::LogicalError("Query not exists.")),
            Some(coordinator) => coordinator.get_fragment_sink(id, endpoint),
        }
    }

    pub fn get_fragment_source(&self, query_id: String, fragment_id: usize, schema: DataSchemaRef, pipeline: &mut NewPipeline) -> Result<()> {
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
    runtime: Arc<Runtime>,
    finished: Arc<AtomicBool>,
    publish_fragments: HashMap<(String, usize), FragmentSender>,
    subscribe_fragments: HashMap<usize, FragmentReceiver>,
    fragments_coordinator: HashMap<usize, Box<FragmentCoordinator>>,
}

impl QueryCoordinator {
    pub fn create(ctx: &Arc<QueryContext>, executor: &ExecutorPacket) -> Result<QueryCoordinator> {
        let mut fragments_coordinator = HashMap::with_capacity(executor.fragments.len());

        for fragment in &executor.fragments {
            fragments_coordinator.insert(
                fragment.fragment_id.to_owned(),
                FragmentCoordinator::create(&fragment),
            );
        }

        Ok(QueryCoordinator {
            ctx: ctx.clone(),
            fragments_coordinator,
            publish_fragments: Default::default(),
            query_id: executor.query_id.to_owned(),
            subscribe_fragments: Default::default(),
            executor_id: executor.executor.to_owned(),
            finished: Arc::new(AtomicBool::new(false)),
            runtime: Arc::new(Runtime::with_worker_threads(2, Some(String::from("Cluster-Pub-Sub")))?),
        })
    }

    pub fn prepare_pipeline(&mut self) -> Result<()> {
        for (_, coordinator) in self.fragments_coordinator.iter_mut() {
            coordinator.prepare_pipeline(&self.ctx)?;
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
        for (_, coordinator) in &self.fragments_coordinator {
            params.push(coordinator.create_exchange_params(self)?);
        }

        for ((_, coordinator), params) in self.fragments_coordinator.iter_mut().zip(params) {
            if let Some(mut pipeline) = coordinator.pipeline.take() {
                pipeline.set_max_threads(max_threads as usize);

                if !pipeline.is_pulling_pipeline()? {
                    return Err(ErrorCode::LogicalError("Logical error, It's a bug"));
                }

                // Add exchange data publisher.
                ExchangePublisher::publisher_sink(&self.ctx, &params, &mut pipeline)?;

                if !pipeline.is_complete_pipeline()? {
                    return Err(ErrorCode::LogicalError("Logical error, It's a bug"));
                }

                QueryCoordinator::init_pipeline(&mut pipeline)?;
                pipelines.push(pipeline);
            }
        }

        let async_runtime = self.ctx.get_storage_runtime();
        let complete_executor = PipelineCompleteExecutor::from_pipelines(async_runtime, pipelines)?;

        Thread::named_spawn(Some(String::from("Executor")), move || {
            if let Err(cause) = complete_executor.execute() {
                // TODO: send error code to request server.
                println!("execute failure: {:?}", cause);
            }

            println!("execute successfully.");
        });

        Ok(())
    }

    pub fn init_publisher(&mut self, config: Config, packet: &PrepareChannel) -> Result<()> {
        for (target, fragments) in &packet.target_2_fragments {
            if self.publish_fragments
                .iter()
                .any(|((t, _), _)| t == target) {
                return Err(ErrorCode::LogicalError("Publisher id already exists"));
            }

            if target == &packet.executor {
                continue;
            }

            let config = config.clone();
            let target_node_info = &packet.target_nodes_info[target];
            let address = target_node_info.flight_address.to_string();
            let tx = self.spawn_pub_worker(config, address)?;

            for fragment_id in fragments {
                let (f_tx, mut f_rx) = async_channel::bounded(1);

                let c_tx = tx.clone();
                let fragment_id_clone = *fragment_id;
                println!("fragment: {}, source: {}, target: {}", fragment_id, packet.executor, target);
                self.runtime.spawn(async move {
                    while let Ok(packet) = f_rx.recv().await {
                        if let Err(cause) = c_tx.send(packet).await {
                            // TODO: channel closed.
                        }
                    }

                    if !f_rx.is_empty() {
                        println!("rx is not empty");
                    }
                    println!("End fragment {}", fragment_id_clone);
                    if let Err(cause) = c_tx.send(DataPacket::EndFragment(fragment_id_clone)).await {
                        // TODO: channel closed.
                    }
                });

                self.publish_fragments.insert(
                    (target.clone(), *fragment_id),
                    FragmentSender::create_unrecorded(f_tx),
                );
            }
        }

        for (_, coordinator) in &mut self.fragments_coordinator {
            if let Some(pipeline) = coordinator.pipeline.as_mut() {
                Self::init_pipeline(pipeline)?;
            }
        }
        // for (executor, info) in &packet.data_endpoints {
        //     // if executor == &packet.request_server {
        //     //     // Send log, metric, trace, (progress?)
        //     // }
        //
        //     // let ctx = self.ctx.clone();
        //     // if executor != &packet.executor {
        //     if self.publish_fragments.contains_key(executor) {
        //         return Err(ErrorCode::LogicalError("Publisher id already exists"));
        //     }
        //
        //     let config = config.clone();
        //     let address = info.flight_address.to_string();
        //     let tx = self.spawn_pub_worker(config, address)?;
        //     self.publish_fragments.insert(executor.to_string(), tx);
        //     // }
        // }

        Ok(())
    }

    pub fn init_pipeline(pipeline: &mut NewPipeline) -> Result<()> {
        for pipe in &mut pipeline.pipes {
            if let NewPipe::SimplePipe { processors, .. } = pipe {
                for processor in processors {
                    ExchangePublisher::init(processor)?;
                }
            }
        }

        Ok(())
    }

    pub fn receive_data(&mut self, mut stream: Streaming<FlightData>) -> JoinHandle<Result<()>> {
        let ctx = self.ctx.clone();
        let finished = self.finished.clone();
        let mut receiver = ExchangeReceiver::create(stream, &self.subscribe_fragments);

        self.runtime.spawn(async move {
            receiver.listen().await?;
            Ok(())
        })
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

    fn spawn_pub_worker(&mut self, config: Config, addr: String) -> Result<Sender<DataPacket>> {
        let ctx = self.ctx.clone();
        let query_id = self.query_id.clone();
        let (tx, rx) = async_channel::bounded(2);

        self.runtime.spawn(async move {
            let mut connection = Self::create_client(config, &addr).await?;

            if let Err(status) = connection.do_put(&query_id, rx).await {
                // finished.store(true, Ordering::Release);

                println!("do put error: {:?}", status);
                // TODO:
                // Shutdown all query fragments executor and report error to request server.
                let exchange_manager = ctx.get_exchange_manager();
                // exchange_manager.shutdown_query(query_id, status);
            }

            common_exception::Result::Ok(())
        });

        // let sender = tx.clone();
        // let finished = self.finished.clone();
        // self.runtime.spawn(async move {
        //     while let Ok(flight_data) = rx.recv().await {
        //         if finished.load(Ordering::Relaxed) {
        //             break;
        //         }
        //
        //         if let Err(TrySendError::Full(data)) = sender.try_send(flight_data) {
        //             if let Err(_) = sender.send(data).await {
        //                 common_tracing::tracing::warn!("Publisher channel is closed.");
        //                 break;
        //             }
        //         }
        //
        //         if finished.load(Ordering::Relaxed) {
        //             break;
        //         }
        //     }
        // });

        Ok(tx)
    }

    pub fn get_fragment_sink(&self, id: usize, endpoint: &str) -> Result<FragmentSender> {
        match self.publish_fragments.get(&(endpoint.to_string(), id)) {
            None => Err(ErrorCode::LogicalError(format!("Not found node {} in cluster", endpoint))),
            Some(publisher) => Ok(publisher.clone())
        }
    }

    pub fn subscribe_fragment(&mut self, fragment_id: usize, schema: DataSchemaRef, pipeline: &mut NewPipeline) -> Result<()> {
        let (tx, rx) = async_channel::bounded(1);

        // Register subscriber for data exchange.
        self.subscribe_fragments.insert(fragment_id, FragmentReceiver::create_unrecorded(tx));

        // Merge pipelines if exist locally pipeline
        if let Some(mut fragment_coordinator) = self.fragments_coordinator.remove(&fragment_id) {
            fragment_coordinator.prepare_pipeline(&self.ctx)?;

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
    node: PlanNode,
    initialized: bool,
    fragment_id: usize,
    data_exchange: DataExchange,
    pipeline: Option<NewPipeline>,
    executor: Option<Arc<PipelineCompleteExecutor>>,
}

impl FragmentCoordinator {
    pub fn create(packet: &FragmentPacket) -> Box<FragmentCoordinator> {
        Box::new(FragmentCoordinator {
            initialized: false,
            node: packet.node.clone(),
            fragment_id: packet.fragment_id,
            data_exchange: packet.data_exchange.clone(),
            pipeline: None,
            executor: None,
        })
    }

    pub fn create_exchange_params(&self, query: &QueryCoordinator) -> Result<ExchangeParams> {
        match &self.data_exchange {
            DataExchange::Merge(exchange) => Ok(ExchangeParams::MergeExchange(
                MergeExchangeParams {
                    schema: self.node.schema(),
                    fragment_id: self.fragment_id,
                    query_id: query.query_id.to_string(),
                    destination_id: exchange.destination_id.clone(),
                }
            )),
            DataExchange::HashDataExchange(exchange) => Ok(ExchangeParams::ShuffleExchange(
                ShuffleExchangeParams {
                    schema: self.node.schema(),
                    fragment_id: self.fragment_id,
                    query_id: query.query_id.to_string(),
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

    pub fn prepare_pipeline(&mut self, ctx: &Arc<QueryContext>) -> Result<()> {
        if !self.initialized {
            self.initialized = true;
            let pipeline_builder = QueryPipelineBuilder::create(ctx.clone());
            self.pipeline = Some(pipeline_builder.finalize(&self.node)?);
        }

        Ok(())
    }
}

struct ExchangeReceiver {
    // finished: bool,
    stream: Streaming<FlightData>,
    fragments_receiver: Vec<Option<FragmentReceiver>>,
}

impl ExchangeReceiver {
    pub fn create(
        stream: Streaming<FlightData>,
        fragments_receiver: &HashMap<usize, FragmentReceiver>,
    ) -> ExchangeReceiver {
        let max_fragments_id = fragments_receiver.keys().max().cloned().unwrap_or(0);
        let mut temp = vec![None; max_fragments_id + 1];

        for (fragment_id, receiver) in fragments_receiver {
            temp[*fragment_id] = Some(receiver.clone());
        }

        ExchangeReceiver {
            stream,
            fragments_receiver: temp,
        }
    }

    async fn process_data(&mut self, flight_data: FlightData) -> Result<()> {
        match DataPacket::from_flight(flight_data)? {
            DataPacket::Data(fragment_id, data) => {
                if let Some(tx) = &self.fragments_receiver[fragment_id] {
                    // println!("Fragment channel closed.");
                    if let Err(cause) = tx.send(Ok(data)).await {
                        println!("Fragment channel closed.");
                        return Ok(());
                    }
                }
            }
            DataPacket::Progress(values) => {
                // ctx.get_scan_progress().incr(&values);
            }
            DataPacket::ErrorCode(error_code) => {
                return Err(error_code);
            }
            DataPacket::EndFragment(fragment) => {
                if let Some(tx) = self.fragments_receiver[fragment].take() {
                    // tokio::time::sleep(Duration::from_secs(5)).await;
                    drop(tx);
                    std::sync::atomic::fence(Acquire);
                }
            }
        };

        Ok(())
    }

    pub async fn listen(&mut self) -> Result<()> {
        // TODO: need select with timeout for stream.next()
        while let Some(flight_data) = self.stream.next().await {
            // if finished.load(Ordering::Relaxed) {
            //     break;
            // }
            match flight_data {
                Err(status) => Err(ErrorCode::from(status)),
                Ok(flight_data) => self.process_data(flight_data).await,
            }?;
        }

        // for (_index, fragment_tx) in &subscribe_fragments {
        //     fragment_tx.close();
        // }

        Ok(())
    }
}
