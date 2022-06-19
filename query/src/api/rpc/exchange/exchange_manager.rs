use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::atomic::Ordering::Acquire;
use std::sync::Arc;

use async_channel::Receiver;
use async_channel::Sender;
use common_arrow::arrow_format::flight::data::FlightData;
use common_arrow::arrow_format::flight::service::flight_service_client::FlightServiceClient;
use common_base::base::tokio::task::JoinHandle;
use common_base::base::Runtime;
use common_base::base::Thread;
use common_base::base::TrySpawn;
use common_base::infallible::ReentrantMutex;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_grpc::ConnectionFactory;
use common_planners::PlanNode;
use futures::StreamExt;
use tonic::Streaming;

use crate::api::rpc::exchange::exchange_channel::FragmentReceiver;
use crate::api::rpc::exchange::exchange_channel::FragmentSender;
use crate::api::rpc::exchange::exchange_params::ExchangeParams;
use crate::api::rpc::exchange::exchange_params::MergeExchangeParams;
use crate::api::rpc::exchange::exchange_params::ShuffleExchangeParams;
use crate::api::rpc::exchange::exchange_publisher::ExchangePublisher;
use crate::api::rpc::exchange::exchange_subscriber::ExchangeSubscriber;
use crate::api::rpc::flight_actions::PreparePipeline;
use crate::api::rpc::flight_actions::PreparePublisher;
use crate::api::rpc::flight_scatter_hash::HashFlightScatter;
use crate::api::rpc::packet::DataPacket;
use crate::api::rpc::packet::ExecutePacket;
use crate::api::DataExchange;
use crate::api::ExecutorPacket;
use crate::api::FlightAction;
use crate::api::FlightClient;
use crate::api::FragmentPacket;
use crate::api::PrepareChannel;
use crate::interpreters::QueryFragmentsActions;
use crate::pipelines::new::executor::PipelineCompleteExecutor;
use crate::pipelines::new::NewPipe;
use crate::pipelines::new::NewPipeline;
use crate::pipelines::new::QueryPipelineBuilder;
use crate::sessions::QueryContext;
use crate::Config;

pub struct DataExchangeManager {
    config: Config,
    queries_coordinator: ReentrantMutex<HashMap<String, QueryCoordinator>>,
}

impl DataExchangeManager {
    pub fn create(config: Config) -> Arc<DataExchangeManager> {
        Arc::new(DataExchangeManager {
            config,
            queries_coordinator: ReentrantMutex::new(HashMap::new()),
        })
    }

    // Create connections for cluster all nodes. We will push data through this connection.
    pub fn handle_prepare_publisher(&self, prepare: &PreparePublisher) -> Result<()> {
        let mut queries_coordinator = self.queries_coordinator.lock();

        let publisher_packet = &prepare.publisher_packet;
        match queries_coordinator.get_mut(&publisher_packet.query_id) {
            None => Err(ErrorCode::LogicalError(format!(
                "Query {} not found in cluster.",
                publisher_packet.query_id
            ))),
            Some(coordinator) => coordinator.init_publisher(self.config.clone(), publisher_packet),
        }
    }

    // Execute query in background
    pub fn handle_execute_pipeline(&self, query_id: &str) -> Result<()> {
        let mut queries_coordinator = self.queries_coordinator.lock();

        match queries_coordinator.get_mut(query_id) {
            None => Err(ErrorCode::LogicalError(format!(
                "Query {} not found in cluster.",
                query_id
            ))),
            Some(coordinator) => coordinator.execute_pipeline(),
        }
    }

    // Receive data by cluster other nodes.
    pub async fn handle_do_put(
        &self,
        id: &str,
        source: &str,
        stream: Streaming<FlightData>,
    ) -> Result<JoinHandle<()>> {
        let mut queries_coordinator = self.queries_coordinator.lock();

        match queries_coordinator.get_mut(id) {
            None => Err(ErrorCode::LogicalError(format!(
                "Query {} not found in cluster.",
                id
            ))),
            Some(coordinator) => coordinator.receive_data(source, stream),
        }
    }

    // Create a pipeline based on query plan
    pub fn handle_prepare_pipeline(
        &self,
        ctx: &Arc<QueryContext>,
        prepare: &PreparePipeline,
    ) -> Result<()> {
        let mut queries_coordinator = self.queries_coordinator.lock();

        let executor_packet = &prepare.executor_packet;
        // TODO: When the query is not executed for a long time after submission, we need to remove it
        match queries_coordinator.entry(executor_packet.query_id.to_owned()) {
            Entry::Occupied(_) => Err(ErrorCode::LogicalError(format!(
                "Already exists query id {:?}",
                executor_packet.query_id
            ))),
            Entry::Vacant(entry) => {
                let query_coordinator = QueryCoordinator::create(ctx, executor_packet)?;
                let query_coordinator = entry.insert(query_coordinator);
                query_coordinator.prepare_pipeline()?;

                if executor_packet.executor != executor_packet.request_executor {
                    query_coordinator.prepare_subscribes_channel(executor_packet)?;
                }

                Ok(())
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
                )
                .await?,
            ))),
            false => Ok(FlightClient::new(FlightServiceClient::new(
                ConnectionFactory::create_rpc_channel(address.to_owned(), None, None).await?,
            ))),
        };
    }

    async fn prepare_pipeline(&self, packets: &[ExecutorPacket], timeout: u64) -> Result<()> {
        for executor_packet in packets {
            if !executor_packet
                .executors_info
                .contains_key(&executor_packet.executor)
            {
                return Err(ErrorCode::LogicalError(format!(
                    "Not found {} node in cluster",
                    &executor_packet.executor
                )));
            }

            let executor_packet = executor_packet.clone();
            let executor_info = &executor_packet.executors_info[&executor_packet.executor];
            let mut connection = self.create_client(&executor_info.flight_address).await?;
            let action = FlightAction::PreparePipeline(PreparePipeline { executor_packet });
            connection.execute_action(action, timeout).await?;
        }

        Ok(())
    }

    async fn prepare_channel(&self, packets: Vec<PrepareChannel>, timeout: u64) -> Result<()> {
        for publisher_packet in packets.into_iter() {
            if !publisher_packet
                .data_endpoints
                .contains_key(&publisher_packet.executor)
            {
                return Err(ErrorCode::LogicalError(format!(
                    "Not found {} node in cluster",
                    &publisher_packet.executor
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
            if !execute_packet
                .executors_info
                .contains_key(&execute_packet.executor)
            {
                return Err(ErrorCode::LogicalError(format!(
                    "Not found {} node in cluster",
                    &execute_packet.executor
                )));
            }

            let executor_info = &execute_packet.executors_info[&execute_packet.executor];
            let mut connection = self.create_client(&executor_info.flight_address).await?;
            let action = FlightAction::ExecutePipeline(execute_packet.query_id);
            connection.execute_action(action, timeout).await?;
        }

        Ok(())
    }

    pub async fn commit_actions(
        &self,
        ctx: Arc<QueryContext>,
        actions: QueryFragmentsActions,
    ) -> Result<NewPipeline> {
        let settings = ctx.get_settings();
        let timeout = settings.get_flight_client_timeout()?;
        let root_actions = actions.get_root_actions()?;
        let root_fragment_id = root_actions.fragment_id.to_owned();

        // Submit distributed tasks to all nodes.
        let prepare_pipeline = actions.prepare_packets(ctx.clone())?;
        self.prepare_pipeline(&prepare_pipeline, timeout).await?;

        // Get local pipeline of local task
        let schema = root_actions.get_schema()?;
        let mut root_pipeline =
            self.build_root_pipeline(ctx.get_id(), root_fragment_id, schema, &prepare_pipeline)?;

        let prepare_channel = actions.prepare_channel(ctx.clone())?;
        self.prepare_channel(prepare_channel, timeout).await?;

        QueryCoordinator::init_pipeline(&mut root_pipeline)?;
        self.execute_pipeline(actions.execute_packets(ctx.clone())?, timeout)
            .await?;

        Ok(root_pipeline)
    }

    fn build_root_pipeline(
        &self,
        query_id: String,
        fragment_id: usize,
        schema: DataSchemaRef,
        executor_packet: &[ExecutorPacket],
    ) -> Result<NewPipeline> {
        let mut pipeline = NewPipeline::create();
        self.get_fragment_source(query_id, fragment_id, schema, &mut pipeline)?;

        for executor_packet in executor_packet {
            if executor_packet.executor == executor_packet.request_executor {
                let mut queries_coordinator = self.queries_coordinator.lock();

                match queries_coordinator.entry(executor_packet.query_id.to_owned()) {
                    Entry::Vacant(_) => Err(ErrorCode::LogicalError(format!(
                        "Already exists query id {:?}",
                        executor_packet.query_id
                    ))),
                    Entry::Occupied(mut entry) => {
                        entry.get_mut().prepare_subscribes_channel(executor_packet)
                    }
                }?;
            }
        }

        Ok(pipeline)
    }

    pub fn get_fragment_sink(
        &self,
        query_id: &str,
        id: usize,
        endpoint: &str,
    ) -> Result<FragmentSender> {
        let queries_coordinator = self.queries_coordinator.lock();

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
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        let mut queries_coordinator = self.queries_coordinator.lock();

        match queries_coordinator.get_mut(&query_id) {
            None => Err(ErrorCode::LogicalError("Query not exists.")),
            Some(query_coordinator) => {
                query_coordinator.subscribe_fragment(fragment_id, schema, pipeline)
            }
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
}

impl QueryCoordinator {
    pub fn create(ctx: &Arc<QueryContext>, executor: &ExecutorPacket) -> Result<QueryCoordinator> {
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
        for (_, coordinator) in self.fragments_coordinator.iter_mut() {
            coordinator.prepare_pipeline(&self.ctx)?;
        }

        Ok(())
    }

    pub fn prepare_subscribes_channel(&mut self, prepare: &ExecutorPacket) -> Result<()> {
        for source in prepare.source_2_fragments.keys() {
            if source != &prepare.executor {
                let (tx, rx) = async_channel::bounded(1);

                let mut receiver = ExchangeReceiver::create(rx, &self.subscribe_fragments);
                self.runtime.spawn(async move { receiver.listen().await });
                self.subscribe_channel.insert(source.to_string(), tx);
            }
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
        let complete_executor = Arc::new(PipelineCompleteExecutor::from_pipelines(
            async_runtime,
            pipelines,
        )?);

        Thread::named_spawn(Some(String::from("Executor")), move || {
            if let Err(cause) = complete_executor.execute() {
                // TODO: send error code to request server.
                // if let Some(request_tx) = self.request_server_tx.take() {
                //     // request_tx.send
                // }
                println!("execute failure: {:?}", cause);
            }

            println!("execute successfully.");
        });

        Ok(())
    }

    pub fn init_publisher(&mut self, config: Config, packet: &PrepareChannel) -> Result<()> {
        for (target, fragments) in &packet.target_2_fragments {
            if target != &packet.executor {
                if self.publish_fragments.iter().any(|((t, _), _)| t == target) {
                    return Err(ErrorCode::LogicalError("Publisher id already exists"));
                }

                let config = config.clone();
                let target_node_info = &packet.target_nodes_info[target];
                let address = target_node_info.flight_address.to_string();
                let tx = self.spawn_pub_worker(config, address)?;

                if target == &packet.request_server {
                    self.request_server_tx = Some(tx.clone());
                }

                for fragment_id in fragments {
                    let (f_tx, f_rx) = async_channel::bounded(1);

                    let c_tx = tx.clone();
                    let fragment_id_clone = *fragment_id;
                    let target_clone = target.clone();
                    let source_clone = packet.executor.clone();
                    self.runtime.spawn(async move {
                        'fragment_loop: while let Ok(packet) = f_rx.recv().await {
                            if let Err(_cause) = c_tx.send(packet).await {
                                common_tracing::tracing::warn!(
                                    "{} to {} channel closed.",
                                    source_clone,
                                    target_clone
                                );

                                break 'fragment_loop;
                            }
                        }

                        if let Err(_cause) =
                            c_tx.send(DataPacket::EndFragment(fragment_id_clone)).await
                        {
                            common_tracing::tracing::warn!(
                                "{} to {} channel closed.",
                                source_clone,
                                target_clone
                            );
                        }
                    });

                    self.publish_fragments.insert(
                        (target.clone(), *fragment_id),
                        FragmentSender::create_unrecorded(f_tx),
                    );
                }
            }
        }

        for coordinator in self.fragments_coordinator.values_mut() {
            if let Some(pipeline) = coordinator.pipeline.as_mut() {
                Self::init_pipeline(pipeline)?;
            }
        }

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

    pub fn receive_data(
        &mut self,
        source: &str,
        mut stream: Streaming<FlightData>,
    ) -> Result<JoinHandle<()>> {
        if let Some(subscribe_channel) = self.subscribe_channel.remove(source) {
            let source = source.to_string();
            let target = self.executor_id.clone();
            return Ok(self.runtime.spawn(async move {
                'fragment_loop: while let Some(flight_data) = stream.next().await {
                    let send_res = match flight_data {
                        Err(status) => subscribe_channel.send(Err(ErrorCode::from(status))).await,
                        Ok(flight_data) => match DataPacket::from_flight(flight_data) {
                            Ok(data_packet) => subscribe_channel.send(Ok(data_packet)).await,
                            Err(error) => subscribe_channel.send(Err(error)).await,
                        },
                    };

                    if send_res.is_err() {
                        common_tracing::tracing::warn!(
                            "Subscribe channel closed, source {}, target {}",
                            source,
                            target
                        );

                        break 'fragment_loop;
                    }
                }
            }));
        };

        Err(ErrorCode::LogicalError(""))
    }

    async fn create_client(config: Config, address: &str) -> Result<FlightClient> {
        return match config.tls_query_cli_enabled() {
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
        };
    }

    fn spawn_pub_worker(&mut self, config: Config, addr: String) -> Result<Sender<DataPacket>> {
        let ctx = self.ctx.clone();
        let query_id = self.query_id.clone();
        let source = self.executor_id.clone();
        let (tx, rx) = async_channel::bounded(2);

        self.runtime.spawn(async move {
            let mut connection = Self::create_client(config, &addr).await?;

            if let Err(status) = connection.do_put(&query_id, &source, rx).await {
                // finished.store(true, Ordering::Release);

                println!("do put error: {:?}", status);
                // TODO: shutdown pipeline executor,
                // Shutdown all query fragments executor and report error to request server.
                let _exchange_manager = ctx.get_exchange_manager();
                // exchange_manager.shutdown_query(query_id, status);
            }

            common_exception::Result::Ok(())
        });

        Ok(tx)
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
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        let (tx, rx) = async_channel::bounded(1);

        // Register subscriber for data exchange.
        self.subscribe_fragments
            .insert(fragment_id, FragmentReceiver::create_unrecorded(tx));

        // Merge pipelines if exist locally pipeline
        if let Some(mut fragment_coordinator) = self.fragments_coordinator.remove(&fragment_id) {
            fragment_coordinator.prepare_pipeline(&self.ctx)?;

            if fragment_coordinator.pipeline.is_none() {
                return Err(ErrorCode::LogicalError(
                    "Pipeline is none, maybe query fragment circular dependency.",
                ));
            }

            if fragment_coordinator.data_exchange.is_none() {
                // When the root fragment and the data has been send to the coordination node,
                // we do not need to wait for the data of other nodes.
                *pipeline = fragment_coordinator.pipeline.unwrap();
                return Ok(());
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
    data_exchange: Option<DataExchange>,
    pipeline: Option<NewPipeline>,
}

impl FragmentCoordinator {
    pub fn create(packet: &FragmentPacket) -> Box<FragmentCoordinator> {
        Box::new(FragmentCoordinator {
            initialized: false,
            node: packet.node.clone(),
            fragment_id: packet.fragment_id,
            data_exchange: packet.data_exchange.clone(),
            pipeline: None,
        })
    }

    pub fn create_exchange_params(&self, query: &QueryCoordinator) -> Result<ExchangeParams> {
        match &self.data_exchange {
            None => Err(ErrorCode::LogicalError("Cannot found data exchange.")),
            Some(DataExchange::Merge(exchange)) => {
                Ok(ExchangeParams::MergeExchange(MergeExchangeParams {
                    schema: self.node.schema(),
                    fragment_id: self.fragment_id,
                    query_id: query.query_id.to_string(),
                    destination_id: exchange.destination_id.clone(),
                }))
            }
            Some(DataExchange::ShuffleDataExchange(exchange)) => {
                Ok(ExchangeParams::ShuffleExchange(ShuffleExchangeParams {
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
                }))
            }
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

pub struct ExchangeReceiver {
    // runtime: Arc<Runtime>,
    rx: Receiver<Result<DataPacket>>,
    fragments_receiver: Vec<Option<FragmentReceiver>>,
}

impl ExchangeReceiver {
    pub fn create(
        // runtime: Arc<Runtime>,
        rx: Receiver<Result<DataPacket>>,
        fragments_receiver: &HashMap<usize, FragmentReceiver>,
    ) -> ExchangeReceiver {
        let max_fragments_id = fragments_receiver.keys().max().cloned().unwrap_or(0);
        let mut temp = vec![None; max_fragments_id + 1];

        for (fragment_id, receiver) in fragments_receiver {
            temp[*fragment_id] = Some(receiver.clone());
        }

        ExchangeReceiver {
            rx,
            fragments_receiver: temp,
        }
    }

    pub async fn listen(&mut self) -> Result<()> {
        // TODO: need select with timeout for stream.next()
        while let Ok(flight_data) = self.rx.recv().await {
            // if finished.load(Ordering::Relaxed) {
            //     break;
            // }
            match flight_data {
                Err(error_code) => {
                    return Err(error_code);
                }
                Ok(data_packet) => match data_packet {
                    DataPacket::Data(fragment_id, data) => {
                        if let Some(tx) = &self.fragments_receiver[fragment_id] {
                            if let Err(_cause) = tx.send(Ok(data)).await {
                                println!("Fragment channel closed.");
                                return Ok(());
                            }
                        }
                    }
                    DataPacket::Progress(_values) => {
                        // ctx.get_scan_progress().incr(&values);
                    }
                    DataPacket::ErrorCode(error_code) => {
                        return Err(error_code);
                    }
                    DataPacket::EndFragment(fragment) => {
                        if fragment < self.fragments_receiver.len() {
                            if let Some(tx) = self.fragments_receiver[fragment].take() {
                                drop(tx);
                                std::sync::atomic::fence(Acquire);
                            }
                        }
                    }
                },
            };
        }

        Ok(())
    }
}
