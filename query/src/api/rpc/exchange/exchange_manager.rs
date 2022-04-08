use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use crate::pipelines::new::processors::Processor;
use crate::pipelines::new::processors::processor::{Event, ProcessorPtr};
use common_exception::{ErrorCode, Result};
use common_infallible::{Mutex, ReentrantMutex, RwLock};
use crate::api::{ExecutorPacket, FlightAction, FragmentPacket};
use crate::interpreters::QueryFragmentsActions;
use crate::pipelines::new::executor::PipelineCompleteExecutor;
use crate::pipelines::new::{NewPipe, NewPipeline, QueryPipelineBuilder};
use crate::sessions::QueryContext;
use common_base::tokio;
use common_base::tokio::sync::mpsc::{Receiver, Sender};
use common_datablocks::DataBlock;
use common_planners::PlanNode;
use crate::api::rpc::flight_actions::PrepareNewPipeline;
use crate::configs::Config;
use crate::pipelines::new::processors::port::InputPort;

pub struct DataExchangeManager {
    queries_coordinator: ReentrantMutex<HashMap<String, QueryCoordinator>>,
}

impl DataExchangeManager {
    pub fn create(config: Config) -> Arc<DataExchangeManager> {
        Arc::new(DataExchangeManager { queries_coordinator: ReentrantMutex::new(HashMap::new()) })
    }

    pub fn handle_prepare(&self, ctx: &Arc<QueryContext>, prepare: &PrepareNewPipeline) -> Result<()> {
        let mut queries_coordinator = self.queries_coordinator.lock();

        // TODO: When the query is not executed for a long time after submission, we need to remove it
        match queries_coordinator.entry(prepare.query_id.to_owned()) {
            Entry::Occupied(_) => Err(ErrorCode::LogicalError(format!("Already exists query id {:?}", prepare.query_id))),
            Entry::Vacant(entry) => {
                let query_coordinator = QueryCoordinator::create(ctx, &prepare.executor_packet);
                entry.insert(query_coordinator).init()
            }
        }
    }

    pub fn submit_local_packet(&self, packet: ExecutorPacket) -> Result<()> {
        // self.handle_prepare(&packet)?;

        unimplemented!()
    }

    pub async fn submit_remote_packet(ctx: &Arc<QueryContext>, packet: ExecutorPacket) -> Result<()> {
        let config = ctx.get_config();
        let cluster = ctx.get_cluster();
        let timeout = ctx.get_settings().get_flight_client_timeout()?;
        let mut connection = cluster.create_node_conn(&packet.executor, &config).await?;

        connection.execute_action(FlightAction::PrepareNewPipeline(PrepareNewPipeline {
            query_id: ctx.get_id(),
            executor_packet: packet,
        }), timeout).await;
        unimplemented!()
    }

    pub async fn submit_packets(&self, ctx: Arc<QueryContext>, packets: Vec<ExecutorPacket>) -> Result<()> {
        let cluster = ctx.get_cluster();
        for executor_packet in packets {
            match executor_packet.executor == cluster.local_id() {
                true => self.submit_local_packet(executor_packet),
                false => Self::submit_remote_packet(&ctx, executor_packet).await
            }?;
        }

        Ok(())
    }

    pub async fn submit_query_actions(&self, ctx: Arc<QueryContext>, actions: QueryFragmentsActions) -> Result<NewPipeline> {
        let root_actions = actions.get_root_actions()?;
        let root_fragment_id = root_actions.fragment_id.to_owned();

        // Submit distributed tasks to all nodes and run them
        self.submit_packets(ctx.clone(), actions.to_packets(&ctx)?).await?;

        // Get local pipeline of local task
        self.build_root_pipeline(ctx.get_id(), root_fragment_id)
    }

    fn build_root_pipeline(&self, query_id: String, fragment_id: String) -> Result<NewPipeline> {
        let mut pipeline = NewPipeline::create();
        self.get_fragment_source(query_id, fragment_id, &mut pipeline)?;
        Ok(pipeline)
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
    publish_fragments: HashMap<String, Sender<DataBlock>>,
    subscribe_fragments: HashMap<String, Sender<Option<DataBlock>>>,
    fragments_coordinator: HashMap<String, FragmentCoordinator>,
}

impl QueryCoordinator {
    pub fn create(ctx: &Arc<QueryContext>, executor: &ExecutorPacket) -> QueryCoordinator {
        let mut fragments_coordinator = HashMap::with_capacity(executor.fragments_packets.len());

        for fragment in &executor.fragments_packets {
            fragments_coordinator.insert(
                fragment.fragment_id.to_owned(),
                FragmentCoordinator::create(&fragment.node),
            );
        }

        QueryCoordinator { ctx: ctx.clone(), publish_fragments: Default::default(), subscribe_fragments: Default::default(), fragments_coordinator }
    }

    pub fn init(&mut self) -> Result<()> {
        for (_, coordinator) in self.fragments_coordinator.iter_mut() {
            coordinator.init(&self.ctx)?;
        }

        Ok(())
    }

    pub fn subscribe_fragment(&mut self, fragment_id: String, pipeline: &mut NewPipeline) -> Result<()> {
        // let (tx, mut rx) = tokio::sync::mpsc::channel(1);

        // Merge pipelines if exist locally pipeline
        if let Some(mut fragment_coordinator) = self.fragments_coordinator.remove(&fragment_id) {
            fragment_coordinator.init(&self.ctx)?;

            // Add exchange data publisher.
            let mut input_pipeline = fragment_coordinator.pipeline.unwrap();
            input_pipeline.add_transform(|transform_input_port, transform_output_port| {
                unimplemented!()
            })?;

            // Add exchange data subscriber.
        }

        // pipeline.add_pipe(NewPipe::ResizePipe {
        //     outputs_port: vec![],
        //     inputs_port: vec![input_port],
        //     // processors: vec![],
        // });
        unimplemented!()
    }
}

struct FragmentCoordinator {
    node: PlanNode,
    initialized: bool,
    pipeline: Option<NewPipeline>,
    executor: Option<Arc<PipelineCompleteExecutor>>,
}

impl FragmentCoordinator {
    pub fn create(node: &PlanNode) -> FragmentCoordinator {
        FragmentCoordinator { node: node.clone(), initialized: false, pipeline: None, executor: None }
    }

    pub fn init(&mut self, ctx: &Arc<QueryContext>) -> Result<()> {
        if self.initialized {
            return Ok(());
        }

        self.initialized = true;
        let async_runtime = ctx.get_storage_runtime();
        let query_pipeline_builder = QueryPipelineBuilder::create(ctx.clone());
        let pipeline = query_pipeline_builder.finalize(&self.node)?;

        if pipeline.is_pushing_pipeline()? || pipeline.is_complete_pipeline()? || !pipeline.is_pulling_pipeline()? {
            return Err(ErrorCode::LogicalError("Logical error, It's a bug"));
        }

        // TODO: add exchange data sink
        self.executor = Some(Arc::new(PipelineCompleteExecutor::try_create(async_runtime, pipeline)?));
        Ok(())
    }
}
