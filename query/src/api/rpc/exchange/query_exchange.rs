use std::collections::HashMap;
use std::sync::Arc;
use crate::pipelines::new::processors::Processor;
use crate::pipelines::new::processors::processor::Event;
use common_exception::Result;
use crate::api::ExecutorPacket;
use crate::interpreters::QueryFragmentsActions;
use crate::pipelines::new::executor::PipelineCompleteExecutor;
use crate::pipelines::new::NewPipeline;
use crate::pipelines::processors::Pipeline;
use crate::sessions::QueryContext;

pub struct DataExchangeManager {
    queries_coordinator: HashMap<String, QueryCoordinator>,
}

struct QueryCoordinator {
    executor: Arc<PipelineCompleteExecutor>,
}

impl DataExchangeManager {
    pub fn create() -> Arc<DataExchangeManager> {
        Arc::new(DataExchangeManager { queries_coordinator: HashMap::new() })
    }

    pub fn executor_packet_handle(&self, packet: ExecutorPacket) -> Result<()> {
        // TODO:
        unimplemented!()
    }

    pub fn submit_query_actions(&self, actions: QueryFragmentsActions) -> Result<NewPipeline> {
        let root_actions = actions.get_root_actions()?;
        self.build_root_pipeline(root_actions.fragment_id.to_owned())
    }

    fn build_root_pipeline(&self, fragment_id: String) -> Result<NewPipeline> {
        // TODO:
        unimplemented!()
    }
}

struct DataExchange {}

#[async_trait::async_trait]
impl Processor for DataExchange {
    fn name(&self) -> &'static str {
        "DataExchange"
    }

    fn event(&mut self) -> Result<Event> {
        todo!()
    }
}
