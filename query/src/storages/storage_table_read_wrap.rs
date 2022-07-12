use std::sync::Arc;
use common_planners::ReadDataSourcePlan;
use crate::sessions::QueryContext;
use crate::storages::Table;
use common_exception::Result;
use common_streams::SendableDataBlockStream;
use crate::interpreters::ProcessorExecutorStream;
use crate::pipelines::new::executor::PipelinePullingExecutor;
use crate::pipelines::new::NewPipeline;

#[async_trait::async_trait]
pub trait TableStreamReadWrap: Send + Sync {
    async fn read(&self, _ctx: Arc<QueryContext>, _plan: &ReadDataSourcePlan) -> Result<SendableDataBlockStream>;
}

#[async_trait::async_trait]
impl<T: Table> TableStreamReadWrap for T {
    async fn read(&self, ctx: Arc<QueryContext>, plan: &ReadDataSourcePlan) -> Result<SendableDataBlockStream> {
        let mut pipeline = NewPipeline::create();
        self.read2(ctx.clone(), plan, &mut pipeline)?;

        let async_runtime = ctx.get_storage_runtime();
        let query_need_abort = ctx.query_need_abort();
        let executor = PipelinePullingExecutor::try_create(async_runtime, query_need_abort, pipeline)?;

        Ok(Box::pin(ProcessorExecutorStream::create(executor)?))
    }
}

#[async_trait::async_trait]
impl TableStreamReadWrap for dyn Table {
    async fn read(&self, ctx: Arc<QueryContext>, plan: &ReadDataSourcePlan) -> Result<SendableDataBlockStream> {
        let mut pipeline = NewPipeline::create();
        self.read2(ctx.clone(), plan, &mut pipeline)?;

        let async_runtime = ctx.get_storage_runtime();
        let query_need_abort = ctx.query_need_abort();
        let executor = PipelinePullingExecutor::try_create(async_runtime, query_need_abort, pipeline)?;

        Ok(Box::pin(ProcessorExecutorStream::create(executor)?))
    }
}
