use std::sync::Arc;
use tokio_stream::StreamExt;
use common_datablocks::DataBlock;
use common_planners::ReadDataSourcePlan;
use crate::pipelines::new::processors::sources::{AsyncSource, ASyncSourceProcessorWrap};
use crate::sessions::QueryContext;
use common_exception::{ErrorCode, Result};
use common_streams::{ProgressStream, SendableDataBlockStream};
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;

pub struct TableSource {
    initialized: bool,

    ctx: Arc<QueryContext>,
    source_plan: ReadDataSourcePlan,
    wrap_stream: Option<SendableDataBlockStream>,
}

impl TableSource {
    pub fn try_create(output: Arc<OutputPort>, ctx: Arc<QueryContext>, source_plan: ReadDataSourcePlan) -> Result<ProcessorPtr> {
        ASyncSourceProcessorWrap::create(
            vec![output],
            TableSource { initialized: false, ctx, source_plan, wrap_stream: None },
        )
    }

    async fn initialize(&mut self) -> Result<()> {
        let table = self.ctx.build_table_from_source_plan(&self.source_plan)?;

        let table_stream = table.read(self.ctx.clone(), &self.source_plan);
        let progress_stream = ProgressStream::try_create(table_stream.await?, self.ctx.get_scan_progress())?;
        self.wrap_stream = Some(Box::pin(progress_stream));
        Ok(())
    }
}

#[async_trait::async_trait]
impl AsyncSource for TableSource {
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if !self.initialized {
            self.initialized = true;
            self.initialize().await?;
        }

        match &mut self.wrap_stream {
            None => Err(ErrorCode::LogicalError("")),
            Some(stream) => match stream.next().await {
                None => Ok(None),
                Some(Err(cause)) => Err(cause),
                Some(Ok(data)) => Ok(Some(data)),
            }
        }
    }
}
