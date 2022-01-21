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

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::ReadDataSourcePlan;
use common_streams::ProgressStream;
use common_streams::SendableDataBlockStream;
use tokio_stream::StreamExt;

use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::sources::async_source::AsyncSource;
use crate::pipelines::new::processors::sources::AsyncSourcer;
use crate::sessions::QueryContext;

pub struct TableSource {
    initialized: bool,

    ctx: Arc<QueryContext>,
    source_plan: ReadDataSourcePlan,
    wrap_stream: Option<SendableDataBlockStream>,
}

impl TableSource {
    pub fn try_create(
        output: Arc<OutputPort>,
        ctx: Arc<QueryContext>,
        source_plan: ReadDataSourcePlan,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(output, TableSource {
            initialized: false,
            ctx,
            source_plan,
            wrap_stream: None,
        })
    }

    async fn initialize(&mut self) -> Result<()> {
        let table = self.ctx.build_table_from_source_plan(&self.source_plan)?;

        let table_stream = table.read(self.ctx.clone(), &self.source_plan);
        let progress_stream =
            ProgressStream::try_create(table_stream.await?, self.ctx.get_scan_progress())?;
        self.wrap_stream = Some(Box::pin(progress_stream));
        Ok(())
    }
}

#[async_trait::async_trait]
impl AsyncSource for TableSource {
    const NAME: &'static str = "TableSource";

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
            },
        }
    }
}
