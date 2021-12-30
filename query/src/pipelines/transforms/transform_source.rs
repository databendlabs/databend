// Copyright 2021 Datafuse Labs.
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

use std::any::Any;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::ReadDataSourcePlan;
use common_streams::CorrectWithSchemaStream;
use common_streams::ProgressStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::Processor;
use crate::sessions::QueryContext;

pub struct SourceTransform {
    ctx: Arc<QueryContext>,
    source_plan: ReadDataSourcePlan,
}

impl SourceTransform {
    pub fn try_create(ctx: Arc<QueryContext>, source_plan: ReadDataSourcePlan) -> Result<Self> {
        Ok(SourceTransform { ctx, source_plan })
    }

    async fn read_table(&self) -> Result<SendableDataBlockStream> {
        let table = self.ctx.build_table_from_source_plan(&self.source_plan)?;

        let table_stream = table.read(self.ctx.clone(), &self.source_plan);
        let progress_stream =
            ProgressStream::try_create(table_stream.await?, self.ctx.get_scan_progress())?;

        Ok(Box::pin(
            self.ctx.try_create_abortable(Box::pin(progress_stream))?,
        ))
    }
}

#[async_trait::async_trait]
impl Processor for SourceTransform {
    fn name(&self) -> &str {
        "SourceTransform"
    }

    fn connect_to(&mut self, _: Arc<dyn Processor>) -> Result<()> {
        Result::Err(ErrorCode::LogicalError(
            "Cannot call SourceTransform connect_to",
        ))
    }

    fn inputs(&self) -> Vec<Arc<dyn Processor>> {
        vec![Arc::new(EmptyProcessor::create())]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    #[tracing::instrument(level = "debug", name="source_execute", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let desc = self.source_plan.table_info.desc.clone();
        tracing::debug!("execute, table:{:#} ...", desc);

        Ok(Box::pin(CorrectWithSchemaStream::new(
            self.read_table().await?,
            self.source_plan.schema(),
        )))
    }
}
