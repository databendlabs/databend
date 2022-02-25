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

use std::any::Any;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::UserStageInfo;
use common_planners::UserStagePlan;
use common_streams::CsvSourceBuilder;
use common_streams::ProgressStream;
use common_streams::SendableDataBlockStream;
use common_streams::SourceStream;
use common_tracing::tracing;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::Processor;
use crate::pipelines::transforms::sources::DataAccessor;
use crate::sessions::QueryContext;

pub struct CsvSourceTransform {
    ctx: Arc<QueryContext>,
    stage_plan: UserStagePlan,
}

impl CsvSourceTransform {
    pub fn try_create(ctx: Arc<QueryContext>, stage_plan: UserStagePlan) -> Result<Self> {
        Ok(CsvSourceTransform { ctx, stage_plan })
    }

    async fn get_csv_stream(&self, stage_info: &UserStageInfo) -> Result<SourceStream> {
        let schema = self.stage_plan.schema.clone();
        let mut builder = CsvSourceBuilder::create(schema);
        let size_limit = stage_info.copy_options.size_limit;

        // Size limit.
        {
            if size_limit > 0 {
                builder.size_limit(size_limit);
            }
        }

        // Block size.
        {
            let max_block_size = self.ctx.get_settings().get_max_block_size()? as usize;
            builder.block_size(max_block_size);
        }

        // Skip header.
        {
            builder.skip_header(stage_info.file_format_options.skip_header);
        }

        // Field delimiter, default ','.
        {
            let field_delimiter = &stage_info.file_format_options.field_delimiter;
            builder.field_delimiter(field_delimiter);
        }

        // Record delimiter, default '\n'.
        {
            let record_delimiter = &stage_info.file_format_options.record_delimiter;
            builder.record_delimiter(record_delimiter);
        }

        let reader = DataAccessor::get_source_reader(&self.ctx, stage_info).await?;
        let source = builder.build(reader)?;

        Ok(SourceStream::new(Box::new(source)))
    }
}

#[async_trait::async_trait]
impl Processor for CsvSourceTransform {
    fn name(&self) -> &str {
        "CsvSourceTransform"
    }

    fn connect_to(&mut self, _: Arc<dyn Processor>) -> Result<()> {
        Result::Err(ErrorCode::LogicalError(
            "Cannot call CsvSourceTransform connect_to",
        ))
    }

    fn inputs(&self) -> Vec<Arc<dyn Processor>> {
        vec![Arc::new(EmptyProcessor::create())]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    #[tracing::instrument(level = "debug", name="csv_source_execute", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let stage_info = &self.stage_plan.stage_info;
        let csv_stream = self.get_csv_stream(stage_info).await?;
        let input_stream = csv_stream.execute().await?;
        let progress_stream =
            ProgressStream::try_create(input_stream, self.ctx.get_scan_progress())?;
        Ok(Box::pin(progress_stream))
    }
}
