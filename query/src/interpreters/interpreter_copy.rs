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

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::StageFileFormatType;
use common_meta_types::StageType;
use common_planners::CopyPlan;
use common_streams::DataBlockStream;
use common_streams::ProgressStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use futures::TryStreamExt;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::pipelines::processors::Processor;
use crate::pipelines::transforms::CsvSourceTransform;
use crate::sessions::QueryContext;

pub struct CopyInterpreter {
    ctx: Arc<QueryContext>,
    plan: CopyPlan,
}

impl CopyInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CopyPlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(CopyInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for CopyInterpreter {
    fn name(&self) -> &str {
        "CopyInterpreter"
    }

    async fn execute(
        &self,
        mut _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        tracing::info!("Plan:{:?}", self.plan);

        let plan = self.plan.stage_plan.clone();
        let source_stream = match plan.stage_info.stage_type {
            StageType::External => {
                match plan.stage_info.file_format_options.format {
                    // CSV.
                    StageFileFormatType::Csv => {
                        CsvSourceTransform::try_create(self.ctx.clone(), plan.clone())?
                            .execute()
                            .await
                    }
                    // Unsupported.
                    format => Err(ErrorCode::LogicalError(format!(
                        "Unsupported file format: {:?}",
                        format
                    ))),
                }
            }

            StageType::Internal => Err(ErrorCode::LogicalError(
                "Unsupported copy from internal stage",
            )),
        }?;

        let ctx = self.ctx.clone();

        let progress_stream = Box::pin(ProgressStream::try_create(
            source_stream,
            ctx.get_scan_progress(),
        )?);

        let table = ctx
            .get_table(&self.plan.db_name, &self.plan.tbl_name)
            .await?;
        let r = table
            .append_data(ctx.clone(), progress_stream)
            .await?
            .try_collect()
            .await?;
        table.commit_insertion(ctx.clone(), r, false).await?;

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}
