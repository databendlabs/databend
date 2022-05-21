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
use common_datavalues::Series;
use common_datavalues::SeriesFrom;
use common_exception::Result;
use common_planners::ShowUserStagePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct ShowUserStageInterpreter {
    ctx: Arc<QueryContext>,
    plan: ShowUserStagePlan,
}

impl ShowUserStageInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ShowUserStagePlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(ShowUserStageInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowUserStageInterpreter {
    fn name(&self) -> &str {
        "ShowUserStageInterpreter"
    }

    #[tracing::instrument(level = "info", skip(self, _input_stream), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let tenant = self.ctx.get_tenant();
        let user_mgr = self.ctx.get_user_manager();

        let stages = user_mgr.get_stages(&tenant).await?;

        let data: Vec<_> = stages
            .iter()
            .map(|stage| {
                let columns = vec![
                    Series::from_data(vec![format!(
                        "{:?}",
                        stage
                            .created_on
                            .format("%Y-%m-%d %H:%M:%S.%3f %z")
                            .to_string()
                    )]),
                    Series::from_data(vec![stage.stage_name.as_str()]),
                ];

                DataBlock::create(self.plan.schema(), columns)
            })
            .collect();

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            data,
        )))
    }
}
