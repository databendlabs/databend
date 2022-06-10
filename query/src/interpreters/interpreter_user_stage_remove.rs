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

use common_exception::Result;
use common_planners::RemoveUserStagePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::interpreters::interpreter_common::list_files;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;
use crate::storages::stage::StageSource;

#[derive(Debug)]
pub struct RemoveUserStageInterpreter {
    ctx: Arc<QueryContext>,
    plan: RemoveUserStagePlan,
}

impl RemoveUserStageInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: RemoveUserStagePlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(RemoveUserStageInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for RemoveUserStageInterpreter {
    fn name(&self) -> &str {
        "RemoveUserStageInterpreter"
    }

    #[tracing::instrument(level = "info", skip(self, _input_stream), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let plan = self.plan.clone();
        let ctx = self.ctx.clone();

        let mut files: Vec<String> = vec![];

        if !plan.file_name.is_empty() {
            files.push(plan.file_name);
        } else if !plan.pattern.is_empty() {
            files.append(
                &mut list_files(
                    ctx.clone(),
                    plan.stage.clone(),
                    plan.path.clone(),
                    plan.pattern,
                )
                .await?,
            );
        }

        let op = StageSource::get_op(&self.ctx, &self.plan.stage).await?;
        for name in files.into_iter() {
            let obj = format!("{}/{}", plan.path, name);
            let _ = op.object(&obj).delete().await;
        }

        Ok(Box::pin(DataBlockStream::create(
            self.schema(),
            None,
            vec![],
        )))
    }
}
