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

use common_exception::Result;
use common_meta_types::StageType;
use common_planners::CreateUserStagePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct CreateUserStageInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateUserStagePlan,
}

impl CreateUserStageInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateUserStagePlan) -> Result<InterpreterPtr> {
        Ok(Arc::new(CreateUserStageInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateUserStageInterpreter {
    fn name(&self) -> &str {
        "CreateUserStageInterpreter"
    }

    #[tracing::instrument(level = "info", skip(self, _input_stream), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let plan = self.plan.clone();
        let user_mgr = self.ctx.get_user_manager();
        let user_stage = plan.user_stage_info;

        if user_stage.stage_type == StageType::Internal {
            let prefix = format!("stage/{}", user_stage.stage_name);
            let op = self.ctx.get_storage_operator().await?;
            let obj = op.object(&prefix);

            // Write file then delete file ensure the directory is created
            // TODO(xuanwo), opendal support mkdir (https://github.com/datafuselabs/opendal/issues/151)
            if obj.metadata().await.is_err() {
                let rand_file = uuid::Uuid::new_v4().to_string();
                let file = format!("{}{}", prefix, rand_file);
                let file_obj = op.object(&file);

                let writer = file_obj.writer();
                writer.write_bytes("opendaldir".as_bytes().to_vec()).await?;
                file_obj.delete().await?;
            }
        }

        let _create_stage = user_mgr
            .add_stage(&plan.tenant, user_stage, plan.if_not_exists)
            .await?;

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}
