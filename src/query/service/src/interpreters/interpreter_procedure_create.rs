// Copyright 2021 Datafuse Labs
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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::CreateProcedureReq;
use databend_common_meta_app::schema::CreateOption;
use databend_common_sql::plans::CreateProcedurePlan;
use databend_common_users::UserApiProvider;
use log::debug;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Debug)]
pub struct CreateProcedureInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateProcedurePlan,
}

impl CreateProcedureInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateProcedurePlan) -> Result<Self> {
        Ok(CreateProcedureInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateProcedureInterpreter {
    fn name(&self) -> &str {
        "CreateProcedureInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "create_procedure_execute");

        let tenant = self.plan.tenant.clone();

        let create_procedure_req: CreateProcedureReq = self.plan.clone().into();

        let reply = UserApiProvider::instance()
            .add_procedure(&tenant, create_procedure_req)
            .await;
        if let Err(e) = reply {
            if e.code() == ErrorCode::PROCEDURE_ALREADY_EXISTS {
                return match self.plan.create_option {
                    CreateOption::Create => Err(ErrorCode::ProcedureAlreadyExists(format!(
                        "Procedure '{}' already exists",
                        self.plan.name,
                    ))),
                    CreateOption::CreateIfNotExists => Ok(PipelineBuildResult::create()),
                    CreateOption::CreateOrReplace => {
                        let _reply = UserApiProvider::instance()
                            .update_procedure(&tenant, &self.plan.name, self.plan.meta.clone())
                            .await?;
                        Ok(PipelineBuildResult::create())
                    }
                };
            }
            return Err(e);
        }
        Ok(PipelineBuildResult::create())
    }
}
