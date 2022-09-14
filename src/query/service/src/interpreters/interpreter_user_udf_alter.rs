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
use common_legacy_planners::AlterUserUDFPlan;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Debug)]
pub struct AlterUserUDFInterpreter {
    ctx: Arc<QueryContext>,
    plan: AlterUserUDFPlan,
}

impl AlterUserUDFInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: AlterUserUDFPlan) -> Result<Self> {
        Ok(AlterUserUDFInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for AlterUserUDFInterpreter {
    fn name(&self) -> &str {
        "AlterUserUDFInterpreter"
    }

    #[tracing::instrument(level = "debug", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let plan = self.plan.clone();

        let tenant = self.ctx.get_tenant();
        let user_mgr = self.ctx.get_user_manager();
        user_mgr.update_udf(&tenant, plan.udf).await?;

        Ok(PipelineBuildResult::create())
    }
}
