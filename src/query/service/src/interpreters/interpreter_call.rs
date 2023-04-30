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

use common_exception::Result;
use common_expression::DataSchemaRef;
use common_sql::plans::CallPlan;

use super::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::procedures::Procedure;
use crate::procedures::ProcedureFactory;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct CallInterpreter {
    func: Box<dyn Procedure>,
    ctx: Arc<QueryContext>,
    plan: CallPlan,
}

impl CallInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CallPlan) -> Result<Self> {
        let func = ProcedureFactory::instance().get(plan.name.clone())?;
        Ok(CallInterpreter { func, ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CallInterpreter {
    fn name(&self) -> &str {
        "CallInterpreter"
    }

    fn schema(&self) -> DataSchemaRef {
        self.func.schema()
    }

    #[tracing::instrument(level = "debug", name = "call_interpreter_execute", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let mut build_res = PipelineBuildResult::create();
        self.func
            .eval(
                self.ctx.clone(),
                self.plan.args.clone(),
                &mut build_res.main_pipeline,
            )
            .await?;

        Ok(build_res)
    }
}
