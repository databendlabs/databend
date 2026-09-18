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

use databend_common_exception::Result;
use databend_common_sql::plans::CreateDynamicTablePlan;

use crate::interpreters::CreateTableInterpreter;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct CreateDynamicTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateDynamicTablePlan,
}

impl CreateDynamicTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateDynamicTablePlan) -> Result<Self> {
        Ok(Self { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateDynamicTableInterpreter {
    fn name(&self) -> &str {
        "CreateDynamicTableInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    fn execute2(&self) -> futures::future::BoxFuture<'_, Result<PipelineBuildResult>> {
        Box::pin(async move {
            let table_interpreter =
                CreateTableInterpreter::try_create(self.ctx.clone(), self.plan.table_plan.clone())?;
            // CTAS handles IF NOT EXISTS and publishes the staged table only after the
            // initial query succeeds, preserving an existing table on replacement failure.
            table_interpreter.execute2().await
        })
    }
}
