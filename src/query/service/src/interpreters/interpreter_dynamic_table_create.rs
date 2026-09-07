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

use databend_common_ast::ast::InitializeMode;
use databend_common_exception::Result;
use databend_common_sql::plans::CreateDynamicTablePlan;
use databend_common_sql::plans::RefreshDynamicTablePlan;

use crate::interpreters::CreateTableInterpreter;
use crate::interpreters::Interpreter;
use crate::interpreters::RefreshDynamicTableInterpreter;
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
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let table_interpreter =
            CreateTableInterpreter::try_create(self.ctx.clone(), self.plan.table_plan.clone())?;
        let result = table_interpreter.execute2().await?;

        if self.plan.initialize == InitializeMode::OnCreate {
            let refresh = RefreshDynamicTableInterpreter::try_create(
                self.ctx.clone(),
                RefreshDynamicTablePlan {
                    tenant: self.plan.table_plan.tenant.clone(),
                    catalog: self.plan.table_plan.catalog.clone(),
                    database: self.plan.table_plan.database.clone(),
                    table: self.plan.table_plan.table.clone(),
                },
            )?;
            refresh.execute2().await?;
        }

        Ok(result)
    }
}
