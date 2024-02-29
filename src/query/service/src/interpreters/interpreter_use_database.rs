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
use databend_common_expression::DataSchema;
use databend_common_sql::plans::UseDatabasePlan;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryAffect;
use crate::sessions::QueryContext;

pub struct UseDatabaseInterpreter {
    ctx: Arc<QueryContext>,
    plan: UseDatabasePlan,
}

impl UseDatabaseInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: UseDatabasePlan) -> Result<Self> {
        Ok(UseDatabaseInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for UseDatabaseInterpreter {
    fn name(&self) -> &str {
        "UseDatabaseInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        if self.plan.database.trim().is_empty() {
            return Err(ErrorCode::UnknownDatabase("No database selected"));
        }
        self.ctx
            .set_current_database(self.plan.database.clone())
            .await?;
        self.ctx.set_affect(QueryAffect::UseDB {
            name: self.plan.database.clone(),
        });
        let _schema = Arc::new(DataSchema::empty());
        Ok(PipelineBuildResult::create())
    }
}
