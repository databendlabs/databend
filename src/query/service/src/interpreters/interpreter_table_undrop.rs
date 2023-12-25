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
use databend_common_sql::plans::UndropTablePlan;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct UndropTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: UndropTablePlan,
}

impl UndropTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: UndropTablePlan) -> Result<Self> {
        Ok(UndropTableInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for UndropTableInterpreter {
    fn name(&self) -> &str {
        "UndropTableInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog_name = self.plan.catalog.as_str();
        let catalog = self.ctx.get_catalog(catalog_name).await?;
        catalog.undrop_table(self.plan.clone().into()).await?;

        Ok(PipelineBuildResult::create())
    }
}
