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

use databend_common_catalog::table::NavigationDescriptor;
use databend_common_catalog::table::TableExt;
use databend_common_exception::Result;
use databend_common_sql::plans::RevertTablePlan;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct RevertTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: RevertTablePlan,
}

impl RevertTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: RevertTablePlan) -> Result<Self> {
        Ok(RevertTableInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for RevertTableInterpreter {
    fn name(&self) -> &str {
        "RevertTableInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tenant = self.ctx.get_tenant();
        let catalog = self.ctx.get_catalog(self.plan.catalog.as_str()).await?;

        let table = catalog
            .get_table(&tenant, &self.plan.database, &self.plan.table)
            .await?;

        // check mutability
        table.check_mutable()?;

        let navigation_descriptor = NavigationDescriptor {
            database_name: self.plan.database.clone(),
            point: self.plan.point.clone(),
        };
        table
            .revert_to(self.ctx.clone(), navigation_descriptor)
            .await?;

        Ok(PipelineBuildResult::create())
    }
}
