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
use databend_common_meta_app::schema::SwapTableReq;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_sql::plans::SwapTablePlan;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct SwapTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: SwapTablePlan,
}

impl SwapTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: SwapTablePlan) -> Result<Self> {
        Ok(SwapTableInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for SwapTableInterpreter {
    fn name(&self) -> &str {
        "SwapTableInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;
        let tenant = self.plan.tenant.clone();
        let db_name = self.plan.database.clone();
        let table_name = self.plan.table.clone();
        let target_table_name = self.plan.target_table.clone();
        let origin_table = catalog.get_table(&tenant, &db_name, &table_name).await?;
        let target_table = catalog
            .get_table(&tenant, &db_name, &target_table_name)
            .await?;
        if origin_table.is_temp() || target_table.is_temp() {
            return Err(ErrorCode::AlterTableError("Can not swap temp table"));
        }
        let _resp = catalog
            .swap_table(SwapTableReq {
                if_exists: self.plan.if_exists,
                origin_table: TableNameIdent {
                    tenant: self.plan.tenant.clone(),
                    db_name: self.plan.database.clone(),
                    table_name: self.plan.table.clone(),
                },
                target_table_name: self.plan.target_table.clone(),
            })
            .await?;

        Ok(PipelineBuildResult::create())
    }
}
