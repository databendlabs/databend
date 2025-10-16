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
use databend_common_meta_app::schema::RenameTableReq;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_sql::plans::RenameTablePlan;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct RenameTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: RenameTablePlan,
}

impl RenameTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: RenameTablePlan) -> Result<Self> {
        Ok(RenameTableInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for RenameTableInterpreter {
    fn name(&self) -> &str {
        "RenameTableInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn build_pipeline(&self) -> Result<PipelineBuildResult> {
        // TODO check privileges
        // You must have ALTER and DROP privileges for the original table,
        // and CREATE and INSERT privileges for the new table.
        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;
        let _resp = catalog
            .rename_table(RenameTableReq {
                if_exists: self.plan.if_exists,
                name_ident: TableNameIdent {
                    tenant: self.plan.tenant.clone(),
                    db_name: self.plan.database.clone(),
                    table_name: self.plan.table.clone(),
                },
                new_db_name: self.plan.new_database.clone(),
                new_table_name: self.plan.new_table.clone(),
            })
            .await?;

        Ok(PipelineBuildResult::create())
    }
}
