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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_sql::plans::AlterTablePartitionByPlan;
use databend_common_storages_fuse::FuseTable;
use databend_meta_client::types::MatchSeq;
use databend_storages_common_table_meta::table::OPT_KEY_PARTITION_BY;

use super::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContextTableAccess;

pub struct AlterTablePartitionByInterpreter {
    ctx: Arc<QueryContext>,
    plan: AlterTablePartitionByPlan,
}

impl AlterTablePartitionByInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: AlterTablePartitionByPlan) -> Result<Self> {
        Ok(Self { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for AlterTablePartitionByInterpreter {
    fn name(&self) -> &str {
        "AlterTablePartitionByInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let plan = &self.plan;
        let catalog = self.ctx.get_catalog(&plan.catalog).await?;
        let table = catalog
            .get_table(&self.ctx.get_tenant(), &plan.database, &plan.table)
            .await?;
        table.check_mutable()?;
        FuseTable::try_from_table(table.as_ref())?;

        let partition_by = format!("({})", plan.partition_keys.join(", "));
        if let Some(current) = table.options().get(OPT_KEY_PARTITION_BY) {
            if current == &partition_by {
                return Ok(PipelineBuildResult::create());
            }
            return Err(ErrorCode::TableOptionInvalid(format!(
                "PARTITION BY is already defined as {current}; changing it is not supported"
            )));
        }

        let req = UpsertTableOptionReq {
            table_id: table.get_id(),
            seq: MatchSeq::Exact(table.get_table_info().ident.seq),
            options: HashMap::from([(OPT_KEY_PARTITION_BY.to_owned(), Some(partition_by))]),
        };
        catalog
            .upsert_table_option(&self.ctx.get_tenant(), &plan.database, req)
            .await?;

        Ok(PipelineBuildResult::create())
    }
}
