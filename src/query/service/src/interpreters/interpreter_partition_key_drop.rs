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

use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::plans::DropTablePartitionKeyPlan;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_table_meta::table::OPT_KEY_PARTITION_BY;
use databend_storages_common_table_meta::table::OPT_KEY_WRITE_DISTRIBUTION_MODE;

use super::Interpreter;
use crate::interpreters::interpreter_table_add_column::update_table_meta;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContextTableAccess;

pub struct DropTablePartitionKeyInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropTablePartitionKeyPlan,
}

impl DropTablePartitionKeyInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropTablePartitionKeyPlan) -> Result<Self> {
        Ok(Self { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropTablePartitionKeyInterpreter {
    fn name(&self) -> &str {
        "DropTablePartitionKeyInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    fn execute2(&self) -> futures::future::BoxFuture<'_, Result<PipelineBuildResult>> {
        Box::pin(async move {
            let plan = &self.plan;
            let Some(table_id) = plan.table_id else {
                return Ok(PipelineBuildResult::create());
            };
            let table = match self
                .ctx
                .get_table(&plan.catalog, &plan.database, &plan.table)
                .await
            {
                Ok(table) => table,
                Err(e)
                    if plan.if_exists
                        && matches!(
                            e.code(),
                            ErrorCode::UNKNOWN_CATALOG
                                | ErrorCode::UNKNOWN_DATABASE
                                | ErrorCode::UNKNOWN_TABLE
                        ) =>
                {
                    return Ok(PipelineBuildResult::create());
                }
                Err(e) => return Err(e),
            };
            if table.get_id() != table_id {
                return Err(ErrorCode::TableVersionMismatched(
                    "DROP PARTITION KEY target table changed after binding",
                ));
            }
            table.check_mutable()?;
            let fuse_table = FuseTable::try_from_table(table.as_ref())?;
            if !table.options().contains_key(OPT_KEY_PARTITION_BY) {
                return Ok(PipelineBuildResult::create());
            }

            let mut new_table_meta = table.get_table_info().meta.clone();
            // Preserve the sequence across DROP, just as for cluster keys.
            // Initialize legacy definitions so old readers cannot re-add a
            // key while ignoring the identity fields after this DROP.
            new_table_meta.partition_key_seq = new_table_meta.partition_key_seq.max(1);
            new_table_meta.partition_key_id = None;
            new_table_meta.options.remove(OPT_KEY_PARTITION_BY);
            // Hash distribution requires a partition key. Leave other explicit
            // distribution modes unchanged.
            if new_table_meta
                .options
                .get(OPT_KEY_WRITE_DISTRIBUTION_MODE)
                .is_some_and(|mode| mode.eq_ignore_ascii_case("hash"))
            {
                new_table_meta
                    .options
                    .remove(OPT_KEY_WRITE_DISTRIBUTION_MODE);
            }

            new_table_meta.updated_on = chrono::Utc::now();
            let catalog = self.ctx.get_catalog(&plan.catalog).await?;
            update_table_meta(fuse_table, &new_table_meta, catalog, self.ctx.get_tenant()).await?;

            Ok(PipelineBuildResult::create())
        })
    }
}
