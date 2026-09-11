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
use databend_common_sql::plans::AlterTableTtlPlan;

use super::Interpreter;
use crate::interpreters::common::check_ttl_supported_table;
use crate::interpreters::interpreter_table_add_column::commit_table_meta;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContextTableAccess;

/// Handles both `ALTER TABLE ... SET TTL <expr>` and `... REMOVE TTL`, which
/// differ only in whether a TTL is written or cleared.
pub struct AlterTableTtlInterpreter {
    ctx: Arc<QueryContext>,
    plan: AlterTableTtlPlan,
}

impl AlterTableTtlInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: AlterTableTtlPlan) -> Result<Self> {
        Ok(AlterTableTtlInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for AlterTableTtlInterpreter {
    fn name(&self) -> &str {
        "AlterTableTtlInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    fn execute2(&self) -> futures::future::BoxFuture<'_, Result<PipelineBuildResult>> {
        Box::pin(async move {
            let plan = &self.plan;
            let tenant = self.ctx.get_tenant();
            let catalog = self.ctx.get_catalog(&plan.catalog).await?;

            let table = catalog
                .get_table(&tenant, &plan.database, &plan.table)
                .await?;

            // Checked before the no-op below, so rejecting a table kind that cannot
            // carry a TTL does not depend on whether one happens to be set.
            check_ttl_supported_table(table.as_ref())?;

            // Covers both setting the value it already has and removing an absent
            // one; neither should burn a table meta version.
            if table.get_table_info().meta.ttl == plan.ttl {
                return Ok(PipelineBuildResult::create());
            }

            commit_table_meta(
                self.ctx.as_ref(),
                table.as_ref(),
                table.get_table_info().meta.clone(),
                catalog,
                |_snapshot_opt, meta| {
                    meta.ttl = plan.ttl.clone();
                },
            )
            .await?;
            Ok(PipelineBuildResult::create())
        })
    }
}
