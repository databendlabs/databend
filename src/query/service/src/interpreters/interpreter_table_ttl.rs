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

use chrono::Utc;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::plans::AlterTableTtlPlan;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_table_meta::table::is_fuse_backed_engine;

use super::Interpreter;
use crate::interpreters::interpreter_table_add_column::update_table_meta;
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
            let Some(table_id) = plan.table_id else {
                return Ok(PipelineBuildResult::create());
            };

            // Keep the schema used during binding and the commit sequence together.
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
                    "TTL target table changed after binding",
                ));
            }
            let catalog = self.ctx.get_catalog(&plan.catalog).await?;

            // Validate engine and mutability before the no-op check so SET and
            // REMOVE behave consistently on unsupported table kinds.
            let engine = table.engine();
            if !is_fuse_backed_engine(engine) {
                return Err(ErrorCode::UnsupportedEngineParams(format!(
                    "Unsupported TTL for engine: {engine}"
                )));
            }
            let fuse_table = FuseTable::try_from_table(table.as_ref())?;
            // Covers ATTACH and materialized views with an explicit READ ONLY message.
            table.check_mutable()?;
            let desc = &table.get_table_info().desc;
            // TTL definitions are limited to persistent tables.
            if fuse_table.is_transient() {
                return Err(ErrorCode::BadArguments(format!(
                    "The table {desc} is transient, TTL is not supported"
                )));
            }
            if table.is_temp() {
                return Err(ErrorCode::BadArguments(format!(
                    "The table {desc} is temporary, TTL is not supported"
                )));
            }

            // Covers both setting the value it already has and removing an absent
            // one; neither should burn a table meta version.
            //
            // Known limitation: this compares against the table version bound by
            // this query, so a concurrent ALTER committed between bind and execute
            // is not observed and the statement reports success without applying
            // anything. Accepted because DDL here is not transactional -- re-reading
            // the latest meta would only narrow the window, not close it -- and the
            // non-no-op path is still protected by the bound sequence, which fails
            // with TABLE_VERSION_MISMATCHED.
            if table.get_table_info().meta.ttl == plan.ttl {
                return Ok(PipelineBuildResult::create());
            }

            let mut meta = table.get_table_info().meta.clone();
            meta.ttl = plan.ttl.clone();
            meta.updated_on = Utc::now();
            // Pure metadata change: the snapshot location does not move, so no LVT fence.
            update_table_meta(fuse_table, &meta, catalog, self.ctx.get_tenant(), None).await?;
            Ok(PipelineBuildResult::create())
        })
    }
}
