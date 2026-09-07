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

use databend_common_catalog::lock::LockTableOption;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::DYNAMIC_TABLE_ENGINE;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_sql::Planner;
use databend_common_sql::plans::Plan;
use databend_common_storages_fuse::FuseTable;
use databend_meta_client::types::MatchSeq;
use databend_storages_common_table_meta::table::OPT_KEY_AS_QUERY;
use databend_storages_common_table_meta::table::OPT_KEY_INITIALIZED;
use databend_storages_common_table_meta::table::OPT_KEY_SOURCE_ENDPOINTS;
use log::info;
use serde::Serialize;

use crate::interpreters::InsertInterpreter;
use crate::interpreters::Interpreter;
use crate::interpreters::QueryFinishHooks;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContextTableAccess;
use crate::sessions::TableContextTableManagement;

#[derive(Serialize)]
struct SourceEndpoint {
    table_id: u64,
    table_seq: u64,
    snapshot_location: Option<String>,
}

pub struct RefreshDynamicTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: databend_common_sql::plans::RefreshDynamicTablePlan,
}

impl RefreshDynamicTableInterpreter {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        plan: databend_common_sql::plans::RefreshDynamicTablePlan,
    ) -> Result<Self> {
        Ok(Self { ctx, plan })
    }

    async fn source_endpoints(&self, query: &str) -> Result<String> {
        let mut planner = Planner::new(self.ctx.clone());
        let (plan, _) = planner.plan_sql(query).await?;
        let Plan::Query { metadata, .. } = plan else {
            return Err(ErrorCode::InvalidOperation(
                "dynamic table definition must be a query",
            ));
        };
        let mut endpoints = metadata
            .read()
            .tables()
            .iter()
            .map(|entry| {
                let table = entry.table();
                let snapshot_location = FuseTable::try_from_table(table.as_ref())
                    .ok()
                    .and_then(|table| table.snapshot_loc());
                SourceEndpoint {
                    table_id: table.get_id(),
                    table_seq: table.get_table_info().ident.seq,
                    snapshot_location,
                }
            })
            .collect::<Vec<_>>();
        endpoints.sort_by_key(|endpoint| endpoint.table_id);
        endpoints.dedup_by_key(|endpoint| endpoint.table_id);
        if endpoints.is_empty() {
            return Err(ErrorCode::InvalidOperation(
                "dynamic table definition must reference at least one source table",
            ));
        }
        Ok(serde_json::to_string(&endpoints)?)
    }

    /// Publish or invalidate the refresh checkpoint.
    ///
    /// `Some(endpoints)` marks the stored data as describing exactly those source endpoints.
    /// `None` marks the object uninitialized, which makes reads fall back to the defining query.
    async fn update_refresh_state(
        &self,
        table_id: u64,
        table_seq: u64,
        endpoints: Option<String>,
    ) -> Result<()> {
        let (initialized, endpoints) = match endpoints {
            Some(endpoints) => ("true".to_string(), endpoints),
            None => ("false".to_string(), "[]".to_string()),
        };
        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;
        catalog
            .upsert_table_option(
                &self.ctx.get_tenant(),
                &self.plan.database,
                UpsertTableOptionReq {
                    table_id,
                    seq: MatchSeq::Exact(table_seq),
                    options: [
                        (OPT_KEY_INITIALIZED.to_string(), Some(initialized)),
                        (OPT_KEY_SOURCE_ENDPOINTS.to_string(), Some(endpoints)),
                    ]
                    .into_iter()
                    .collect(),
                },
            )
            .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Interpreter for RefreshDynamicTableInterpreter {
    fn name(&self) -> &str {
        "RefreshDynamicTableInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let _lock_guard = self
            .ctx
            .clone()
            .acquire_table_lock(
                &self.plan.catalog,
                &self.plan.database,
                &self.plan.table,
                &LockTableOption::LockWithRetry,
            )
            .await?;
        self.ctx.evict_table_from_cache(
            &self.plan.catalog,
            &self.plan.database,
            &self.plan.table,
        )?;
        let table = self
            .ctx
            .get_table(&self.plan.catalog, &self.plan.database, &self.plan.table)
            .await?;
        if table.engine() != DYNAMIC_TABLE_ENGINE {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "{}.{} is not a dynamic table",
                self.plan.database, self.plan.table
            )));
        }
        let query = table
            .get_table_info()
            .meta
            .options
            .get(OPT_KEY_AS_QUERY)
            .cloned()
            .ok_or_else(|| ErrorCode::InvalidOperation("dynamic table definition is missing"))?;

        // Capture the endpoints that this refresh intends to materialize.
        let start_endpoints = self.source_endpoints(&query).await?;

        // Invalidate the checkpoint before touching data. The overwrite below is not atomic with
        // the checkpoint update, so a crash or a mid-flight source change must never leave stored
        // data that a later read could match against a checkpoint describing a different state.
        // While uninitialized, reads fall back to the defining query and stay correct.
        self.update_refresh_state(table.get_id(), table.get_table_info().ident.seq, None)
            .await?;

        let insert_sql = format!(
            "INSERT OVERWRITE `{}`.`{}`.`{}` {}",
            self.plan.catalog, self.plan.database, self.plan.table, query
        );
        let mut planner = Planner::new(self.ctx.clone());
        let (plan, _) = planner.plan_sql(&insert_sql).await?;
        let Plan::Insert(insert) = plan else {
            return Err(ErrorCode::InvalidOperation(
                "dynamic table refresh did not produce an INSERT plan",
            ));
        };
        let interpreter =
            InsertInterpreter::try_create_refresh(self.ctx.clone(), *insert, table.get_id())?;
        let stream = interpreter
            .execute_with_hooks(self.ctx.clone(), QueryFinishHooks::nested_with_hooks())
            .await?;
        futures::pin_mut!(stream);
        use futures::TryStreamExt;
        while stream.try_next().await?.is_some() {}

        // Publish the checkpoint only when the sources still describe the state that was just
        // materialized. Otherwise the object stays uninitialized and readers fall back.
        let endpoints = self.source_endpoints(&query).await?;
        if endpoints != start_endpoints {
            info!(
                "dynamic table {}.{} sources changed during refresh; leaving it uninitialized so reads fall back",
                self.plan.database, self.plan.table
            );
            return Ok(PipelineBuildResult::create());
        }
        self.ctx.evict_table_from_cache(
            &self.plan.catalog,
            &self.plan.database,
            &self.plan.table,
        )?;
        let refreshed = self
            .ctx
            .get_table(&self.plan.catalog, &self.plan.database, &self.plan.table)
            .await?;
        self.update_refresh_state(
            refreshed.get_id(),
            refreshed.get_table_info().ident.seq,
            Some(endpoints),
        )
        .await?;
        Ok(PipelineBuildResult::create())
    }
}
