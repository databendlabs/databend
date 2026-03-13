// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use chrono::Utc;
use databend_common_base::base::GlobalInstance;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::CreateTableTagReq;
use databend_common_meta_app::schema::DropTableTagReq;
use databend_common_meta_app::schema::TableLvtCheck;
use databend_common_sql::plans::CreateTableBranchPlan;
use databend_common_sql::plans::CreateTableTagPlan;
use databend_common_sql::plans::DropTableBranchPlan;
use databend_common_sql::plans::DropTableTagPlan;
use databend_common_storages_fuse::FuseTable;
use databend_enterprise_table_ref_handler::TableRefHandler;
use databend_enterprise_table_ref_handler::TableRefHandlerWrapper;
use databend_meta_types::MatchSeq;
use databend_storages_common_table_meta::meta::is_uuid_v7;

pub struct RealTableRefHandler {}

const LEGACY_TABLE_REF_HANDLER_MESSAGE: &str = "Legacy experimental table refs were removed; binder should reject CREATE/DROP BRANCH|TAG before execution";

#[async_trait::async_trait]
impl TableRefHandler for RealTableRefHandler {
    #[async_backtrace::framed]
    async fn do_create_table_branch(
        &self,
        _ctx: Arc<dyn TableContext>,
        _plan: &CreateTableBranchPlan,
    ) -> Result<()> {
        // Keep this placeholder for the upcoming table-ref redesign. The legacy
        // implementation has been removed, so reaching this handler is unexpected.
        Err(ErrorCode::Unimplemented(LEGACY_TABLE_REF_HANDLER_MESSAGE))
    }

    #[async_backtrace::framed]
    async fn do_create_table_tag(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &CreateTableTagPlan,
    ) -> Result<()> {
        ctx.check_table_ref_access()?;

        let table = self
            .load_source_table(ctx.clone(), &plan.catalog, &plan.database, &plan.table)
            .await?;
        let table_info = table.get_table_info();
        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        let snapshot_loc = match &plan.navigation {
            Some(navigation) => {
                fuse_table
                    .navigate_to_location(ctx.clone(), navigation)
                    .await?
            }
            None => fuse_table.snapshot_loc(),
        }
        .ok_or_else(|| {
            ErrorCode::IllegalReference(format!(
                "The table '{}.{}' has no snapshot to create TAG '{}'",
                plan.database, plan.table, plan.name
            ))
        })?;

        let Some(snapshot) = fuse_table
            .read_table_snapshot_with_location(Some(snapshot_loc.clone()))
            .await?
        else {
            return Err(ErrorCode::TableHistoricalDataNotFound(format!(
                "Snapshot '{}' not found when creating TAG '{}'",
                snapshot_loc, plan.name
            )));
        };

        if !is_uuid_v7(&snapshot.snapshot_id) {
            return Err(ErrorCode::IllegalReference(format!(
                "Cannot create TAG '{}': snapshot '{}' is not based on uuid v7",
                plan.name, snapshot_loc
            )));
        }

        let Some(snapshot_timestamp) = snapshot.timestamp else {
            return Err(ErrorCode::IllegalReference(format!(
                "Table {} snapshot lacks required timestamp",
                table_info.ident.table_id
            )));
        };

        let catalog = ctx.get_catalog(&plan.catalog).await?;
        catalog
            .create_table_tag(CreateTableTagReq {
                table_id: table_info.ident.table_id,
                seq: MatchSeq::Exact(table_info.ident.seq),
                tag_name: plan.name.clone(),
                snapshot_loc,
                expire_at: plan.retain.map(|v| Utc::now() + v),
                lvt_check: Some(TableLvtCheck {
                    tenant: ctx.get_tenant(),
                    time: snapshot_timestamp,
                }),
            })
            .await
    }

    #[async_backtrace::framed]
    async fn do_drop_table_branch(
        &self,
        _ctx: Arc<dyn TableContext>,
        _plan: &DropTableBranchPlan,
    ) -> Result<()> {
        Err(ErrorCode::Unimplemented(LEGACY_TABLE_REF_HANDLER_MESSAGE))
    }

    #[async_backtrace::framed]
    async fn do_drop_table_tag(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DropTableTagPlan,
    ) -> Result<()> {
        ctx.check_table_ref_access()?;

        let table = ctx
            .get_table(&plan.catalog, &plan.database, &plan.table)
            .await?;
        let table_id = table.get_table_info().ident.table_id;
        let catalog = ctx.get_catalog(&plan.catalog).await?;
        let seq_tag = catalog
            .get_table_tag_with_expire_ctl(table_id, &plan.name, true)
            .await?;
        let Some(seq_tag) = seq_tag else {
            return Err(ErrorCode::UnknownReference(format!(
                "Unknown tag '{}'",
                plan.name
            )));
        };

        catalog
            .drop_table_tag(DropTableTagReq {
                table_id,
                tag_name: plan.name.clone(),
                seq: MatchSeq::Exact(seq_tag.seq),
            })
            .await
    }
}

impl RealTableRefHandler {
    pub fn init() -> Result<()> {
        let handler = RealTableRefHandler {};
        let wrapper = TableRefHandlerWrapper::new(Box::new(handler));
        GlobalInstance::set(Arc::new(wrapper));
        Ok(())
    }

    #[async_backtrace::framed]
    async fn load_source_table(
        &self,
        ctx: Arc<dyn TableContext>,
        catalog: &str,
        database: &str,
        table_name: &str,
    ) -> Result<Arc<dyn Table>> {
        let table = ctx.get_table(catalog, database, table_name).await?;

        if table.is_temp() {
            return Err(ErrorCode::IllegalReference(format!(
                "The table '{}.{}' is temporary, can't create TAG",
                database, table_name
            )));
        }

        let table_info = table.get_table_info();
        if table_info.engine() != "FUSE" {
            return Err(ErrorCode::IllegalReference(format!(
                "The table '{}.{}' uses engine '{}', only FUSE tables support TAG",
                database,
                table_name,
                table_info.engine(),
            )));
        }

        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        if fuse_table.is_transient() {
            return Err(ErrorCode::IllegalReference(format!(
                "The table '{}.{}' is transient, can't create TAG",
                database, table_name
            )));
        }

        Ok(table)
    }
}
