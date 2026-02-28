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
use databend_common_meta_app::schema::CreateTableBranchReq;
use databend_common_meta_app::schema::CreateTableTagReq;
use databend_common_meta_app::schema::DropTableBranchReq;
use databend_common_meta_app::schema::DropTableTagReq;
use databend_common_meta_app::schema::OPT_KEY_BASE_TABLE_ID;
use databend_common_meta_app::schema::RefNameIdent;
use databend_common_meta_app::schema::TableLvtCheck;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_sql::plans::CreateTableBranchPlan;
use databend_common_sql::plans::CreateTableTagPlan;
use databend_common_sql::plans::DropTableBranchPlan;
use databend_common_sql::plans::DropTableTagPlan;
use databend_common_storages_fuse::FuseTable;
use databend_enterprise_table_ref_handler::TableRefHandler;
use databend_enterprise_table_ref_handler::TableRefHandlerWrapper;
use databend_meta_types::MatchSeq;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;

pub struct RealTableRefHandler {}

#[async_trait::async_trait]
impl TableRefHandler for RealTableRefHandler {
    #[async_backtrace::framed]
    async fn do_create_table_branch(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &CreateTableBranchPlan,
    ) -> Result<()> {
        if !ctx
            .get_settings()
            .get_enable_experimental_table_ref()
            .unwrap_or_default()
        {
            return Err(ErrorCode::Unimplemented(
                "Table ref is an experimental feature, `set enable_experimental_table_ref=1` to use this feature",
            ));
        }

        self.create_table_branch_impl(ctx, plan).await
    }

    #[async_backtrace::framed]
    async fn do_create_table_tag(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &CreateTableTagPlan,
    ) -> Result<()> {
        if !ctx
            .get_settings()
            .get_enable_experimental_table_ref()
            .unwrap_or_default()
        {
            return Err(ErrorCode::Unimplemented(
                "Table ref is an experimental feature, `set enable_experimental_table_ref=1` to use this feature",
            ));
        }

        self.create_table_tag_impl(ctx, plan).await
    }

    #[async_backtrace::framed]
    async fn do_drop_table_branch(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DropTableBranchPlan,
    ) -> Result<()> {
        self.drop_table_branch_impl(ctx, plan).await
    }

    #[async_backtrace::framed]
    async fn do_drop_table_tag(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DropTableTagPlan,
    ) -> Result<()> {
        self.drop_table_tag_impl(ctx, plan).await
    }
}

impl RealTableRefHandler {
    pub fn init() -> Result<()> {
        let handler = RealTableRefHandler {};
        let wrapper = TableRefHandlerWrapper::new(Box::new(handler));
        GlobalInstance::set(Arc::new(wrapper));
        Ok(())
    }

    fn name_ident(ctx: &Arc<dyn TableContext>, db: &str, table: &str, ident: &str) -> RefNameIdent {
        RefNameIdent::new(&ctx.get_tenant(), db, table, ident)
    }

    #[async_backtrace::framed]
    async fn load_base_table(
        &self,
        ctx: Arc<dyn TableContext>,
        catalog: &str,
        database: &str,
        table_name: &str,
        target_type: &str,
    ) -> Result<Arc<dyn Table>> {
        let table = ctx.get_table(catalog, database, table_name).await?;

        if table.is_temp() {
            return Err(ErrorCode::IllegalReference(format!(
                "The table '{}.{}' is temporary, can't create {}",
                database, table_name, target_type
            )));
        }

        let table_info = table.get_table_info();
        if table_info.engine() != "FUSE" {
            return Err(ErrorCode::IllegalReference(format!(
                "The table '{}.{}' uses engine '{}', only FUSE tables support {} creation",
                database,
                table_name,
                table_info.engine(),
                target_type
            )));
        }

        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        if fuse_table.is_transient() {
            return Err(ErrorCode::IllegalReference(format!(
                "The table '{}.{}' is transient, can't create {}",
                database, table_name, target_type
            )));
        }

        Ok(table)
    }

    #[async_backtrace::framed]
    async fn create_table_tag_impl(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &CreateTableTagPlan,
    ) -> Result<()> {
        let table = self
            .load_base_table(
                ctx.clone(),
                &plan.catalog,
                &plan.database,
                &plan.table,
                "TAG",
            )
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
                plan.database, plan.table, plan.tag_name
            ))
        })?;
        let Some(snapshot) = fuse_table
            .read_table_snapshot_with_location(Some(snapshot_loc.clone()))
            .await?
        else {
            return Err(ErrorCode::TableHistoricalDataNotFound(format!(
                "Snapshot '{}' not found when creating TAG '{}'",
                snapshot_loc, plan.tag_name
            )));
        };
        let Some(snapshot_timestamp) = snapshot.timestamp else {
            return Err(ErrorCode::IllegalReference(format!(
                "Table {} snapshot lacks required timestamp. This table was created with a significantly outdated version \
                that is no longer directly supported by the current version and requires migration. \
                Please contact us at https://www.databend.com/contact-us/ or email hi@databend.com",
                table_info.ident.table_id
            )));
        };

        let catalog = ctx.get_catalog(&plan.catalog).await?;
        catalog
            .create_table_tag(CreateTableTagReq {
                table_id: table_info.ident.table_id,
                seq: MatchSeq::Exact(table_info.ident.seq),
                tag_name: plan.tag_name.clone(),
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
    async fn create_table_branch_impl(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &CreateTableBranchPlan,
    ) -> Result<()> {
        let table = self
            .load_base_table(
                ctx.clone(),
                &plan.catalog,
                &plan.database,
                &plan.table,
                "BRANCH",
            )
            .await?;
        let table_info = table.get_table_info();
        let table_id = table_info.ident.table_id;
        let seq = table_info.ident.seq;

        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        let source_snapshot_location = match &plan.navigation {
            Some(navigation) => {
                fuse_table
                    .navigate_to_location(ctx.clone(), navigation)
                    .await?
            }
            None => fuse_table.snapshot_loc(),
        };

        let (new_snapshot, lvt_check) = if let Some(location) = source_snapshot_location {
            let Some(snapshot) = fuse_table
                .read_table_snapshot_with_location(Some(location.clone()))
                .await?
            else {
                return Err(ErrorCode::TableHistoricalDataNotFound(format!(
                    "Snapshot '{}' not found when creating BRANCH '{}'",
                    location, plan.branch_name
                )));
            };

            if snapshot.timestamp.is_none() {
                return Err(ErrorCode::IllegalReference(format!(
                    "Table {} snapshot lacks required timestamp. This table was created with a significantly outdated version \
                    that is no longer directly supported by the current version and requires migration. \
                    Please contact us at https://www.databend.com/contact-us/ or email hi@databend.com",
                    table_id
                )));
            }

            let mut branch_snapshot = TableSnapshot::try_from_previous(
                snapshot.clone(),
                Some(seq),
                ctx.get_table_meta_timestamps(fuse_table, Some(snapshot.clone()))?,
            )?;
            branch_snapshot.prev_snapshot_id = None;
            (
                branch_snapshot,
                Some(TableLvtCheck {
                    tenant: ctx.get_tenant(),
                    time: snapshot.timestamp.unwrap(),
                }),
            )
        } else {
            let branch_snapshot = TableSnapshot::try_new(
                Some(seq),
                None,
                table_info.schema().as_ref().clone(),
                Default::default(),
                vec![],
                fuse_table.cluster_key_meta(),
                None,
                ctx.get_table_meta_timestamps(fuse_table, None)?,
            )?;
            (branch_snapshot, None)
        };

        let mut branch_table_meta = table_info.meta.clone();
        branch_table_meta
            .options
            .insert(OPT_KEY_BASE_TABLE_ID.to_string(), table_id.to_string());

        let catalog = ctx.get_catalog(&plan.catalog).await?;
        let name_ident = Self::name_ident(&ctx, &plan.database, &plan.table, &plan.branch_name);
        let branch_table_info = catalog
            .create_table_branch(CreateTableBranchReq {
                name_ident,
                table_id,
                seq: MatchSeq::Exact(seq),
                table_meta: branch_table_meta,
                expire_at: plan.retain.map(|v| Utc::now() + v),
                lvt_check,
            })
            .await?;

        let branch_table = catalog.get_table_by_info(branch_table_info.as_ref())?;
        let branch_fuse_table = FuseTable::try_from_table(branch_table.as_ref())?;
        let new_snapshot_location = branch_fuse_table
            .meta_location_generator()
            .gen_snapshot_location(&new_snapshot.snapshot_id, new_snapshot.format_version)?;
        branch_fuse_table
            .get_operator_ref()
            .write(&new_snapshot_location, new_snapshot.to_bytes()?)
            .await?;

        let mut new_branch_meta = branch_table_info.meta.clone();
        new_branch_meta
            .options
            .insert(OPT_KEY_SNAPSHOT_LOCATION.to_string(), new_snapshot_location);

        catalog
            .update_single_table_meta(
                UpdateTableMetaReq {
                    table_id: branch_table_info.ident.table_id,
                    seq: MatchSeq::Exact(branch_table_info.ident.seq),
                    new_table_meta: new_branch_meta,
                    base_snapshot_location: branch_fuse_table.snapshot_loc(),
                },
                branch_table_info.as_ref(),
            )
            .await?;

        Ok(())
    }

    #[async_backtrace::framed]
    async fn drop_table_tag_impl(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DropTableTagPlan,
    ) -> Result<()> {
        let catalog = ctx.get_catalog(&plan.catalog).await?;
        let name_ident = Self::name_ident(&ctx, &plan.database, &plan.table, &plan.tag_name);
        catalog.drop_table_tag(DropTableTagReq { name_ident }).await
    }

    #[async_backtrace::framed]
    async fn drop_table_branch_impl(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DropTableBranchPlan,
    ) -> Result<()> {
        let catalog = ctx.get_catalog(&plan.catalog).await?;
        let name_ident = Self::name_ident(&ctx, &plan.database, &plan.table, &plan.branch_name);
        catalog
            .drop_table_branch(DropTableBranchReq {
                name_ident,
                catalog_name: Some(plan.catalog.clone()),
            })
            .await
    }
}
