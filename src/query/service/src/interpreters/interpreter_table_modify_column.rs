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

use std::collections::BTreeMap;
use std::sync::Arc;

use common_ast::ast::ModifyColumnAction;
use common_catalog::table::Table;
use common_exception::ErrorCode;
use common_exception::Result;
use common_license::license::Feature::DataMask;
use common_license::license_manager::get_license_manager;
use common_meta_app::schema::DatabaseType;
use common_meta_app::schema::TableMeta;
use common_meta_app::schema::UpdateTableMetaReq;
use common_meta_types::MatchSeq;
use common_sql::plans::ModifyTableColumnPlan;
use common_storages_share::save_share_table_info;
use common_storages_view::view_table::VIEW_ENGINE;
use common_users::UserApiProvider;
use data_mask_feature::get_datamask_handler;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct ModifyTableColumnInterpreter {
    ctx: Arc<QueryContext>,
    plan: ModifyTableColumnPlan,
}

impl ModifyTableColumnInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ModifyTableColumnPlan) -> Result<Self> {
        Ok(ModifyTableColumnInterpreter { ctx, plan })
    }

    // Set data mask policy to a column is a ee feature.
    async fn do_set_data_mask_policy(
        &self,
        table: &Arc<dyn Table>,
        table_meta: TableMeta,
        mask_name: String,
    ) -> Result<TableMeta> {
        let license_manager = get_license_manager();
        license_manager.manager.check_enterprise_enabled(
            &self.ctx.get_settings(),
            self.ctx.get_tenant(),
            DataMask,
        )?;

        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let handler = get_datamask_handler();
        let policy = handler
            .get_data_mask(meta_api, self.ctx.get_tenant(), mask_name.clone())
            .await?;

        let schema = table.schema();
        if let Some((_, data_field)) = schema.column_with_name(&self.plan.column) {
            let data_type = data_field.data_type().to_string().to_lowercase();
            let policy_data_type = policy.args[0].1.to_string().to_lowercase();
            if data_type != policy_data_type {
                return Err(ErrorCode::UnmatchColumnDataType(format!(
                    "Column '{}' data type {} does not match to the mask policy type {}",
                    self.plan.column, data_type, policy_data_type,
                )));
            }
        } else {
            return Err(ErrorCode::UnknownColumn(format!(
                "Cannot find column {}",
                self.plan.column
            )));
        }

        let mut new_table_meta = table_meta;

        let mut column_mask_policy = match &new_table_meta.column_mask_policy {
            Some(column_mask_policy) => column_mask_policy.clone(),
            None => BTreeMap::new(),
        };
        column_mask_policy.insert(self.plan.column.clone(), mask_name);
        new_table_meta.column_mask_policy = Some(column_mask_policy);
        Ok(new_table_meta)
    }
}

#[async_trait::async_trait]
impl Interpreter for ModifyTableColumnInterpreter {
    fn name(&self) -> &str {
        "ModifyTableColumnInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog_name = self.plan.catalog.as_str();
        let db_name = self.plan.database.as_str();
        let tbl_name = self.plan.table.as_str();

        let tbl = self
            .ctx
            .get_catalog(catalog_name)?
            .get_table(self.ctx.get_tenant().as_str(), db_name, tbl_name)
            .await
            .ok();

        let table = if let Some(table) = &tbl {
            table
        } else {
            return Ok(PipelineBuildResult::create());
        };

        let table_info = table.get_table_info();
        if table_info.engine() == VIEW_ENGINE {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "{}.{} engine is VIEW that doesn't support alter",
                &self.plan.database, &self.plan.table
            )));
        }
        if table_info.db_type != DatabaseType::NormalDB {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "{}.{} doesn't support alter",
                &self.plan.database, &self.plan.table
            )));
        }

        let catalog = self.ctx.get_catalog(catalog_name)?;
        let table_meta = table.get_table_info().meta.clone();

        // NOTICE: if we support modify column data type,
        // need to check whether this column is referenced by other computed columns.
        let new_table_meta = match &self.plan.action {
            ModifyColumnAction::SetMaskingPolicy(mask_name) => {
                self.do_set_data_mask_policy(table, table_meta, mask_name.clone())
                    .await?
            }
        };

        let table_id = table_info.ident.table_id;
        let table_version = table_info.ident.seq;

        let req = UpdateTableMetaReq {
            table_id,
            seq: MatchSeq::Exact(table_version),
            new_table_meta,
            copied_files: None,
            deduplicated_label: None,
        };

        let res = catalog.update_table_meta(table_info, req).await?;

        if let Some(share_table_info) = res.share_table_info {
            save_share_table_info(
                &self.ctx.get_tenant(),
                self.ctx.get_data_operator()?.operator(),
                share_table_info,
            )
            .await?;
        }

        Ok(PipelineBuildResult::create())
    }
}
