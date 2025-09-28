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
use databend_common_license::license::Feature::RowAccessPolicy;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::schema::DatabaseType;
use databend_common_meta_app::schema::SetTableRowAccessPolicyAction;
use databend_common_meta_app::schema::SetTableRowAccessPolicyReq;
use databend_common_sql::plans::AddTableRowAccessPolicyPlan;
use databend_common_storages_basic::view_table::VIEW_ENGINE;
use databend_common_storages_stream::stream_table::STREAM_ENGINE;
use databend_common_users::UserApiProvider;
use databend_enterprise_row_access_policy_feature::get_row_access_policy_handler;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct AddTableRowAccessPolicyInterpreter {
    ctx: Arc<QueryContext>,
    plan: AddTableRowAccessPolicyPlan,
}

impl AddTableRowAccessPolicyInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: AddTableRowAccessPolicyPlan) -> Result<Self> {
        Ok(AddTableRowAccessPolicyInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for AddTableRowAccessPolicyInterpreter {
    fn name(&self) -> &str {
        "AddTableRowAccessPolicyInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), RowAccessPolicy)?;

        let catalog_name = self.plan.catalog.as_str();
        let db_name = self.plan.database.as_str();
        let tbl_name = self.plan.table.as_str();
        let catalog = self.ctx.get_catalog(catalog_name).await?;

        let table = self.ctx.get_table(catalog_name, db_name, tbl_name).await?;

        table.check_mutable()?;

        let table_info = table.get_table_info();

        if table.is_temp() {
            return Err(ErrorCode::StorageOther(format!(
                "Table {} is temporary table, setting row access policy not allowed",
                table.name()
            )));
        }
        let engine = table.engine();
        if matches!(engine, VIEW_ENGINE | STREAM_ENGINE) {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "{}.{} engine is {} that doesn't support alter",
                db_name, tbl_name, engine
            )));
        }
        if table_info.db_type != DatabaseType::NormalDB {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "{}.{} doesn't support alter",
                db_name, tbl_name
            )));
        }

        let policy_name = self.plan.policy.to_string();

        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let handler = get_row_access_policy_handler();
        let (policy_id, policy) = handler
            .get_row_access(meta_api, &self.ctx.get_tenant(), policy_name.clone())
            .await?;

        // check if column type match to the input type
        let policy_data_types: Vec<String> =
            policy.args.iter().map(|arg| arg.1.to_lowercase()).collect();

        let mut columns_ids = vec![];
        let schema = table.schema();
        let table_info = table.get_table_info();
        let columns = self.plan.columns.clone();
        for column in &columns {
            if let Some((_, data_field)) = schema.column_with_name(column) {
                let data_type = data_field.data_type().to_string().to_lowercase();
                if !policy_data_types.contains(&data_type.to_string()) {
                    return Err(ErrorCode::UnmatchColumnDataType(format!(
                        "Column '{}' data type {} does not match to the row access policy {}",
                        column, data_type, policy_name,
                    )));
                } else {
                    columns_ids.push(data_field.column_id);
                }
            } else {
                return Err(ErrorCode::UnknownColumn(format!(
                    "Cannot find column {}",
                    column
                )));
            }
        }

        let table_id = table_info.ident.table_id;

        let req = SetTableRowAccessPolicyReq {
            tenant: self.ctx.get_tenant(),
            table_id,
            action: SetTableRowAccessPolicyAction::Set(*policy_id.data, columns_ids),
        };

        let _resp = catalog.set_table_row_access_policy(req).await?;

        Ok(PipelineBuildResult::create())
    }
}
