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
use databend_common_license::license::Feature::RowAccessPolicy;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::schema::SetSecurityPolicyAction;
use databend_common_meta_app::schema::SetTableRowAccessPolicyReq;
use databend_common_sql::plans::DropTableRowAccessPolicyPlan;
use databend_common_users::UserApiProvider;
use databend_enterprise_row_access_policy_feature::get_row_access_policy_handler;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct DropTableRowAccessPolicyInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropTableRowAccessPolicyPlan,
}

impl DropTableRowAccessPolicyInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropTableRowAccessPolicyPlan) -> Result<Self> {
        Ok(DropTableRowAccessPolicyInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropTableRowAccessPolicyInterpreter {
    fn name(&self) -> &str {
        "DropTableRowAccessPolicyInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn build_pipeline(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), RowAccessPolicy)?;

        let catalog_name = self.plan.catalog.as_str();
        let db_name = self.plan.database.as_str();
        let tbl_name = self.plan.table.as_str();
        let catalog = self.ctx.get_catalog(catalog_name).await?;

        let table = self.ctx.get_table(catalog_name, db_name, tbl_name).await?;

        let table_info = table.get_table_info();
        let table_id = table_info.ident.table_id;

        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let handler = get_row_access_policy_handler();
        let (policy_id, _) = handler
            .get_row_access(
                meta_api,
                &self.ctx.get_tenant(),
                self.plan.policy.to_string(),
            )
            .await?;

        let req = SetTableRowAccessPolicyReq {
            tenant: self.ctx.get_tenant(),
            table_id,
            action: SetSecurityPolicyAction::Unset(*policy_id.data),
        };

        let _resp = catalog.set_table_row_access_policy(req).await?;

        Ok(PipelineBuildResult::create())
    }
}
