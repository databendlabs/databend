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
use databend_common_meta_app::schema::SetTableRowAccessPolicyAction;
use databend_common_meta_app::schema::SetTableRowAccessPolicyReq;
use databend_common_sql::plans::DropAllTableRowAccessPoliciesPlan;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct DropAllTableRowAccessPoliciesInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropAllTableRowAccessPoliciesPlan,
}

impl DropAllTableRowAccessPoliciesInterpreter {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        plan: DropAllTableRowAccessPoliciesPlan,
    ) -> Result<Self> {
        Ok(DropAllTableRowAccessPoliciesInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropAllTableRowAccessPoliciesInterpreter {
    fn name(&self) -> &str {
        "DropAllTableRowAccessPoliciesInterpreter"
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

        let table_info = table.get_table_info();
        let table_id = table_info.ident.table_id;

        if let Some(row_access_policy) = &table_info.meta.row_access_policy_columns_ids {
            let req = SetTableRowAccessPolicyReq {
                tenant: self.ctx.get_tenant(),
                table_id,
                action: SetTableRowAccessPolicyAction::Unset(row_access_policy.policy_id),
            };
            let _resp = catalog.set_table_row_access_policy(req).await?;
        }

        Ok(PipelineBuildResult::create())
    }
}
