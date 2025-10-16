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

use databend_common_base::base::GlobalInstance;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_management::RoleApi;
use databend_common_management::WarehouseInfo;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_sql::plans::DropWarehousePlan;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;
use databend_enterprise_resources_management::ResourcesManagement;

use crate::interpreters::util::AuditElement;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct DropWarehouseInterpreter {
    #[allow(dead_code)]
    ctx: Arc<QueryContext>,
    plan: DropWarehousePlan,
}

impl DropWarehouseInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropWarehousePlan) -> Result<Self> {
        Ok(DropWarehouseInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropWarehouseInterpreter {
    fn name(&self) -> &str {
        "DropWarehouseInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn build_pipeline(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::SystemManagement)?;

        let tenant = self.ctx.get_tenant();
        let warehouse = GlobalInstance::get::<Arc<dyn ResourcesManagement>>()
            .drop_warehouse(self.plan.warehouse.clone())
            .await?;

        let user_info = self.ctx.get_current_user()?;
        log::info!(
            target: "databend::log::audit",
            "{}",
            serde_json::to_string(&AuditElement::create(&user_info, "drop_warehouse", &self.plan))?
        );

        if let WarehouseInfo::SystemManaged(sw) = warehouse {
            if let Some(current_role) = self.ctx.get_current_role() {
                let role_api = UserApiProvider::instance().role_api(&tenant);
                role_api
                    .grant_ownership(
                        &OwnershipObject::Warehouse { id: sw.role_id },
                        &current_role.name,
                    )
                    .await?;
                RoleCacheManager::instance().invalidate_cache(&tenant);
            }
        }

        Ok(PipelineBuildResult::create())
    }
}
