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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_management::RoleApi;
use databend_common_management::SelectedNode;
use databend_common_management::WarehouseInfo;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_sql::plans::CreateWarehousePlan;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;
use databend_enterprise_resources_management::ResourcesManagement;

use crate::interpreters::util::AuditElement;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct CreateWarehouseInterpreter {
    #[allow(dead_code)]
    ctx: Arc<QueryContext>,
    plan: CreateWarehousePlan,
}

impl CreateWarehouseInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateWarehousePlan) -> Result<Self> {
        Ok(CreateWarehouseInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateWarehouseInterpreter {
    fn name(&self) -> &str {
        "CreateWarehouseInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn build_pipeline(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::SystemManagement)?;

        let tenant = self.ctx.get_tenant();
        if let Some(warehouse_size) = self.plan.options.get("warehouse_size") {
            if !self.plan.nodes.is_empty() {
                return Err(ErrorCode::InvalidArgument(
                    "WAREHOUSE_SIZE option and node list exists in one query.",
                ));
            }

            let Ok(warehouse_size) = warehouse_size.parse::<usize>() else {
                return Err(ErrorCode::InvalidArgument(
                    "WAREHOUSE_SIZE must be a <number>",
                ));
            };

            let warehouse = GlobalInstance::get::<Arc<dyn ResourcesManagement>>()
                .create_warehouse(self.plan.warehouse.clone(), vec![
                    SelectedNode::Random(None);
                    warehouse_size
                ])
                .await?;

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

            return Ok(PipelineBuildResult::create());
        }

        if self.plan.nodes.is_empty() {
            return Err(ErrorCode::InvalidArgument("Warehouse nodes list is empty"));
        }

        let mut selected_nodes = Vec::with_capacity(self.plan.nodes.len());
        for (group, nodes) in &self.plan.nodes {
            for _ in 0..*nodes {
                selected_nodes.push(SelectedNode::Random(group.clone()));
            }
        }

        let warehouse = GlobalInstance::get::<Arc<dyn ResourcesManagement>>()
            .create_warehouse(self.plan.warehouse.clone(), selected_nodes)
            .await?;

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

        let user_info = self.ctx.get_current_user()?;
        log::info!(
            target: "databend::log::audit",
            "{}",
            serde_json::to_string(&AuditElement::create(&user_info, "create_warehouse", &self.plan))?
        );

        Ok(PipelineBuildResult::create())
    }
}
