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
use databend_common_catalog::table_context::TableContextCluster;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_management::WarehouseInfo;
use databend_enterprise_resources_management::ResourcesManagement;
use databend_query_task_support::TaskContext;

use crate::sessions::QueryContext;
use crate::sessions::TableContextAuthorization;
use crate::sessions::TableContextLicense;
use crate::sessions::TableContextQueryIdentity;
use crate::sessions::TableContextTableAccess;

#[async_trait::async_trait]
impl TaskContext for QueryContext {
    fn current_role_identity(&self) -> String {
        self.get_current_role()
            .unwrap_or_default()
            .identity()
            .to_string()
    }

    fn current_user_display(&self) -> Result<String> {
        Ok(self.get_current_user()?.identity().display().to_string())
    }

    fn current_user_encoded(&self) -> Result<String> {
        Ok(self.get_current_user()?.identity().encode())
    }

    fn license_key(&self) -> String {
        self.get_license_key()
    }

    fn tenant_name(&self) -> String {
        self.get_tenant().tenant_name().to_string()
    }

    fn query_id(&self) -> String {
        self.get_id()
    }

    async fn validate_warehouse_exists(&self, warehouse: Option<&str>) -> Result<()> {
        let Some(warehouse) = warehouse else {
            return Ok(());
        };

        if matches!(self.get_cluster().get_warehouse_id(), Ok(current) if current == warehouse) {
            return Ok(());
        }

        let warehouses = GlobalInstance::get::<Arc<dyn ResourcesManagement>>()
            .list_warehouses()
            .await
            .map_err(|err| {
                if err.code() == ErrorCode::Unimplemented("").code() {
                    ErrorCode::UnknownWarehouse(format!("warehouse {warehouse} not exists"))
                } else {
                    err
                }
            })?;

        if warehouses.iter().any(|info| match info {
            WarehouseInfo::SelfManaged(id) => id == warehouse,
            WarehouseInfo::SystemManaged(info) => info.id == warehouse,
        }) {
            return Ok(());
        }

        Err(ErrorCode::UnknownWarehouse(format!(
            "warehouse {warehouse} not exists"
        )))
    }

    async fn available_role_identities(&self) -> Result<Vec<String>> {
        Ok(self
            .get_current_session()
            .get_all_available_roles()
            .await?
            .into_iter()
            .map(|role| role.identity().to_string())
            .collect())
    }
}
