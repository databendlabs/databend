// Copyright 2021 Datafuse Labs.
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

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::WarehouseInfo;

use crate::users::UserApiProvider;

/// user warehouse operations.
impl UserApiProvider {
    // Create a warehouse for given tenant.
    pub async fn create_warehouse(
        &self,
        tenant: &str,
        info: WarehouseInfo,
        if_not_exists: bool,
    ) -> Result<u64> {
        let warehouse_api_provider = self.get_warehouse_api_client(tenant)?;
        let create_warehouse = warehouse_api_provider.create_warehouse(info);
        match create_warehouse.await {
            Ok(res) => Ok(res),
            Err(e) => {
                if if_not_exists && e.code() == ErrorCode::warehouse_already_exists_code() {
                    Ok(u64::MIN)
                } else {
                    Err(e)
                }
            }
        }
    }

    // Get one warehouse from by tenant.
    pub async fn get_warehouse(&self, tenant: &str, warehouse_name: &str) -> Result<WarehouseInfo> {
        let warehouse_api_provider = self.get_warehouse_api_client(tenant)?;
        let get_warehouse = warehouse_api_provider.get_warehouse(warehouse_name, None);
        Ok(get_warehouse.await?.data)
    }

    // Get the tenant all warehouse list.
    pub async fn get_warehouses(&self, tenant: &str) -> Result<Vec<WarehouseInfo>> {
        let warehouse_api_provider = self.get_warehouse_api_client(tenant)?;
        let get_warehouses = warehouse_api_provider.get_warehouses();

        match get_warehouses.await {
            Err(e) => Err(e.add_message_back("(while get warehouses).")),
            Ok(warehouse_info) => Ok(warehouse_info.iter().map(|x| x.data.clone()).collect()),
        }
    }

    // Drop a warehouse by name.
    pub async fn drop_warehouse(&self, tenant: &str, name: &str, if_exists: bool) -> Result<()> {
        let warehouse_api_provider = self.get_warehouse_api_client(tenant)?;
        let drop_warehouse = warehouse_api_provider.drop_warehouse(name, None);
        match drop_warehouse.await {
            Ok(res) => Ok(res),
            Err(e) => {
                if if_exists && e.code() == ErrorCode::unknown_warehouse_code() {
                    Ok(())
                } else {
                    Err(e.add_message_back("(while drop warehouse)"))
                }
            }
        }
    }

    // Update the size of given warehouse
    pub async fn update_warehouse_size(&self, tenant: &str, name: &str, size: &str) -> Result<u64> {
        let warehouse_api_provider = self.get_warehouse_api_client(tenant)?;
        let update_warehouse_size = warehouse_api_provider.update_warehouse_size(name, size, None);
        match update_warehouse_size.await {
            Ok(res) => Ok(res),
            Err(e) => Err(e.add_message_back("(while update warehouse size)")),
        }
    }
}
