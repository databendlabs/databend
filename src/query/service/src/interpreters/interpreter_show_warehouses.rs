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
use databend_common_expression::types::DataType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_management::WarehouseInfo;
use databend_common_users::Object;
use databend_enterprise_resources_management::ResourcesManagement;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct ShowWarehousesInterpreter {
    ctx: Arc<QueryContext>,
}

impl ShowWarehousesInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>) -> Result<Self> {
        Ok(ShowWarehousesInterpreter { ctx })
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowWarehousesInterpreter {
    fn name(&self) -> &str {
        "ShowWarehousesInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn build_pipeline(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::SystemManagement)?;

        let warehouses = GlobalInstance::get::<Arc<dyn ResourcesManagement>>()
            .list_warehouses()
            .await?;
        let mut warehouses_name = ColumnBuilder::with_capacity(&DataType::String, warehouses.len());
        let mut warehouses_type = ColumnBuilder::with_capacity(&DataType::String, warehouses.len());
        let mut warehouses_status =
            ColumnBuilder::with_capacity(&DataType::String, warehouses.len());

        let visibility_checker = self
            .ctx
            .get_visibility_checker(false, Object::Warehouse)
            .await?;
        for warehouse in warehouses {
            match warehouse {
                WarehouseInfo::SelfManaged(name) => {
                    warehouses_name.push(Scalar::String(name).as_ref());
                    warehouses_type.push(Scalar::String(String::from("Self-Managed")).as_ref());
                    warehouses_status.push(Scalar::String(String::from("Running")).as_ref());
                }
                WarehouseInfo::SystemManaged(v) => {
                    if visibility_checker.check_warehouse_visibility(&v.role_id) {
                        warehouses_name.push(Scalar::String(v.id.clone()).as_ref());
                        warehouses_type
                            .push(Scalar::String(String::from("System-Managed")).as_ref());
                        warehouses_status.push(Scalar::String(v.status.clone()).as_ref());
                    }
                }
            }
        }

        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
            warehouses_name.build(),
            warehouses_type.build(),
            warehouses_status.build(),
        ])])
    }
}
