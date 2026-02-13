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
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_sql::plans::InspectWarehousePlan;
use databend_enterprise_resources_management::ResourcesManagement;
use databend_meta_types::NodeType;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct InspectWarehouseInterpreter {
    ctx: Arc<QueryContext>,
    plan: InspectWarehousePlan,
}

impl InspectWarehouseInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: InspectWarehousePlan) -> Result<Self> {
        Ok(InspectWarehouseInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for InspectWarehouseInterpreter {
    fn name(&self) -> &str {
        "InspectWarehouseInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::SystemManagement)?;

        let mut warehouse_nodes = GlobalInstance::get::<Arc<dyn ResourcesManagement>>()
            .inspect_warehouse(self.plan.warehouse.clone())
            .await?;

        warehouse_nodes.sort_by(|left, right| {
            (&left.cluster_id, &left.id).cmp(&(&right.cluster_id, &right.id))
        });

        let mut clusters = ColumnBuilder::with_capacity(&DataType::String, warehouse_nodes.len());
        let mut nodes_id = ColumnBuilder::with_capacity(&DataType::String, warehouse_nodes.len());
        let mut nodes_type = ColumnBuilder::with_capacity(&DataType::String, warehouse_nodes.len());
        // let mut nodes_pool = ColumnBuilder::with_capacity(&DataType::String, warehouse_nodes.len());

        for warehouse_node in warehouse_nodes {
            nodes_id.push(Scalar::String(warehouse_node.id.clone()).as_ref());
            clusters.push(Scalar::String(warehouse_node.cluster_id.clone()).as_ref());
            nodes_type.push(
                Scalar::String(match warehouse_node.node_type {
                    NodeType::SelfManaged => String::from("SelfManaged"),
                    NodeType::SystemManaged => String::from("SystemManaged"),
                })
                .as_ref(),
            );
        }

        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
            clusters.build(),
            nodes_id.build(),
            nodes_type.build(),
        ])])
    }
}
