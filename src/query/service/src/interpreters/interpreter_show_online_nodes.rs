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
use databend_enterprise_resources_management::ResourcesManagement;
use databend_meta_types::NodeType;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct ShowOnlineNodesInterpreter {
    ctx: Arc<QueryContext>,
}

impl ShowOnlineNodesInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>) -> Result<Self> {
        Ok(ShowOnlineNodesInterpreter { ctx })
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowOnlineNodesInterpreter {
    fn name(&self) -> &str {
        "ShowWarehousesInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::SystemManagement)?;

        let online_nodes = GlobalInstance::get::<Arc<dyn ResourcesManagement>>()
            .list_online_nodes()
            .await?;

        let mut nodes_id = ColumnBuilder::with_capacity(&DataType::String, online_nodes.len());
        let mut nodes_type = ColumnBuilder::with_capacity(&DataType::String, online_nodes.len());
        let mut nodes_group = ColumnBuilder::with_capacity(&DataType::String, online_nodes.len());
        let mut nodes_warehouse =
            ColumnBuilder::with_capacity(&DataType::String, online_nodes.len());
        let mut nodes_cluster = ColumnBuilder::with_capacity(&DataType::String, online_nodes.len());
        let mut nodes_version = ColumnBuilder::with_capacity(&DataType::String, online_nodes.len());

        for node in online_nodes {
            let node_type = match node.node_type {
                NodeType::SelfManaged => String::from("SelfManaged"),
                NodeType::SystemManaged => String::from("SystemManaged"),
            };

            let binary_version = match node.binary_version.split_once('(') {
                None => node.binary_version,
                Some((left, _right)) => left.to_string(),
            };

            nodes_id.push(Scalar::String(node.id).as_ref());
            nodes_type.push(Scalar::String(node_type).as_ref());
            nodes_group.push(Scalar::String(node.node_group.clone().unwrap_or_default()).as_ref());
            nodes_warehouse.push(Scalar::String(node.warehouse_id).as_ref());
            nodes_cluster.push(Scalar::String(node.cluster_id).as_ref());
            nodes_version.push(Scalar::String(binary_version).as_ref());
        }

        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
            nodes_id.build(),
            nodes_type.build(),
            nodes_group.build(),
            nodes_warehouse.build(),
            nodes_cluster.build(),
            nodes_version.build(),
        ])])
    }
}
