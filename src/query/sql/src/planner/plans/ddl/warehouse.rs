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

use std::collections::BTreeMap;
use std::collections::HashMap;

use databend_common_expression::types::DataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateWarehousePlan {
    pub warehouse: String,
    pub nodes: HashMap<Option<String>, u64>,
    pub options: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropWarehousePlan {
    pub warehouse: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResumeWarehousePlan {
    pub warehouse: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SuspendWarehousePlan {
    pub warehouse: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenameWarehousePlan {
    pub warehouse: String,
    pub new_warehouse: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InspectWarehousePlan {
    pub warehouse: String,
}

impl InspectWarehousePlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![
            DataField::new("cluster", DataType::String),
            DataField::new("node_name", DataType::String),
            DataField::new("node_type", DataType::String),
        ])
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AddWarehouseClusterPlan {
    warehouse: String,
    cluster: String,
    // nodes:Vec<Selected>
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropWarehouseClusterPlan {
    pub warehouse: String,
    pub cluster: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenameWarehouseClusterPlan {
    pub warehouse: String,
    pub cluster: String,
    pub new_cluster: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AddWarehouseClusterNodePlan {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropWarehouseClusterNodePlan {}
