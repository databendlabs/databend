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

use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::types::DataType;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct UseWarehousePlan {
    pub warehouse: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct CreateWarehousePlan {
    pub warehouse: String,
    pub nodes: HashMap<Option<String>, u64>,
    pub options: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct DropWarehousePlan {
    pub warehouse: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct ResumeWarehousePlan {
    pub warehouse: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct SuspendWarehousePlan {
    pub warehouse: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
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

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct AddWarehouseClusterPlan {
    pub warehouse: String,
    pub cluster: String,
    pub nodes: HashMap<Option<String>, u64>,
    pub options: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct DropWarehouseClusterPlan {
    pub warehouse: String,
    pub cluster: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct RenameWarehouseClusterPlan {
    pub warehouse: String,
    pub cluster: String,
    pub new_cluster: String,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct AssignWarehouseNodesPlan {
    pub warehouse: String,
    #[serde(with = "vectorize_cluster_map")]
    pub assign_clusters: HashMap<String, HashMap<Option<String>, usize>>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct UnassignWarehouseNodesPlan {
    pub warehouse: String,
    #[serde(with = "vectorize_cluster_map")]
    pub unassign_clusters: HashMap<String, HashMap<Option<String>, usize>>,
}

mod vectorize_cluster_map {
    use std::collections::HashMap;

    use serde::ser::SerializeMap;
    use serde::ser::Serializer;

    pub fn serialize<S>(
        map: &HashMap<String, HashMap<Option<String>, usize>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut serialize_map = serializer.serialize_map(Some(map.len()))?;
        for (key, value) in map {
            serialize_map.serialize_key(&key)?;
            let vec = value
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect::<Vec<_>>();
            serialize_map.serialize_value(&vec)?;
        }

        serialize_map.end()
    }
}
