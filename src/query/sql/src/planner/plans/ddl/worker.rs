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
use std::sync::Arc;

use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::types::DataType;
use databend_common_meta_app::tenant::Tenant;

pub fn worker_schema() -> DataSchemaRef {
    Arc::new(DataSchema::new(vec![
        DataField::new("name", DataType::String),
        DataField::new("tags", DataType::String),
        DataField::new("options", DataType::String),
        DataField::new("created_at", DataType::String),
        DataField::new("updated_at", DataType::String),
    ]))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateWorkerPlan {
    pub if_not_exists: bool,
    pub tenant: Tenant,
    pub name: String,
    pub tags: BTreeMap<String, String>,
    pub options: BTreeMap<String, String>,
}

impl CreateWorkerPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![])
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterWorkerPlan {
    pub tenant: Tenant,
    pub name: String,
    pub set_tags: BTreeMap<String, String>,
    pub unset_tags: Vec<String>,
    pub set_options: BTreeMap<String, String>,
    pub unset_options: Vec<String>,
    pub suspend: bool,
    pub resume: bool,
}

impl AlterWorkerPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![])
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropWorkerPlan {
    pub if_exists: bool,
    pub tenant: Tenant,
    pub name: String,
}

impl DropWorkerPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![])
    }
}
