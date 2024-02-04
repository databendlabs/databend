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
use std::fmt::Debug;

use databend_common_expression::types::DataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_meta_app::schema::CreateOption;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateConnectionPlan {
    pub name: String,
    pub storage_type: String,
    pub storage_params: BTreeMap<String, String>,
    pub create_option: CreateOption,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropConnectionPlan {
    pub if_exists: bool,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DescConnectionPlan {
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShowConnectionsPlan {}

impl ShowConnectionsPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![
            DataField::new("name", DataType::String),
            DataField::new("storage_type", DataType::String),
            DataField::new("storage_params", DataType::String),
        ])
    }
}

impl DescConnectionPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![
            DataField::new("name", DataType::String),
            DataField::new("storage_type", DataType::String),
            DataField::new("storage_params", DataType::String),
        ])
    }
}
