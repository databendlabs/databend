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

use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::VirtualField;
use databend_storages_common_table_meta::meta::Location;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateVirtualColumnPlan {
    pub create_option: CreateOption,
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub virtual_columns: Vec<VirtualField>,
    pub auto_generated: bool,
}

impl CreateVirtualColumnPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterVirtualColumnPlan {
    pub if_exists: bool,
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub virtual_columns: Vec<VirtualField>,
    pub auto_generated: bool,
}

impl AlterVirtualColumnPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropVirtualColumnPlan {
    pub if_exists: bool,
    pub catalog: String,
    pub database: String,
    pub table: String,
}

impl DropVirtualColumnPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefreshVirtualColumnPlan {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub segment_locs: Option<Vec<Location>>,
}

impl RefreshVirtualColumnPlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }
}
