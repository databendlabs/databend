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

use databend_common_expression::DataSchemaRef;
use databend_common_expression::TableSchemaRef;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::MVDefinition;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_meta_app::tenant::Tenant;

use crate::plans::TableOptions;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateMaterializedViewPlan {
    pub create_option: CreateOption,
    pub tenant: Tenant,
    pub catalog: String,
    pub database: String,
    pub view_name: String,
    pub schema: TableSchemaRef,
    pub options: TableOptions,
    pub mv_definition: MVDefinition,
    /// Source option update to commit atomically with MV publication, if needed.
    pub source_table_option: Option<UpsertTableOptionReq>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropMaterializedViewPlan {
    pub if_exists: bool,
    pub tenant: Tenant,
    pub catalog: String,
    pub database: String,
    pub view_name: String,
}

#[derive(Clone, Debug)]
pub struct DescribeMaterializedViewPlan {
    pub catalog: String,
    pub database: String,
    pub view_name: String,
    pub schema: DataSchemaRef,
}

impl DescribeMaterializedViewPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefreshMaterializedViewPlan {
    pub tenant: Tenant,
    pub catalog: String,
    pub database: String,
    pub view_name: String,
}
