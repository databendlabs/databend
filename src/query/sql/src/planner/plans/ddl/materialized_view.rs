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

use databend_common_meta_app::schema::SourceProgress;
use databend_common_meta_app::tenant::Tenant;

use crate::plans::CreateTablePlan;

#[derive(Clone, Debug)]
pub struct CreateMaterializedViewPlan {
    pub table_plan: CreateTablePlan,
    pub original_query: String,
    pub query: String,
    pub source_table_id: u64,
    pub source_progress: SourceProgress,
}

#[derive(Clone, Debug)]
pub struct ShowCreateMaterializedViewPlan {
    pub catalog: String,
    pub database: String,
    pub view_name: String,
    pub schema: databend_common_expression::DataSchemaRef,
}

impl ShowCreateMaterializedViewPlan {
    pub fn schema(&self) -> databend_common_expression::DataSchemaRef {
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropMaterializedViewPlan {
    pub if_exists: bool,
    pub tenant: Tenant,
    pub catalog: String,
    pub database: String,
    pub view_name: String,
}
