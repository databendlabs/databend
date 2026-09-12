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

use databend_common_ast::ast::TargetLag;
use databend_common_ast::ast::WarehouseOptions;
use databend_common_expression::TableSchemaRef;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;

use crate::plans::CreateTablePlan;
use crate::plans::TableOptions;

#[derive(Clone, Debug)]
pub struct CreateDynamicTablePlan {
    pub table_plan: CreateTablePlan,
    pub as_query: String,
    pub target_lag: TargetLag,
    pub warehouse_opts: WarehouseOptions,
}

impl CreateDynamicTablePlan {
    pub fn create_option(&self) -> CreateOption {
        self.table_plan.create_option
    }

    pub fn tenant(&self) -> &Tenant {
        &self.table_plan.tenant
    }

    pub fn schema(&self) -> TableSchemaRef {
        self.table_plan.schema.clone()
    }

    pub fn options(&self) -> &TableOptions {
        &self.table_plan.options
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefreshDynamicTablePlan {
    pub tenant: Tenant,
    pub catalog: String,
    pub database: String,
    pub table: String,
}
