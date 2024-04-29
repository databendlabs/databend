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

use databend_common_ast::ast::InitializeMode;
use databend_common_ast::ast::RefreshMode;
use databend_common_ast::ast::TargetLag;
use databend_common_ast::ast::WarehouseOptions;
use databend_common_expression::TableSchemaRef;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;

use crate::plans::TableOptions;

#[derive(Clone, Debug)]
pub struct CreateDynamicTablePlan {
    pub create_option: CreateOption,
    pub tenant: Tenant,
    pub catalog: String,
    pub database: String,
    pub table: String,

    pub schema: TableSchemaRef,
    pub options: TableOptions,
    pub field_comments: Vec<String>,
    pub cluster_key: Option<String>,
    pub as_query: String,

    pub traget_lag: TargetLag,
    pub warehouse_opts: WarehouseOptions,
    pub refresh_mode: RefreshMode,
    pub initialize: InitializeMode,
}
