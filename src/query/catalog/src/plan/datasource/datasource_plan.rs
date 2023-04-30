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

use common_expression::TableSchemaRef;

use crate::plan::datasource::datasource_info::DataSourceInfo;
use crate::plan::PartStatistics;
use crate::plan::Partitions;
use crate::plan::PushDownInfo;
use crate::table_args::TableArgs;

// TODO: Delete the scan plan field, but it depends on plan_parser:L394
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct DataSourcePlan {
    // TODO catalog id is better
    pub catalog: String,
    pub source_info: DataSourceInfo,

    pub output_schema: TableSchemaRef,

    pub parts: Partitions,
    pub statistics: PartStatistics,
    pub description: String,

    pub tbl_args: Option<TableArgs>,
    pub push_downs: Option<PushDownInfo>,
    pub query_internal_columns: bool,
}

impl DataSourcePlan {
    #[inline]
    pub fn schema(&self) -> TableSchemaRef {
        self.output_schema.clone()
    }
}
