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

use databend_common_expression::FieldIndex;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;

use crate::plan::InternalColumn;
use crate::plan::PartStatistics;
use crate::plan::Partitions;
use crate::plan::PushDownInfo;
use crate::plan::datasource::datasource_info::DataSourceInfo;
use crate::table_args::TableArgs;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct BlockMetaOptions {
    // for merge_into target build.
    pub reserve_block_index: bool,
    // Whether to update stream columns.
    pub update_stream_columns: bool,
    // used to query internal columns.
    pub query_internal_columns: bool,
}

impl BlockMetaOptions {
    pub fn set_reserve_block_index(mut self, reserve_block_index: bool) -> Self {
        self.reserve_block_index = reserve_block_index;
        self
    }

    pub fn set_update_stream_columns(mut self, update_stream_columns: bool) -> Self {
        self.update_stream_columns = update_stream_columns;
        self
    }

    pub fn set_query_internal_columns(mut self, query_internal_columns: bool) -> Self {
        self.query_internal_columns = query_internal_columns;
        self
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct DataSourcePlan {
    pub source_info: DataSourceInfo,

    pub output_schema: TableSchemaRef,

    pub parts: Partitions,
    pub statistics: PartStatistics,
    pub description: String,

    pub tbl_args: Option<TableArgs>,
    pub push_downs: Option<PushDownInfo>,
    pub internal_columns: Option<BTreeMap<FieldIndex, InternalColumn>>,
    pub base_block_ids: Option<Scalar>,
    pub block_meta_options: BlockMetaOptions,

    pub table_index: usize,
    pub scan_id: usize,
}

impl DataSourcePlan {
    #[inline]
    pub fn schema(&self) -> TableSchemaRef {
        self.output_schema.clone()
    }
}
