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

use databend_common_arrow::arrow::datatypes::Schema as ArrowSchema;
use databend_common_arrow::parquet::metadata::SchemaDescriptor;
use databend_common_expression::TableSchema;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::schema::TableInfo;
use databend_common_storage::StageFileInfo;
use databend_common_storage::StageFilesInfo;

use crate::plan::datasource::datasource_info::parquet_read_options::ParquetReadOptions;
use crate::table::Parquet2TableColumnStatisticsProvider;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct Parquet2TableInfo {
    pub read_options: ParquetReadOptions,
    pub stage_info: StageInfo,
    pub files_info: StageFilesInfo,

    pub table_info: TableInfo,
    pub arrow_schema: ArrowSchema,
    pub schema_descr: SchemaDescriptor,
    pub files_to_read: Option<Vec<StageFileInfo>>,
    pub schema_from: String,
    pub compression_ratio: f64,

    pub column_statistics_provider: Parquet2TableColumnStatisticsProvider,
}

impl Parquet2TableInfo {
    pub fn schema(&self) -> Arc<TableSchema> {
        self.table_info.schema()
    }

    pub fn desc(&self) -> String {
        self.stage_info.stage_name.clone()
    }
}
