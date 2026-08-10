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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::StringType;
use paimon::table::referenced_files::collect_referenced_files_summary;

use crate::error::map_paimon_result;

pub async fn read(table: &paimon::Table) -> Result<DataBlock> {
    let schema = table.schema();
    let mut summaries = map_paimon_result(
        collect_referenced_files_summary(
            table.file_io(),
            table.location(),
            schema.partition_keys(),
            schema.fields(),
        )
        .await,
    )?;
    summaries.sort_by(|a, b| a.source.cmp(&b.source));

    Ok(DataBlock::new_from_columns(vec![
        StringType::from_data(summaries.iter().map(|s| s.source.as_str()).collect()),
        Int64Type::from_data(summaries.iter().map(|s| s.manifest_file_count).collect()),
        Int64Type::from_data(summaries.iter().map(|s| s.manifest_file_size).collect()),
        Int64Type::from_data(summaries.iter().map(|s| s.data_file_count).collect()),
        Int64Type::from_data(summaries.iter().map(|s| s.data_file_size).collect()),
        Int64Type::from_data(summaries.iter().map(|s| s.index_file_count).collect()),
        Int64Type::from_data(summaries.iter().map(|s| s.index_file_size).collect()),
    ]))
}
