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
use paimon::table::referenced_files::collect_physical_files_summary;

use crate::error::map_paimon_result;

pub async fn read(table: &paimon::Table) -> Result<DataBlock> {
    let summary =
        map_paimon_result(collect_physical_files_summary(table.file_io(), table.location()).await)?;
    Ok(DataBlock::new_from_columns(vec![
        Int64Type::from_data(vec![summary.manifest_file_count]),
        Int64Type::from_data(vec![summary.manifest_file_size]),
        Int64Type::from_data(vec![summary.data_file_count]),
        Int64Type::from_data(vec![summary.data_file_size]),
        Int64Type::from_data(vec![summary.index_file_count]),
        Int64Type::from_data(vec![summary.index_file_size]),
    ]))
}
