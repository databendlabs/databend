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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use paimon::table::BranchManager;

use crate::error::map_paimon_result;

pub async fn read(table: &paimon::Table) -> Result<DataBlock> {
    let manager = BranchManager::new(table.file_io().clone(), table.location().to_string());
    let branches = map_paimon_result(manager.list_all().await)?;
    let mut create_times = Vec::with_capacity(branches.len());
    for branch in &branches {
        let status = map_paimon_result(
            table
                .file_io()
                .get_status(&manager.branch_path(branch))
                .await,
        )?;
        let modified = status.last_modified.ok_or_else(|| {
            ErrorCode::ReadTableDataError(format!(
                "Paimon branch '{}' has no filesystem modification time",
                branch
            ))
        })?;
        create_times.push(modified.timestamp_micros());
    }
    Ok(DataBlock::new_from_columns(vec![
        StringType::from_data(branches),
        TimestampType::from_data(create_times),
    ]))
}
