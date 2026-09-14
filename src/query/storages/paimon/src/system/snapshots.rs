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
use databend_common_expression::types::TimestampType;
use paimon::SnapshotManager;

use super::unsigned_millis_to_micros;
use crate::error::map_paimon_result;

pub async fn read(table: &paimon::Table) -> Result<DataBlock> {
    let manager = SnapshotManager::new(table.file_io().clone(), table.location().to_string());
    let snapshots = map_paimon_result(manager.list_all().await)?;
    let commit_times = snapshots
        .iter()
        .map(|snapshot| unsigned_millis_to_micros(snapshot.time_millis(), "snapshot commit"))
        .collect::<Result<Vec<_>>>()?;

    Ok(DataBlock::new_from_columns(vec![
        Int64Type::from_data(snapshots.iter().map(|v| v.id()).collect()),
        Int64Type::from_data(snapshots.iter().map(|v| v.schema_id()).collect()),
        StringType::from_data(snapshots.iter().map(|v| v.commit_user()).collect()),
        Int64Type::from_data(snapshots.iter().map(|v| v.commit_identifier()).collect()),
        StringType::from_data(
            snapshots
                .iter()
                .map(|v| v.commit_kind().to_string())
                .collect(),
        ),
        TimestampType::from_data(commit_times),
        StringType::from_data(snapshots.iter().map(|v| v.base_manifest_list()).collect()),
        StringType::from_data(snapshots.iter().map(|v| v.delta_manifest_list()).collect()),
        StringType::from_opt_data(
            snapshots
                .iter()
                .map(|v| v.changelog_manifest_list())
                .collect(),
        ),
        Int64Type::from_opt_data(snapshots.iter().map(|v| v.total_record_count()).collect()),
        Int64Type::from_opt_data(snapshots.iter().map(|v| v.delta_record_count()).collect()),
        Int64Type::from_opt_data(
            snapshots
                .iter()
                .map(|v| v.changelog_record_count())
                .collect(),
        ),
        Int64Type::from_opt_data(snapshots.iter().map(|v| v.watermark()).collect()),
        Int64Type::from_opt_data(snapshots.iter().map(|v| v.next_row_id()).collect()),
    ]))
}
