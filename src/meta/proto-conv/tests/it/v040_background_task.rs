// Copyright 2023 Datafuse Labs.
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

use std::time::Duration;
use chrono::TimeZone;
use chrono::Utc;
use common_meta_app::background::{BackgroundTaskState, BackgroundTaskType, CompactionStats, VacuumStats};
use common_meta_app::principal::UserIdentity;
use common_meta_app::schema::TableStatistics;

use crate::common;

// These bytes are built when a new version in introduced,
// and are kept for backward compatibility test.
//
// *************************************************************
// * These messages should never be updated,                   *
// * only be added when a new version is added,                *
// * or be removed when an old version is no longer supported. *
// *************************************************************
//
// The message bytes are built from the output of `test_build_pb_buf()`
#[test]
fn test_decode_v40_background_task() -> anyhow::Result<()> {
    let bytes = vec![26, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50, 56, 32, 49, 50, 58, 48, 48, 58, 48, 57, 32, 85, 84, 67, 40, 1, 218, 5, 23, 49, 57, 55, 48, 45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48, 48, 58, 48, 48, 32, 85, 84, 67, 160, 6, 40, 168, 6, 24];

    let want = || common_meta_app::background::BackgroundTaskInfo {
        task_id: Default::default(),
        last_updated: Some(Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap(),),
        task_type: BackgroundTaskType::COMPACTION,
        task_state: BackgroundTaskState::DONE,
        message: "".to_string(),
        compaction_task_stats: None,
        vacuum_stats: None,
        creator: None,

        created_at: Default::default(),
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), bytes.as_slice(), 40, want())
}

#[test]
fn test_decode_v40_background_task_case_2() -> anyhow::Result<()> {
    let bytes = vec![26, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50, 56, 32, 49, 50, 58, 48, 48, 58, 48, 57, 32, 85, 84, 67, 32, 1, 50, 24, 100, 97, 116, 97, 98, 101, 110, 100, 32, 98, 97, 99, 107, 103, 114, 111, 117, 110, 100, 32, 116, 97, 115, 107, 58, 52, 8, 21, 16, 91, 26, 17, 8, 144, 78, 16, 160, 156, 1, 24, 30, 32, 40, 160, 6, 40, 168, 6, 24, 34, 16, 8, 232, 7, 16, 208, 15, 24, 3, 32, 4, 160, 6, 40, 168, 6, 24, 45, 0, 0, 200, 66, 160, 6, 40, 168, 6, 24, 66, 6, 160, 6, 40, 168, 6, 24, 210, 5, 30, 10, 13, 100, 97, 116, 97, 98, 101, 110, 100, 95, 117, 115, 101, 114, 18, 7, 48, 46, 48, 46, 48, 46, 48, 160, 6, 40, 168, 6, 24, 218, 5, 23, 49, 57, 55, 48, 45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48, 48, 58, 48, 48, 32, 85, 84, 67, 160, 6, 40, 168, 6, 24];
    let want = || common_meta_app::background::BackgroundTaskInfo {
        task_id: Default::default(),
        last_updated: Some(Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap(),),
        task_type: BackgroundTaskType::VACUUM,
        task_state: BackgroundTaskState::STARTED,
        message: "databend background task".to_string(),
        compaction_task_stats: Some(CompactionStats{
            db_id: 21,
            table_id: 91,
            before_compaction_stats: Some(TableStatistics{
                number_of_rows: 10000,
                data_bytes: 20000,
                compressed_data_bytes: 30,
                index_data_bytes: 40,
            }),
            after_compaction_stats: Some(TableStatistics{
                number_of_rows: 1000,
                data_bytes: 2000,
                compressed_data_bytes: 3,
                index_data_bytes: 4,
            }),
            total_compaction_time: Some(Duration::from_secs(100)),
        }),
        vacuum_stats: Some(VacuumStats{}),
        creator: Some(UserIdentity::new("databend_user", "0.0.0.0")),

        created_at: Default::default(),
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), bytes.as_slice(), 40, want())
}

