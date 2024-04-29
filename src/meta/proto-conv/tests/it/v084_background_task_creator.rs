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
use databend_common_meta_app::background::task_creator::BackgroundTaskCreator;
use databend_common_meta_app::background::BackgroundJobParams;
use databend_common_meta_app::background::BackgroundJobState;
use databend_common_meta_app::background::BackgroundJobStatus;
use databend_common_meta_app::background::BackgroundJobType;
use databend_common_meta_app::background::BackgroundTaskState;
use databend_common_meta_app::background::BackgroundTaskType;
use databend_common_meta_app::background::CompactionStats;
use databend_common_meta_app::background::ManualTriggerParams;
use databend_common_meta_app::background::VacuumStats;
use databend_common_meta_app::schema::TableStatistics;
use minitrace::func_name;

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
// The message bytes are built from the output of `test_pb_from_to()`
#[test]
fn test_decode_v48_background_task_case_2() -> anyhow::Result<()> {
    let bytes = vec![
        26, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50, 56, 32, 49, 50, 58, 48, 48, 58, 48, 57, 32, 85,
        84, 67, 32, 1, 50, 24, 100, 97, 116, 97, 98, 101, 110, 100, 32, 98, 97, 99, 107, 103, 114,
        111, 117, 110, 100, 32, 116, 97, 115, 107, 58, 60, 8, 21, 16, 92, 26, 21, 8, 144, 78, 16,
        160, 156, 1, 24, 30, 32, 40, 40, 10, 48, 11, 160, 6, 48, 168, 6, 24, 34, 20, 8, 232, 7, 16,
        208, 15, 24, 3, 32, 4, 40, 5, 48, 6, 160, 6, 48, 168, 6, 24, 45, 0, 0, 200, 66, 160, 6, 48,
        168, 6, 24, 66, 6, 160, 6, 48, 168, 6, 24, 74, 45, 10, 4, 49, 50, 51, 49, 18, 6, 160, 6,
        48, 168, 6, 24, 26, 23, 49, 57, 55, 48, 45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48, 48, 58,
        48, 48, 32, 85, 84, 67, 160, 6, 48, 168, 6, 24, 210, 5, 28, 10, 5, 116, 101, 115, 116, 49,
        18, 13, 99, 111, 109, 112, 97, 99, 116, 111, 114, 95, 106, 111, 98, 160, 6, 48, 168, 6, 24,
        218, 5, 23, 49, 57, 55, 48, 45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48, 48, 58, 48, 48, 32,
        85, 84, 67, 160, 6, 48, 168, 6, 24,
    ];

    let want = || databend_common_meta_app::background::BackgroundTaskInfo {
        last_updated: Some(Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap()),
        task_type: BackgroundTaskType::VACUUM,
        task_state: BackgroundTaskState::STARTED,
        message: "databend background task".to_string(),
        compaction_task_stats: Some(CompactionStats {
            db_id: 21,
            table_id: 92,
            before_compaction_stats: Some(TableStatistics {
                number_of_rows: 10000,
                data_bytes: 20000,
                compressed_data_bytes: 30,
                index_data_bytes: 40,
                number_of_segments: Some(10),
                number_of_blocks: Some(11),
            }),
            after_compaction_stats: Some(TableStatistics {
                number_of_rows: 1000,
                data_bytes: 2000,
                compressed_data_bytes: 3,
                index_data_bytes: 4,
                number_of_segments: Some(5),
                number_of_blocks: Some(6),
            }),
            total_compaction_time: Some(Duration::from_secs(100)),
        }),
        vacuum_stats: Some(VacuumStats {}),
        manual_trigger: Some(ManualTriggerParams {
            id: "1231".to_string(),
            trigger: Default::default(),

            triggered_at: Default::default(),
        }),
        creator: Some(BackgroundTaskCreator {
            tenant: "test1".to_string(),
            name: "compactor_job".to_string(),
        }),

        created_at: Default::default(),
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), bytes.as_slice(), 48, want())
}

#[test]
fn test_decode_v48_background_job() -> anyhow::Result<()> {
    let bytes = vec![
        10, 78, 8, 1, 16, 100, 34, 19, 65, 109, 101, 114, 105, 99, 97, 47, 76, 111, 115, 95, 65,
        110, 103, 101, 108, 101, 115, 42, 45, 10, 4, 49, 50, 51, 49, 18, 6, 160, 6, 48, 168, 6, 24,
        26, 23, 49, 57, 55, 48, 45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48, 48, 58, 48, 48, 32, 85,
        84, 67, 160, 6, 48, 168, 6, 24, 160, 6, 48, 168, 6, 24, 18, 37, 34, 4, 116, 101, 115, 116,
        42, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50, 56, 32, 49, 50, 58, 48, 48, 58, 48, 57, 32, 85,
        84, 67, 160, 6, 48, 168, 6, 24, 42, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50, 56, 32, 49, 50,
        58, 48, 48, 58, 48, 57, 32, 85, 84, 67, 218, 5, 23, 49, 57, 55, 48, 45, 48, 49, 45, 48, 49,
        32, 48, 48, 58, 48, 48, 58, 48, 48, 32, 85, 84, 67, 160, 6, 48, 168, 6, 24,
    ];

    let want = || databend_common_meta_app::background::BackgroundJobInfo {
        job_params: Some(BackgroundJobParams {
            job_type: BackgroundJobType::INTERVAL,
            scheduled_job_interval: std::time::Duration::from_secs(100),
            scheduled_job_cron: "".to_string(),
            scheduled_job_timezone: Some(chrono_tz::America::Los_Angeles),
            manual_trigger_params: Some(ManualTriggerParams {
                id: "1231".to_string(),
                trigger: Default::default(),
                triggered_at: Default::default(),
            }),
        }),
        last_updated: Some(Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap()),
        task_type: BackgroundTaskType::COMPACTION,
        message: "".to_string(),
        creator: None,
        created_at: Default::default(),
        job_status: Some(BackgroundJobStatus {
            job_state: BackgroundJobState::RUNNING,
            last_task_id: Some("test".to_string()),
            last_task_run_at: Some(Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap()),
            next_task_scheduled_time: None,
        }),
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), bytes.as_slice(), 48, want())
}
