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

use chrono::DateTime;
use databend_common_meta_app::principal as mt;
use databend_common_meta_app::principal::ScheduleOptions;
use databend_common_meta_app::principal::ScheduleType;
use databend_common_meta_app::principal::WarehouseOptions;
use fastrace::func_name;
use maplit::btreemap;

use crate::common;

#[test]
fn test_decode_v136_add_task() -> anyhow::Result<()> {
    let task_v136 = vec![
        8, 11, 18, 6, 116, 97, 115, 107, 95, 99, 34, 16, 83, 69, 76, 69, 67, 84, 32, 42, 32, 70,
        82, 79, 77, 32, 116, 49, 42, 7, 99, 111, 109, 109, 101, 110, 116, 50, 6, 112, 117, 98, 108,
        105, 99, 58, 22, 8, 11, 18, 11, 51, 48, 32, 49, 50, 32, 42, 32, 42, 32, 42, 26, 3, 85, 84,
        67, 40, 11, 66, 17, 10, 11, 119, 97, 114, 101, 104, 111, 117, 115, 101, 95, 97, 18, 2, 49,
        48, 74, 23, 49, 57, 55, 48, 45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48, 48, 58, 49, 48, 32,
        85, 84, 67, 80, 10, 114, 23, 49, 57, 55, 48, 45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48,
        48, 58, 49, 49, 32, 85, 84, 67, 122, 23, 49, 57, 55, 48, 45, 48, 49, 45, 48, 49, 32, 48,
        48, 58, 48, 48, 58, 49, 50, 32, 85, 84, 67, 130, 1, 23, 49, 57, 55, 48, 45, 48, 49, 45, 48,
        49, 32, 48, 48, 58, 48, 48, 58, 49, 51, 32, 85, 84, 67, 138, 1, 6, 116, 97, 115, 107, 95,
        97, 138, 1, 6, 116, 97, 115, 107, 95, 98, 146, 1, 6, 99, 49, 32, 62, 32, 49, 154, 1, 6, 10,
        1, 97, 18, 1, 98, 170, 1, 2, 109, 101, 160, 6, 136, 1, 168, 6, 24,
    ];

    let want = || mt::Task {
        task_id: 11,
        task_name: "task_c".to_string(),
        query_text: "SELECT * FROM t1".to_string(),
        when_condition: Some("c1 > 1".to_string()),
        after: vec!["task_a".to_string(), "task_b".to_string()],
        comment: Some("comment".to_string()),
        owner: mt::BUILTIN_ROLE_PUBLIC.to_string(),
        owner_user: "me".to_string(),
        schedule_options: Some(ScheduleOptions {
            interval: Some(11),
            cron: Some("30 12 * * *".to_string()),
            time_zone: Some("UTC".to_string()),
            schedule_type: ScheduleType::IntervalType,
            milliseconds_interval: Some(11),
        }),
        warehouse_options: Some(WarehouseOptions {
            warehouse: Some("warehouse_a".to_string()),
            using_warehouse_size: Some("10".to_string()),
        }),
        next_scheduled_at: Some(DateTime::from_timestamp(10, 0).unwrap()),
        suspend_task_after_num_failures: Some(10),
        error_integration: None,
        status: mt::Status::Suspended,
        created_at: DateTime::from_timestamp(11, 0).unwrap(),
        updated_at: DateTime::from_timestamp(12, 0).unwrap(),
        last_suspended_at: Some(DateTime::from_timestamp(13, 0).unwrap()),
        session_params: btreemap! { s("a") => s("b") },
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), task_v136.as_slice(), 136, want())?;

    Ok(())
}

#[test]
fn test_decode_v136_task_message() -> anyhow::Result<()> {
    let want_task = || mt::Task {
        task_id: 11,
        task_name: "task_c".to_string(),
        query_text: "SELECT * FROM t1".to_string(),
        when_condition: Some("c1 > 1".to_string()),
        after: vec!["task_a".to_string(), "task_b".to_string()],
        comment: Some("comment".to_string()),
        owner: mt::BUILTIN_ROLE_PUBLIC.to_string(),
        owner_user: "me".to_string(),
        schedule_options: Some(ScheduleOptions {
            interval: Some(11),
            cron: Some("30 12 * * *".to_string()),
            time_zone: Some("UTC".to_string()),
            schedule_type: ScheduleType::IntervalType,
            milliseconds_interval: Some(11),
        }),
        warehouse_options: Some(WarehouseOptions {
            warehouse: Some("warehouse_a".to_string()),
            using_warehouse_size: Some("10".to_string()),
        }),
        next_scheduled_at: Some(DateTime::from_timestamp(10, 0).unwrap()),
        suspend_task_after_num_failures: Some(10),
        error_integration: None,
        status: mt::Status::Suspended,
        created_at: DateTime::from_timestamp(11, 0).unwrap(),
        updated_at: DateTime::from_timestamp(12, 0).unwrap(),
        last_suspended_at: Some(DateTime::from_timestamp(13, 0).unwrap()),
        session_params: btreemap! { s("a") => s("b") },
    };

    {
        let task_message_execute_v136 = vec![
            10, 239, 1, 8, 11, 18, 6, 116, 97, 115, 107, 95, 99, 34, 16, 83, 69, 76, 69, 67, 84,
            32, 42, 32, 70, 82, 79, 77, 32, 116, 49, 42, 7, 99, 111, 109, 109, 101, 110, 116, 50,
            6, 112, 117, 98, 108, 105, 99, 58, 22, 8, 11, 18, 11, 51, 48, 32, 49, 50, 32, 42, 32,
            42, 32, 42, 26, 3, 85, 84, 67, 40, 11, 66, 17, 10, 11, 119, 97, 114, 101, 104, 111,
            117, 115, 101, 95, 97, 18, 2, 49, 48, 74, 23, 49, 57, 55, 48, 45, 48, 49, 45, 48, 49,
            32, 48, 48, 58, 48, 48, 58, 49, 48, 32, 85, 84, 67, 80, 10, 114, 23, 49, 57, 55, 48,
            45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48, 48, 58, 49, 49, 32, 85, 84, 67, 122, 23,
            49, 57, 55, 48, 45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48, 48, 58, 49, 50, 32, 85, 84,
            67, 130, 1, 23, 49, 57, 55, 48, 45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48, 48, 58, 49,
            51, 32, 85, 84, 67, 138, 1, 6, 116, 97, 115, 107, 95, 97, 138, 1, 6, 116, 97, 115, 107,
            95, 98, 146, 1, 6, 99, 49, 32, 62, 32, 49, 154, 1, 6, 10, 1, 97, 18, 1, 98, 170, 1, 2,
            109, 101, 160, 6, 136, 1, 168, 6, 24, 160, 6, 136, 1, 168, 6, 24,
        ];
        let want_execute = || mt::TaskMessage::ExecuteTask(want_task());

        common::test_pb_from_to(func_name!(), want_execute())?;
        common::test_load_old(
            func_name!(),
            task_message_execute_v136.as_slice(),
            136,
            want_execute(),
        )?;
    }
    {
        let task_message_schedule_v136 = vec![
            18, 239, 1, 8, 11, 18, 6, 116, 97, 115, 107, 95, 99, 34, 16, 83, 69, 76, 69, 67, 84,
            32, 42, 32, 70, 82, 79, 77, 32, 116, 49, 42, 7, 99, 111, 109, 109, 101, 110, 116, 50,
            6, 112, 117, 98, 108, 105, 99, 58, 22, 8, 11, 18, 11, 51, 48, 32, 49, 50, 32, 42, 32,
            42, 32, 42, 26, 3, 85, 84, 67, 40, 11, 66, 17, 10, 11, 119, 97, 114, 101, 104, 111,
            117, 115, 101, 95, 97, 18, 2, 49, 48, 74, 23, 49, 57, 55, 48, 45, 48, 49, 45, 48, 49,
            32, 48, 48, 58, 48, 48, 58, 49, 48, 32, 85, 84, 67, 80, 10, 114, 23, 49, 57, 55, 48,
            45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48, 48, 58, 49, 49, 32, 85, 84, 67, 122, 23,
            49, 57, 55, 48, 45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48, 48, 58, 49, 50, 32, 85, 84,
            67, 130, 1, 23, 49, 57, 55, 48, 45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48, 48, 58, 49,
            51, 32, 85, 84, 67, 138, 1, 6, 116, 97, 115, 107, 95, 97, 138, 1, 6, 116, 97, 115, 107,
            95, 98, 146, 1, 6, 99, 49, 32, 62, 32, 49, 154, 1, 6, 10, 1, 97, 18, 1, 98, 170, 1, 2,
            109, 101, 160, 6, 136, 1, 168, 6, 24, 160, 6, 136, 1, 168, 6, 24,
        ];
        let want_schedule = || mt::TaskMessage::ScheduleTask(want_task());

        common::test_pb_from_to(func_name!(), want_schedule())?;
        common::test_load_old(
            func_name!(),
            task_message_schedule_v136.as_slice(),
            136,
            want_schedule(),
        )?;
    }
    {
        let task_message_after_v136 = vec![
            34, 239, 1, 8, 11, 18, 6, 116, 97, 115, 107, 95, 99, 34, 16, 83, 69, 76, 69, 67, 84,
            32, 42, 32, 70, 82, 79, 77, 32, 116, 49, 42, 7, 99, 111, 109, 109, 101, 110, 116, 50,
            6, 112, 117, 98, 108, 105, 99, 58, 22, 8, 11, 18, 11, 51, 48, 32, 49, 50, 32, 42, 32,
            42, 32, 42, 26, 3, 85, 84, 67, 40, 11, 66, 17, 10, 11, 119, 97, 114, 101, 104, 111,
            117, 115, 101, 95, 97, 18, 2, 49, 48, 74, 23, 49, 57, 55, 48, 45, 48, 49, 45, 48, 49,
            32, 48, 48, 58, 48, 48, 58, 49, 48, 32, 85, 84, 67, 80, 10, 114, 23, 49, 57, 55, 48,
            45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48, 48, 58, 49, 49, 32, 85, 84, 67, 122, 23,
            49, 57, 55, 48, 45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48, 48, 58, 49, 50, 32, 85, 84,
            67, 130, 1, 23, 49, 57, 55, 48, 45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48, 48, 58, 49,
            51, 32, 85, 84, 67, 138, 1, 6, 116, 97, 115, 107, 95, 97, 138, 1, 6, 116, 97, 115, 107,
            95, 98, 146, 1, 6, 99, 49, 32, 62, 32, 49, 154, 1, 6, 10, 1, 97, 18, 1, 98, 170, 1, 2,
            109, 101, 160, 6, 136, 1, 168, 6, 24, 160, 6, 136, 1, 168, 6, 24,
        ];
        let want_after = || mt::TaskMessage::AfterTask(want_task());

        common::test_pb_from_to(func_name!(), want_after())?;
        common::test_load_old(
            func_name!(),
            task_message_after_v136.as_slice(),
            136,
            want_after(),
        )?;
    }
    {
        let task_message_delete_v136 =
            vec![26, 6, 116, 97, 115, 107, 95, 99, 160, 6, 136, 1, 168, 6, 24];
        let want_delete = || {
            let task = want_task();
            mt::TaskMessage::DeleteTask(task.task_name, None)
        };

        common::test_pb_from_to(func_name!(), want_delete())?;
        common::test_load_old(
            func_name!(),
            task_message_delete_v136.as_slice(),
            136,
            want_delete(),
        )?;
    }

    Ok(())
}

fn s(ss: impl ToString) -> String {
    ss.to_string()
}
