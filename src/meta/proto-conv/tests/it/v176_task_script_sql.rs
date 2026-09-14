// Copyright 2026 Datafuse Labs.
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
use fastrace::func_name;
use maplit::btreemap;

use crate::common;

#[test]
fn test_decode_v176_task_script_sql() -> anyhow::Result<()> {
    let task_v176 = vec![
        8, 11, 18, 6, 116, 97, 115, 107, 95, 99, 34, 60, 66, 69, 71, 73, 78, 10, 73, 78, 83, 69,
        82, 84, 32, 73, 78, 84, 79, 32, 116, 32, 86, 65, 76, 85, 69, 83, 40, 49, 41, 59, 10, 73,
        78, 83, 69, 82, 84, 32, 73, 78, 84, 79, 32, 116, 32, 86, 65, 76, 85, 69, 83, 40, 50, 41,
        59, 10, 69, 78, 68, 59, 50, 6, 112, 117, 98, 108, 105, 99, 114, 23, 49, 57, 55, 48, 45, 48,
        49, 45, 48, 49, 32, 48, 48, 58, 48, 48, 58, 49, 49, 32, 85, 84, 67, 122, 23, 49, 57, 55,
        48, 45, 48, 49, 45, 48, 49, 32, 48, 48, 58, 48, 48, 58, 49, 50, 32, 85, 84, 67, 170, 1, 2,
        109, 101, 178, 1, 23, 73, 78, 83, 69, 82, 84, 32, 73, 78, 84, 79, 32, 116, 32, 86, 65, 76,
        85, 69, 83, 40, 49, 41, 178, 1, 23, 73, 78, 83, 69, 82, 84, 32, 73, 78, 84, 79, 32, 116,
        32, 86, 65, 76, 85, 69, 83, 40, 50, 41, 160, 6, 176, 1, 168, 6, 24,
    ];

    let want = mt::Task {
        task_id: 11,
        task_name: "task_c".to_string(),
        task_sql: mt::TaskSql::Script(vec![
            "INSERT INTO t VALUES(1)".to_string(),
            "INSERT INTO t VALUES(2)".to_string(),
        ]),
        when_condition: None,
        after: vec![],
        comment: None,
        owner: mt::BUILTIN_ROLE_PUBLIC.to_string(),
        owner_user: "me".to_string(),
        schedule_options: None,
        warehouse_options: None,
        next_scheduled_at: None,
        suspend_task_after_num_failures: None,
        error_integration: None,
        status: mt::Status::Suspended,
        created_at: DateTime::from_timestamp(11, 0).unwrap(),
        updated_at: DateTime::from_timestamp(12, 0).unwrap(),
        last_suspended_at: None,
        session_params: btreemap! {},
    };

    common::test_pb_from_to(func_name!(), want.clone())?;
    common::test_load_old(func_name!(), task_v176.as_slice(), 176, want)?;

    Ok(())
}
