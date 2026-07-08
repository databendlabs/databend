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

use databend_common_meta_app::principal as mt;
use fastrace::func_name;

use crate::common;

#[test]
fn test_decode_v179_task_delete_task_id() -> anyhow::Result<()> {
    let task_message_delete_v179 = vec![
        42, 29, 10, 6, 116, 97, 115, 107, 95, 99, 18, 17, 10, 11, 119, 97, 114, 101, 104, 111, 117,
        115, 101, 95, 97, 18, 2, 49, 48, 24, 11, 160, 6, 179, 1, 168, 6, 24,
    ];

    let want = mt::TaskMessage::DeleteTask(
        "task_c".to_string(),
        Some(mt::WarehouseOptions {
            warehouse: Some("warehouse_a".to_string()),
            using_warehouse_size: Some("10".to_string()),
        }),
        Some(11),
    );

    common::test_pb_from_to(func_name!(), want.clone())?;
    common::test_load_old(func_name!(), task_message_delete_v179.as_slice(), 179, want)?;

    Ok(())
}
