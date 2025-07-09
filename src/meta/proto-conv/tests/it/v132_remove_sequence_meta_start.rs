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
use chrono::Utc;
use databend_common_meta_app::schema as mt;
use fastrace::func_name;

use crate::common;

#[test]
fn test_decode_v132_remove_sequence_meta_step() -> anyhow::Result<()> {
    let sequence_meta_v132 = vec![
        10, 23, 49, 57, 55, 48, 45, 48, 49, 45, 48, 49, 32, 48, 50, 58, 53, 49, 58, 48, 55, 32, 85,
        84, 67, 18, 23, 49, 57, 55, 48, 45, 48, 49, 45, 48, 49, 32, 48, 50, 58, 53, 49, 58, 48, 55,
        32, 85, 84, 67, 26, 3, 115, 101, 113, 40, 5, 48, 10, 160, 6, 132, 1, 168, 6, 24,
    ];

    let want = || mt::SequenceMeta {
        create_on: DateTime::<Utc>::from_timestamp(10267, 0).unwrap(),
        update_on: DateTime::<Utc>::from_timestamp(10267, 0).unwrap(),
        comment: Some("seq".to_string()),
        step: 5,
        current: 10,
        storage_version: 0,
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), sequence_meta_v132.as_slice(), 132, want())?;

    Ok(())
}
