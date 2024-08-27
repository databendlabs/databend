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

use chrono::TimeZone;
use chrono::Utc;
use databend_common_expression::types::DataType;
use databend_common_meta_app::principal as mt;
use fastrace::func_name;

use crate::common;

#[test]
fn v108_procedure_meta() -> anyhow::Result<()> {
    let procedure_meta_v108: Vec<u8> = vec![
        34, 9, 146, 2, 0, 160, 6, 108, 168, 6, 24, 82, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50, 56,
        32, 49, 50, 58, 48, 48, 58, 48, 57, 32, 85, 84, 67, 90, 23, 50, 48, 49, 52, 45, 49, 49, 45,
        50, 57, 32, 49, 50, 58, 48, 48, 58, 48, 57, 32, 85, 84, 67, 98, 7, 102, 111, 111, 32, 98,
        97, 114, 114, 3, 83, 81, 76, 160, 6, 108, 168, 6, 24,
    ];

    let want = || mt::ProcedureMeta {
        return_types: vec![DataType::String],
        created_on: Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap(),
        updated_on: Utc.with_ymd_and_hms(2014, 11, 29, 12, 0, 9).unwrap(),
        script: "".to_string(),
        comment: "foo bar".to_string(),
        procedure_language: "SQL".to_string(),
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), procedure_meta_v108.as_slice(), 108, want())
}
