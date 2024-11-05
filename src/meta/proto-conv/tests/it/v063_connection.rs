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

use std::collections::BTreeMap;

use databend_common_meta_app::principal::UserDefinedConnection;
use fastrace::func_name;

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
#[test]
fn test_decode_v62_connection() -> anyhow::Result<()> {
    let user_defined_connection_v62 = vec![
        10, 7, 109, 121, 95, 99, 111, 110, 110, 18, 2, 115, 51, 26, 10, 10, 3, 107, 101, 121, 18,
        3, 118, 97, 108, 160, 6, 63, 168, 6, 24,
    ];
    let want = || UserDefinedConnection {
        name: "my_conn".to_string(),
        storage_type: "s3".to_string(),
        storage_params: BTreeMap::from([("key".to_string(), "val".to_string())]),
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(
        func_name!(),
        user_defined_connection_v62.as_slice(),
        63,
        want(),
    )?;
    Ok(())
}
