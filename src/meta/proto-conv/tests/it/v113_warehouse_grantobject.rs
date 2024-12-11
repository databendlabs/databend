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

use databend_common_meta_app as mt;
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
fn test_decode_v113_grant_object() -> anyhow::Result<()> {
    let grant_object_v113 = vec![66, 4, 10, 2, 119, 116, 160, 6, 113, 168, 6, 24];

    let want = || mt::principal::GrantObject::Warehouse("wt".to_string());

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), grant_object_v113.as_slice(), 113, want())?;

    Ok(())
}
