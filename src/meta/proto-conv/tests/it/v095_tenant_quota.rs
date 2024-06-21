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

use databend_common_meta_app::tenant::TenantQuota;
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
#[test]
fn test_decode_v94_tenant_quota() -> anyhow::Result<()> {
    let tenant_quota_v94 = vec![8, 1, 16, 2, 24, 3, 32, 4, 40, 5, 160, 6, 94, 168, 6, 24];
    let want = || TenantQuota {
        max_databases: 1,
        max_tables_per_database: 2,
        max_stages: 3,
        max_files_per_stage: 4,
        max_users: 5,
    };
    common::test_load_old(func_name!(), tenant_quota_v94.as_slice(), 94, want())?;
    common::test_pb_from_to(func_name!(), want())?;
    Ok(())
}
