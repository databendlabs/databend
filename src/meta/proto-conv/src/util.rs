// Copyright 2021 Datafuse Labs.
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

use crate::Incompatible;

/// Describes metadata changes.
///
/// This is a list of every `VER` and the corresponding change it introduces.
///
/// ## For developers
///
/// Every time fields are added/removed into/from data types in this crate:
/// - Add a new line to this list to describe what changed.
/// - Add a test case to ensure protobuf message serialized by this this version can be loaded,
///   similar to: test_user_stage_fs_v6() in tests/it/user_stage.rs;
///
/// `VER` is the current metadata version and is automatically set to the last version.
/// `MIN_COMPATIBLE_VER` is the oldest compatible version.
const META_CHANGE_LOG: &[(u64, &str)] = &[
    //
    (1, "----------: Initial"),
    (2, "2022-07-13: Add: share.proto"),
    (3, "2022-07-29: Add: user.proto/UserOption::default_role"),
    (4, "2022-08-22: Add: config.proto/GcsStorageConfig"),
    (
        5,
        "2022-08-25: Add: ShareMeta::share_from_db_ids; DatabaseMeta::from_share",
    ),
    (6, "2022-09-08: Add: users.proto/CopyOptions::purge"),
    (7, "2022-09-09: Add: table.proto/TableCopiedFileInfo type"),
];

pub const VER: u64 = META_CHANGE_LOG.last().unwrap().0;
pub const MIN_COMPATIBLE_VER: u64 = 1;

pub fn check_ver(msg_ver: u64, msg_min_compatible: u64) -> Result<(), Incompatible> {
    if VER < msg_min_compatible {
        return Err(Incompatible {
            reason: format!(
                "executable ver={} is smaller than the message min compatible ver: {}",
                VER, msg_min_compatible
            ),
        });
    }
    if msg_ver < MIN_COMPATIBLE_VER {
        return Err(Incompatible {
            reason: format!(
                "message ver={} is smaller than executable min compatible ver: {}",
                msg_ver, MIN_COMPATIBLE_VER
            ),
        });
    }
    Ok(())
}

pub fn missing(reason: impl ToString) -> impl FnOnce() -> Incompatible {
    let s = reason.to_string();
    move || Incompatible { reason: s }
}
