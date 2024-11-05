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
use databend_common_meta_app as mt;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::storage::StorageFsConfig;
use databend_common_meta_app::storage::StorageParams;
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
// The message bytes are built from the output of `test_user_stage_fs_latest()`
#[test]
fn test_decode_v25_user_stage() -> anyhow::Result<()> {
    let stage_info_v25 = vec![
        10, 17, 102, 115, 58, 47, 47, 100, 105, 114, 47, 116, 111, 47, 102, 105, 108, 101, 115, 26,
        25, 10, 23, 18, 21, 10, 13, 47, 100, 105, 114, 47, 116, 111, 47, 102, 105, 108, 101, 115,
        160, 6, 25, 168, 6, 24, 34, 37, 8, 1, 16, 128, 8, 26, 1, 124, 34, 2, 47, 47, 40, 2, 50, 1,
        92, 58, 3, 114, 111, 119, 66, 3, 78, 97, 78, 74, 2, 39, 39, 160, 6, 25, 168, 6, 24, 42, 9,
        10, 2, 48, 2, 16, 142, 8, 24, 1, 50, 4, 116, 101, 115, 116, 56, 100, 66, 29, 10, 8, 100,
        97, 116, 97, 98, 101, 110, 100, 18, 11, 100, 97, 116, 97, 98, 101, 110, 100, 46, 114, 115,
        160, 6, 25, 168, 6, 24, 160, 6, 25, 168, 6, 24,
    ];

    let want = || mt::principal::StageInfo {
        stage_name: "fs://dir/to/files".to_string(),
        stage_type: mt::principal::StageType::LegacyInternal,
        stage_params: mt::principal::StageParams {
            storage: StorageParams::Fs(StorageFsConfig {
                root: "/dir/to/files".to_string(),
            }),
        },
        is_temporary: false,
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::AbortNum(2),
            size_limit: 1038,
            max_files: 0,
            split_size: 0,
            purge: true,
            single: false,
            max_file_size: 0,
            disable_variant_check: false,
            return_failed_only: false,
            detailed_output: false,
        },
        comment: "test".to_string(),
        number_of_files: 100,
        creator: Some(UserIdentity {
            username: "databend".to_string(),
            hostname: "databend.rs".to_string(),
        }),
        created_on: DateTime::<Utc>::default(),
    };
    common::test_load_old(func_name!(), stage_info_v25.as_slice(), 25, want())?;
    common::test_pb_from_to(func_name!(), want())?;

    Ok(())
}
