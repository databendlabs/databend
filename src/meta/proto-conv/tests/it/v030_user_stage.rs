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
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::storage::StorageWebhdfsConfig;
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
// The message bytes are built from the output of `test_user_stage_webhdfs_latest()`
#[test]
fn test_decode_v30_user_stage() -> anyhow::Result<()> {
    // Encoded data of version 30 of common_meta_app::principal::user_stage::StageInfo:
    // It is generated with common::test_pb_from_to().
    let stage_info_v30 = vec![
        10, 29, 119, 101, 98, 104, 100, 102, 115, 58, 47, 47, 112, 97, 116, 104, 47, 116, 111, 47,
        115, 116, 97, 103, 101, 47, 102, 105, 108, 101, 115, 16, 1, 26, 81, 10, 79, 42, 77, 10, 27,
        104, 116, 116, 112, 115, 58, 47, 47, 119, 101, 98, 104, 100, 102, 115, 46, 101, 120, 97,
        109, 112, 108, 101, 46, 99, 111, 109, 18, 20, 47, 112, 97, 116, 104, 47, 116, 111, 47, 115,
        116, 97, 103, 101, 47, 102, 105, 108, 101, 115, 26, 18, 60, 100, 101, 108, 101, 103, 97,
        116, 105, 111, 110, 95, 116, 111, 107, 101, 110, 62, 160, 6, 30, 168, 6, 24, 34, 30, 8, 1,
        16, 128, 8, 26, 1, 124, 34, 2, 47, 47, 40, 2, 58, 3, 114, 111, 119, 66, 3, 78, 97, 78, 160,
        6, 30, 168, 6, 24, 42, 10, 10, 3, 32, 197, 24, 16, 142, 8, 24, 1, 50, 4, 116, 101, 115,
        116, 160, 6, 30, 168, 6, 24,
    ];

    let want = || mt::principal::StageInfo {
        stage_name: "webhdfs://path/to/stage/files".to_string(),
        stage_type: mt::principal::StageType::External,
        stage_params: mt::principal::StageParams {
            storage: StorageParams::Webhdfs(StorageWebhdfsConfig {
                endpoint_url: "https://webhdfs.example.com".to_string(),
                root: "/path/to/stage/files".to_string(),
                delegation: "<delegation_token>".to_string(),
            }),
        },
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::SkipFileNum(3141),
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
        ..Default::default()
    };
    common::test_load_old(func_name!(), stage_info_v30.as_slice(), 30, want())?;
    common::test_pb_from_to(func_name!(), want())?;

    Ok(())
}
