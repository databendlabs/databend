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
use databend_common_meta_app::storage::StorageCosConfig;
use databend_common_meta_app::storage::StorageObsConfig;
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
// The message bytes are built from the output of `test_pb_from_to()`
#[test]
fn test_decode_v51_obs_stage() -> anyhow::Result<()> {
    let bytes = vec![
        10, 27, 111, 98, 115, 58, 47, 47, 98, 117, 99, 107, 101, 116, 47, 116, 111, 47, 115, 116,
        97, 103, 101, 47, 102, 105, 108, 101, 115, 16, 1, 26, 99, 10, 97, 50, 95, 10, 23, 104, 116,
        116, 112, 115, 58, 47, 47, 111, 98, 115, 46, 101, 120, 97, 109, 112, 108, 101, 46, 99, 111,
        109, 18, 6, 98, 117, 99, 107, 101, 116, 26, 20, 47, 112, 97, 116, 104, 47, 116, 111, 47,
        115, 116, 97, 103, 101, 47, 102, 105, 108, 101, 115, 34, 13, 97, 99, 99, 101, 115, 115, 95,
        107, 101, 121, 95, 105, 100, 42, 17, 115, 101, 99, 114, 101, 116, 95, 97, 99, 99, 101, 115,
        115, 95, 107, 101, 121, 160, 6, 51, 168, 6, 24, 42, 10, 10, 3, 32, 197, 24, 16, 142, 8, 24,
        1, 50, 4, 116, 101, 115, 116, 74, 10, 34, 8, 8, 2, 160, 6, 51, 168, 6, 24, 160, 6, 51, 168,
        6, 24,
    ];

    let want = || mt::principal::StageInfo {
        stage_name: "obs://bucket/to/stage/files".to_string(),
        stage_type: mt::principal::StageType::External,
        stage_params: mt::principal::StageParams {
            storage: StorageParams::Obs(StorageObsConfig {
                endpoint_url: "https://obs.example.com".to_string(),
                root: "/path/to/stage/files".to_string(),
                access_key_id: "access_key_id".to_string(),
                secret_access_key: "secret_access_key".to_string(),
                bucket: "bucket".to_string(),
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

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), bytes.as_slice(), 51, want())
}

#[test]
fn test_decode_v51_cos_stage() -> anyhow::Result<()> {
    let bytes = vec![
        10, 27, 99, 111, 115, 58, 47, 47, 98, 117, 99, 107, 101, 116, 47, 116, 111, 47, 115, 116,
        97, 103, 101, 47, 102, 105, 108, 101, 115, 16, 1, 26, 88, 10, 86, 58, 84, 10, 23, 104, 116,
        116, 112, 115, 58, 47, 47, 99, 111, 115, 46, 101, 120, 97, 109, 112, 108, 101, 46, 99, 111,
        109, 18, 6, 98, 117, 99, 107, 101, 116, 26, 20, 47, 112, 97, 116, 104, 47, 116, 111, 47,
        115, 116, 97, 103, 101, 47, 102, 105, 108, 101, 115, 34, 9, 115, 101, 99, 114, 101, 116,
        95, 105, 100, 42, 10, 115, 101, 99, 114, 101, 116, 95, 107, 101, 121, 160, 6, 51, 168, 6,
        24, 42, 10, 10, 3, 32, 197, 24, 16, 142, 8, 24, 1, 50, 4, 116, 101, 115, 116, 74, 10, 34,
        8, 8, 2, 160, 6, 51, 168, 6, 24, 160, 6, 51, 168, 6, 24,
    ];

    let want = || mt::principal::StageInfo {
        stage_name: "cos://bucket/to/stage/files".to_string(),
        stage_type: mt::principal::StageType::External,
        stage_params: mt::principal::StageParams {
            storage: StorageParams::Cos(StorageCosConfig {
                endpoint_url: "https://cos.example.com".to_string(),
                root: "/path/to/stage/files".to_string(),
                secret_id: "secret_id".to_string(),
                secret_key: "secret_key".to_string(),
                bucket: "bucket".to_string(),
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

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), bytes.as_slice(), 51, want())
}
