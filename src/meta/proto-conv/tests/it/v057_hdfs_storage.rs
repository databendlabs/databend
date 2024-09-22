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
use databend_common_meta_app::storage::StorageHdfsConfig;
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
fn test_decode_v57_hdfs_storage() -> anyhow::Result<()> {
    let bytes = vec![
        10, 27, 111, 98, 115, 58, 47, 47, 98, 117, 99, 107, 101, 116, 47, 116, 111, 47, 115, 116,
        97, 103, 101, 47, 102, 105, 108, 101, 115, 16, 1, 26, 55, 10, 53, 66, 51, 10, 20, 47, 112,
        97, 116, 104, 47, 116, 111, 47, 115, 116, 97, 103, 101, 47, 102, 105, 108, 101, 115, 18,
        21, 104, 100, 102, 115, 58, 47, 47, 108, 111, 99, 97, 108, 104, 111, 115, 116, 58, 56, 48,
        50, 48, 160, 6, 57, 168, 6, 24, 42, 10, 10, 3, 32, 197, 24, 16, 142, 8, 24, 1, 50, 4, 116,
        101, 115, 116, 74, 10, 34, 8, 8, 2, 160, 6, 57, 168, 6, 24, 160, 6, 57, 168, 6, 24,
    ];

    let want = || mt::principal::StageInfo {
        stage_name: "obs://bucket/to/stage/files".to_string(),
        stage_type: mt::principal::StageType::External,
        stage_params: mt::principal::StageParams {
            storage: StorageParams::Hdfs(StorageHdfsConfig {
                root: "/path/to/stage/files".to_string(),
                name_node: "hdfs://localhost:8020".to_string(),
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
    common::test_load_old(func_name!(), bytes.as_slice(), 57, want())
}
