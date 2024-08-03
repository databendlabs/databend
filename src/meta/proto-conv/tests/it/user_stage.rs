// Copyright 2022 Datafuse Labs.
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

//! Test StageInfo

use databend_common_meta_app as mt;
use fastrace::func_name;

use crate::common;
use crate::user_proto_conv::test_cos_stage_info;
use crate::user_proto_conv::test_fs_stage_info;
use crate::user_proto_conv::test_gcs_stage_info;
use crate::user_proto_conv::test_internal_stage_info_v17;
use crate::user_proto_conv::test_obs_stage_info;
use crate::user_proto_conv::test_oss_stage_info;
use crate::user_proto_conv::test_s3_stage_info;
use crate::user_proto_conv::test_stage_info_v18;
use crate::user_proto_conv::test_webhdfs_stage_info;

#[test]
fn test_user_stage_fs_latest() -> anyhow::Result<()> {
    common::test_pb_from_to("user_stage_fs", test_fs_stage_info())?;
    Ok(())
}

#[test]
fn test_user_stage_s3_latest() -> anyhow::Result<()> {
    common::test_pb_from_to("user_stage_s3", test_s3_stage_info())?;
    Ok(())
}

#[test]
fn test_user_stage_gcs_latest() -> anyhow::Result<()> {
    common::test_pb_from_to("user_stage_gcs", test_gcs_stage_info())?;
    Ok(())
}

#[test]
fn test_user_stage_oss_latest() -> anyhow::Result<()> {
    common::test_pb_from_to("user_stage_oss", test_oss_stage_info())?;
    Ok(())
}

#[test]
fn test_user_stage_webhdfs_latest() -> anyhow::Result<()> {
    common::test_pb_from_to("user_stage_webhdfs", test_webhdfs_stage_info())?;
    Ok(())
}

#[test]
fn test_user_stage_obs_latest() -> anyhow::Result<()> {
    common::test_pb_from_to("user_stage_obs", test_obs_stage_info())?;
    Ok(())
}

#[test]
fn test_user_stage_cos_latest() -> anyhow::Result<()> {
    common::test_pb_from_to("user_stage_cos", test_cos_stage_info())?;
    Ok(())
}

#[test]
fn test_user_stage_webhdfs_v30() -> anyhow::Result<()> {
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
            storage: mt::storage::StorageParams::Webhdfs(mt::storage::StorageWebhdfsConfig {
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

#[test]
fn test_user_stage_fs_v22() -> anyhow::Result<()> {
    // Encoded data of version 21 of user_stage_fs:
    // It is generated with common::test_pb_from_to.
    let user_stage_fs_v22 = vec![
        10, 17, 102, 115, 58, 47, 47, 100, 105, 114, 47, 116, 111, 47, 102, 105, 108, 101, 115, 26,
        25, 10, 23, 18, 21, 10, 13, 47, 100, 105, 114, 47, 116, 111, 47, 102, 105, 108, 101, 115,
        160, 6, 22, 168, 6, 1, 34, 37, 8, 1, 16, 128, 8, 26, 1, 124, 34, 2, 47, 47, 40, 2, 50, 1,
        92, 58, 3, 114, 111, 119, 66, 3, 78, 97, 78, 74, 2, 39, 39, 160, 6, 22, 168, 6, 1, 42, 10,
        10, 3, 32, 154, 5, 16, 142, 8, 24, 1, 50, 4, 116, 101, 115, 116, 160, 6, 22, 168, 6, 1,
    ];

    let want = mt::principal::StageInfo {
        stage_name: "fs://dir/to/files".to_string(),
        stage_type: mt::principal::StageType::LegacyInternal,
        stage_params: mt::principal::StageParams {
            storage: mt::storage::StorageParams::Fs(mt::storage::StorageFsConfig {
                root: "/dir/to/files".to_string(),
            }),
        },
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::SkipFileNum(666),
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
    common::test_load_old(func_name!(), user_stage_fs_v22.as_slice(), 22, want)?;

    Ok(())
}

#[test]
fn test_user_stage_fs_v21() -> anyhow::Result<()> {
    // Encoded data of version 21 of user_stage_fs:
    // It is generated with common::test_pb_from_to.
    let user_stage_fs_v21 = vec![
        10, 17, 102, 115, 58, 47, 47, 100, 105, 114, 47, 116, 111, 47, 102, 105, 108, 101, 115, 26,
        25, 10, 23, 18, 21, 10, 13, 47, 100, 105, 114, 47, 116, 111, 47, 102, 105, 108, 101, 115,
        160, 6, 21, 168, 6, 1, 34, 33, 8, 1, 16, 128, 8, 26, 1, 124, 34, 2, 47, 47, 40, 2, 50, 1,
        92, 58, 3, 114, 111, 119, 66, 3, 78, 97, 78, 160, 6, 21, 168, 6, 1, 42, 10, 10, 3, 32, 154,
        5, 16, 142, 8, 24, 1, 50, 4, 116, 101, 115, 116, 160, 6, 21, 168, 6, 1,
    ];

    let want = mt::principal::StageInfo {
        stage_name: "fs://dir/to/files".to_string(),
        stage_type: mt::principal::StageType::LegacyInternal,
        stage_params: mt::principal::StageParams {
            storage: mt::storage::StorageParams::Fs(mt::storage::StorageFsConfig {
                root: "/dir/to/files".to_string(),
            }),
        },
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::SkipFileNum(666),
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
    common::test_load_old(func_name!(), user_stage_fs_v21.as_slice(), 21, want)?;

    Ok(())
}

#[test]
fn test_user_stage_fs_v20() -> anyhow::Result<()> {
    // Encoded data of version 20 of user_stage_fs:
    // It is generated with common::test_pb_from_to.
    let user_stage_fs_v20 = vec![
        10, 17, 102, 115, 58, 47, 47, 100, 105, 114, 47, 116, 111, 47, 102, 105, 108, 101, 115, 26,
        25, 10, 23, 18, 21, 10, 13, 47, 100, 105, 114, 47, 116, 111, 47, 102, 105, 108, 101, 115,
        160, 6, 20, 168, 6, 1, 34, 28, 8, 1, 16, 128, 8, 26, 1, 124, 34, 2, 47, 47, 40, 2, 50, 1,
        92, 58, 3, 114, 111, 119, 160, 6, 20, 168, 6, 1, 42, 10, 10, 3, 32, 154, 5, 16, 142, 8, 24,
        1, 50, 4, 116, 101, 115, 116, 160, 6, 20, 168, 6, 1,
    ];
    let want = mt::principal::StageInfo {
        stage_name: "fs://dir/to/files".to_string(),
        stage_type: mt::principal::StageType::LegacyInternal,
        stage_params: mt::principal::StageParams {
            storage: mt::storage::StorageParams::Fs(mt::storage::StorageFsConfig {
                root: "/dir/to/files".to_string(),
            }),
        },
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::SkipFileNum(666),
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
    common::test_load_old(func_name!(), user_stage_fs_v20.as_slice(), 20, want)?;

    Ok(())
}

#[test]
fn test_user_stage_fs_v16() -> anyhow::Result<()> {
    // Encoded data of version 16 of user_stage_fs:
    // It is generated with common::test_pb_from_to.
    let user_stage_fs_v16 = vec![
        10, 17, 102, 115, 58, 47, 47, 100, 105, 114, 47, 116, 111, 47, 102, 105, 108, 101, 115, 26,
        25, 10, 23, 18, 21, 10, 13, 47, 100, 105, 114, 47, 116, 111, 47, 102, 105, 108, 101, 115,
        160, 6, 16, 168, 6, 1, 34, 20, 8, 1, 16, 128, 8, 26, 1, 124, 34, 2, 47, 47, 40, 2, 160, 6,
        16, 168, 6, 1, 42, 10, 10, 3, 32, 154, 5, 16, 142, 8, 24, 1, 50, 4, 116, 101, 115, 116,
        160, 6, 16, 168, 6, 1,
    ];

    let want = mt::principal::StageInfo {
        stage_name: "fs://dir/to/files".to_string(),
        stage_type: mt::principal::StageType::LegacyInternal,
        stage_params: mt::principal::StageParams {
            storage: mt::storage::StorageParams::Fs(mt::storage::StorageFsConfig {
                root: "/dir/to/files".to_string(),
            }),
        },
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::SkipFileNum(666),
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

    common::test_load_old(func_name!(), user_stage_fs_v16.as_slice(), 16, want)?;

    Ok(())
}

#[test]
fn test_user_stage_s3_v16() -> anyhow::Result<()> {
    // Encoded data of version 16 of user_stage_s3:
    // It is generated with common::test_pb_from_to.
    let user_stage_s3_v16 = vec![
        10, 24, 115, 51, 58, 47, 47, 109, 121, 98, 117, 99, 107, 101, 116, 47, 100, 97, 116, 97,
        47, 102, 105, 108, 101, 115, 16, 1, 26, 119, 10, 117, 10, 115, 18, 24, 104, 116, 116, 112,
        115, 58, 47, 47, 115, 51, 46, 97, 109, 97, 122, 111, 110, 97, 119, 115, 46, 99, 111, 109,
        26, 9, 109, 121, 95, 107, 101, 121, 95, 105, 100, 34, 13, 109, 121, 95, 115, 101, 99, 114,
        101, 116, 95, 107, 101, 121, 42, 8, 109, 121, 98, 117, 99, 107, 101, 116, 50, 11, 47, 100,
        97, 116, 97, 47, 102, 105, 108, 101, 115, 58, 13, 109, 121, 95, 109, 97, 115, 116, 101,
        114, 95, 107, 101, 121, 82, 17, 109, 121, 95, 115, 101, 99, 117, 114, 105, 116, 121, 95,
        116, 111, 107, 101, 110, 160, 6, 16, 168, 6, 1, 34, 20, 8, 1, 16, 128, 8, 26, 1, 124, 34,
        2, 47, 47, 40, 2, 160, 6, 16, 168, 6, 1, 42, 10, 10, 3, 32, 154, 5, 16, 142, 8, 24, 1, 50,
        4, 116, 101, 115, 116, 160, 6, 16, 168, 6, 1,
    ];

    let want = mt::principal::StageInfo {
        stage_name: "s3://mybucket/data/files".to_string(),
        stage_type: mt::principal::StageType::External,
        stage_params: mt::principal::StageParams {
            storage: mt::storage::StorageParams::S3(mt::storage::StorageS3Config {
                bucket: "mybucket".to_string(),
                root: "/data/files".to_string(),
                access_key_id: "my_key_id".to_string(),
                secret_access_key: "my_secret_key".to_string(),
                master_key: "my_master_key".to_string(),
                security_token: "my_security_token".to_string(),
                ..Default::default()
            }),
        },
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::SkipFileNum(666),
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

    common::test_load_old(func_name!(), user_stage_s3_v16.as_slice(), 16, want)?;
    Ok(())
}

#[test]
fn test_user_stage_gcs_v16() -> anyhow::Result<()> {
    // Encoded data of version 16 of user_stage_gcs:
    // It is generated with common::test_pb_from_to.
    let user_stage_gcs_v16 = vec![
        10, 26, 103, 99, 115, 58, 47, 47, 109, 121, 95, 98, 117, 99, 107, 101, 116, 47, 100, 97,
        116, 97, 47, 102, 105, 108, 101, 115, 16, 1, 26, 81, 10, 79, 26, 77, 10, 30, 104, 116, 116,
        112, 115, 58, 47, 47, 115, 116, 111, 114, 97, 103, 101, 46, 103, 111, 111, 103, 108, 101,
        97, 112, 105, 115, 46, 99, 111, 109, 18, 9, 109, 121, 95, 98, 117, 99, 107, 101, 116, 26,
        11, 47, 100, 97, 116, 97, 47, 102, 105, 108, 101, 115, 34, 13, 109, 121, 95, 99, 114, 101,
        100, 101, 110, 116, 105, 97, 108, 160, 6, 16, 168, 6, 1, 34, 20, 8, 1, 16, 128, 8, 26, 1,
        124, 34, 2, 47, 47, 40, 2, 160, 6, 16, 168, 6, 1, 42, 10, 10, 3, 32, 154, 5, 16, 142, 8,
        24, 1, 50, 4, 116, 101, 115, 116, 160, 6, 16, 168, 6, 1,
    ];
    //
    let want = mt::principal::StageInfo {
        stage_name: "gcs://my_bucket/data/files".to_string(),
        stage_type: mt::principal::StageType::External,
        stage_params: mt::principal::StageParams {
            storage: mt::storage::StorageParams::Gcs(mt::storage::StorageGcsConfig {
                endpoint_url: "https://storage.googleapis.com".to_string(),
                bucket: "my_bucket".to_string(),
                root: "/data/files".to_string(),
                credential: "my_credential".to_string(),
            }),
        },
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::SkipFileNum(666),
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
    common::test_load_old(func_name!(), user_stage_gcs_v16.as_slice(), 16, want)?;
    Ok(())
}

#[test]
fn test_user_stage_oss_v16() -> anyhow::Result<()> {
    // Encoded data of version 16 of user_stage_oss:
    // It is generated with common::test_pb_from_to.
    let user_stage_oss_v16 = vec![
        10, 26, 111, 115, 115, 58, 47, 47, 109, 121, 95, 98, 117, 99, 107, 101, 116, 47, 100, 97,
        116, 97, 47, 102, 105, 108, 101, 115, 16, 1, 26, 103, 10, 101, 34, 99, 10, 33, 104, 116,
        116, 112, 115, 58, 47, 47, 111, 115, 115, 45, 99, 110, 45, 108, 105, 116, 97, 110, 103, 46,
        101, 120, 97, 109, 112, 108, 101, 46, 99, 111, 109, 18, 9, 109, 121, 95, 98, 117, 99, 107,
        101, 116, 26, 11, 47, 100, 97, 116, 97, 47, 102, 105, 108, 101, 115, 34, 13, 97, 99, 99,
        101, 115, 115, 95, 107, 101, 121, 95, 105, 100, 42, 17, 97, 99, 99, 101, 115, 115, 95, 107,
        101, 121, 95, 115, 101, 99, 114, 101, 116, 160, 6, 16, 168, 6, 1, 34, 20, 8, 1, 16, 128, 8,
        26, 1, 124, 34, 2, 47, 47, 40, 2, 160, 6, 16, 168, 6, 1, 42, 10, 10, 3, 32, 154, 5, 16,
        142, 8, 24, 1, 50, 4, 116, 101, 115, 116, 160, 6, 16, 168, 6, 1,
    ];

    let want = mt::principal::StageInfo {
        stage_name: "oss://my_bucket/data/files".to_string(),
        stage_type: mt::principal::StageType::External,
        stage_params: mt::principal::StageParams {
            storage: mt::storage::StorageParams::Oss(mt::storage::StorageOssConfig {
                endpoint_url: "https://oss-cn-litang.example.com".to_string(),
                bucket: "my_bucket".to_string(),
                root: "/data/files".to_string(),
                server_side_encryption: "".to_string(),
                presign_endpoint_url: "".to_string(),

                access_key_id: "access_key_id".to_string(),
                access_key_secret: "access_key_secret".to_string(),
                server_side_encryption_key_id: "".to_string(),
            }),
        },
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::SkipFileNum(666),
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

    common::test_load_old(func_name!(), user_stage_oss_v16.as_slice(), 16, want)?;
    Ok(())
}

#[test]
fn test_user_stage_oss_v13() -> anyhow::Result<()> {
    // Encoded data of version 13 of user_stage_oss:
    // It is generated with common::test_pb_from_to.
    let user_stage_oss_v13 = vec![
        10, 26, 111, 115, 115, 58, 47, 47, 109, 121, 95, 98, 117, 99, 107, 101, 116, 47, 100, 97,
        116, 97, 47, 102, 105, 108, 101, 115, 16, 1, 26, 125, 10, 123, 34, 121, 10, 33, 104, 116,
        116, 112, 115, 58, 47, 47, 111, 115, 115, 45, 99, 110, 45, 108, 105, 116, 97, 110, 103, 46,
        101, 120, 97, 109, 112, 108, 101, 46, 99, 111, 109, 18, 9, 109, 121, 95, 98, 117, 99, 107,
        101, 116, 26, 11, 47, 100, 97, 116, 97, 47, 102, 105, 108, 101, 115, 34, 13, 97, 99, 99,
        101, 115, 115, 95, 107, 101, 121, 95, 105, 100, 42, 17, 97, 99, 99, 101, 115, 115, 95, 107,
        101, 121, 95, 115, 101, 99, 114, 101, 116, 50, 10, 111, 105, 100, 99, 95, 116, 111, 107,
        101, 110, 58, 8, 114, 111, 108, 101, 95, 97, 114, 110, 160, 6, 13, 168, 6, 1, 34, 20, 8, 1,
        16, 128, 8, 26, 1, 124, 34, 2, 47, 47, 40, 2, 160, 6, 13, 168, 6, 1, 42, 10, 10, 3, 32,
        154, 5, 16, 142, 8, 24, 1, 50, 4, 116, 101, 115, 116, 160, 6, 13, 168, 6, 1,
    ];

    let want = mt::principal::StageInfo {
        stage_name: "oss://my_bucket/data/files".to_string(),
        stage_type: mt::principal::StageType::External,
        stage_params: mt::principal::StageParams {
            storage: mt::storage::StorageParams::Oss(mt::storage::StorageOssConfig {
                endpoint_url: "https://oss-cn-litang.example.com".to_string(),
                presign_endpoint_url: "".to_string(),
                bucket: "my_bucket".to_string(),
                root: "/data/files".to_string(),
                access_key_id: "access_key_id".to_string(),
                access_key_secret: "access_key_secret".to_string(),
                ..Default::default()
            }),
        },
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::SkipFileNum(666),
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

    common::test_load_old(func_name!(), user_stage_oss_v13.as_slice(), 13, want)?;
    Ok(())
}

#[test]
fn test_user_stage_s3_v11() -> anyhow::Result<()> {
    // Encoded data of version 11 of user_stage_s3:
    // It is generated with common::test_pb_from_to.
    let user_stage_s3_v11 = vec![
        10, 24, 115, 51, 58, 47, 47, 109, 121, 98, 117, 99, 107, 101, 116, 47, 100, 97, 116, 97,
        47, 102, 105, 108, 101, 115, 16, 1, 26, 119, 10, 117, 10, 115, 18, 24, 104, 116, 116, 112,
        115, 58, 47, 47, 115, 51, 46, 97, 109, 97, 122, 111, 110, 97, 119, 115, 46, 99, 111, 109,
        26, 9, 109, 121, 95, 107, 101, 121, 95, 105, 100, 34, 13, 109, 121, 95, 115, 101, 99, 114,
        101, 116, 95, 107, 101, 121, 42, 8, 109, 121, 98, 117, 99, 107, 101, 116, 50, 11, 47, 100,
        97, 116, 97, 47, 102, 105, 108, 101, 115, 58, 13, 109, 121, 95, 109, 97, 115, 116, 101,
        114, 95, 107, 101, 121, 82, 17, 109, 121, 95, 115, 101, 99, 117, 114, 105, 116, 121, 95,
        116, 111, 107, 101, 110, 160, 6, 11, 168, 6, 1, 34, 20, 8, 1, 16, 128, 8, 26, 1, 124, 34,
        2, 47, 47, 40, 2, 160, 6, 11, 168, 6, 1, 42, 10, 10, 3, 32, 154, 5, 16, 142, 8, 24, 1, 50,
        4, 116, 101, 115, 116, 160, 6, 11, 168, 6, 1,
    ];

    let want = mt::principal::StageInfo {
        stage_name: "s3://mybucket/data/files".to_string(),
        stage_type: mt::principal::StageType::External,
        stage_params: mt::principal::StageParams {
            storage: mt::storage::StorageParams::S3(mt::storage::StorageS3Config {
                bucket: "mybucket".to_string(),
                root: "/data/files".to_string(),
                access_key_id: "my_key_id".to_string(),
                secret_access_key: "my_secret_key".to_string(),
                master_key: "my_master_key".to_string(),
                security_token: "my_security_token".to_string(),
                ..Default::default()
            }),
        },
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::SkipFileNum(666),
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

    common::test_load_old(func_name!(), user_stage_s3_v11.as_slice(), 11, want)?;
    Ok(())
}

#[test]
fn test_user_stage_s3_v8() -> anyhow::Result<()> {
    // Encoded data of version 9 of user_stage_s3:
    // It is generated with common::test_pb_from_to.
    let user_stage_s3_v8 = vec![
        10, 24, 115, 51, 58, 47, 47, 109, 121, 98, 117, 99, 107, 101, 116, 47, 100, 97, 116, 97,
        47, 102, 105, 108, 101, 115, 16, 1, 26, 100, 10, 98, 10, 96, 18, 24, 104, 116, 116, 112,
        115, 58, 47, 47, 115, 51, 46, 97, 109, 97, 122, 111, 110, 97, 119, 115, 46, 99, 111, 109,
        26, 9, 109, 121, 95, 107, 101, 121, 95, 105, 100, 34, 13, 109, 121, 95, 115, 101, 99, 114,
        101, 116, 95, 107, 101, 121, 42, 8, 109, 121, 98, 117, 99, 107, 101, 116, 50, 11, 47, 100,
        97, 116, 97, 47, 102, 105, 108, 101, 115, 58, 13, 109, 121, 95, 109, 97, 115, 116, 101,
        114, 95, 107, 101, 121, 160, 6, 8, 168, 6, 1, 34, 20, 8, 1, 16, 128, 8, 26, 1, 124, 34, 2,
        47, 47, 40, 2, 160, 6, 8, 168, 6, 1, 42, 10, 10, 3, 32, 154, 5, 16, 142, 8, 24, 1, 50, 4,
        116, 101, 115, 116, 160, 6, 8, 168, 6, 1,
    ];

    let want = mt::principal::StageInfo {
        stage_name: "s3://mybucket/data/files".to_string(),
        stage_type: mt::principal::StageType::External,
        stage_params: mt::principal::StageParams {
            storage: mt::storage::StorageParams::S3(mt::storage::StorageS3Config {
                bucket: "mybucket".to_string(),
                root: "/data/files".to_string(),
                access_key_id: "my_key_id".to_string(),
                secret_access_key: "my_secret_key".to_string(),
                master_key: "my_master_key".to_string(),
                ..Default::default()
            }),
        },
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::SkipFileNum(666),
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

    common::test_load_old(func_name!(), user_stage_s3_v8.as_slice(), 8, want)?;
    Ok(())
}

#[test]
fn test_user_stage_fs_v6() -> anyhow::Result<()> {
    // Encoded data of version 6 of user_stage_fs:
    // It is generated with common::test_pb_from_to.
    let user_stage_fs_v6 = vec![
        10, 17, 102, 115, 58, 47, 47, 100, 105, 114, 47, 116, 111, 47, 102, 105, 108, 101, 115, 26,
        25, 10, 23, 18, 21, 10, 13, 47, 100, 105, 114, 47, 116, 111, 47, 102, 105, 108, 101, 115,
        160, 6, 6, 168, 6, 1, 34, 20, 8, 1, 16, 128, 8, 26, 1, 124, 34, 2, 47, 47, 40, 2, 160, 6,
        6, 168, 6, 1, 42, 10, 10, 3, 32, 154, 5, 16, 142, 8, 24, 1, 50, 4, 116, 101, 115, 116, 160,
        6, 6, 168, 6, 1,
    ];

    let want = mt::principal::StageInfo {
        stage_name: "fs://dir/to/files".to_string(),
        stage_type: mt::principal::StageType::LegacyInternal,
        stage_params: mt::principal::StageParams {
            storage: mt::storage::StorageParams::Fs(mt::storage::StorageFsConfig {
                root: "/dir/to/files".to_string(),
            }),
        },
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::SkipFileNum(666),
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

    common::test_load_old(func_name!(), user_stage_fs_v6.as_slice(), 6, want)?;

    Ok(())
}

#[test]
fn test_user_stage_s3_v6() -> anyhow::Result<()> {
    // Encoded data of version 6 of user_stage_s3:
    // It is generated with common::test_pb_from_to.
    let user_stage_s3_v6 = vec![
        10, 24, 115, 51, 58, 47, 47, 109, 121, 98, 117, 99, 107, 101, 116, 47, 100, 97, 116, 97,
        47, 102, 105, 108, 101, 115, 16, 1, 26, 100, 10, 98, 10, 96, 18, 24, 104, 116, 116, 112,
        115, 58, 47, 47, 115, 51, 46, 97, 109, 97, 122, 111, 110, 97, 119, 115, 46, 99, 111, 109,
        26, 9, 109, 121, 95, 107, 101, 121, 95, 105, 100, 34, 13, 109, 121, 95, 115, 101, 99, 114,
        101, 116, 95, 107, 101, 121, 42, 8, 109, 121, 98, 117, 99, 107, 101, 116, 50, 11, 47, 100,
        97, 116, 97, 47, 102, 105, 108, 101, 115, 58, 13, 109, 121, 95, 109, 97, 115, 116, 101,
        114, 95, 107, 101, 121, 160, 6, 6, 168, 6, 1, 34, 20, 8, 1, 16, 128, 8, 26, 1, 124, 34, 2,
        47, 47, 40, 2, 160, 6, 6, 168, 6, 1, 42, 10, 10, 3, 32, 154, 5, 16, 142, 8, 24, 1, 50, 4,
        116, 101, 115, 116, 160, 6, 6, 168, 6, 1,
    ];

    let want = mt::principal::StageInfo {
        stage_name: "s3://mybucket/data/files".to_string(),
        stage_type: mt::principal::StageType::External,
        stage_params: mt::principal::StageParams {
            storage: mt::storage::StorageParams::S3(mt::storage::StorageS3Config {
                bucket: "mybucket".to_string(),
                root: "/data/files".to_string(),
                access_key_id: "my_key_id".to_string(),
                secret_access_key: "my_secret_key".to_string(),
                master_key: "my_master_key".to_string(),
                ..Default::default()
            }),
        },
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::SkipFileNum(666),
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

    common::test_load_old(func_name!(), user_stage_s3_v6.as_slice(), 6, want)?;
    Ok(())
}

#[test]
fn test_user_stage_gcs_v6() -> anyhow::Result<()> {
    // Encoded data of version 6 of user_stage_gcs:
    // It is generated with common::test_pb_from_to.
    let user_stage_gcs_v6 = vec![
        10, 26, 103, 99, 115, 58, 47, 47, 109, 121, 95, 98, 117, 99, 107, 101, 116, 47, 100, 97,
        116, 97, 47, 102, 105, 108, 101, 115, 16, 1, 26, 81, 10, 79, 26, 77, 10, 30, 104, 116, 116,
        112, 115, 58, 47, 47, 115, 116, 111, 114, 97, 103, 101, 46, 103, 111, 111, 103, 108, 101,
        97, 112, 105, 115, 46, 99, 111, 109, 18, 9, 109, 121, 95, 98, 117, 99, 107, 101, 116, 26,
        11, 47, 100, 97, 116, 97, 47, 102, 105, 108, 101, 115, 34, 13, 109, 121, 95, 99, 114, 101,
        100, 101, 110, 116, 105, 97, 108, 160, 6, 6, 168, 6, 1, 34, 20, 8, 1, 16, 128, 8, 26, 1,
        124, 34, 2, 47, 47, 40, 2, 160, 6, 6, 168, 6, 1, 42, 10, 10, 3, 32, 154, 5, 16, 142, 8, 24,
        1, 50, 4, 116, 101, 115, 116, 160, 6, 6, 168, 6, 1,
    ];
    //
    let want = mt::principal::StageInfo {
        stage_name: "gcs://my_bucket/data/files".to_string(),
        stage_type: mt::principal::StageType::External,
        stage_params: mt::principal::StageParams {
            storage: mt::storage::StorageParams::Gcs(mt::storage::StorageGcsConfig {
                endpoint_url: "https://storage.googleapis.com".to_string(),
                bucket: "my_bucket".to_string(),
                root: "/data/files".to_string(),
                credential: "my_credential".to_string(),
            }),
        },
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::SkipFileNum(666),
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
    common::test_load_old(func_name!(), user_stage_gcs_v6.as_slice(), 6, want)?;
    Ok(())
}

#[test]
fn test_user_stage_fs_v4() -> anyhow::Result<()> {
    // Encoded data of version 4 of user_stage_fs:
    // It is generated with common::test_pb_from_to.
    let user_stage_fs_v4 = vec![
        10, 17, 102, 115, 58, 47, 47, 100, 105, 114, 47, 116, 111, 47, 102, 105, 108, 101, 115, 26,
        25, 10, 23, 18, 21, 10, 13, 47, 100, 105, 114, 47, 116, 111, 47, 102, 105, 108, 101, 115,
        160, 6, 4, 168, 6, 1, 34, 20, 8, 1, 16, 128, 8, 26, 1, 124, 34, 2, 47, 47, 40, 2, 160, 6,
        4, 168, 6, 1, 42, 8, 10, 3, 32, 154, 5, 16, 142, 8, 50, 4, 116, 101, 115, 116, 160, 6, 4,
        168, 6, 1,
    ];

    let want = mt::principal::StageInfo {
        stage_name: "fs://dir/to/files".to_string(),
        stage_type: mt::principal::StageType::LegacyInternal,
        stage_params: mt::principal::StageParams {
            storage: mt::storage::StorageParams::Fs(mt::storage::StorageFsConfig {
                root: "/dir/to/files".to_string(),
            }),
        },
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::SkipFileNum(666),
            size_limit: 1038,
            max_files: 0,
            split_size: 0,
            purge: false,
            single: false,
            max_file_size: 0,
            disable_variant_check: false,
            return_failed_only: false,
            detailed_output: false,
        },
        comment: "test".to_string(),
        ..Default::default()
    };

    common::test_load_old(func_name!(), user_stage_fs_v4.as_slice(), 4, want)?;

    Ok(())
}

#[test]
fn test_user_stage_s3_v4() -> anyhow::Result<()> {
    // Encoded data of version 4 of user_stage_s3:
    // It is generated with common::test_pb_from_to.
    let user_stage_s3_v4 = vec![
        10, 24, 115, 51, 58, 47, 47, 109, 121, 98, 117, 99, 107, 101, 116, 47, 100, 97, 116, 97,
        47, 102, 105, 108, 101, 115, 16, 1, 26, 100, 10, 98, 10, 96, 18, 24, 104, 116, 116, 112,
        115, 58, 47, 47, 115, 51, 46, 97, 109, 97, 122, 111, 110, 97, 119, 115, 46, 99, 111, 109,
        26, 9, 109, 121, 95, 107, 101, 121, 95, 105, 100, 34, 13, 109, 121, 95, 115, 101, 99, 114,
        101, 116, 95, 107, 101, 121, 42, 8, 109, 121, 98, 117, 99, 107, 101, 116, 50, 11, 47, 100,
        97, 116, 97, 47, 102, 105, 108, 101, 115, 58, 13, 109, 121, 95, 109, 97, 115, 116, 101,
        114, 95, 107, 101, 121, 160, 6, 4, 168, 6, 1, 34, 20, 8, 1, 16, 128, 8, 26, 1, 124, 34, 2,
        47, 47, 40, 2, 160, 6, 4, 168, 6, 1, 42, 8, 10, 3, 32, 154, 5, 16, 142, 8, 50, 4, 116, 101,
        115, 116, 160, 6, 4, 168, 6, 1,
    ];

    let want = mt::principal::StageInfo {
        stage_name: "s3://mybucket/data/files".to_string(),
        stage_type: mt::principal::StageType::External,
        stage_params: mt::principal::StageParams {
            storage: mt::storage::StorageParams::S3(mt::storage::StorageS3Config {
                bucket: "mybucket".to_string(),
                root: "/data/files".to_string(),
                access_key_id: "my_key_id".to_string(),
                secret_access_key: "my_secret_key".to_string(),
                master_key: "my_master_key".to_string(),
                ..Default::default()
            }),
        },
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::SkipFileNum(666),
            size_limit: 1038,
            max_files: 0,
            split_size: 0,
            purge: false,
            single: false,
            max_file_size: 0,
            disable_variant_check: false,
            return_failed_only: false,
            detailed_output: false,
        },
        comment: "test".to_string(),
        ..Default::default()
    };

    common::test_load_old(func_name!(), user_stage_s3_v4.as_slice(), 4, want)?;
    Ok(())
}

#[test]
fn test_user_stage_gcs_v4() -> anyhow::Result<()> {
    // Encoded data of version 4 of user_stage_gcs:
    // It is generated with common::test_pb_from_to.
    let user_stage_gcs_v4 = vec![
        10, 26, 103, 99, 115, 58, 47, 47, 109, 121, 95, 98, 117, 99, 107, 101, 116, 47, 100, 97,
        116, 97, 47, 102, 105, 108, 101, 115, 16, 1, 26, 81, 10, 79, 26, 77, 10, 30, 104, 116, 116,
        112, 115, 58, 47, 47, 115, 116, 111, 114, 97, 103, 101, 46, 103, 111, 111, 103, 108, 101,
        97, 112, 105, 115, 46, 99, 111, 109, 18, 9, 109, 121, 95, 98, 117, 99, 107, 101, 116, 26,
        11, 47, 100, 97, 116, 97, 47, 102, 105, 108, 101, 115, 34, 13, 109, 121, 95, 99, 114, 101,
        100, 101, 110, 116, 105, 97, 108, 160, 6, 4, 168, 6, 1, 34, 20, 8, 1, 16, 128, 8, 26, 1,
        124, 34, 2, 47, 47, 40, 2, 160, 6, 4, 168, 6, 1, 42, 8, 10, 3, 32, 154, 5, 16, 142, 8, 50,
        4, 116, 101, 115, 116, 160, 6, 4, 168, 6, 1,
    ];
    let want = mt::principal::StageInfo {
        stage_name: "gcs://my_bucket/data/files".to_string(),
        stage_type: mt::principal::StageType::External,
        stage_params: mt::principal::StageParams {
            storage: mt::storage::StorageParams::Gcs(mt::storage::StorageGcsConfig {
                endpoint_url: "https://storage.googleapis.com".to_string(),
                bucket: "my_bucket".to_string(),
                root: "/data/files".to_string(),
                credential: "my_credential".to_string(),
            }),
        },
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::SkipFileNum(666),
            size_limit: 1038,
            max_files: 0,
            split_size: 0,
            purge: false,
            single: false,
            max_file_size: 0,
            disable_variant_check: false,
            return_failed_only: false,
            detailed_output: false,
        },
        comment: "test".to_string(),
        ..Default::default()
    };
    common::test_load_old(func_name!(), user_stage_gcs_v4.as_slice(), 4, want)?;
    Ok(())
}

#[test]
fn test_user_stage_s3_v1() -> anyhow::Result<()> {
    // Encoded data of version 1 of user_stage_s3:
    // It is generated with common::test_pb_from_to.
    let user_stage_s3_v1 = vec![
        10, 24, 115, 51, 58, 47, 47, 109, 121, 98, 117, 99, 107, 101, 116, 47, 100, 97, 116, 97,
        47, 102, 105, 108, 101, 115, 16, 1, 26, 97, 10, 95, 10, 93, 18, 24, 104, 116, 116, 112,
        115, 58, 47, 47, 115, 51, 46, 97, 109, 97, 122, 111, 110, 97, 119, 115, 46, 99, 111, 109,
        26, 9, 109, 121, 95, 107, 101, 121, 95, 105, 100, 34, 13, 109, 121, 95, 115, 101, 99, 114,
        101, 116, 95, 107, 101, 121, 42, 8, 109, 121, 98, 117, 99, 107, 101, 116, 50, 11, 47, 100,
        97, 116, 97, 47, 102, 105, 108, 101, 115, 58, 13, 109, 121, 95, 109, 97, 115, 116, 101,
        114, 95, 107, 101, 121, 160, 6, 1, 34, 17, 8, 1, 16, 128, 8, 26, 1, 124, 34, 2, 47, 47, 40,
        2, 160, 6, 1, 42, 8, 10, 3, 32, 154, 5, 16, 142, 8, 50, 4, 116, 101, 115, 116, 160, 6, 1,
    ];

    let want = mt::principal::StageInfo {
        stage_name: "s3://mybucket/data/files".to_string(),
        stage_type: mt::principal::StageType::External,
        stage_params: mt::principal::StageParams {
            storage: mt::storage::StorageParams::S3(mt::storage::StorageS3Config {
                bucket: "mybucket".to_string(),
                root: "/data/files".to_string(),
                access_key_id: "my_key_id".to_string(),
                secret_access_key: "my_secret_key".to_string(),
                master_key: "my_master_key".to_string(),
                ..Default::default()
            }),
        },
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::SkipFileNum(666),
            size_limit: 1038,
            max_files: 0,
            split_size: 0,
            purge: false,
            single: false,
            max_file_size: 0,
            disable_variant_check: false,
            return_failed_only: false,
            detailed_output: false,
        },
        comment: "test".to_string(),
        ..Default::default()
    };

    common::test_load_old(func_name!(), user_stage_s3_v1.as_slice(), 1, want)?;
    Ok(())
}

#[test]
fn test_internal_stage_v17() -> anyhow::Result<()> {
    common::test_pb_from_to("internal_stage_v17", test_internal_stage_info_v17())?;

    // Encoded data of version v17 of internal:
    // It is generated with common::test_pb_from_to.
    let internal_stage_v17 = vec![
        10, 17, 102, 115, 58, 47, 47, 100, 105, 114, 47, 116, 111, 47, 102, 105, 108, 101, 115, 16,
        2, 26, 25, 10, 23, 18, 21, 10, 13, 47, 100, 105, 114, 47, 116, 111, 47, 102, 105, 108, 101,
        115, 160, 6, 17, 168, 6, 1, 34, 20, 8, 1, 16, 128, 8, 26, 1, 124, 34, 2, 47, 47, 40, 2,
        160, 6, 17, 168, 6, 1, 42, 10, 10, 3, 32, 154, 5, 16, 142, 8, 24, 1, 50, 4, 116, 101, 115,
        116, 160, 6, 17, 168, 6, 1,
    ];

    let want = mt::principal::StageInfo {
        stage_name: "fs://dir/to/files".to_string(),
        stage_type: mt::principal::StageType::Internal,
        stage_params: mt::principal::StageParams {
            storage: mt::storage::StorageParams::Fs(mt::storage::StorageFsConfig {
                root: "/dir/to/files".to_string(),
            }),
        },
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::SkipFileNum(666),
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

    common::test_load_old(func_name!(), internal_stage_v17.as_slice(), 17, want)?;
    Ok(())
}

#[test]
fn test_user_stage_v19() -> anyhow::Result<()> {
    common::test_pb_from_to("user_stage_v19", test_stage_info_v18())?;

    // Encoded data of version v18 of user_stage:
    // It is generated with common::test_pb_from_to.
    let user_stage_v19 = vec![
        10, 4, 114, 111, 111, 116, 16, 3, 26, 25, 10, 23, 18, 21, 10, 13, 47, 100, 105, 114, 47,
        116, 111, 47, 102, 105, 108, 101, 115, 160, 6, 19, 168, 6, 1, 34, 20, 8, 1, 16, 128, 8, 26,
        1, 124, 34, 2, 47, 47, 40, 2, 160, 6, 19, 168, 6, 1, 42, 10, 10, 3, 32, 154, 5, 16, 142, 8,
        24, 1, 50, 4, 116, 101, 115, 116, 160, 6, 19, 168, 6, 1,
    ];

    let want = mt::principal::StageInfo {
        stage_name: "root".to_string(),
        stage_type: mt::principal::StageType::User,
        stage_params: mt::principal::StageParams {
            storage: mt::storage::StorageParams::Fs(mt::storage::StorageFsConfig {
                root: "/dir/to/files".to_string(),
            }),
        },
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::SkipFileNum(666),
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

    common::test_load_old(func_name!(), user_stage_v19.as_slice(), 19, want)?;
    Ok(())
}
