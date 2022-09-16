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

//! Test UserStageInfo

use common_datavalues::chrono::DateTime;
use common_datavalues::chrono::NaiveDate;
use common_datavalues::chrono::NaiveDateTime;
use common_datavalues::chrono::NaiveTime;
use common_datavalues::chrono::Utc;
use common_meta_types as mt;
use common_storage::StorageFsConfig;
use common_storage::StorageGcsConfig;
use common_storage::StorageParams;
use common_storage::StorageS3Config;

use crate::common;
use crate::user_proto_conv::test_fs_stage_info;
use crate::user_proto_conv::test_gcs_stage_info;
use crate::user_proto_conv::test_s3_stage_info;
use crate::user_proto_conv::test_stage_file;

#[test]
fn test_stage_file_latest() -> anyhow::Result<()> {
    common::test_pb_from_to("stage_file", test_stage_file())?;
    Ok(())
}

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
fn test_stage_file_v7() -> anyhow::Result<()> {
    // Encoded data of version 6 of StageFile:
    // Generated with common::test_pb_from_to.
    let stage_file_v7 = vec![
        10, 14, 47, 112, 97, 116, 104, 47, 116, 111, 47, 115, 116, 97, 103, 101, 16, 233, 1, 34,
        23, 50, 48, 50, 50, 45, 48, 57, 45, 49, 54, 32, 48, 48, 58, 48, 49, 58, 48, 50, 32, 85, 84,
        67, 42, 37, 10, 12, 100, 97, 116, 97, 102, 117, 115, 101, 108, 97, 98, 115, 18, 15, 100,
        97, 116, 97, 102, 117, 115, 101, 108, 97, 98, 115, 46, 114, 115, 160, 6, 8, 168, 6, 1, 160,
        6, 8, 168, 6, 1,
    ];

    let dt = NaiveDateTime::new(
        NaiveDate::from_ymd(2022, 9, 16),
        NaiveTime::from_hms(0, 1, 2),
    );
    let user_id = mt::UserIdentity::new("datafuselabs", "datafuselabs.rs");
    let want = mt::StageFile {
        path: "/path/to/stage".to_string(),
        size: 233,
        md5: None,
        last_modified: DateTime::from_utc(dt, Utc),
        creator: Some(user_id),
        ..Default::default()
    };

    common::test_load_old(func_name!(), stage_file_v7.as_slice(), want)?;
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

    let want = mt::UserStageInfo {
        stage_name: "fs://dir/to/files".to_string(),
        stage_type: mt::StageType::Internal,
        stage_params: mt::StageParams {
            storage: StorageParams::Fs(StorageFsConfig {
                root: "/dir/to/files".to_string(),
            }),
        },
        file_format_options: mt::FileFormatOptions {
            format: mt::StageFileFormatType::Json,
            skip_header: 1024,
            field_delimiter: "|".to_string(),
            record_delimiter: "//".to_string(),
            compression: mt::StageFileCompression::Bz2,
        },
        copy_options: mt::CopyOptions {
            on_error: mt::OnErrorMode::SkipFileNum(666),
            size_limit: 1038,
            purge: true,
        },
        comment: "test".to_string(),
        ..Default::default()
    };

    common::test_load_old(func_name!(), user_stage_fs_v6.as_slice(), want)?;

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

    let want = mt::UserStageInfo {
        stage_name: "s3://mybucket/data/files".to_string(),
        stage_type: mt::StageType::External,
        stage_params: mt::StageParams {
            storage: StorageParams::S3(StorageS3Config {
                bucket: "mybucket".to_string(),
                root: "/data/files".to_string(),
                access_key_id: "my_key_id".to_string(),
                secret_access_key: "my_secret_key".to_string(),
                master_key: "my_master_key".to_string(),
                ..Default::default()
            }),
        },
        file_format_options: mt::FileFormatOptions {
            format: mt::StageFileFormatType::Json,
            skip_header: 1024,
            field_delimiter: "|".to_string(),
            record_delimiter: "//".to_string(),
            compression: mt::StageFileCompression::Bz2,
        },
        copy_options: mt::CopyOptions {
            on_error: mt::OnErrorMode::SkipFileNum(666),
            size_limit: 1038,
            purge: true,
        },
        comment: "test".to_string(),
        ..Default::default()
    };

    common::test_load_old(func_name!(), user_stage_s3_v6.as_slice(), want)?;
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
    let want = mt::UserStageInfo {
        stage_name: "gcs://my_bucket/data/files".to_string(),
        stage_type: mt::StageType::External,
        stage_params: mt::StageParams {
            storage: StorageParams::Gcs(StorageGcsConfig {
                endpoint_url: "https://storage.googleapis.com".to_string(),
                bucket: "my_bucket".to_string(),
                root: "/data/files".to_string(),
                credential: "my_credential".to_string(),
            }),
        },
        file_format_options: mt::FileFormatOptions {
            format: mt::StageFileFormatType::Json,
            skip_header: 1024,
            field_delimiter: "|".to_string(),
            record_delimiter: "//".to_string(),
            compression: mt::StageFileCompression::Bz2,
        },
        copy_options: mt::CopyOptions {
            on_error: mt::OnErrorMode::SkipFileNum(666),
            size_limit: 1038,
            purge: true,
        },
        comment: "test".to_string(),
        ..Default::default()
    };
    common::test_load_old(func_name!(), user_stage_gcs_v6.as_slice(), want)?;
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

    let want = mt::UserStageInfo {
        stage_name: "fs://dir/to/files".to_string(),
        stage_type: mt::StageType::Internal,
        stage_params: mt::StageParams {
            storage: StorageParams::Fs(StorageFsConfig {
                root: "/dir/to/files".to_string(),
            }),
        },
        file_format_options: mt::FileFormatOptions {
            format: mt::StageFileFormatType::Json,
            skip_header: 1024,
            field_delimiter: "|".to_string(),
            record_delimiter: "//".to_string(),
            compression: mt::StageFileCompression::Bz2,
        },
        copy_options: mt::CopyOptions {
            on_error: mt::OnErrorMode::SkipFileNum(666),
            size_limit: 1038,
            purge: false,
        },
        comment: "test".to_string(),
        ..Default::default()
    };

    common::test_load_old(func_name!(), user_stage_fs_v4.as_slice(), want)?;

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

    let want = mt::UserStageInfo {
        stage_name: "s3://mybucket/data/files".to_string(),
        stage_type: mt::StageType::External,
        stage_params: mt::StageParams {
            storage: StorageParams::S3(StorageS3Config {
                bucket: "mybucket".to_string(),
                root: "/data/files".to_string(),
                access_key_id: "my_key_id".to_string(),
                secret_access_key: "my_secret_key".to_string(),
                master_key: "my_master_key".to_string(),
                ..Default::default()
            }),
        },
        file_format_options: mt::FileFormatOptions {
            format: mt::StageFileFormatType::Json,
            skip_header: 1024,
            field_delimiter: "|".to_string(),
            record_delimiter: "//".to_string(),
            compression: mt::StageFileCompression::Bz2,
        },
        copy_options: mt::CopyOptions {
            on_error: mt::OnErrorMode::SkipFileNum(666),
            size_limit: 1038,
            purge: false,
        },
        comment: "test".to_string(),
        ..Default::default()
    };

    common::test_load_old(func_name!(), user_stage_s3_v4.as_slice(), want)?;
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
    let want = mt::UserStageInfo {
        stage_name: "gcs://my_bucket/data/files".to_string(),
        stage_type: mt::StageType::External,
        stage_params: mt::StageParams {
            storage: StorageParams::Gcs(StorageGcsConfig {
                endpoint_url: "https://storage.googleapis.com".to_string(),
                bucket: "my_bucket".to_string(),
                root: "/data/files".to_string(),
                credential: "my_credential".to_string(),
            }),
        },
        file_format_options: mt::FileFormatOptions {
            format: mt::StageFileFormatType::Json,
            skip_header: 1024,
            field_delimiter: "|".to_string(),
            record_delimiter: "//".to_string(),
            compression: mt::StageFileCompression::Bz2,
        },
        copy_options: mt::CopyOptions {
            on_error: mt::OnErrorMode::SkipFileNum(666),
            size_limit: 1038,
            purge: false,
        },
        comment: "test".to_string(),
        ..Default::default()
    };
    common::test_load_old(func_name!(), user_stage_gcs_v4.as_slice(), want)?;
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

    let want = mt::UserStageInfo {
        stage_name: "s3://mybucket/data/files".to_string(),
        stage_type: mt::StageType::External,
        stage_params: mt::StageParams {
            storage: StorageParams::S3(StorageS3Config {
                bucket: "mybucket".to_string(),
                root: "/data/files".to_string(),
                access_key_id: "my_key_id".to_string(),
                secret_access_key: "my_secret_key".to_string(),
                master_key: "my_master_key".to_string(),
                ..Default::default()
            }),
        },
        file_format_options: mt::FileFormatOptions {
            format: mt::StageFileFormatType::Json,
            skip_header: 1024,
            field_delimiter: "|".to_string(),
            record_delimiter: "//".to_string(),
            compression: mt::StageFileCompression::Bz2,
        },
        copy_options: mt::CopyOptions {
            on_error: mt::OnErrorMode::SkipFileNum(666),
            size_limit: 1038,
            purge: false,
        },
        comment: "test".to_string(),
        ..Default::default()
    };

    common::test_load_old(func_name!(), user_stage_s3_v1.as_slice(), want)?;
    Ok(())
}
