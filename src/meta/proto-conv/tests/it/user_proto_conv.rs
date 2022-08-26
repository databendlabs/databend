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

use std::collections::HashSet;
use std::fmt::Debug;

use common_meta_types as mt;
use common_meta_types::UserInfo;
use common_meta_types::UserPrivilegeType;
use common_proto_conv::FromToProto;
use common_proto_conv::Incompatible;
use common_protos::pb;
use common_storage::StorageFsConfig;
use common_storage::StorageGcsConfig;
use common_storage::StorageParams;
use common_storage::StorageS3Config;
use enumflags2::make_bitflags;

fn s(ss: impl ToString) -> String {
    ss.to_string()
}

fn test_user_info() -> UserInfo {
    let option = mt::UserOption::default()
        .with_set_flag(mt::UserOptionFlag::TenantSetting)
        .with_default_role(Some("role1".into()));

    mt::UserInfo {
        name: "test_user".to_string(),
        hostname: "localhost".to_string(),
        auth_info: mt::AuthInfo::Password {
            hash_value: [
                116, 101, 115, 116, 95, 112, 97, 115, 115, 119, 111, 114, 100,
            ]
            .to_vec(),
            hash_method: mt::PasswordHashMethod::DoubleSha1,
        },
        grants: mt::UserGrantSet::new(
            vec![mt::GrantEntry::new(
                mt::GrantObject::Global,
                make_bitflags!(UserPrivilegeType::{Create}),
            )],
            HashSet::new(),
        ),
        quota: mt::UserQuota {
            max_cpu: 10,
            max_memory_in_bytes: 10240,
            max_storage_in_bytes: 20480,
        },
        option,
    }
}

fn test_fs_stage_info() -> mt::UserStageInfo {
    mt::UserStageInfo {
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
        },
        comment: "test".to_string(),
        ..Default::default()
    }
}

fn test_s3_stage_info() -> mt::UserStageInfo {
    mt::UserStageInfo {
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
        },
        comment: "test".to_string(),
        ..Default::default()
    }
}

// Version 4 added Google Cloud Storage as a stage backend, should be tested
fn test_gcs_stage_info() -> mt::UserStageInfo {
    mt::UserStageInfo {
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
        },
        comment: "test".to_string(),
        ..Default::default()
    }
}

#[test]
fn test_user_pb_from_to() -> anyhow::Result<()> {
    let test_user_info = test_user_info();
    let test_user_info_pb = test_user_info.to_pb()?;
    let got = mt::UserInfo::from_pb(test_user_info_pb)?;
    assert_eq!(got, test_user_info);

    Ok(())
}

#[test]
fn test_user_stage_pb_from_to() -> anyhow::Result<()> {
    let test_fs_stage_info = test_fs_stage_info();
    let test_user_stage_info_pb = test_fs_stage_info.to_pb()?;
    let got = mt::UserStageInfo::from_pb(test_user_stage_info_pb)?;
    assert_eq!(got, test_fs_stage_info);

    let test_s3_stage_info = test_s3_stage_info();
    let test_user_stage_info_pb = test_s3_stage_info.to_pb()?;
    let got = mt::UserStageInfo::from_pb(test_user_stage_info_pb)?;
    assert_eq!(got, test_s3_stage_info);

    let test_gcs_stage_info = test_gcs_stage_info();
    let test_user_stage_info_pb = test_gcs_stage_info.to_pb()?;
    let got = mt::UserStageInfo::from_pb(test_user_stage_info_pb)?;
    assert_eq!(got, test_gcs_stage_info);

    Ok(())
}

#[test]
fn test_user_incompatible() -> anyhow::Result<()> {
    {
        let user_info = test_user_info();
        let mut p = user_info.to_pb()?;
        p.ver = 6;
        p.min_compatible = 6;

        let res = mt::UserInfo::from_pb(p);
        assert_eq!(
            Incompatible {
                reason: s("executable ver=5 is smaller than the message min compatible ver: 6")
            },
            res.unwrap_err()
        );
    }

    {
        let fs_stage_info = test_fs_stage_info();
        let mut p = fs_stage_info.to_pb()?;
        p.ver = 6;
        p.min_compatible = 6;

        let res = mt::UserStageInfo::from_pb(p);
        assert_eq!(
            Incompatible {
                reason: s("executable ver=5 is smaller than the message min compatible ver: 6")
            },
            res.unwrap_err()
        )
    }

    {
        let s3_stage_info = test_s3_stage_info();
        let mut p = s3_stage_info.to_pb()?;
        p.ver = 6;
        p.min_compatible = 6;

        let res = mt::UserStageInfo::from_pb(p);
        assert_eq!(
            Incompatible {
                reason: s("executable ver=5 is smaller than the message min compatible ver: 6")
            },
            res.unwrap_err()
        );
    }

    {
        let gcs_stage_info = test_gcs_stage_info();
        let mut p = gcs_stage_info.to_pb()?;
        p.ver = 6;
        p.min_compatible = 6;

        let res = mt::UserStageInfo::from_pb(p);
        assert_eq!(
            Incompatible {
                reason: s("executable ver=5 is smaller than the message min compatible ver: 6")
            },
            res.unwrap_err()
        );
    }

    Ok(())
}

#[test]
fn test_build_user_pb_buf() -> anyhow::Result<()> {
    // build serialized buf of protobuf data, for backward compatibility test with a new version binary.

    // UserInfo
    {
        let user_info = test_user_info();
        let p = user_info.to_pb()?;

        let mut buf = vec![];
        common_protos::prost::Message::encode(&p, &mut buf)?;
        println!("{:?}", buf);
    }

    // Stage on local file system
    {
        let fs_stage_info = test_fs_stage_info();

        let p = fs_stage_info.to_pb()?;

        let mut buf = vec![];
        common_protos::prost::Message::encode(&p, &mut buf)?;
        println!("{:?}", buf);
    }

    // Stage on S3
    {
        let s3_stage_info = test_s3_stage_info();

        let p = s3_stage_info.to_pb()?;

        let mut buf = vec![];
        common_protos::prost::Message::encode(&p, &mut buf)?;
        println!("{:?}", buf);
    }

    // Stage on GCS, supported in version >=4.
    {
        let gcs_stage_info = test_gcs_stage_info();
        let p = gcs_stage_info.to_pb()?;
        let mut buf = vec![];
        common_protos::prost::Message::encode(&p, &mut buf)?;
        println!("{:?}", buf);
    }

    Ok(())
}

#[test]
fn test_load_old_user() -> anyhow::Result<()> {
    // built with `test_build_user_pb_buf()`

    {
        // User information generated by test_build_user_pb_buf()
        let user_info_v4: Vec<u8> = vec![
            10, 9, 116, 101, 115, 116, 95, 117, 115, 101, 114, 18, 9, 108, 111, 99, 97, 108, 104,
            111, 115, 116, 26, 25, 18, 17, 10, 13, 116, 101, 115, 116, 95, 112, 97, 115, 115, 119,
            111, 114, 100, 16, 1, 160, 6, 4, 168, 6, 1, 34, 26, 10, 18, 10, 8, 10, 0, 160, 6, 4,
            168, 6, 1, 16, 2, 160, 6, 4, 168, 6, 1, 160, 6, 4, 168, 6, 1, 42, 15, 8, 10, 16, 128,
            80, 24, 128, 160, 1, 160, 6, 4, 168, 6, 1, 50, 15, 8, 1, 18, 5, 114, 111, 108, 101, 49,
            160, 6, 4, 168, 6, 1, 160, 6, 4, 168, 6, 1,
        ];
        let p: pb::UserInfo =
            common_protos::prost::Message::decode(user_info_v4.as_slice()).map_err(print_err)?;
        let got = mt::UserInfo::from_pb(p).map_err(print_err)?;
        let want = test_user_info();
        assert_eq!(want, got);
    }

    // UserInfo is loadable
    {
        let user_info_v1: Vec<u8> = vec![
            10, 9, 116, 101, 115, 116, 95, 117, 115, 101, 114, 18, 9, 108, 111, 99, 97, 108, 104,
            111, 115, 116, 26, 22, 18, 17, 10, 13, 116, 101, 115, 116, 95, 112, 97, 115, 115, 119,
            111, 114, 100, 16, 1, 160, 6, 1, 34, 17, 10, 12, 10, 5, 10, 0, 160, 6, 1, 16, 2, 160,
            6, 1, 160, 6, 1, 42, 12, 8, 10, 16, 128, 80, 24, 128, 160, 1, 160, 6, 1, 50, 5, 8, 1,
            160, 6, 1, 160, 6, 1,
        ];
        let p: pb::UserInfo =
            common_protos::prost::Message::decode(user_info_v1.as_slice()).map_err(print_err)?;
        let got = mt::UserInfo::from_pb(p).map_err(print_err)?;
        println!("got: {:?}", got);
        assert_eq!(got.name, "test_user".to_string());
        assert_eq!(got.option.default_role().clone(), None);
    }

    {
        let user_info_v3: Vec<u8> = vec![
            10, 9, 116, 101, 115, 116, 95, 117, 115, 101, 114, 18, 9, 108, 111, 99, 97, 108, 104,
            111, 115, 116, 26, 25, 18, 17, 10, 13, 116, 101, 115, 116, 95, 112, 97, 115, 115, 119,
            111, 114, 100, 16, 1, 160, 6, 3, 168, 6, 1, 34, 26, 10, 18, 10, 8, 10, 0, 160, 6, 3,
            168, 6, 1, 16, 2, 160, 6, 3, 168, 6, 1, 160, 6, 3, 168, 6, 1, 42, 15, 8, 10, 16, 128,
            80, 24, 128, 160, 1, 160, 6, 3, 168, 6, 1, 50, 15, 8, 1, 18, 5, 114, 111, 108, 101, 49,
            160, 6, 3, 168, 6, 1, 160, 6, 3, 168, 6, 1,
        ];
        let p: pb::UserInfo =
            common_protos::prost::Message::decode(user_info_v3.as_slice()).map_err(print_err)?;
        let got = mt::UserInfo::from_pb(p).map_err(print_err)?;
        let want = test_user_info();
        assert_eq!(want, got);
    }

    {
        // a legacy UserInfo with ConfigReload flag set, running on S3 service
        let user_info_v3: Vec<u8> = vec![
            10, 9, 116, 101, 115, 116, 95, 117, 115, 101, 114, 18, 9, 108, 111, 99, 97, 108, 104,
            111, 115, 116, 26, 25, 18, 17, 10, 13, 116, 101, 115, 116, 95, 112, 97, 115, 115, 119,
            111, 114, 100, 16, 1, 160, 6, 3, 168, 6, 1, 34, 26, 10, 18, 10, 8, 10, 0, 160, 6, 3,
            168, 6, 1, 16, 2, 160, 6, 3, 168, 6, 1, 160, 6, 3, 168, 6, 1, 42, 15, 8, 10, 16, 128,
            80, 24, 128, 160, 1, 160, 6, 3, 168, 6, 1, 50, 15, 8, 2, 18, 5, 114, 111, 108, 101, 49,
            160, 6, 3, 168, 6, 1, 160, 6, 3, 168, 6, 1,
        ];
        let p: pb::UserInfo =
            common_protos::prost::Message::decode(user_info_v3.as_slice()).map_err(print_err)?;
        let got = mt::UserInfo::from_pb(p).map_err(print_err)?;
        assert!(got.option.flags().is_empty());
    }

    // UserStage is loadable
    {
        // Stage service running on local filesystem
        let fs_stage_info_v4: Vec<u8> = vec![
            10, 17, 102, 115, 58, 47, 47, 100, 105, 114, 47, 116, 111, 47, 102, 105, 108, 101, 115,
            26, 25, 10, 23, 18, 21, 10, 13, 47, 100, 105, 114, 47, 116, 111, 47, 102, 105, 108,
            101, 115, 160, 6, 4, 168, 6, 1, 34, 20, 8, 1, 16, 128, 8, 26, 1, 124, 34, 2, 47, 47,
            40, 2, 160, 6, 4, 168, 6, 1, 42, 8, 10, 3, 32, 154, 5, 16, 142, 8, 50, 4, 116, 101,
            115, 116, 160, 6, 4, 168, 6, 1,
        ];
        let p: pb::UserStageInfo =
            common_protos::prost::Message::decode(fs_stage_info_v4.as_slice())
                .map_err(print_err)?;

        let got = mt::UserStageInfo::from_pb(p).map_err(print_err)?;

        let want = test_fs_stage_info();

        assert_eq!(want, got);
    }

    {
        // Stage service running on S3
        let s3_stage_info_v4: Vec<u8> = vec![
            10, 24, 115, 51, 58, 47, 47, 109, 121, 98, 117, 99, 107, 101, 116, 47, 100, 97, 116,
            97, 47, 102, 105, 108, 101, 115, 16, 1, 26, 100, 10, 98, 10, 96, 18, 24, 104, 116, 116,
            112, 115, 58, 47, 47, 115, 51, 46, 97, 109, 97, 122, 111, 110, 97, 119, 115, 46, 99,
            111, 109, 26, 9, 109, 121, 95, 107, 101, 121, 95, 105, 100, 34, 13, 109, 121, 95, 115,
            101, 99, 114, 101, 116, 95, 107, 101, 121, 42, 8, 109, 121, 98, 117, 99, 107, 101, 116,
            50, 11, 47, 100, 97, 116, 97, 47, 102, 105, 108, 101, 115, 58, 13, 109, 121, 95, 109,
            97, 115, 116, 101, 114, 95, 107, 101, 121, 160, 6, 4, 168, 6, 1, 34, 20, 8, 1, 16, 128,
            8, 26, 1, 124, 34, 2, 47, 47, 40, 2, 160, 6, 4, 168, 6, 1, 42, 8, 10, 3, 32, 154, 5,
            16, 142, 8, 50, 4, 116, 101, 115, 116, 160, 6, 4, 168, 6, 1,
        ];

        let p: pb::UserStageInfo =
            common_protos::prost::Message::decode(s3_stage_info_v4.as_slice())
                .map_err(print_err)?;

        let got = mt::UserStageInfo::from_pb(p).map_err(print_err)?;

        let want = test_s3_stage_info();

        assert_eq!(want, got);
    }

    {
        // Stage on Google Cloud Storage, supported on version >= 4
        let gcs_stage_info_v4 = vec![
            10, 26, 103, 99, 115, 58, 47, 47, 109, 121, 95, 98, 117, 99, 107, 101, 116, 47, 100,
            97, 116, 97, 47, 102, 105, 108, 101, 115, 16, 1, 26, 81, 10, 79, 26, 77, 10, 30, 104,
            116, 116, 112, 115, 58, 47, 47, 115, 116, 111, 114, 97, 103, 101, 46, 103, 111, 111,
            103, 108, 101, 97, 112, 105, 115, 46, 99, 111, 109, 18, 9, 109, 121, 95, 98, 117, 99,
            107, 101, 116, 26, 11, 47, 100, 97, 116, 97, 47, 102, 105, 108, 101, 115, 34, 13, 109,
            121, 95, 99, 114, 101, 100, 101, 110, 116, 105, 97, 108, 160, 6, 4, 168, 6, 1, 34, 20,
            8, 1, 16, 128, 8, 26, 1, 124, 34, 2, 47, 47, 40, 2, 160, 6, 4, 168, 6, 1, 42, 8, 10, 3,
            32, 154, 5, 16, 142, 8, 50, 4, 116, 101, 115, 116, 160, 6, 4, 168, 6, 1,
        ];

        let p: pb::UserStageInfo =
            common_protos::prost::Message::decode(gcs_stage_info_v4.as_slice())
                .map_err(print_err)?;

        let got = mt::UserStageInfo::from_pb(p).map_err(print_err)?;

        let want = test_gcs_stage_info();

        assert_eq!(want, got);
    }

    {
        // legacy user stage info, running on S3 service
        let user_stage_info_v1: Vec<u8> = vec![
            10, 24, 115, 51, 58, 47, 47, 109, 121, 98, 117, 99, 107, 101, 116, 47, 100, 97, 116,
            97, 47, 102, 105, 108, 101, 115, 16, 1, 26, 97, 10, 95, 10, 93, 18, 24, 104, 116, 116,
            112, 115, 58, 47, 47, 115, 51, 46, 97, 109, 97, 122, 111, 110, 97, 119, 115, 46, 99,
            111, 109, 26, 9, 109, 121, 95, 107, 101, 121, 95, 105, 100, 34, 13, 109, 121, 95, 115,
            101, 99, 114, 101, 116, 95, 107, 101, 121, 42, 8, 109, 121, 98, 117, 99, 107, 101, 116,
            50, 11, 47, 100, 97, 116, 97, 47, 102, 105, 108, 101, 115, 58, 13, 109, 121, 95, 109,
            97, 115, 116, 101, 114, 95, 107, 101, 121, 160, 6, 1, 34, 17, 8, 1, 16, 128, 8, 26, 1,
            124, 34, 2, 47, 47, 40, 2, 160, 6, 1, 42, 8, 10, 3, 32, 154, 5, 16, 142, 8, 50, 4, 116,
            101, 115, 116, 160, 6, 1,
        ];

        let p: pb::UserStageInfo =
            common_protos::prost::Message::decode(user_stage_info_v1.as_slice())
                .map_err(print_err)?;

        let got = mt::UserStageInfo::from_pb(p).map_err(print_err)?;

        println!("got stage: {:?}", got);

        let want = test_s3_stage_info();

        assert_eq!(want, got);
    }

    Ok(())
}

fn print_err<T: Debug>(e: T) -> T {
    eprintln!("Error: {:?}", e);
    e
}
