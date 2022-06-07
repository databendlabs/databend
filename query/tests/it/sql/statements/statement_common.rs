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

use std::collections::BTreeMap;

use common_base::base::tokio;
use common_exception::Result;
use common_io::prelude::StorageParams;
use common_io::prelude::StorageS3Config;
use common_meta_types::StageParams;
use common_meta_types::StageType;
use common_meta_types::UserStageInfo;
use databend_query::sql::statements::parse_stage_location;
use databend_query::sql::statements::parse_uri_location;
use pretty_assertions::assert_eq;

use crate::tests::create_query_context;

#[tokio::test]
async fn test_parse_stage_location_internal() -> Result<()> {
    let ctx = create_query_context().await?;
    let user_stage_info = UserStageInfo {
        stage_name: "test".to_string(),
        stage_type: StageType::Internal,
        ..Default::default()
    };
    // Add an internal stage for testing.
    ctx.get_user_manager()
        .add_stage(&ctx.get_tenant(), user_stage_info.clone(), false)
        .await?;

    // Cases are in the format: `name`, `input location`, `output path`.
    let cases = vec![
        ("root", "@test", "/stage/test/"),
        ("root with trailing /", "@test/", "/stage/test/"),
        (
            "file path",
            "@test/path/to/file",
            "/stage/test/path/to/file",
        ),
        ("dir path", "@test/path/to/dir/", "/stage/test/path/to/dir/"),
    ];

    for (name, input, expected) in cases {
        let (stage, path) = parse_stage_location(&ctx, input).await?;

        assert_eq!(stage, user_stage_info, "{}", name);
        assert_eq!(path, expected, "{}", name);
    }

    Ok(())
}

#[tokio::test]
async fn test_parse_stage_location_external() -> Result<()> {
    let ctx = create_query_context().await?;
    let user_stage_info = UserStageInfo {
        stage_name: "test".to_string(),
        stage_type: StageType::External,
        ..Default::default()
    };
    // Add an external stage for testing.
    ctx.get_user_manager()
        .add_stage(&ctx.get_tenant(), user_stage_info.clone(), false)
        .await?;

    // Cases are in the format: `name`, `input location`, `output path`.
    let cases = vec![
        ("root", "@test", "/"),
        ("root with trailing /", "@test/", "/"),
        ("file path", "@test/path/to/file", "/path/to/file"),
        ("dir path", "@test/path/to/dir/", "/path/to/dir/"),
    ];

    for (name, input, expected) in cases {
        let (stage, path) = parse_stage_location(&ctx, input).await?;

        assert_eq!(stage, user_stage_info, "{}", name);
        assert_eq!(path, expected, "{}", name);
    }

    Ok(())
}

#[test]
fn test_parse_uri_location() -> Result<()> {
    // Cases are in the format:
    // - `name`
    // - `input location`
    // - `input credential option`
    // - `input encryption option`
    // - `expected user stage info`
    // - `expected path`
    let cases = vec![
        (
            "s3 root",
            "s3://test",
            BTreeMap::from([
                ("aws_key_id".into(), "test_aws_key_id".into()),
                ("aws_secret_key".into(), "test_aws_secret_key".into()),
            ]),
            BTreeMap::from([("master_key".into(), "test_master_key".into())]),
            UserStageInfo {
                stage_name: "s3://test".to_string(),
                stage_type: StageType::External,
                stage_params: StageParams {
                    storage: StorageParams::S3(StorageS3Config {
                        bucket: "test".to_string(),
                        access_key_id: "test_aws_key_id".to_string(),
                        secret_access_key: "test_aws_secret_key".to_string(),
                        master_key: "test_master_key".to_string(),
                        root: "/".to_string(),
                        disable_credential_loader: true,
                        ..Default::default()
                    }),
                },
                ..Default::default()
            },
            "/",
        ),
        (
            "s3 root with suffix",
            "s3://test/",
            BTreeMap::from([
                ("aws_key_id".into(), "test_aws_key_id".into()),
                ("aws_secret_key".into(), "test_aws_secret_key".into()),
            ]),
            BTreeMap::from([("master_key".into(), "test_master_key".into())]),
            UserStageInfo {
                stage_name: "s3://test/".to_string(),
                stage_type: StageType::External,
                stage_params: StageParams {
                    storage: StorageParams::S3(StorageS3Config {
                        bucket: "test".to_string(),
                        access_key_id: "test_aws_key_id".to_string(),
                        secret_access_key: "test_aws_secret_key".to_string(),
                        master_key: "test_master_key".to_string(),
                        root: "/".to_string(),
                        disable_credential_loader: true,
                        ..Default::default()
                    }),
                },
                ..Default::default()
            },
            "/",
        ),
        (
            "s3 file path",
            "s3://test/path/to/file",
            BTreeMap::from([
                ("aws_key_id".into(), "test_aws_key_id".into()),
                ("aws_secret_key".into(), "test_aws_secret_key".into()),
            ]),
            BTreeMap::from([("master_key".into(), "test_master_key".into())]),
            UserStageInfo {
                stage_name: "s3://test/path/to/file".to_string(),
                stage_type: StageType::External,
                stage_params: StageParams {
                    storage: StorageParams::S3(StorageS3Config {
                        bucket: "test".to_string(),
                        access_key_id: "test_aws_key_id".to_string(),
                        secret_access_key: "test_aws_secret_key".to_string(),
                        master_key: "test_master_key".to_string(),
                        root: "/".to_string(),
                        disable_credential_loader: true,
                        ..Default::default()
                    }),
                },
                ..Default::default()
            },
            "/path/to/file",
        ),
        (
            "s3 dir path",
            "s3://test/path/to/dir/",
            BTreeMap::from([
                ("aws_key_id".into(), "test_aws_key_id".into()),
                ("aws_secret_key".into(), "test_aws_secret_key".into()),
            ]),
            BTreeMap::from([("master_key".into(), "test_master_key".into())]),
            UserStageInfo {
                stage_name: "s3://test/path/to/dir/".to_string(),
                stage_type: StageType::External,
                stage_params: StageParams {
                    storage: StorageParams::S3(StorageS3Config {
                        bucket: "test".to_string(),
                        access_key_id: "test_aws_key_id".to_string(),
                        secret_access_key: "test_aws_secret_key".to_string(),
                        master_key: "test_master_key".to_string(),
                        root: "/path/to/dir/".to_string(),
                        disable_credential_loader: true,
                        ..Default::default()
                    }),
                },
                ..Default::default()
            },
            "/",
        ),
    ];

    for (name, input_location, input_credential, input_encryption, expected_stage, expected_path) in
        cases
    {
        let (stage, path) =
            parse_uri_location(input_location, &input_credential, &input_encryption)?;

        assert_eq!(stage, expected_stage, "{}", name);
        assert_eq!(path, expected_path, "{}", name);
    }

    Ok(())
}
