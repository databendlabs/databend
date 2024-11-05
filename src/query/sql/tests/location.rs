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

#![allow(clippy::uninlined_format_args)]

mod optimizer;

use std::collections::BTreeMap;

use anyhow::Result;
use databend_common_ast::ast::UriLocation;
use databend_common_base::base::tokio;
use databend_common_base::base::GlobalInstance;
use databend_common_config::GlobalConfig;
use databend_common_config::InnerConfig;
use databend_common_meta_app::storage::StorageFsConfig;
// use databend_common_storage::StorageFtpConfig;
use databend_common_meta_app::storage::StorageGcsConfig;
use databend_common_meta_app::storage::StorageHttpConfig;
use databend_common_meta_app::storage::StorageIpfsConfig;
use databend_common_meta_app::storage::StorageOssConfig;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::storage::StorageS3Config;
use databend_common_meta_app::storage::StorageWebhdfsConfig;
use databend_common_meta_app::storage::STORAGE_GCS_DEFAULT_ENDPOINT;
use databend_common_meta_app::storage::STORAGE_IPFS_DEFAULT_ENDPOINT;
use databend_common_meta_app::storage::STORAGE_S3_DEFAULT_ENDPOINT;
use databend_common_sql::planner::binder::parse_uri_location;

#[tokio::test]
async fn test_parse_uri_location() -> Result<()> {
    let thread_name = std::thread::current()
        .name()
        .map(ToString::to_string)
        .expect("thread should has a name");

    GlobalInstance::init_testing(&thread_name);
    GlobalConfig::init(&InnerConfig::default())?;

    let cases = vec![
        (
            "secure scheme by default",
            UriLocation::new(
                "ipfs".to_string(),
                "".to_string(),
                "too-naive".to_string(),
                "".to_string(),
                vec![("endpoint_url", "ipfs.filebase.io")]
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect::<BTreeMap<String, String>>(),
            ),
            (
                StorageParams::Ipfs(StorageIpfsConfig {
                    endpoint_url: "https://ipfs.filebase.io".to_string(),
                    root: "/ipfs/".to_string(),
                }),
                "too-naive".to_string(),
            ),
        ),
        (
            "oss location",
            UriLocation::new(
                "oss".to_string(),
                "zhen".to_string(),
                "/highest/".to_string(),
                "".to_string(),
                vec![
                    ("endpoint_url", "https://oss-cn-litang.example.com"),
                    ("access_key_id", "dzin"),
                    ("access_key_secret", "p=ear1"),
                ]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<BTreeMap<String, String>>(),
            ),
            (
                StorageParams::Oss(StorageOssConfig {
                    endpoint_url: "https://oss-cn-litang.example.com".to_string(),
                    presign_endpoint_url: "".to_string(),
                    root: "/highest/".to_string(),
                    server_side_encryption: "".to_string(),
                    bucket: "zhen".to_string(),
                    access_key_id: "dzin".to_string(),
                    access_key_secret: "p=ear1".to_string(),
                    server_side_encryption_key_id: "".to_string(),
                }),
                "/".to_string(),
            ),
        ),
        // (
        //     "ftps location",
        //     UriLocation {
        //         protocol: "ftps".to_string(),
        //         name: "too-simple:1926".to_string(),
        //         path: "/".to_string(),
        //         connection: vec![("username", "user"), ("password", "pwd")]
        //             .into_iter()
        //             .map(|(k, v)| (k.to_string(), v.to_string()))
        //             .collect::<BTreeMap<String, String>>(),
        //     },
        //     (
        //         StorageParams::Ftp(StorageFtpConfig {
        //             endpoint: "ftps://too-simple:1926".to_string(),
        //             root: "/".to_string(),
        //             username: "user".to_string(),
        //             password: "pwd".to_string(),
        //         }),
        //         "/".to_string(),
        //     ),
        // ),
        (
            "ipfs-default-endpoint",
            UriLocation::new(
                "ipfs".to_string(),
                "".to_string(),
                "too-simple".to_string(),
                "".to_string(),
                BTreeMap::new(),
            ),
            (
                StorageParams::Ipfs(StorageIpfsConfig {
                    endpoint_url: STORAGE_IPFS_DEFAULT_ENDPOINT.to_string(),
                    root: "/ipfs/".to_string(),
                }),
                "too-simple".to_string(),
            ),
        ),
        (
            "ipfs-change-endpoint",
            UriLocation::new(
                "ipfs".to_string(),
                "".to_string(),
                "too-naive".to_string(),
                "".to_string(),
                vec![("endpoint_url", "https://ipfs.filebase.io")]
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect::<BTreeMap<String, String>>(),
            ),
            (
                StorageParams::Ipfs(StorageIpfsConfig {
                    endpoint_url: "https://ipfs.filebase.io".to_string(),
                    root: "/ipfs/".to_string(),
                }),
                "too-naive".to_string(),
            ),
        ),
        (
            "s3_with_access_key_id",
            UriLocation::new(
                "s3".to_string(),
                "test".to_string(),
                "/tmp/".to_string(),
                "".to_string(),
                [
                    ("access_key_id", "access_key_id"),
                    ("secret_access_key", "secret_access_key"),
                    ("session_token", "session_token"),
                    ("region", "us-east-2"),
                ]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<BTreeMap<String, String>>(),
            ),
            (
                StorageParams::S3(StorageS3Config {
                    endpoint_url: STORAGE_S3_DEFAULT_ENDPOINT.to_string(),
                    region: "us-east-2".to_string(),
                    bucket: "test".to_string(),
                    access_key_id: "access_key_id".to_string(),
                    secret_access_key: "secret_access_key".to_string(),
                    security_token: "session_token".to_string(),
                    master_key: "".to_string(),
                    root: "/tmp/".to_string(),
                    disable_credential_loader: true,
                    enable_virtual_host_style: false,
                    role_arn: "".to_string(),
                    external_id: "".to_string(),
                }),
                "/".to_string(),
            ),
        ),
        (
            "s3_with_aws_key_id",
            UriLocation::new(
                "s3".to_string(),
                "test".to_string(),
                "/tmp/".to_string(),
                "".to_string(),
                [
                    ("aws_key_id", "access_key_id"),
                    ("aws_secret_key", "secret_access_key"),
                    ("session_token", "security_token"),
                    ("region", "us-east-2"),
                ]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<BTreeMap<String, String>>(),
            ),
            (
                StorageParams::S3(StorageS3Config {
                    endpoint_url: STORAGE_S3_DEFAULT_ENDPOINT.to_string(),
                    region: "us-east-2".to_string(),
                    bucket: "test".to_string(),
                    access_key_id: "access_key_id".to_string(),
                    secret_access_key: "secret_access_key".to_string(),
                    security_token: "security_token".to_string(),
                    master_key: "".to_string(),
                    root: "/tmp/".to_string(),
                    disable_credential_loader: true,
                    enable_virtual_host_style: false,
                    role_arn: "".to_string(),
                    external_id: "".to_string(),
                }),
                "/".to_string(),
            ),
        ),
        (
            "s3_with_aws_token",
            UriLocation::new(
                "s3".to_string(),
                "test".to_string(),
                "/tmp/".to_string(),
                "".to_string(),
                [
                    ("aws_key_id", "access_key_id"),
                    ("aws_secret_key", "secret_access_key"),
                    ("aws_token", "security_token"),
                    ("region", "us-east-2"),
                ]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<BTreeMap<String, String>>(),
            ),
            (
                StorageParams::S3(StorageS3Config {
                    endpoint_url: STORAGE_S3_DEFAULT_ENDPOINT.to_string(),
                    region: "us-east-2".to_string(),
                    bucket: "test".to_string(),
                    access_key_id: "access_key_id".to_string(),
                    secret_access_key: "secret_access_key".to_string(),
                    security_token: "security_token".to_string(),
                    master_key: "".to_string(),
                    root: "/tmp/".to_string(),
                    disable_credential_loader: true,
                    enable_virtual_host_style: false,
                    role_arn: "".to_string(),
                    external_id: "".to_string(),
                }),
                "/".to_string(),
            ),
        ),
        (
            "s3_with_role_arn",
            UriLocation::new(
                "s3".to_string(),
                "test".to_string(),
                "/tmp/".to_string(),
                "".to_string(),
                [("role_arn", "aws::iam::xxxx"), ("region", "us-east-2")]
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect::<BTreeMap<String, String>>(),
            ),
            (
                StorageParams::S3(StorageS3Config {
                    endpoint_url: STORAGE_S3_DEFAULT_ENDPOINT.to_string(),
                    region: "us-east-2".to_string(),
                    bucket: "test".to_string(),
                    access_key_id: "".to_string(),
                    secret_access_key: "".to_string(),
                    security_token: "".to_string(),
                    master_key: "".to_string(),
                    root: "/tmp/".to_string(),
                    disable_credential_loader: false,
                    enable_virtual_host_style: false,
                    role_arn: "aws::iam::xxxx".to_string(),
                    external_id: "".to_string(),
                }),
                "/".to_string(),
            ),
        ),
        (
            "fs",
            UriLocation::new(
                "fs".to_string(),
                "".to_string(),
                "/tmp/".to_string(),
                "".to_string(),
                BTreeMap::default(),
            ),
            (
                StorageParams::Fs(StorageFsConfig {
                    root: "/tmp/".to_string(),
                }),
                "/".to_string(),
            ),
        ),
        (
            "gcs_with_credential",
            UriLocation::new(
                "gcs".to_string(),
                "example".to_string(),
                "/tmp/".to_string(),
                "".to_string(),
                vec![("credential", "gcs.credential")]
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect::<BTreeMap<String, String>>(),
            ),
            (
                StorageParams::Gcs(StorageGcsConfig {
                    endpoint_url: STORAGE_GCS_DEFAULT_ENDPOINT.to_string(),
                    bucket: "example".to_string(),
                    root: "/tmp/".to_string(),
                    credential: "gcs.credential".to_string(),
                }),
                "/".to_string(),
            ),
        ),
        (
            "http_without_glob",
            UriLocation::new(
                "https".to_string(),
                "example.com".to_string(),
                "/tmp.csv".to_string(),
                "".to_string(),
                BTreeMap::default(),
            ),
            (
                StorageParams::Http(StorageHttpConfig {
                    endpoint_url: "https://example.com".to_string(),
                    paths: ["/tmp.csv"].iter().map(|v| v.to_string()).collect(),
                }),
                "/".to_string(),
            ),
        ),
        (
            "http_with_set_glob",
            UriLocation::new(
                "https".to_string(),
                "example.com".to_string(),
                "/tmp-{a,b,c}.csv".to_string(),
                "".to_string(),
                BTreeMap::default(),
            ),
            (
                StorageParams::Http(StorageHttpConfig {
                    endpoint_url: "https://example.com".to_string(),
                    paths: ["/tmp-a.csv", "/tmp-b.csv", "/tmp-c.csv"]
                        .iter()
                        .map(|v| v.to_string())
                        .collect(),
                }),
                "/".to_string(),
            ),
        ),
        (
            "http_with_range_glob",
            UriLocation::new(
                "https".to_string(),
                "example.com".to_string(),
                "/tmp-[11-15].csv".to_string(),
                "".to_string(),
                BTreeMap::default(),
            ),
            (
                StorageParams::Http(StorageHttpConfig {
                    endpoint_url: "https://example.com".to_string(),
                    paths: [
                        "/tmp-11.csv",
                        "/tmp-12.csv",
                        "/tmp-13.csv",
                        "/tmp-14.csv",
                        "/tmp-15.csv",
                    ]
                    .iter()
                    .map(|v| v.to_string())
                    .collect(),
                }),
                "/".to_string(),
            ),
        ),
        (
            "webhdfs",
            UriLocation::new(
                "webhdfs".to_string(),
                "example.com".to_string(),
                "/path/to/dir/".to_string(),
                "".to_string(),
                vec![("https", "TrUE"), ("delegation", "databendthebest")]
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect::<BTreeMap<_, _>>(),
            ),
            (
                StorageParams::Webhdfs(StorageWebhdfsConfig {
                    root: "/path/to/dir/".to_string(),
                    endpoint_url: "https://example.com".to_string(),
                    delegation: "databendthebest".to_string(),
                }),
                "/".to_string(),
            ),
        ),
    ];

    for (name, mut input, expected) in cases {
        let actual = parse_uri_location(&mut input, None).await?;
        assert_eq!(expected, actual, "{}", name);
    }

    Ok(())
}
