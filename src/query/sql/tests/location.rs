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
use std::io::Result;

use common_ast::ast::UriLocation;
use common_sql::planner::binder::parse_uri_location;
use common_storage::StorageFsConfig;
use common_storage::StorageFtpConfig;
use common_storage::StorageGcsConfig;
use common_storage::StorageHttpConfig;
use common_storage::StorageIpfsConfig;
use common_storage::StorageOssConfig;
use common_storage::StorageParams;
use common_storage::StorageS3Config;
use common_storage::STORAGE_GCS_DEFAULT_ENDPOINT;
use common_storage::STORAGE_IPFS_DEFAULT_ENDPOINT;
use common_storage::STORAGE_S3_DEFAULT_ENDPOINT;

#[test]
fn test_parse_uri_location() -> Result<()> {
    let cases = vec![
        (
            "secure scheme by default",
            UriLocation {
                protocol: "ipfs".to_string(),
                name: "too-naive".to_string(),
                path: "/".to_string(),
                connection: vec![("endpoint_url", "ipfs.filebase.io")]
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            },
            (
                StorageParams::Ipfs(StorageIpfsConfig {
                    endpoint_url: "https://ipfs.filebase.io".to_string(),
                    root: "/ipfs/too-naive".to_string(),
                }),
                "/".to_string(),
            ),
        ),
        (
            "oss location",
            UriLocation {
                protocol: "oss".to_string(),
                name: "zhen".to_string(),
                path: "/highest/".to_string(),
                connection: vec![
                    ("endpoint_url", "https://oss-cn-litang.example.com"),
                    ("access_key_id", "dzin"),
                    ("access_key_secret", "p=ear1"),
                ]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<BTreeMap<String, String>>(),
            },
            (
                StorageParams::Oss(StorageOssConfig {
                    endpoint_url: "https://oss-cn-litang.example.com".to_string(),
                    root: "/highest/".to_string(),
                    bucket: "zhen".to_string(),
                    access_key_id: "dzin".to_string(),
                    access_key_secret: "p=ear1".to_string(),
                }),
                "/".to_string(),
            ),
        ),
        (
            "ftps location",
            UriLocation {
                protocol: "ftps".to_string(),
                name: "too-simple:1926".to_string(),
                path: "/".to_string(),
                connection: vec![("username", "user"), ("password", "pwd")]
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect::<BTreeMap<String, String>>(),
            },
            (
                StorageParams::Ftp(StorageFtpConfig {
                    endpoint: "ftps://too-simple:1926".to_string(),
                    root: "/".to_string(),
                    username: "user".to_string(),
                    password: "pwd".to_string(),
                }),
                "/".to_string(),
            ),
        ),
        (
            "ipfs-default-endpoint",
            UriLocation {
                protocol: "ipfs".to_string(),
                name: "too-simple".to_string(),
                path: "/".to_string(),
                connection: BTreeMap::new(),
            },
            (
                StorageParams::Ipfs(StorageIpfsConfig {
                    endpoint_url: STORAGE_IPFS_DEFAULT_ENDPOINT.to_string(),
                    root: "/ipfs/too-simple".to_string(),
                }),
                "/".to_string(),
            ),
        ),
        (
            "ipfs-change-endpoint",
            UriLocation {
                protocol: "ipfs".to_string(),
                name: "too-naive".to_string(),
                path: "/".to_string(),
                connection: vec![("endpoint_url", "https://ipfs.filebase.io")]
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            },
            (
                StorageParams::Ipfs(StorageIpfsConfig {
                    endpoint_url: "https://ipfs.filebase.io".to_string(),
                    root: "/ipfs/too-naive".to_string(),
                }),
                "/".to_string(),
            ),
        ),
        (
            "s3_with_access_key_id",
            UriLocation {
                protocol: "s3".to_string(),
                name: "test".to_string(),
                path: "/tmp/".to_string(),
                connection: vec![
                    ("access_key_id", "access_key_id"),
                    ("secret_access_key", "secret_access_key"),
                    ("session_token", "session_token"),
                ]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            },
            (
                StorageParams::S3(StorageS3Config {
                    endpoint_url: STORAGE_S3_DEFAULT_ENDPOINT.to_string(),
                    region: "".to_string(),
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
            UriLocation {
                protocol: "s3".to_string(),
                name: "test".to_string(),
                path: "/tmp/".to_string(),
                connection: vec![
                    ("aws_key_id", "access_key_id"),
                    ("aws_secret_key", "secret_access_key"),
                    ("session_token", "security_token"),
                ]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            },
            (
                StorageParams::S3(StorageS3Config {
                    endpoint_url: STORAGE_S3_DEFAULT_ENDPOINT.to_string(),
                    region: "".to_string(),
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
            UriLocation {
                protocol: "s3".to_string(),
                name: "test".to_string(),
                path: "/tmp/".to_string(),
                connection: vec![
                    ("aws_key_id", "access_key_id"),
                    ("aws_secret_key", "secret_access_key"),
                    ("aws_token", "security_token"),
                ]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            },
            (
                StorageParams::S3(StorageS3Config {
                    endpoint_url: STORAGE_S3_DEFAULT_ENDPOINT.to_string(),
                    region: "".to_string(),
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
            UriLocation {
                protocol: "s3".to_string(),
                name: "test".to_string(),
                path: "/tmp/".to_string(),
                connection: vec![("role_arn", "aws::iam::xxxx")]
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            },
            (
                StorageParams::S3(StorageS3Config {
                    endpoint_url: STORAGE_S3_DEFAULT_ENDPOINT.to_string(),
                    region: "".to_string(),
                    bucket: "test".to_string(),
                    access_key_id: "".to_string(),
                    secret_access_key: "".to_string(),
                    security_token: "".to_string(),
                    master_key: "".to_string(),
                    root: "/tmp/".to_string(),
                    disable_credential_loader: true,
                    enable_virtual_host_style: false,
                    role_arn: "aws::iam::xxxx".to_string(),
                    external_id: "".to_string(),
                }),
                "/".to_string(),
            ),
        ),
        (
            "fs",
            UriLocation {
                protocol: "fs".to_string(),
                name: "".to_string(),
                path: "/tmp/".to_string(),
                connection: BTreeMap::default(),
            },
            (
                StorageParams::Fs(StorageFsConfig {
                    root: "/tmp/".to_string(),
                }),
                "/".to_string(),
            ),
        ),
        (
            "gcs_with_credential",
            UriLocation {
                protocol: "gcs".to_string(),
                name: "example".to_string(),
                path: "/tmp/".to_string(),
                connection: vec![("credential", "gcs.credential")]
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            },
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
            UriLocation {
                protocol: "https".to_string(),
                name: "example.com".to_string(),
                path: "/tmp.csv".to_string(),
                connection: BTreeMap::default(),
            },
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
            UriLocation {
                protocol: "https".to_string(),
                name: "example.com".to_string(),
                path: "/tmp-{a,b,c}.csv".to_string(),
                connection: BTreeMap::default(),
            },
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
            UriLocation {
                protocol: "https".to_string(),
                name: "example.com".to_string(),
                path: "/tmp-[11-15].csv".to_string(),
                connection: BTreeMap::default(),
            },
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
    ];

    for (name, input, expected) in cases {
        let actual = parse_uri_location(&input)?;
        assert_eq!(expected, actual, "{}", name);
    }

    Ok(())
}
