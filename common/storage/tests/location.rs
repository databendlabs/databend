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

use std::io::Result;

use common_storage::parse_uri_location;
use common_storage::StorageParams;
use common_storage::StorageS3Config;
use common_storage::UriLocation;
use common_storage::STORAGE_S3_DEFAULT_ENDPOINT;

#[test]
fn test_parse_uri_location() -> Result<()> {
    let cases = vec![
        (
            "s3_with_access_key_id",
            UriLocation {
                protocol: "s3".to_string(),
                name: "test".to_string(),
                path: "/tmp/".to_string(),
                endpoint_url: None,
                credentials: vec![
                    ("access_key_id", "access_key_id"),
                    ("secret_access_key", "secret_access_key"),
                ]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
                encryption: Default::default(),
            },
            (
                StorageParams::S3(StorageS3Config {
                    endpoint_url: STORAGE_S3_DEFAULT_ENDPOINT.to_string(),
                    region: "".to_string(),
                    bucket: "test".to_string(),
                    access_key_id: "access_key_id".to_string(),
                    secret_access_key: "secret_access_key".to_string(),
                    master_key: "".to_string(),
                    root: "/tmp/".to_string(),
                    disable_credential_loader: true,
                    enable_virtual_host_style: false,
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
                endpoint_url: None,
                credentials: vec![
                    ("aws_key_id", "access_key_id"),
                    ("aws_secret_key", "secret_access_key"),
                ]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
                encryption: Default::default(),
            },
            (
                StorageParams::S3(StorageS3Config {
                    endpoint_url: STORAGE_S3_DEFAULT_ENDPOINT.to_string(),
                    region: "".to_string(),
                    bucket: "test".to_string(),
                    access_key_id: "access_key_id".to_string(),
                    secret_access_key: "secret_access_key".to_string(),
                    master_key: "".to_string(),
                    root: "/tmp/".to_string(),
                    disable_credential_loader: true,
                    enable_virtual_host_style: false,
                }),
                "/".to_string(),
            ),
        ),
    ];

    for (name, input, expected) in cases {
        let actual = parse_uri_location(&input)?;
        assert_eq!(expected, actual, "{name}");
    }

    Ok(())
}
