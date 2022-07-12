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
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;

use anyhow::anyhow;
use opendal::Scheme;

use crate::config::STORAGE_S3_DEFAULT_ENDPOINT;
use crate::StorageAzblobConfig;
use crate::StorageParams;
use crate::StorageS3Config;

#[derive(Clone, Debug)]
pub struct UriLocation {
    pub protocol: String,
    pub name: String,
    pub path: String,
    pub endpoint_url: Option<String>,
    pub credentials: BTreeMap<String, String>,
    pub encryption: BTreeMap<String, String>,
}

/// parse_uri_location will parse given UriLocation into StorageParams and Path.
pub fn parse_uri_location(l: &UriLocation) -> Result<(StorageParams, String)> {
    // Path endswith `/` means it's a directory, otherwise it's a file.
    // If the path is a directory, we will use this path as root.
    // If the path is a file, we will use `/` as root (which is the default value)
    let (root, path) = if l.path.ends_with('/') {
        (l.path.as_str(), "/")
    } else {
        ("/", l.path.as_str())
    };

    let protocol = l.protocol.parse::<Scheme>()?;

    let sp = match protocol {
        Scheme::Azblob => StorageParams::Azblob(StorageAzblobConfig {
            endpoint_url: l.endpoint_url.clone().ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidInput,
                    anyhow!("endpoint_url is required for storage azblob"),
                )
            })?,
            container: l.name.to_string(),
            account_name: l
                .credentials
                .get("account_name")
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::InvalidInput,
                        anyhow!("account_name is required for storage azblob"),
                    )
                })?
                .to_string(),
            account_key: l
                .credentials
                .get("account_key")
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::InvalidInput,
                        anyhow!("account_name is required for storage azblob"),
                    )
                })?
                .to_string(),
            root: root.to_string(),
        }),
        #[cfg(feature = "storage-hdfs")]
        Scheme::Hdfs => StorageParams::Hdfs(crate::StorageHdfsConfig {
            name_node: l
                .endpoint_url
                .clone()
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::InvalidInput,
                        anyhow!("endpoint_url is required for storage hdfs"),
                    )
                })?
                .to_string(),
            root: root.to_string(),
        }),
        Scheme::S3 => StorageParams::S3(StorageS3Config {
            endpoint_url: l
                .endpoint_url
                .clone()
                .unwrap_or(STORAGE_S3_DEFAULT_ENDPOINT.to_string()),
            // TODO: support this options.
            region: "".to_string(),
            bucket: l.name.to_string(),
            access_key_id: l
                .credentials
                .get("access_key_id")
                .or_else(|| l.credentials.get("aws_key_id"))
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::InvalidInput,
                        anyhow!("access_key_id is required for storage s3"),
                    )
                })?
                .to_string(),
            secret_access_key: l
                .credentials
                .get("secret_access_key")
                .or_else(|| l.credentials.get("aws_secret_key"))
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::InvalidInput,
                        anyhow!("secret_access_key is required for storage s3"),
                    )
                })?
                .to_string(),
            master_key: l.encryption.get("master_key").cloned().unwrap_or_default(),
            root: root.to_string(),
            disable_credential_loader: true,
            // TODO: support this options.
            enable_virtual_host_style: false,
        }),
        v => {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                anyhow!("{v} is not allowed to be used as uri location"),
            ))
        }
    };

    Ok((sp, path.to_string()))
}
