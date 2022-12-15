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

use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;

use anyhow::anyhow;
use common_ast::ast::UriLocation;
use common_storage::StorageAzblobConfig;
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
use opendal::Scheme;
use percent_encoding::percent_decode_str;

/// secure_omission will fix omitted endpoint url schemes into 'https://'
#[inline]
fn secure_omission(endpoint: String) -> String {
    // checking with starts_with() should be enough here
    if !endpoint.starts_with("https://") && !endpoint.starts_with("http://") {
        format!("https://{}", endpoint)
    } else {
        endpoint
    }
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
        Scheme::Azblob => {
            let endpoint = l.connection.get("endpoint_url").cloned().ok_or_else(|| {
                Error::new(
                    ErrorKind::InvalidInput,
                    anyhow!("endpoint_url is required for storage azblob"),
                )
            })?;
            StorageParams::Azblob(StorageAzblobConfig {
                endpoint_url: secure_omission(endpoint),
                container: l.name.to_string(),
                account_name: l
                    .connection
                    .get("account_name")
                    .cloned()
                    .unwrap_or_default(),
                account_key: l.connection.get("account_key").cloned().unwrap_or_default(),
                root: root.to_string(),
            })
        }
        Scheme::Ftp => StorageParams::Ftp(StorageFtpConfig {
            endpoint: if !l.protocol.is_empty() {
                format!("{}://{}", l.protocol, l.name)
            } else {
                // no protocol prefix will be seen as using FTPS connection
                format!("ftps://{}", l.name)
            },
            root: root.to_string(),
            username: l.connection.get("username").cloned().unwrap_or_default(),
            password: l.connection.get("password").cloned().unwrap_or_default(),
        }),
        Scheme::Gcs => {
            let endpoint = l
                .connection
                .get("endpoint_url")
                .cloned()
                .unwrap_or_else(|| STORAGE_GCS_DEFAULT_ENDPOINT.to_string());
            StorageParams::Gcs(StorageGcsConfig {
                endpoint_url: secure_omission(endpoint),
                bucket: l.name.clone(),
                root: l.path.clone(),
                credential: l.connection.get("credential").cloned().unwrap_or_default(),
            })
        }
        #[cfg(feature = "storage-hdfs")]
        Scheme::Hdfs => StorageParams::Hdfs(crate::StorageHdfsConfig {
            name_node: l
                .connection
                .get("name_node")
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::InvalidInput,
                        anyhow!("name_node is required for storage hdfs"),
                    )
                })?
                .to_string(),
            root: root.to_string(),
        }),
        Scheme::Ipfs => {
            let endpoint = l
                .connection
                .get("endpoint_url")
                .cloned()
                .unwrap_or_else(|| STORAGE_IPFS_DEFAULT_ENDPOINT.to_string());
            StorageParams::Ipfs(StorageIpfsConfig {
                endpoint_url: secure_omission(endpoint),
                root: "/ipfs/".to_string() + l.name.as_str(),
            })
        }
        Scheme::S3 => {
            let endpoint = l
                .connection
                .get("endpoint_url")
                .cloned()
                .unwrap_or_else(|| STORAGE_S3_DEFAULT_ENDPOINT.to_string());
            StorageParams::S3(StorageS3Config {
                endpoint_url: secure_omission(endpoint),
                region: l.connection.get("region").cloned().unwrap_or_default(),
                bucket: l.name.to_string(),
                access_key_id: l
                    .connection
                    .get("access_key_id")
                    .or_else(|| l.connection.get("aws_key_id"))
                    .cloned()
                    .unwrap_or_default(),
                secret_access_key: l
                    .connection
                    .get("secret_access_key")
                    .or_else(|| l.connection.get("aws_secret_key"))
                    .cloned()
                    .unwrap_or_default(),
                security_token: l
                    .connection
                    .get("session_token")
                    .or_else(|| l.connection.get("aws_token"))
                    .or_else(|| l.connection.get("security_token"))
                    .cloned()
                    .unwrap_or_default(),
                master_key: l.connection.get("master_key").cloned().unwrap_or_default(),
                root: root.to_string(),
                disable_credential_loader: true,
                enable_virtual_host_style: l
                    .connection
                    .get("enable_virtual_host_style")
                    .cloned()
                    .unwrap_or_else(|| "false".to_string())
                    .parse()
                    .map_err(|err| {
                        Error::new(
                            ErrorKind::InvalidInput,
                            anyhow!("value for enable_virtual_host_style is invalid: {err:?}"),
                        )
                    })?,
                role_arn: l
                    .connection
                    .get("role_arn")
                    .or_else(|| l.connection.get("aws_role_arn"))
                    .cloned()
                    .unwrap_or_default(),
                external_id: l
                    .connection
                    .get("external_id")
                    .or_else(|| l.connection.get("aws_external_id"))
                    .cloned()
                    .unwrap_or_default(),
            })
        }
        Scheme::Oss => {
            let endpoint = l
                .connection
                .get("endpoint_url")
                .cloned()
                .map(secure_omission)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::InvalidInput,
                        anyhow!("endpoint_url is required for storage oss"),
                    )
                })?;
            StorageParams::Oss(StorageOssConfig {
                endpoint_url: endpoint,
                bucket: l.name.to_string(),
                access_key_id: l
                    .connection
                    .get("access_key_id")
                    .cloned()
                    .unwrap_or_default(),
                access_key_secret: l
                    .connection
                    .get("access_key_secret")
                    .cloned()
                    .unwrap_or_default(),
                root: root.to_string(),
            })
        }
        Scheme::Http => {
            // Make sure path has been percent decoded before parse pattern.
            let path = percent_decode_str(&l.path).decode_utf8_lossy();
            let cfg = StorageHttpConfig {
                endpoint_url: format!("{}://{}", l.protocol, l.name),
                paths: globiter::Pattern::parse(&path)
                    .map_err(|err| {
                        Error::new(
                            ErrorKind::InvalidInput,
                            anyhow!("input path is not a valid glob: {err:?}"),
                        )
                    })?
                    .iter()
                    .collect(),
            };

            // HTTP is special that we don't support dir, always return / instead.
            return Ok((StorageParams::Http(cfg), "/".to_string()));
        }
        Scheme::Fs => {
            let cfg = StorageFsConfig {
                root: root.to_string(),
            };
            StorageParams::Fs(cfg)
        }
        v => {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                anyhow!("{v} is not allowed to be used as uri location"),
            ));
        }
    };

    Ok((sp, path.to_string()))
}
