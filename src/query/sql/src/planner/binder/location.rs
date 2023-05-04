// Copyright 2021 Datafuse Labs
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
use common_config::GlobalConfig;
use common_meta_app::storage::StorageAzblobConfig;
use common_meta_app::storage::StorageFsConfig;
use common_meta_app::storage::StorageGcsConfig;
use common_meta_app::storage::StorageHttpConfig;
use common_meta_app::storage::StorageIpfsConfig;
use common_meta_app::storage::StorageOssConfig;
use common_meta_app::storage::StorageParams;
use common_meta_app::storage::StorageS3Config;
use common_meta_app::storage::StorageWebhdfsConfig;
use common_meta_app::storage::STORAGE_GCS_DEFAULT_ENDPOINT;
use common_meta_app::storage::STORAGE_IPFS_DEFAULT_ENDPOINT;
use common_meta_app::storage::STORAGE_S3_DEFAULT_ENDPOINT;
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

fn parse_azure_params(l: &mut UriLocation, root: String) -> Result<StorageParams> {
    let endpoint = l.connection.get("endpoint_url").cloned().ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidInput,
            anyhow!("endpoint_url is required for storage azblob"),
        )
    })?;
    let sp = StorageParams::Azblob(StorageAzblobConfig {
        endpoint_url: secure_omission(endpoint),
        container: l.name.to_string(),
        account_name: l
            .connection
            .get("account_name")
            .cloned()
            .unwrap_or_default(),
        account_key: l.connection.get("account_key").cloned().unwrap_or_default(),
        root,
    });

    l.connection.check()?;

    Ok(sp)
}

fn parse_s3_params(l: &mut UriLocation, root: String) -> Result<StorageParams> {
    let endpoint = l
        .connection
        .get("endpoint_url")
        .cloned()
        .unwrap_or_else(|| STORAGE_S3_DEFAULT_ENDPOINT.to_string());

    // we split those field out to make borrow checker happy.
    let region = l.connection.get("region").cloned().unwrap_or_default();

    let access_key_id = {
        if let Some(id) = l.connection.get("access_key_id") {
            id
        } else if let Some(id) = l.connection.get("aws_key_id") {
            id
        } else {
            ""
        }
    }
    .to_string();

    let secret_access_key = {
        if let Some(key) = l.connection.get("secret_access_key") {
            key
        } else if let Some(key) = l.connection.get("aws_secret_key") {
            key
        } else {
            ""
        }
    }
    .to_string();

    let security_token = {
        if let Some(token) = l.connection.get("session_token") {
            token
        } else if let Some(token) = l.connection.get("aws_token") {
            token
        } else if let Some(token) = l.connection.get("security_token") {
            token
        } else {
            ""
        }
    }
    .to_string();

    let master_key = l.connection.get("master_key").cloned().unwrap_or_default();

    let enable_virtual_host_style = {
        if let Some(s) = l.connection.get("enable_virtual_host_style") {
            s
        } else {
            "false"
        }
    }
    .to_string()
    .parse()
    .map_err(|err| {
        Error::new(
            ErrorKind::InvalidInput,
            anyhow!("value for enable_virtual_host_style is invalid: {err:?}"),
        )
    })?;

    let role_arn = {
        if let Some(role) = l.connection.get("role_arn") {
            role
        } else if let Some(role) = l.connection.get("aws_role_arn") {
            role
        } else {
            ""
        }
    }
    .to_string();

    let external_id = {
        if let Some(id) = l.connection.get("external_id") {
            id
        } else if let Some(id) = l.connection.get("aws_external_id") {
            id
        } else {
            ""
        }
    }
    .to_string();

    let sp = StorageParams::S3(StorageS3Config {
        endpoint_url: secure_omission(endpoint),
        region,
        bucket: l.name.to_string(),
        access_key_id,
        secret_access_key,
        security_token,
        master_key,
        root,
        // Disable credential load by default.
        // TODO(xuanwo): we should support AssumeRole.
        disable_credential_loader: !GlobalConfig::instance().storage.allow_insecure,
        enable_virtual_host_style,
        role_arn,
        external_id,
    });

    l.connection.check()?;

    Ok(sp)
}

fn parse_gcs_params(l: &mut UriLocation) -> Result<StorageParams> {
    let endpoint = l
        .connection
        .get("endpoint_url")
        .cloned()
        .unwrap_or_else(|| STORAGE_GCS_DEFAULT_ENDPOINT.to_string());
    let sp = StorageParams::Gcs(StorageGcsConfig {
        endpoint_url: secure_omission(endpoint),
        bucket: l.name.clone(),
        root: l.path.clone(),
        credential: l.connection.get("credential").cloned().unwrap_or_default(),
    });

    l.connection.check()?;

    Ok(sp)
}

fn parse_ipfs_params(l: &mut UriLocation) -> Result<StorageParams> {
    let endpoint = l
        .connection
        .get("endpoint_url")
        .cloned()
        .unwrap_or_else(|| STORAGE_IPFS_DEFAULT_ENDPOINT.to_string());
    let sp = StorageParams::Ipfs(StorageIpfsConfig {
        endpoint_url: secure_omission(endpoint),
        root: "/ipfs/".to_string() + l.name.as_str(),
    });

    l.connection.check()?;

    Ok(sp)
}

fn parse_oss_params(l: &mut UriLocation, root: String) -> Result<StorageParams> {
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
    let sp = StorageParams::Oss(StorageOssConfig {
        endpoint_url: endpoint,
        presign_endpoint_url: "".to_string(),
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
        root,
    });

    l.connection.check()?;

    Ok(sp)
}

#[cfg(feature = "storage-hdfs")]
fn parse_hdfs_params(l: &mut UriLocation) -> Result<StorageParams> {
    let sp = StorageParams::Hdfs(crate::StorageHdfsConfig {
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
    });

    l.connection.check()?;

    Ok(sp)
}

// The FileSystem scheme of WebHDFS is “webhdfs://”. A WebHDFS FileSystem URI has the following format.
// webhdfs://<HOST>:<HTTP_PORT>/<PATH>
fn parse_webhdfs_params(l: &mut UriLocation) -> Result<StorageParams> {
    let is_https = l
        .connection
        .get("https")
        .map(|s| s.to_lowercase().parse::<bool>())
        .unwrap_or(Ok(true))
        .map_err(|e| {
            Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "HTTPS should be `TRUE` or `FALSE`, parse error with: {:?}",
                    e,
                ),
            )
        })?;
    let prefix = if is_https { "https" } else { "http" };
    let endpoint_url = format!("{prefix}://{}", l.name);

    let root = l.path.clone();

    let delegation = l.connection.get("delegation").cloned().unwrap_or_default();

    let sp = StorageParams::Webhdfs(StorageWebhdfsConfig {
        endpoint_url,
        root,
        delegation,
    });

    l.connection.check()?;

    Ok(sp)
}

/// parse_uri_location will parse given UriLocation into StorageParams and Path.
pub fn parse_uri_location(l: &mut UriLocation) -> Result<(StorageParams, String)> {
    // Path endswith `/` means it's a directory, otherwise it's a file.
    // If the path is a directory, we will use this path as root.
    // If the path is a file, we will use `/` as root (which is the default value)
    let (root, path) = if l.path.ends_with('/') {
        (l.path.clone(), "/".to_string())
    } else {
        ("/".to_string(), l.path.clone())
    };

    let protocol = l.protocol.parse::<Scheme>()?;

    let sp = match protocol {
        Scheme::Azblob => parse_azure_params(l, root)?,
        // Wait for https://github.com/datafuselabs/opendal/pull/1101
        //
        // Scheme::Ftp => StorageParams::Ftp(StorageFtpConfig {
        //     endpoint: if !l.protocol.is_empty() {
        //         format!("{}://{}", l.protocol, l.name)
        //     } else {
        //         // no protocol prefix will be seen as using FTPS connection
        //         format!("ftps://{}", l.name)
        //     },
        //     root: root.to_string(),
        //     username: l.connection.get("username").cloned().unwrap_or_default(),
        //     password: l.connection.get("password").cloned().unwrap_or_default(),
        // }),
        Scheme::Gcs => parse_gcs_params(l)?,
        #[cfg(feature = "storage-hdfs")]
        Scheme::Hdfs => parse_hdfs_params(l)?,
        Scheme::Ipfs => parse_ipfs_params(l)?,
        Scheme::S3 => parse_s3_params(l, root)?,
        Scheme::Oss => parse_oss_params(l, root)?,
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
            let cfg = StorageFsConfig { root };
            StorageParams::Fs(cfg)
        }
        Scheme::Webhdfs => parse_webhdfs_params(l)?,
        v => {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                anyhow!("{v} is not allowed to be used as uri location"),
            ));
        }
    };

    Ok((sp, path))
}
