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

use std::collections::BTreeMap;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;

use anyhow::anyhow;
use databend_common_ast::ast::Connection;
use databend_common_ast::ast::UriLocation;
use databend_common_catalog::table_context::TableContext;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_meta_app::storage::StorageAzblobConfig;
use databend_common_meta_app::storage::StorageCosConfig;
use databend_common_meta_app::storage::StorageFsConfig;
use databend_common_meta_app::storage::StorageGcsConfig;
use databend_common_meta_app::storage::StorageHttpConfig;
use databend_common_meta_app::storage::StorageHuggingfaceConfig;
use databend_common_meta_app::storage::StorageIpfsConfig;
use databend_common_meta_app::storage::StorageObsConfig;
use databend_common_meta_app::storage::StorageOssConfig;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::storage::StorageS3Config;
use databend_common_meta_app::storage::StorageWebhdfsConfig;
use databend_common_meta_app::storage::STORAGE_GCS_DEFAULT_ENDPOINT;
use databend_common_meta_app::storage::STORAGE_IPFS_DEFAULT_ENDPOINT;
use databend_common_meta_app::storage::STORAGE_S3_DEFAULT_ENDPOINT;
use databend_common_storage::STDIN_FD;
use opendal::raw::normalize_path;
use opendal::raw::normalize_root;
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

    l.connection
        .check()
        .map_err(|err| Error::new(ErrorKind::InvalidInput, err.to_string()))?;

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

    // If role_arn is empty and we don't allow allow insecure, we should disable credential loader.
    let disable_credential_loader =
        role_arn.is_empty() && !GlobalConfig::instance().storage.allow_insecure;

    let sp = StorageParams::S3(StorageS3Config {
        endpoint_url: secure_omission(endpoint),
        region,
        bucket: l.name.to_string(),
        access_key_id,
        secret_access_key,
        security_token,
        master_key,
        root,
        disable_credential_loader,
        enable_virtual_host_style,
        role_arn,
        external_id,
    });

    l.connection
        .check()
        .map_err(|err| Error::new(ErrorKind::InvalidInput, err.to_string()))?;

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

    l.connection
        .check()
        .map_err(|err| Error::new(ErrorKind::InvalidInput, err.to_string()))?;

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

    l.connection
        .check()
        .map_err(|err| Error::new(ErrorKind::InvalidInput, err.to_string()))?;

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
        // TODO(xuanwo): Support SSE in stage later.
        server_side_encryption: "".to_string(),
        server_side_encryption_key_id: "".to_string(),
    });

    l.connection
        .check()
        .map_err(|err| Error::new(ErrorKind::InvalidInput, err.to_string()))?;

    Ok(sp)
}

fn parse_obs_params(l: &mut UriLocation, root: String) -> Result<StorageParams> {
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
    let sp = StorageParams::Obs(StorageObsConfig {
        endpoint_url: endpoint,
        bucket: l.name.to_string(),
        access_key_id: l
            .connection
            .get("access_key_id")
            .cloned()
            .unwrap_or_default(),
        secret_access_key: l
            .connection
            .get("secret_access_key")
            .cloned()
            .unwrap_or_default(),
        root,
    });

    l.connection
        .check()
        .map_err(|err| Error::new(ErrorKind::InvalidInput, err.to_string()))?;

    Ok(sp)
}

fn parse_cos_params(l: &mut UriLocation, root: String) -> Result<StorageParams> {
    let endpoint = l
        .connection
        .get("endpoint_url")
        .cloned()
        .map(secure_omission)
        .ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                anyhow!("endpoint_url is required for storage cos"),
            )
        })?;
    let sp = StorageParams::Cos(StorageCosConfig {
        endpoint_url: endpoint,
        bucket: l.name.to_string(),
        secret_id: l.connection.get("secret_id").cloned().unwrap_or_default(),
        secret_key: l.connection.get("secret_key").cloned().unwrap_or_default(),
        root,
    });

    l.connection
        .check()
        .map_err(|err| Error::new(ErrorKind::InvalidInput, err.to_string()))?;

    Ok(sp)
}

/// Generally, the URI is in the pattern hdfs://<namenode>/<path>.
/// If <namenode> is empty (i.e. `hdfs:///<path>`),  use <namenode> configured somewhere else, e.g. in XML config file.
/// For databend user can specify <namenode> in connection options.
/// refer to https://www.vertica.com/docs/9.3.x/HTML/Content/Authoring/HadoopIntegrationGuide/libhdfs/HdfsURL.htm
#[cfg(feature = "storage-hdfs")]
fn parse_hdfs_params(l: &mut UriLocation) -> Result<StorageParams> {
    let name_node_from_uri = if l.name.is_empty() {
        None
    } else {
        Some(format!("hdfs://{}", l.name))
    };
    let name_node_option = l.connection.get("name_node");

    let name_node = match (name_node_option, name_node_from_uri) {
        (Some(n1), Some(n2)) => {
            if n1 != &n2 {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    format!(
                        "name_node in uri({n2}) and from connection option 'name_node'({n1}) not match."
                    ),
                ));
            } else {
                n2
            }
        }
        (Some(n1), None) => n1.to_string(),
        (None, Some(n2)) => n2,
        (None, None) => {
            // we prefer user to specify name_node in options
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "name_node is required for storage hdfs",
            ));
        }
    };
    let sp = StorageParams::Hdfs(databend_common_meta_app::storage::StorageHdfsConfig {
        name_node,
        root: l.path.clone(),
    });
    l.connection
        .check()
        .map_err(|err| Error::new(ErrorKind::InvalidInput, err.to_string()))?;
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

    l.connection
        .check()
        .map_err(|err| Error::new(ErrorKind::InvalidInput, err.to_string()))?;

    Ok(sp)
}

/// Huggingface uri looks like `hf://opendal/huggingface-testdata/path/to/file`.
///
/// We need to parse `huggingface-testdata` from the root.
fn parse_huggingface_params(l: &mut UriLocation, root: String) -> Result<StorageParams> {
    let (repo_name, root) = root
        .trim_start_matches('/')
        .split_once('/')
        .ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                "input uri is not a valid huggingface uri",
            )
        })?;

    let sp = StorageParams::Huggingface(StorageHuggingfaceConfig {
        repo_id: format!("{}/{repo_name}", l.name),
        repo_type: l
            .connection
            .get("repo_type")
            .cloned()
            .unwrap_or_else(|| "dataset".to_string()),
        revision: l
            .connection
            .get("revision")
            .cloned()
            .unwrap_or_else(|| "main".to_string()),
        root: root.to_string(),
        token: l.connection.get("token").cloned().unwrap_or_default(),
    });

    l.connection
        .check()
        .map_err(|err| Error::new(ErrorKind::InvalidInput, err.to_string()))?;

    Ok(sp)
}

pub async fn parse_storage_params_from_uri(
    l: &mut UriLocation,
    ctx: Option<&dyn TableContext>,
    usage: &str,
) -> Result<StorageParams> {
    if !l.path.ends_with('/') {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            anyhow!("path in URL must end with '/' {usage}. Got '{}'.", l.path),
        ));
    }
    Ok(parse_uri_location(l, ctx).await?.0)
}

/// parse_uri_location will parse given UriLocation into StorageParams and Path.
pub async fn parse_uri_location(
    l: &mut UriLocation,
    ctx: Option<&dyn TableContext>,
) -> Result<(StorageParams, String)> {
    // Path ends with `/` means it's a directory, otherwise it's a file.
    // If the path is a directory, we will use this path as root.
    // If the path is a file, we will use `/` as root (which is the default value)
    let (root, path) = if l.path.ends_with('/') {
        (l.path.clone(), "/".to_string())
    } else {
        ("/".to_string(), l.path.clone())
    };
    let root = normalize_root(&root);
    let path = normalize_path(&path);

    let protocol = l.protocol.parse::<Scheme>()?;
    if let Scheme::Custom(_) = protocol {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            anyhow!("protocol {protocol} is not supported yet."),
        ));
    }

    match (ctx, l.connection.get("connection_name")) {
        (None, Some(_)) => {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                anyhow!("can not use connection_name when create connection"),
            ));
        }
        (_, None) => {}
        (Some(ctx), Some(name)) => {
            let conn = ctx.get_connection(name).await.map_err(|err| {
                Error::new(
                    ErrorKind::InvalidInput,
                    anyhow!("fail to get connection_name {name}: {err:?}"),
                )
            })?;
            let proto = conn.storage_type.parse::<Scheme>().map_err(|err| {
                Error::new(
                    ErrorKind::InvalidInput,
                    anyhow!("input connection is not a valid protocol: {err:?}"),
                )
            })?;
            if proto != protocol {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    anyhow!(
                        "protocol from connection_name={name} ({proto}) not match with uri protocol ({protocol})."
                    ),
                ));
            }
            l.connection.check().map_err(|_| {
                Error::new(
                    ErrorKind::InvalidInput,
                    anyhow!("connection_name can not be used with other connection options"),
                )
            })?;
            l.connection = Connection::new(conn.storage_params);
        }
    }

    let sp = match protocol {
        Scheme::Azblob => parse_azure_params(l, root)?,
        Scheme::Gcs => parse_gcs_params(l)?,
        #[cfg(feature = "storage-hdfs")]
        Scheme::Hdfs => parse_hdfs_params(l)?,
        Scheme::Ipfs => parse_ipfs_params(l)?,
        Scheme::S3 => parse_s3_params(l, root)?,
        Scheme::Obs => parse_obs_params(l, root)?,
        Scheme::Oss => parse_oss_params(l, root)?,
        Scheme::Cos => parse_cos_params(l, root)?,
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
            if root == "/" && path == STDIN_FD {
                StorageParams::Memory
            } else {
                let cfg = StorageFsConfig { root };
                StorageParams::Fs(cfg)
            }
        }
        Scheme::Webhdfs => parse_webhdfs_params(l)?,
        Scheme::Huggingface => parse_huggingface_params(l, root)?,
        v => {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                anyhow!("URI protocol {v} is not supported yet."),
            ));
        }
    };

    let sp = sp.auto_detect().await.map_err(|err| {
        Error::new(
            ErrorKind::InvalidInput,
            anyhow!("storage params is invalid for it's auto detect failed for {err:?}"),
        )
    })?;

    Ok((sp, path))
}

pub async fn get_storage_params_from_options(
    ctx: &dyn TableContext,
    options: &BTreeMap<String, String>,
) -> databend_common_exception::Result<StorageParams> {
    let location = options
        .get("location")
        .ok_or_else(|| ErrorCode::BadArguments("missing option 'location'".to_string()))?;
    let connection = options.get("connection_name");

    let mut location = if let Some(connection) = connection {
        let connection = ctx.get_connection(connection).await?;
        let location = UriLocation::from_uri(
            location.to_string(),
            "".to_string(),
            connection.storage_params,
        )?;
        if location.protocol.to_lowercase() != connection.storage_type {
            return Err(ErrorCode::BadArguments(format!(
                "Incorrect CREATE query: protocol in location {:?} is not equal to connection {:?}",
                location.protocol, connection.storage_type
            )));
        };
        location
    } else {
        UriLocation::from_uri(location.to_string(), "".to_string(), BTreeMap::new())?
    };
    let sp = parse_storage_params_from_uri(
        &mut location,
        None,
        "when loading/creating ICEBERG/DELTA table",
    )
    .await?;
    Ok(sp)
}
