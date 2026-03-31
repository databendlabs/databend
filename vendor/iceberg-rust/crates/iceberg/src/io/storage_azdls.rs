// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::fmt::Display;
use std::str::FromStr;

use opendal::Configurator;
use opendal::services::AzdlsConfig;
use url::Url;

use crate::{Error, ErrorKind, Result, ensure_data_valid};

/// A connection string.
///
/// Note, this string is parsed first, and any other passed adls.* properties
/// will override values from the connection string.
const ADLS_CONNECTION_STRING: &str = "adls.connection-string";

/// The account that you want to connect to.
pub const ADLS_ACCOUNT_NAME: &str = "adls.account-name";

/// The key to authentication against the account.
pub const ADLS_ACCOUNT_KEY: &str = "adls.account-key";

/// The shared access signature.
pub const ADLS_SAS_TOKEN: &str = "adls.sas-token";

/// The tenant-id.
pub const ADLS_TENANT_ID: &str = "adls.tenant-id";

/// The client-id.
pub const ADLS_CLIENT_ID: &str = "adls.client-id";

/// The client-secret.
pub const ADLS_CLIENT_SECRET: &str = "adls.client-secret";

/// The authority host of the service principal.
/// - required for client_credentials authentication
/// - default value: `https://login.microsoftonline.com`
pub const ADLS_AUTHORITY_HOST: &str = "adls.authority-host";

/// Parses adls.* prefixed configuration properties.
pub(crate) fn azdls_config_parse(mut properties: HashMap<String, String>) -> Result<AzdlsConfig> {
    let mut config = AzdlsConfig::default();

    if let Some(_conn_str) = properties.remove(ADLS_CONNECTION_STRING) {
        return Err(Error::new(
            ErrorKind::FeatureUnsupported,
            "Azdls: connection string currently not supported",
        ));
    }

    if let Some(account_name) = properties.remove(ADLS_ACCOUNT_NAME) {
        config.account_name = Some(account_name);
    }

    if let Some(account_key) = properties.remove(ADLS_ACCOUNT_KEY) {
        config.account_key = Some(account_key);
    }

    if let Some(sas_token) = properties.remove(ADLS_SAS_TOKEN) {
        config.sas_token = Some(sas_token);
    }

    if let Some(tenant_id) = properties.remove(ADLS_TENANT_ID) {
        config.tenant_id = Some(tenant_id);
    }

    if let Some(client_id) = properties.remove(ADLS_CLIENT_ID) {
        config.client_id = Some(client_id);
    }

    if let Some(client_secret) = properties.remove(ADLS_CLIENT_SECRET) {
        config.client_secret = Some(client_secret);
    }

    if let Some(authority_host) = properties.remove(ADLS_AUTHORITY_HOST) {
        config.authority_host = Some(authority_host);
    }

    Ok(config)
}

/// Builds an OpenDAL operator from the AzdlsConfig and path.
///
/// The path is expected to include the scheme in a format like:
/// `abfss://<myfs>@<myaccount>.dfs.core.windows.net/mydir/myfile.parquet`.
pub(crate) fn azdls_create_operator<'a>(
    absolute_path: &'a str,
    config: &AzdlsConfig,
    configured_scheme: &AzureStorageScheme,
) -> Result<(opendal::Operator, &'a str)> {
    let path = absolute_path.parse::<AzureStoragePath>()?;
    match_path_with_config(&path, config, configured_scheme)?;

    let op = azdls_config_build(config, &path)?;

    // Paths to files in ADLS tend to be written in fully qualified form,
    // including their filesystem and account name.
    // OpenDAL's operator methods expect only the relative path, so we split it
    // off and save it for later use.
    let relative_path_len = path.path.len();
    let (_, relative_path) = absolute_path.split_at(absolute_path.len() - relative_path_len);

    Ok((op, relative_path))
}

/// Note that `abf[s]` and `wasb[s]` variants have different implications:
/// - `abfs[s]` is used to refer to files in ADLS Gen2, backed by blob storage;
///   paths are expected to contain the `dfs` storage service.
/// - `wasb[s]` is used to refer to files in Blob Storage directly; paths are
///   expected to contain the `blob` storage service.
#[derive(Debug, PartialEq)]
pub(crate) enum AzureStorageScheme {
    Abfs,
    Abfss,
    Wasb,
    Wasbs,
}

impl AzureStorageScheme {
    // Returns the respective encrypted or plain-text HTTP scheme.
    pub fn as_http_scheme(&self) -> &str {
        match self {
            AzureStorageScheme::Abfs | AzureStorageScheme::Wasb => "http",
            AzureStorageScheme::Abfss | AzureStorageScheme::Wasbs => "https",
        }
    }
}

impl Display for AzureStorageScheme {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AzureStorageScheme::Abfs => write!(f, "abfs"),
            AzureStorageScheme::Abfss => write!(f, "abfss"),
            AzureStorageScheme::Wasb => write!(f, "wasb"),
            AzureStorageScheme::Wasbs => write!(f, "wasbs"),
        }
    }
}

impl FromStr for AzureStorageScheme {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "abfs" => Ok(AzureStorageScheme::Abfs),
            "abfss" => Ok(AzureStorageScheme::Abfss),
            "wasb" => Ok(AzureStorageScheme::Wasb),
            "wasbs" => Ok(AzureStorageScheme::Wasbs),
            _ => Err(Error::new(
                ErrorKind::DataInvalid,
                format!("Unexpected Azure Storage scheme: {s}"),
            )),
        }
    }
}

/// Validates whether the given path matches what's configured for the backend.
fn match_path_with_config(
    path: &AzureStoragePath,
    config: &AzdlsConfig,
    configured_scheme: &AzureStorageScheme,
) -> Result<()> {
    ensure_data_valid!(
        &path.scheme == configured_scheme,
        "Storage::Azdls: Scheme mismatch: configured {}, passed {}",
        configured_scheme,
        path.scheme
    );

    if let Some(ref configured_account_name) = config.account_name {
        ensure_data_valid!(
            &path.account_name == configured_account_name,
            "Storage::Azdls: Account name mismatch: configured {}, path {}",
            configured_account_name,
            path.account_name
        );
    }

    if let Some(ref configured_endpoint) = config.endpoint {
        let passed_http_scheme = path.scheme.as_http_scheme();
        ensure_data_valid!(
            configured_endpoint.starts_with(passed_http_scheme),
            "Storage::Azdls: Endpoint {} does not use the expected http scheme {}.",
            configured_endpoint,
            passed_http_scheme
        );

        let ends_with_expected_suffix = configured_endpoint
            .trim_end_matches('/')
            .ends_with(&path.endpoint_suffix);
        ensure_data_valid!(
            ends_with_expected_suffix,
            "Storage::Azdls: Endpoint suffix {} used with configured endpoint {}.",
            path.endpoint_suffix,
            configured_endpoint,
        );
    }

    Ok(())
}

fn azdls_config_build(config: &AzdlsConfig, path: &AzureStoragePath) -> Result<opendal::Operator> {
    let mut builder = config.clone().into_builder();

    if config.endpoint.is_none() {
        // If no endpoint is provided, we construct it from the fully-qualified path.
        builder = builder.endpoint(&path.as_endpoint());
    }
    builder = builder.filesystem(&path.filesystem);

    Ok(opendal::Operator::new(builder)?.finish())
}

/// Represents a fully qualified path to blob/ file in Azure Storage.
#[derive(Debug, PartialEq)]
struct AzureStoragePath {
    /// The scheme of the URL, e.g., `abfss`, `abfs`, `wasbs`, or `wasb`.
    scheme: AzureStorageScheme,

    /// Under Blob Storage, this is considered the _container_.
    filesystem: String,

    account_name: String,

    /// The endpoint suffix, e.g., `core.windows.net` for the public cloud
    /// endpoint.
    endpoint_suffix: String,

    /// Path to the file.
    ///
    /// It is relative to the `root` of the `AzdlsConfig`.
    path: String,
}

impl AzureStoragePath {
    /// Converts the AzureStoragePath into a full endpoint URL.
    ///
    /// This is possible because the path is fully qualified.
    fn as_endpoint(&self) -> String {
        format!(
            "{}://{}.dfs.{}",
            self.scheme.as_http_scheme(),
            self.account_name,
            self.endpoint_suffix
        )
    }
}

impl FromStr for AzureStoragePath {
    type Err = Error;

    fn from_str(path: &str) -> Result<Self> {
        let url = Url::parse(path)?;

        let filesystem = url.username();
        ensure_data_valid!(
            !filesystem.is_empty(),
            "AzureStoragePath: No container or filesystem name in path: {}",
            path
        );

        let (account_name, storage_service, endpoint_suffix) = parse_azure_storage_endpoint(&url)?;
        let scheme = validate_storage_and_scheme(storage_service, url.scheme())?;

        Ok(AzureStoragePath {
            scheme,
            filesystem: filesystem.to_string(),
            account_name: account_name.to_string(),
            endpoint_suffix: endpoint_suffix.to_string(),
            path: url.path().to_string(),
        })
    }
}

fn parse_azure_storage_endpoint(url: &Url) -> Result<(&str, &str, &str)> {
    let host = url.host_str().ok_or(Error::new(
        ErrorKind::DataInvalid,
        "AzureStoragePath: No host",
    ))?;

    let (account_name, endpoint) = host.split_once('.').ok_or(Error::new(
        ErrorKind::DataInvalid,
        "AzureStoragePath: No account name",
    ))?;
    if account_name.is_empty() {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            "AzureStoragePath: No account name",
        ));
    }

    let (storage, endpoint_suffix) = endpoint.split_once('.').ok_or(Error::new(
        ErrorKind::DataInvalid,
        "AzureStoragePath: No storage service",
    ))?;

    Ok((account_name, storage, endpoint_suffix))
}

fn validate_storage_and_scheme(
    storage_service: &str,
    scheme_str: &str,
) -> Result<AzureStorageScheme> {
    let scheme = scheme_str.parse::<AzureStorageScheme>()?;
    match scheme {
        AzureStorageScheme::Abfss | AzureStorageScheme::Abfs => {
            ensure_data_valid!(
                storage_service == "dfs",
                "AzureStoragePath: Unexpected storage service for abfs[s]: {}",
                storage_service
            );
            Ok(scheme)
        }
        AzureStorageScheme::Wasbs | AzureStorageScheme::Wasb => {
            ensure_data_valid!(
                storage_service == "blob",
                "AzureStoragePath: Unexpected storage service for wasb[s]: {}",
                storage_service
            );
            Ok(scheme)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use opendal::services::AzdlsConfig;

    use super::{AzureStoragePath, AzureStorageScheme, azdls_create_operator};
    use crate::io::azdls_config_parse;

    #[test]
    fn test_azdls_config_parse() {
        let test_cases = vec![
            (
                "account name and key",
                HashMap::from([
                    (super::ADLS_ACCOUNT_NAME.to_string(), "test".to_string()),
                    (super::ADLS_ACCOUNT_KEY.to_string(), "secret".to_string()),
                ]),
                Some(AzdlsConfig {
                    account_name: Some("test".to_string()),
                    account_key: Some("secret".to_string()),
                    ..Default::default()
                }),
            ),
            (
                "account name and SAS token",
                HashMap::from([
                    (super::ADLS_ACCOUNT_NAME.to_string(), "test".to_string()),
                    (super::ADLS_SAS_TOKEN.to_string(), "token".to_string()),
                ]),
                Some(AzdlsConfig {
                    account_name: Some("test".to_string()),
                    sas_token: Some("token".to_string()),
                    ..Default::default()
                }),
            ),
            (
                "account name and ADD credentials",
                HashMap::from([
                    (super::ADLS_ACCOUNT_NAME.to_string(), "test".to_string()),
                    (super::ADLS_CLIENT_ID.to_string(), "abcdef".to_string()),
                    (super::ADLS_CLIENT_SECRET.to_string(), "secret".to_string()),
                    (super::ADLS_TENANT_ID.to_string(), "12345".to_string()),
                ]),
                Some(AzdlsConfig {
                    account_name: Some("test".to_string()),
                    client_id: Some("abcdef".to_string()),
                    client_secret: Some("secret".to_string()),
                    tenant_id: Some("12345".to_string()),
                    ..Default::default()
                }),
            ),
        ];

        for (name, properties, expected) in test_cases {
            let config = azdls_config_parse(properties);
            match expected {
                Some(expected_config) => {
                    assert!(config.is_ok(), "Test case {name} failed: {config:?}");
                    assert_eq!(config.unwrap(), expected_config, "Test case: {name}");
                }
                None => {
                    assert!(config.is_err(), "Test case {name} expected error.");
                }
            }
        }
    }

    #[test]
    fn test_azdls_create_operator() {
        let test_cases = vec![
            (
                "basic",
                (
                    "abfss://myfs@myaccount.dfs.core.windows.net/path/to/file.parquet",
                    AzdlsConfig {
                        account_name: Some("myaccount".to_string()),
                        endpoint: Some("https://myaccount.dfs.core.windows.net".to_string()),
                        ..Default::default()
                    },
                    AzureStorageScheme::Abfss,
                ),
                Some(("myfs", "/path/to/file.parquet")),
            ),
            (
                "different account",
                (
                    "abfss://myfs@anotheraccount.dfs.core.windows.net/path/to/file.parquet",
                    AzdlsConfig {
                        account_name: Some("myaccount".to_string()),
                        endpoint: Some("https://myaccount.dfs.core.windows.net".to_string()),
                        ..Default::default()
                    },
                    AzureStorageScheme::Abfss,
                ),
                None,
            ),
            (
                "different scheme",
                (
                    "wasbs://myfs@myaccount.dfs.core.windows.net/path/to/file.parquet",
                    AzdlsConfig {
                        account_name: Some("myaccount".to_string()),
                        endpoint: Some("https://myaccount.dfs.core.windows.net".to_string()),
                        ..Default::default()
                    },
                    AzureStorageScheme::Abfss,
                ),
                None,
            ),
            (
                "incompatible scheme for endpoint",
                (
                    "abfs://myfs@myaccount.dfs.core.windows.net/path/to/file.parquet",
                    AzdlsConfig {
                        account_name: Some("myaccount".to_string()),
                        endpoint: Some("http://myaccount.dfs.core.windows.net".to_string()),
                        ..Default::default()
                    },
                    AzureStorageScheme::Abfss,
                ),
                None,
            ),
            (
                "different endpoint suffix",
                (
                    "abfss://somefs@myaccount.dfs.core.windows.net/path/to/file.parquet",
                    AzdlsConfig {
                        account_name: Some("myaccount".to_string()),
                        endpoint: Some("https://myaccount.dfs.core.chinacloudapi.cn".to_string()),
                        ..Default::default()
                    },
                    AzureStorageScheme::Abfss,
                ),
                None,
            ),
            (
                "endpoint inferred from fully qualified path",
                (
                    "abfs://myfs@myaccount.dfs.core.windows.net/path/to/file.parquet",
                    AzdlsConfig {
                        filesystem: "myfs".to_string(),
                        account_name: Some("myaccount".to_string()),
                        endpoint: None,
                        ..Default::default()
                    },
                    AzureStorageScheme::Abfs,
                ),
                Some(("myfs", "/path/to/file.parquet")),
            ),
        ];

        for (name, input, expected) in test_cases {
            let result = azdls_create_operator(input.0, &input.1, &input.2);
            match expected {
                Some((expected_filesystem, expected_path)) => {
                    assert!(result.is_ok(), "Test case {name} failed: {result:?}");

                    let (op, relative_path) = result.unwrap();
                    assert_eq!(op.info().name(), expected_filesystem);
                    assert_eq!(relative_path, expected_path);
                }
                None => {
                    assert!(result.is_err(), "Test case {name} expected error.");
                }
            }
        }
    }

    #[test]
    fn test_azure_storage_path_parse() {
        let test_cases = vec![
            (
                "succeeds",
                "abfss://somefs@myaccount.dfs.core.windows.net/path/to/file.parquet",
                Some(AzureStoragePath {
                    scheme: AzureStorageScheme::Abfss,
                    filesystem: "somefs".to_string(),
                    account_name: "myaccount".to_string(),
                    endpoint_suffix: "core.windows.net".to_string(),
                    path: "/path/to/file.parquet".to_string(),
                }),
            ),
            (
                "unexpected scheme",
                "adls://somefs@myaccount.dfs.core.windows.net/path/to/file.parquet",
                None,
            ),
            (
                "no filesystem",
                "abfss://myaccount.dfs.core.windows.net/path/to/file.parquet",
                None,
            ),
            (
                "no account name",
                "abfs://myfs@dfs.core.windows.net/path/to/file.parquet",
                None,
            ),
        ];

        for (name, input, expected) in test_cases {
            let result = input.parse::<AzureStoragePath>();
            match expected {
                Some(expected_path) => {
                    assert!(result.is_ok(), "Test case {name} failed: {result:?}");
                    assert_eq!(result.unwrap(), expected_path, "Test case: {name}");
                }
                None => {
                    assert!(result.is_err(), "Test case {name} expected error.");
                }
            }
        }
    }

    #[test]
    fn test_azure_storage_path_endpoint() {
        let test_cases = vec![
            (
                "abfss uses https",
                AzureStoragePath {
                    scheme: AzureStorageScheme::Abfss,
                    filesystem: "myfs".to_string(),
                    account_name: "myaccount".to_string(),
                    endpoint_suffix: "core.windows.net".to_string(),
                    path: "/path/to/file.parquet".to_string(),
                },
                "https://myaccount.dfs.core.windows.net",
            ),
            (
                "abfs uses http",
                AzureStoragePath {
                    scheme: AzureStorageScheme::Abfs,
                    filesystem: "myfs".to_string(),
                    account_name: "myaccount".to_string(),
                    endpoint_suffix: "core.windows.net".to_string(),
                    path: "/path/to/file.parquet".to_string(),
                },
                "http://myaccount.dfs.core.windows.net",
            ),
            (
                "wasbs uses https and dfs",
                AzureStoragePath {
                    scheme: AzureStorageScheme::Abfss,
                    filesystem: "myfs".to_string(),
                    account_name: "myaccount".to_string(),
                    endpoint_suffix: "core.windows.net".to_string(),
                    path: "/path/to/file.parquet".to_string(),
                },
                "https://myaccount.dfs.core.windows.net",
            ),
        ];

        for (name, path, expected) in test_cases {
            let endpoint = path.as_endpoint();
            assert_eq!(endpoint, expected, "Test case: {name}");
        }
    }
}
