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

#[cfg(any(
    feature = "storage-s3",
    feature = "storage-gcs",
    feature = "storage-oss",
    feature = "storage-azdls",
))]
use std::sync::Arc;

use opendal::layers::RetryLayer;
#[cfg(feature = "storage-azdls")]
use opendal::services::AzdlsConfig;
#[cfg(feature = "storage-gcs")]
use opendal::services::GcsConfig;
#[cfg(feature = "storage-oss")]
use opendal::services::OssConfig;
#[cfg(feature = "storage-s3")]
use opendal::services::S3Config;
use opendal::{Operator, Scheme};

#[cfg(feature = "storage-azdls")]
use super::AzureStorageScheme;
use super::FileIOBuilder;
#[cfg(feature = "storage-s3")]
use crate::io::CustomAwsCredentialLoader;
use crate::{Error, ErrorKind};

/// The storage carries all supported storage services in iceberg
#[derive(Debug)]
pub(crate) enum Storage {
    #[cfg(feature = "storage-memory")]
    Memory(Operator),
    #[cfg(feature = "storage-fs")]
    LocalFs,
    /// Expects paths of the form `s3[a]://<bucket>/<path>`.
    #[cfg(feature = "storage-s3")]
    S3 {
        /// s3 storage could have `s3://` and `s3a://`.
        /// Storing the scheme string here to return the correct path.
        configured_scheme: String,
        config: Arc<S3Config>,
        customized_credential_load: Option<CustomAwsCredentialLoader>,
    },
    #[cfg(feature = "storage-gcs")]
    Gcs { config: Arc<GcsConfig> },
    #[cfg(feature = "storage-oss")]
    Oss { config: Arc<OssConfig> },
    /// Expects paths of the form
    /// `abfs[s]://<filesystem>@<account>.dfs.<endpoint-suffix>/<path>` or
    /// `wasb[s]://<container>@<account>.blob.<endpoint-suffix>/<path>`.
    #[cfg(feature = "storage-azdls")]
    Azdls {
        /// Because Azdls accepts multiple possible schemes, we store the full
        /// passed scheme here to later validate schemes passed via paths.
        configured_scheme: AzureStorageScheme,
        config: Arc<AzdlsConfig>,
    },
}

impl Storage {
    /// Convert iceberg config to opendal config.
    pub(crate) fn build(file_io_builder: FileIOBuilder) -> crate::Result<Self> {
        let (scheme_str, props, extensions) = file_io_builder.into_parts();
        let _ = (&props, &extensions);
        let scheme = Self::parse_scheme(&scheme_str)?;

        match scheme {
            #[cfg(feature = "storage-memory")]
            Scheme::Memory => Ok(Self::Memory(super::memory_config_build()?)),
            #[cfg(feature = "storage-fs")]
            Scheme::Fs => Ok(Self::LocalFs),
            #[cfg(feature = "storage-s3")]
            Scheme::S3 => Ok(Self::S3 {
                configured_scheme: scheme_str,
                config: super::s3_config_parse(props)?.into(),
                customized_credential_load: extensions
                    .get::<CustomAwsCredentialLoader>()
                    .map(Arc::unwrap_or_clone),
            }),
            #[cfg(feature = "storage-gcs")]
            Scheme::Gcs => Ok(Self::Gcs {
                config: super::gcs_config_parse(props)?.into(),
            }),
            #[cfg(feature = "storage-oss")]
            Scheme::Oss => Ok(Self::Oss {
                config: super::oss_config_parse(props)?.into(),
            }),
            #[cfg(feature = "storage-azdls")]
            Scheme::Azdls => {
                let scheme = scheme_str.parse::<AzureStorageScheme>()?;
                Ok(Self::Azdls {
                    config: super::azdls_config_parse(props)?.into(),
                    configured_scheme: scheme,
                })
            }
            // Update doc on [`FileIO`] when adding new schemes.
            _ => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                format!("Constructing file io from scheme: {scheme} not supported now",),
            )),
        }
    }

    /// Creates operator from path.
    ///
    /// # Arguments
    ///
    /// * path: It should be *absolute* path starting with scheme string used to construct [`FileIO`].
    ///
    /// # Returns
    ///
    /// The return value consists of two parts:
    ///
    /// * An [`opendal::Operator`] instance used to operate on file.
    /// * Relative path to the root uri of [`opendal::Operator`].
    pub(crate) fn create_operator<'a>(
        &self,
        path: &'a impl AsRef<str>,
    ) -> crate::Result<(Operator, &'a str)> {
        let path = path.as_ref();
        let _ = path;
        let (operator, relative_path): (Operator, &str) = match self {
            #[cfg(feature = "storage-memory")]
            Storage::Memory(op) => {
                if let Some(stripped) = path.strip_prefix("memory:/") {
                    Ok::<_, crate::Error>((op.clone(), stripped))
                } else {
                    Ok::<_, crate::Error>((op.clone(), &path[1..]))
                }
            }
            #[cfg(feature = "storage-fs")]
            Storage::LocalFs => {
                let op = super::fs_config_build()?;

                if let Some(stripped) = path.strip_prefix("file:/") {
                    Ok::<_, crate::Error>((op, stripped))
                } else {
                    Ok::<_, crate::Error>((op, &path[1..]))
                }
            }
            #[cfg(feature = "storage-s3")]
            Storage::S3 {
                configured_scheme,
                config,
                customized_credential_load,
            } => {
                let op = super::s3_config_build(config, customized_credential_load, path)?;
                let op_info = op.info();

                // Check prefix of s3 path.
                let prefix = format!("{}://{}/", configured_scheme, op_info.name());
                if path.starts_with(&prefix) {
                    Ok((op, &path[prefix.len()..]))
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid s3 url: {path}, should start with {prefix}"),
                    ))
                }
            }
            #[cfg(feature = "storage-gcs")]
            Storage::Gcs { config } => {
                let operator = super::gcs_config_build(config, path)?;
                let prefix = format!("gs://{}/", operator.info().name());
                if path.starts_with(&prefix) {
                    Ok((operator, &path[prefix.len()..]))
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid gcs url: {path}, should start with {prefix}"),
                    ))
                }
            }
            #[cfg(feature = "storage-oss")]
            Storage::Oss { config } => {
                let op = super::oss_config_build(config, path)?;

                // Check prefix of oss path.
                let prefix = format!("oss://{}/", op.info().name());
                if path.starts_with(&prefix) {
                    Ok((op, &path[prefix.len()..]))
                } else {
                    Err(Error::new(
                        ErrorKind::DataInvalid,
                        format!("Invalid oss url: {path}, should start with {prefix}"),
                    ))
                }
            }
            #[cfg(feature = "storage-azdls")]
            Storage::Azdls {
                configured_scheme,
                config,
            } => super::azdls_create_operator(path, config, configured_scheme),
            #[cfg(all(
                not(feature = "storage-s3"),
                not(feature = "storage-fs"),
                not(feature = "storage-gcs"),
                not(feature = "storage-oss"),
                not(feature = "storage-azdls"),
            ))]
            _ => Err(Error::new(
                ErrorKind::FeatureUnsupported,
                "No storage service has been enabled",
            )),
        }?;

        // Transient errors are common for object stores; however there's no
        // harm in retrying temporary failures for other storage backends as well.
        let operator = operator.layer(RetryLayer::new());

        Ok((operator, relative_path))
    }

    /// Parse scheme.
    fn parse_scheme(scheme: &str) -> crate::Result<Scheme> {
        match scheme {
            "memory" => Ok(Scheme::Memory),
            "file" | "" => Ok(Scheme::Fs),
            "s3" | "s3a" => Ok(Scheme::S3),
            "gs" | "gcs" => Ok(Scheme::Gcs),
            "oss" => Ok(Scheme::Oss),
            "abfss" | "abfs" | "wasbs" | "wasb" => Ok(Scheme::Azdls),
            s => Ok(s.parse::<Scheme>()?),
        }
    }
}
