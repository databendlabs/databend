// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Trait for commit implementations.
//!
//! In Lance, a transaction is committed by writing the next manifest file.
//! However, care should be taken to ensure that the manifest file is written
//! only once, even if there are concurrent writers. Different stores have
//! different abilities to handle concurrent writes, so a trait is provided
//! to allow for different implementations.
//!
//! The trait [CommitHandler] can be implemented to provide different commit
//! strategies. The default implementation for most object stores is
//! [RenameCommitHandler], which writes the manifest to a temporary path, then
//! renames the temporary path to the final path if no object already exists
//! at the final path. This is an atomic operation in most object stores, but
//! not in AWS S3. So for AWS S3, the default commit handler is
//! [UnsafeCommitHandler], which writes the manifest to the final path without
//! any checks.
//!
//! When providing your own commit handler, most often you are implementing in
//! terms of a lock. The trait [CommitLock] can be implemented as a simpler
//! alternative to [CommitHandler].

use std::io;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::{fmt::Debug, fs::DirEntry};

use super::manifest::write_manifest;
use futures::future::Either;
use futures::Stream;
use futures::{
    future::{self, BoxFuture},
    stream::BoxStream,
    StreamExt, TryStreamExt,
};
use lance_file::format::{MAGIC, MAJOR_VERSION, MINOR_VERSION};
use lance_io::object_writer::{ObjectWriter, WriteResult};
use log::warn;
use object_store::PutOptions;
use object_store::{path::Path, Error as ObjectStoreError, ObjectStore as OSObjectStore};
use snafu::location;
use tracing::info;
use url::Url;

#[cfg(feature = "dynamodb")]
pub mod dynamodb;
pub mod external_manifest;

use lance_core::{Error, Result};
use lance_io::object_store::{ObjectStore, ObjectStoreExt, ObjectStoreParams};
use lance_io::traits::WriteExt;

use crate::format::{is_detached_version, IndexMetadata, Manifest, Transaction};
use lance_core::utils::tracing::{AUDIT_MODE_CREATE, AUDIT_TYPE_MANIFEST, TRACE_FILE_AUDIT};
#[cfg(feature = "dynamodb")]
use {
    self::external_manifest::{ExternalManifestCommitHandler, ExternalManifestStore},
    aws_credential_types::provider::error::CredentialsError,
    aws_credential_types::provider::ProvideCredentials,
    lance_io::object_store::{providers::aws::build_aws_credential, StorageOptions},
    object_store::aws::AmazonS3ConfigKey,
    object_store::aws::AwsCredentialProvider,
    std::borrow::Cow,
    std::time::{Duration, SystemTime},
};

const VERSIONS_DIR: &str = "_versions";
const MANIFEST_EXTENSION: &str = "manifest";
const DETACHED_VERSION_PREFIX: &str = "d";

/// How manifest files should be named.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManifestNamingScheme {
    /// `_versions/{version}.manifest`
    V1,
    /// `_manifests/{u64::MAX - version}.manifest`
    ///
    /// Zero-padded and reversed for O(1) lookup of latest version on object stores.
    V2,
}

impl ManifestNamingScheme {
    pub fn manifest_path(&self, base: &Path, version: u64) -> Path {
        let directory = base.child(VERSIONS_DIR);
        if is_detached_version(version) {
            // Detached versions should never show up first in a list operation which
            // means it needs to come lexicographically after all attached manifest
            // files and so we add the prefix `d`.  There is no need to invert the
            // version number since detached versions are not part of the version
            let directory = base.child(VERSIONS_DIR);
            directory.child(format!(
                "{DETACHED_VERSION_PREFIX}{version}.{MANIFEST_EXTENSION}"
            ))
        } else {
            match self {
                Self::V1 => directory.child(format!("{version}.{MANIFEST_EXTENSION}")),
                Self::V2 => {
                    let inverted_version = u64::MAX - version;
                    directory.child(format!("{inverted_version:020}.{MANIFEST_EXTENSION}"))
                }
            }
        }
    }

    pub fn parse_version(&self, filename: &str) -> Option<u64> {
        let file_number = filename
            .split_once('.')
            // Detached versions will fail the `parse` step, which is ok.
            .and_then(|(version_str, _)| version_str.parse::<u64>().ok());
        match self {
            Self::V1 => file_number,
            Self::V2 => file_number.map(|v| u64::MAX - v),
        }
    }

    pub fn detect_scheme(filename: &str) -> Option<Self> {
        if filename.starts_with(DETACHED_VERSION_PREFIX) {
            // Currently, detached versions must imply V2
            return Some(Self::V2);
        }
        if filename.ends_with(MANIFEST_EXTENSION) {
            const V2_LEN: usize = 20 + 1 + MANIFEST_EXTENSION.len();
            if filename.len() == V2_LEN {
                Some(Self::V2)
            } else {
                Some(Self::V1)
            }
        } else {
            None
        }
    }

    pub fn detect_scheme_staging(filename: &str) -> Self {
        // We shouldn't have to worry about detached versions here since there is no
        // such thing as "detached" and "staged" at the same time.
        if filename.chars().nth(20) == Some('.') {
            Self::V2
        } else {
            Self::V1
        }
    }
}

/// Migrate all V1 manifests to V2 naming scheme.
///
/// This function will rename all V1 manifests to V2 naming scheme.
///
/// This function is idempotent, and can be run multiple times without
/// changing the state of the object store.
///
/// However, it should not be run while other concurrent operations are happening.
/// And it should also run until completion before resuming other operations.
pub async fn migrate_scheme_to_v2(object_store: &ObjectStore, dataset_base: &Path) -> Result<()> {
    object_store
        .inner
        .list(Some(&dataset_base.child(VERSIONS_DIR)))
        .try_filter(|res| {
            let res = if let Some(filename) = res.location.filename() {
                ManifestNamingScheme::detect_scheme(filename) == Some(ManifestNamingScheme::V1)
            } else {
                false
            };
            future::ready(res)
        })
        .try_for_each_concurrent(object_store.io_parallelism(), |meta| async move {
            let filename = meta.location.filename().unwrap();
            let version = ManifestNamingScheme::V1.parse_version(filename).unwrap();
            let path = ManifestNamingScheme::V2.manifest_path(dataset_base, version);
            object_store.inner.rename(&meta.location, &path).await?;
            Ok(())
        })
        .await?;

    Ok(())
}

/// Function that writes the manifest to the object store.
///
/// Returns the size of the written manifest.
pub type ManifestWriter = for<'a> fn(
    object_store: &'a ObjectStore,
    manifest: &'a mut Manifest,
    indices: Option<Vec<IndexMetadata>>,
    path: &'a Path,
    transaction: Option<Transaction>,
) -> BoxFuture<'a, Result<WriteResult>>;

/// Canonical manifest writer; its function item type exactly matches `ManifestWriter`.
/// Rationale: keep a crate-local writer implementation so call sites can pass this function
/// directly without non-primitive casts or lifetime coercions.
pub fn write_manifest_file_to_path<'a>(
    object_store: &'a ObjectStore,
    manifest: &'a mut Manifest,
    indices: Option<Vec<IndexMetadata>>,
    path: &'a Path,
    transaction: Option<Transaction>,
) -> BoxFuture<'a, Result<WriteResult>> {
    Box::pin(async move {
        let mut object_writer = ObjectWriter::new(object_store, path).await?;
        let pos = write_manifest(&mut object_writer, manifest, indices, transaction).await?;
        object_writer
            .write_magics(pos, MAJOR_VERSION, MINOR_VERSION, MAGIC)
            .await?;
        let res = object_writer.shutdown().await?;
        info!(target: TRACE_FILE_AUDIT, mode=AUDIT_MODE_CREATE, r#type=AUDIT_TYPE_MANIFEST, path = path.to_string());
        Ok(res)
    })
}

#[derive(Debug, Clone)]
pub struct ManifestLocation {
    /// The version the manifest corresponds to.
    pub version: u64,
    /// Path of the manifest file, relative to the table root.
    pub path: Path,
    /// Size, in bytes, of the manifest file. If it is not known, this field should be `None`.
    pub size: Option<u64>,
    /// Naming scheme of the manifest file.
    pub naming_scheme: ManifestNamingScheme,
    /// Optional e-tag, used for integrity checks. Manifests should be immutable, so
    /// if we detect a change in the e-tag, it means the manifest was tampered with.
    /// This might happen if the dataset was deleted and then re-created.
    pub e_tag: Option<String>,
}

impl TryFrom<object_store::ObjectMeta> for ManifestLocation {
    type Error = Error;

    fn try_from(meta: object_store::ObjectMeta) -> Result<Self> {
        let filename = meta.location.filename().ok_or_else(|| Error::Internal {
            message: "ObjectMeta location does not have a filename".to_string(),
            location: location!(),
        })?;
        let scheme =
            ManifestNamingScheme::detect_scheme(filename).ok_or_else(|| Error::Internal {
                message: format!("Invalid manifest filename: '{}'", filename),
                location: location!(),
            })?;
        let version = scheme
            .parse_version(filename)
            .ok_or_else(|| Error::Internal {
                message: format!("Invalid manifest filename: '{}'", filename),
                location: location!(),
            })?;
        Ok(Self {
            version,
            path: meta.location,
            size: Some(meta.size),
            naming_scheme: scheme,
            e_tag: meta.e_tag,
        })
    }
}

/// Get the latest manifest path
async fn current_manifest_path(
    object_store: &ObjectStore,
    base: &Path,
) -> Result<ManifestLocation> {
    if object_store.is_local() {
        if let Ok(Some(location)) = current_manifest_local(base) {
            return Ok(location);
        }
    }

    let manifest_files = object_store.list(Some(base.child(VERSIONS_DIR)));

    let mut valid_manifests = manifest_files.try_filter_map(|res| {
        if let Some(scheme) = ManifestNamingScheme::detect_scheme(res.location.filename().unwrap())
        {
            future::ready(Ok(Some((scheme, res))))
        } else {
            future::ready(Ok(None))
        }
    });

    let first = valid_manifests.next().await.transpose()?;
    match (first, object_store.list_is_lexically_ordered) {
        // If the first valid manifest we see is V2, we can assume that we are using
        // V2 naming scheme for all manifests.
        (Some((scheme @ ManifestNamingScheme::V2, meta)), true) => {
            let version = scheme
                .parse_version(meta.location.filename().unwrap())
                .unwrap();

            // Sanity check: verify at least for the first 1k files that they are all V2
            // and that the version numbers are decreasing. We use the first 1k because
            // this is the typical size of an object store list endpoint response page.
            for (scheme, meta) in valid_manifests.take(999).try_collect::<Vec<_>>().await? {
                if scheme != ManifestNamingScheme::V2 {
                    warn!(
                        "Found V1 Manifest in a V2 directory. Use `migrate_manifest_paths_v2` \
                         to migrate the directory."
                    );
                    break;
                }
                let next_version = scheme
                    .parse_version(meta.location.filename().unwrap())
                    .unwrap();
                if next_version >= version {
                    warn!(
                        "List operation was expected to be lexically ordered, but was not. This \
                         could mean a corrupt read. Please make a bug report on the lance-format/lance \
                         GitHub repository."
                    );
                    break;
                }
            }

            Ok(ManifestLocation {
                version,
                path: meta.location,
                size: Some(meta.size),
                naming_scheme: scheme,
                e_tag: meta.e_tag,
            })
        }
        // If the list is not lexically ordered, we need to iterate all manifests
        // to find the latest version. This works for both V1 and V2 schemes.
        (Some((first_scheme, meta)), _) => {
            let mut current_version = first_scheme
                .parse_version(meta.location.filename().unwrap())
                .unwrap();
            let mut current_meta = meta;
            let scheme = first_scheme;

            while let Some((entry_scheme, meta)) = valid_manifests.next().await.transpose()? {
                if entry_scheme != scheme {
                    return Err(Error::Internal {
                        message: format!(
                            "Found multiple manifest naming schemes in the same directory: {:?} and {:?}. \
                             Use `migrate_manifest_paths_v2` to migrate the directory.",
                            scheme, entry_scheme
                        ),
                        location: location!(),
                    });
                }
                let version = entry_scheme
                    .parse_version(meta.location.filename().unwrap())
                    .unwrap();
                if version > current_version {
                    current_version = version;
                    current_meta = meta;
                }
            }
            Ok(ManifestLocation {
                version: current_version,
                path: current_meta.location,
                size: Some(current_meta.size),
                naming_scheme: scheme,
                e_tag: current_meta.e_tag,
            })
        }
        (None, _) => Err(Error::NotFound {
            uri: base.child(VERSIONS_DIR).to_string(),
            location: location!(),
        }),
    }
}

// This is an optimized function that searches for the latest manifest. In
// object_store, list operations lookup metadata for each file listed. This
// method only gets the metadata for the found latest manifest.
fn current_manifest_local(base: &Path) -> std::io::Result<Option<ManifestLocation>> {
    let path = lance_io::local::to_local_path(&base.child(VERSIONS_DIR));
    let entries = std::fs::read_dir(path)?;

    let mut latest_entry: Option<(u64, DirEntry)> = None;

    let mut scheme: Option<ManifestNamingScheme> = None;

    for entry in entries {
        let entry = entry?;
        let filename_raw = entry.file_name();
        let filename = filename_raw.to_string_lossy();

        let Some(entry_scheme) = ManifestNamingScheme::detect_scheme(&filename) else {
            // Need to ignore temporary files, such as
            // .tmp_7.manifest_9c100374-3298-4537-afc6-f5ee7913666d
            continue;
        };

        if let Some(scheme) = scheme {
            if scheme != entry_scheme {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "Found multiple manifest naming schemes in the same directory: {:?} and {:?}",
                        scheme, entry_scheme
                    ),
                ));
            }
        } else {
            scheme = Some(entry_scheme);
        }

        let Some(version) = entry_scheme.parse_version(&filename) else {
            continue;
        };

        if let Some((latest_version, _)) = &latest_entry {
            if version > *latest_version {
                latest_entry = Some((version, entry));
            }
        } else {
            latest_entry = Some((version, entry));
        }
    }

    if let Some((version, entry)) = latest_entry {
        let path = Path::from_filesystem_path(entry.path())
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
        let metadata = entry.metadata()?;
        Ok(Some(ManifestLocation {
            version,
            path,
            size: Some(metadata.len()),
            naming_scheme: scheme.unwrap(),
            e_tag: Some(get_etag(&metadata)),
        }))
    } else {
        Ok(None)
    }
}

// Based on object store's implementation.
fn get_etag(metadata: &std::fs::Metadata) -> String {
    let inode = get_inode(metadata);
    let size = metadata.len();
    let mtime = metadata
        .modified()
        .ok()
        .and_then(|mtime| mtime.duration_since(std::time::SystemTime::UNIX_EPOCH).ok())
        .unwrap_or_default()
        .as_micros();

    // Use an ETag scheme based on that used by many popular HTTP servers
    // <https://httpd.apache.org/docs/2.2/mod/core.html#fileetag>
    // <https://stackoverflow.com/questions/47512043/how-etags-are-generated-and-configured>
    format!("{inode:x}-{mtime:x}-{size:x}")
}

#[cfg(unix)]
/// We include the inode when available to yield an ETag more resistant to collisions
/// and as used by popular web servers such as [Apache](https://httpd.apache.org/docs/2.2/mod/core.html#fileetag)
fn get_inode(metadata: &std::fs::Metadata) -> u64 {
    std::os::unix::fs::MetadataExt::ino(metadata)
}

#[cfg(not(unix))]
/// On platforms where an inode isn't available, fallback to just relying on size and mtime
fn get_inode(_metadata: &std::fs::Metadata) -> u64 {
    0
}

fn list_manifests<'a>(
    base_path: &Path,
    object_store: &'a dyn OSObjectStore,
) -> impl Stream<Item = Result<ManifestLocation>> + 'a {
    object_store
        .read_dir_all(&base_path.child(VERSIONS_DIR), None)
        .filter_map(|obj_meta| {
            futures::future::ready(
                obj_meta
                    .map(|m| ManifestLocation::try_from(m).ok())
                    .transpose(),
            )
        })
        .boxed()
}

fn make_staging_manifest_path(base: &Path) -> Result<Path> {
    let id = uuid::Uuid::new_v4().to_string();
    Path::parse(format!("{base}-{id}")).map_err(|e| Error::IO {
        source: Box::new(e),
        location: location!(),
    })
}

#[cfg(feature = "dynamodb")]
const DDB_URL_QUERY_KEY: &str = "ddbTableName";

/// Handle commits that prevent conflicting writes.
///
/// Commit implementations ensure that if there are multiple concurrent writers
/// attempting to write the next version of a table, only one will win. In order
/// to work, all writers must use the same commit handler type.
/// This trait is also responsible for resolving where the manifests live.
///
// TODO: pub(crate)
#[async_trait::async_trait]
#[allow(clippy::too_many_arguments)]
pub trait CommitHandler: Debug + Send + Sync {
    async fn resolve_latest_location(
        &self,
        base_path: &Path,
        object_store: &ObjectStore,
    ) -> Result<ManifestLocation> {
        Ok(current_manifest_path(object_store, base_path).await?)
    }

    async fn resolve_version_location(
        &self,
        base_path: &Path,
        version: u64,
        object_store: &dyn OSObjectStore,
    ) -> Result<ManifestLocation> {
        default_resolve_version(base_path, version, object_store).await
    }

    /// If `sorted_descending` is `true`, the stream will yield manifests in descending
    /// order of version. When the object store has a lexicographically
    /// ordered list and the naming scheme is V2, this will use an optimized
    /// list operation. Otherwise, it will list all manifests and sort them
    /// in memory. When `sorted_descending` is `false`, the stream will yield manifests
    /// in arbitrary order.
    fn list_manifest_locations<'a>(
        &self,
        base_path: &Path,
        object_store: &'a ObjectStore,
        sorted_descending: bool,
    ) -> BoxStream<'a, Result<ManifestLocation>> {
        let underlying_stream = list_manifests(base_path, &object_store.inner);

        if !sorted_descending {
            return underlying_stream.boxed();
        }

        async fn sort_stream(
            input_stream: impl futures::Stream<Item = Result<ManifestLocation>> + Unpin,
        ) -> Result<impl Stream<Item = Result<ManifestLocation>> + Unpin> {
            let mut locations = input_stream.try_collect::<Vec<_>>().await?;
            locations.sort_by_key(|m| std::cmp::Reverse(m.version));
            Ok(futures::stream::iter(locations.into_iter().map(Ok)))
        }

        // If the object store supports lexicographically ordered lists and
        // the naming scheme is V2, we can use an optimized list operation.
        if object_store.list_is_lexically_ordered {
            // We don't know the naming scheme until we see the first manifest.
            let mut peekable = underlying_stream.peekable();

            futures::stream::once(async move {
                let naming_scheme = match Pin::new(&mut peekable).peek().await {
                    Some(Ok(m)) => m.naming_scheme,
                    // If we get an error or no manifests are found, we default
                    // to V2 naming scheme, since it doesn't matter.
                    Some(Err(_)) => ManifestNamingScheme::V2,
                    None => ManifestNamingScheme::V2,
                };

                if naming_scheme == ManifestNamingScheme::V2 {
                    // If the first manifest is V2, we can use the optimized list operation.
                    Ok(Either::Left(peekable))
                } else {
                    sort_stream(peekable).await.map(Either::Right)
                }
            })
            .try_flatten()
            .boxed()
        } else {
            // If the object store does not support lexicographically ordered lists,
            // we need to sort the manifests in memory. Systems where this isn't
            // supported (local fs, S3 express) are typically fast enough
            // that this is not a problem.
            futures::stream::once(sort_stream(underlying_stream))
                .try_flatten()
                .boxed()
        }
    }

    /// Commit a manifest.
    ///
    /// This function should return an [CommitError::CommitConflict] if another
    /// transaction has already been committed to the path.
    async fn commit(
        &self,
        manifest: &mut Manifest,
        indices: Option<Vec<IndexMetadata>>,
        base_path: &Path,
        object_store: &ObjectStore,
        manifest_writer: ManifestWriter,
        naming_scheme: ManifestNamingScheme,
        transaction: Option<Transaction>,
    ) -> std::result::Result<ManifestLocation, CommitError>;

    /// Delete the recorded manifest information for a dataset at the base_path
    async fn delete(&self, _base_path: &Path) -> Result<()> {
        Ok(())
    }
}

async fn default_resolve_version(
    base_path: &Path,
    version: u64,
    object_store: &dyn OSObjectStore,
) -> Result<ManifestLocation> {
    if is_detached_version(version) {
        return Ok(ManifestLocation {
            version,
            // Detached versions are not supported with V1 naming scheme.  If we need
            // to support in the future we could use a different prefix (e.g. 'x' or something)
            naming_scheme: ManifestNamingScheme::V2,
            // Both V1 and V2 should give the same path for detached versions
            path: ManifestNamingScheme::V2.manifest_path(base_path, version),
            size: None,
            e_tag: None,
        });
    }

    // try V2, fallback to V1.
    let scheme = ManifestNamingScheme::V2;
    let path = scheme.manifest_path(base_path, version);
    match object_store.head(&path).await {
        Ok(meta) => Ok(ManifestLocation {
            version,
            path,
            size: Some(meta.size),
            naming_scheme: scheme,
            e_tag: meta.e_tag,
        }),
        Err(ObjectStoreError::NotFound { .. }) => {
            // fallback to V1
            let scheme = ManifestNamingScheme::V1;
            Ok(ManifestLocation {
                version,
                path: scheme.manifest_path(base_path, version),
                size: None,
                naming_scheme: scheme,
                e_tag: None,
            })
        }
        Err(e) => Err(e.into()),
    }
}
/// Adapt an object_store credentials into AWS SDK creds
#[cfg(feature = "dynamodb")]
#[derive(Debug)]
struct OSObjectStoreToAwsCredAdaptor(AwsCredentialProvider);

#[cfg(feature = "dynamodb")]
impl ProvideCredentials for OSObjectStoreToAwsCredAdaptor {
    fn provide_credentials<'a>(
        &'a self,
    ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        aws_credential_types::provider::future::ProvideCredentials::new(async {
            let creds = self
                .0
                .get_credential()
                .await
                .map_err(|e| CredentialsError::provider_error(Box::new(e)))?;
            Ok(aws_credential_types::Credentials::new(
                &creds.key_id,
                &creds.secret_key,
                creds.token.clone(),
                Some(
                    SystemTime::now()
                        .checked_add(Duration::from_secs(
                            60 * 10, //  10 min
                        ))
                        .expect("overflow"),
                ),
                "",
            ))
        })
    }
}

#[cfg(feature = "dynamodb")]
async fn build_dynamodb_external_store(
    table_name: &str,
    creds: AwsCredentialProvider,
    region: &str,
    endpoint: Option<String>,
    app_name: &str,
) -> Result<Arc<dyn ExternalManifestStore>> {
    use super::commit::dynamodb::DynamoDBExternalManifestStore;
    use aws_sdk_dynamodb::{
        config::{retry::RetryConfig, IdentityCache, Region},
        Client,
    };

    let mut dynamodb_config = aws_sdk_dynamodb::config::Builder::new()
        .behavior_version_latest()
        .region(Some(Region::new(region.to_string())))
        .credentials_provider(OSObjectStoreToAwsCredAdaptor(creds))
        // caching should be handled by passed AwsCredentialProvider
        .identity_cache(IdentityCache::no_cache())
        // Be more resilient to transient network issues.
        // 5 attempts = 1 initial + 4 retries with exponential backoff.
        .retry_config(RetryConfig::standard().with_max_attempts(5));

    if let Some(endpoint) = endpoint {
        dynamodb_config = dynamodb_config.endpoint_url(endpoint);
    }
    let client = Client::from_conf(dynamodb_config.build());

    DynamoDBExternalManifestStore::new_external_store(client.into(), table_name, app_name).await
}

pub async fn commit_handler_from_url(
    url_or_path: &str,
    // This looks unused if dynamodb feature disabled
    #[allow(unused_variables)] options: &Option<ObjectStoreParams>,
) -> Result<Arc<dyn CommitHandler>> {
    let local_handler: Arc<dyn CommitHandler> = if cfg!(windows) {
        Arc::new(RenameCommitHandler)
    } else {
        Arc::new(ConditionalPutCommitHandler)
    };

    let url = match Url::parse(url_or_path) {
        Ok(url) if url.scheme().len() == 1 && cfg!(windows) => {
            // On Windows, the drive is parsed as a scheme
            return Ok(local_handler);
        }
        Ok(url) => url,
        Err(_) => {
            return Ok(local_handler);
        }
    };

    match url.scheme() {
        "file" | "file-object-store" => Ok(local_handler),
        "s3" | "gs" | "az" | "memory" | "oss" => Ok(Arc::new(ConditionalPutCommitHandler)),
        #[cfg(not(feature = "dynamodb"))]
        "s3+ddb" => Err(Error::InvalidInput {
            source: "`s3+ddb://` scheme requires `dynamodb` feature to be enabled".into(),
            location: location!(),
        }),
        #[cfg(feature = "dynamodb")]
        "s3+ddb" => {
            if url.query_pairs().count() != 1 {
                return Err(Error::InvalidInput {
                    source: "`s3+ddb://` scheme and expects exactly one query `ddbTableName`"
                        .into(),
                    location: location!(),
                });
            }
            let table_name = match url.query_pairs().next() {
                Some((Cow::Borrowed(key), Cow::Borrowed(table_name)))
                    if key == DDB_URL_QUERY_KEY =>
                {
                    if table_name.is_empty() {
                        return Err(Error::InvalidInput {
                            source: "`s3+ddb://` scheme requires non empty dynamodb table name"
                                .into(),
                            location: location!(),
                        });
                    }
                    table_name
                }
                _ => {
                    return Err(Error::InvalidInput {
                        source: "`s3+ddb://` scheme and expects exactly one query `ddbTableName`"
                            .into(),
                        location: location!(),
                    });
                }
            };
            let options = options.clone().unwrap_or_default();
            let storage_options_raw =
                StorageOptions(options.storage_options().cloned().unwrap_or_default());
            let dynamo_endpoint = get_dynamodb_endpoint(&storage_options_raw);
            let storage_options = storage_options_raw.as_s3_options();

            let region = storage_options.get(&AmazonS3ConfigKey::Region).cloned();

            // Get accessor from the options
            let accessor = options.get_accessor();

            let (aws_creds, region) = build_aws_credential(
                options.s3_credentials_refresh_offset,
                options.aws_credentials.clone(),
                Some(&storage_options),
                region,
                accessor,
            )
            .await?;

            Ok(Arc::new(ExternalManifestCommitHandler {
                external_manifest_store: build_dynamodb_external_store(
                    table_name,
                    aws_creds.clone(),
                    &region,
                    dynamo_endpoint,
                    "lancedb",
                )
                .await?,
            }))
        }
        _ => Ok(Arc::new(UnsafeCommitHandler)),
    }
}

#[cfg(feature = "dynamodb")]
fn get_dynamodb_endpoint(storage_options: &StorageOptions) -> Option<String> {
    if let Some(endpoint) = storage_options.0.get("dynamodb_endpoint") {
        Some(endpoint.clone())
    } else {
        std::env::var("DYNAMODB_ENDPOINT").ok()
    }
}

/// Errors that can occur when committing a manifest.
#[derive(Debug)]
pub enum CommitError {
    /// Another transaction has already been written to the path
    CommitConflict,
    /// Something else went wrong
    OtherError(Error),
}

impl From<Error> for CommitError {
    fn from(e: Error) -> Self {
        Self::OtherError(e)
    }
}

impl From<CommitError> for Error {
    fn from(e: CommitError) -> Self {
        match e {
            CommitError::CommitConflict => Self::Internal {
                message: "Commit conflict".to_string(),
                location: location!(),
            },
            CommitError::OtherError(e) => e,
        }
    }
}

/// Whether we have issued a warning about using the unsafe commit handler.
static WARNED_ON_UNSAFE_COMMIT: AtomicBool = AtomicBool::new(false);

/// A naive commit implementation that does not prevent conflicting writes.
///
/// This will log a warning the first time it is used.
pub struct UnsafeCommitHandler;

#[async_trait::async_trait]
#[allow(clippy::too_many_arguments)]
impl CommitHandler for UnsafeCommitHandler {
    async fn commit(
        &self,
        manifest: &mut Manifest,
        indices: Option<Vec<IndexMetadata>>,
        base_path: &Path,
        object_store: &ObjectStore,
        manifest_writer: ManifestWriter,
        naming_scheme: ManifestNamingScheme,
        transaction: Option<Transaction>,
    ) -> std::result::Result<ManifestLocation, CommitError> {
        // Log a one-time warning
        if !WARNED_ON_UNSAFE_COMMIT.load(std::sync::atomic::Ordering::Relaxed) {
            WARNED_ON_UNSAFE_COMMIT.store(true, std::sync::atomic::Ordering::Relaxed);
            log::warn!(
                "Using unsafe commit handler. Concurrent writes may result in data loss. \
                 Consider providing a commit handler that prevents conflicting writes."
            );
        }

        let version_path = naming_scheme.manifest_path(base_path, manifest.version);
        let res =
            manifest_writer(object_store, manifest, indices, &version_path, transaction).await?;

        Ok(ManifestLocation {
            version: manifest.version,
            size: Some(res.size as u64),
            naming_scheme,
            path: version_path,
            e_tag: res.e_tag,
        })
    }
}

impl Debug for UnsafeCommitHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnsafeCommitHandler").finish()
    }
}

/// A commit implementation that uses a lock to prevent conflicting writes.
#[async_trait::async_trait]
pub trait CommitLock: Debug {
    type Lease: CommitLease;

    /// Attempt to lock the table for the given version.
    ///
    /// If it is already locked by another transaction, wait until it is unlocked.
    /// Once it is unlocked, return [CommitError::CommitConflict] if the version
    /// has already been committed. Otherwise, return the lock.
    ///
    /// To prevent poisoned locks, it's recommended to set a timeout on the lock
    /// of at least 30 seconds.
    ///
    /// It is not required that the lock tracks the version. It is provided in
    /// case the locking is handled by a catalog service that needs to know the
    /// current version of the table.
    async fn lock(&self, version: u64) -> std::result::Result<Self::Lease, CommitError>;
}

#[async_trait::async_trait]
pub trait CommitLease: Send + Sync {
    /// Return the lease, indicating whether the commit was successful.
    async fn release(&self, success: bool) -> std::result::Result<(), CommitError>;
}

#[async_trait::async_trait]
impl<T: CommitLock + Send + Sync> CommitHandler for T {
    async fn commit(
        &self,
        manifest: &mut Manifest,
        indices: Option<Vec<IndexMetadata>>,
        base_path: &Path,
        object_store: &ObjectStore,
        manifest_writer: ManifestWriter,
        naming_scheme: ManifestNamingScheme,
        transaction: Option<Transaction>,
    ) -> std::result::Result<ManifestLocation, CommitError> {
        let path = naming_scheme.manifest_path(base_path, manifest.version);
        // NOTE: once we have the lease we cannot use ? to return errors, since
        // we must release the lease before returning.
        let lease = self.lock(manifest.version).await?;

        // Head the location and make sure it's not already committed
        match object_store.inner.head(&path).await {
            Ok(_) => {
                // The path already exists, so it's already committed
                // Release the lock
                lease.release(false).await?;

                return Err(CommitError::CommitConflict);
            }
            Err(ObjectStoreError::NotFound { .. }) => {}
            Err(e) => {
                // Something else went wrong
                // Release the lock
                lease.release(false).await?;

                return Err(CommitError::OtherError(e.into()));
            }
        }
        let res = manifest_writer(object_store, manifest, indices, &path, transaction).await;

        // Release the lock
        lease.release(res.is_ok()).await?;

        let res = res?;
        Ok(ManifestLocation {
            version: manifest.version,
            size: Some(res.size as u64),
            naming_scheme,
            path,
            e_tag: res.e_tag,
        })
    }
}

#[async_trait::async_trait]
impl<T: CommitLock + Send + Sync> CommitHandler for Arc<T> {
    async fn commit(
        &self,
        manifest: &mut Manifest,
        indices: Option<Vec<IndexMetadata>>,
        base_path: &Path,
        object_store: &ObjectStore,
        manifest_writer: ManifestWriter,
        naming_scheme: ManifestNamingScheme,
        transaction: Option<Transaction>,
    ) -> std::result::Result<ManifestLocation, CommitError> {
        self.as_ref()
            .commit(
                manifest,
                indices,
                base_path,
                object_store,
                manifest_writer,
                naming_scheme,
                transaction,
            )
            .await
    }
}

/// A commit implementation that uses a temporary path and renames the object.
///
/// This only works for object stores that support atomic rename if not exist.
pub struct RenameCommitHandler;

#[async_trait::async_trait]
impl CommitHandler for RenameCommitHandler {
    async fn commit(
        &self,
        manifest: &mut Manifest,
        indices: Option<Vec<IndexMetadata>>,
        base_path: &Path,
        object_store: &ObjectStore,
        manifest_writer: ManifestWriter,
        naming_scheme: ManifestNamingScheme,
        transaction: Option<Transaction>,
    ) -> std::result::Result<ManifestLocation, CommitError> {
        // Create a temporary object, then use `rename_if_not_exists` to commit.
        // If failed, clean up the temporary object.

        let path = naming_scheme.manifest_path(base_path, manifest.version);
        let tmp_path = make_staging_manifest_path(&path)?;

        let res = manifest_writer(object_store, manifest, indices, &tmp_path, transaction).await?;

        match object_store
            .inner
            .rename_if_not_exists(&tmp_path, &path)
            .await
        {
            Ok(_) => {
                // Successfully committed
                Ok(ManifestLocation {
                    version: manifest.version,
                    path,
                    size: Some(res.size as u64),
                    naming_scheme,
                    e_tag: None, // Re-name can change e-tag.
                })
            }
            Err(ObjectStoreError::AlreadyExists { .. }) => {
                // Another transaction has already been committed
                // Attempt to clean up temporary object, but ignore errors if we can't
                let _ = object_store.delete(&tmp_path).await;

                return Err(CommitError::CommitConflict);
            }
            Err(e) => {
                // Something else went wrong
                return Err(CommitError::OtherError(e.into()));
            }
        }
    }
}

impl Debug for RenameCommitHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RenameCommitHandler").finish()
    }
}

pub struct ConditionalPutCommitHandler;

#[async_trait::async_trait]
impl CommitHandler for ConditionalPutCommitHandler {
    async fn commit(
        &self,
        manifest: &mut Manifest,
        indices: Option<Vec<IndexMetadata>>,
        base_path: &Path,
        object_store: &ObjectStore,
        manifest_writer: ManifestWriter,
        naming_scheme: ManifestNamingScheme,
        transaction: Option<Transaction>,
    ) -> std::result::Result<ManifestLocation, CommitError> {
        let path = naming_scheme.manifest_path(base_path, manifest.version);

        let memory_store = ObjectStore::memory();
        let dummy_path = "dummy";
        manifest_writer(
            &memory_store,
            manifest,
            indices,
            &dummy_path.into(),
            transaction,
        )
        .await?;
        let dummy_data = memory_store.read_one_all(&dummy_path.into()).await?;
        let size = dummy_data.len() as u64;
        let res = object_store
            .inner
            .put_opts(
                &path,
                dummy_data.into(),
                PutOptions {
                    mode: object_store::PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .map_err(|err| match err {
                ObjectStoreError::AlreadyExists { .. } | ObjectStoreError::Precondition { .. } => {
                    CommitError::CommitConflict
                }
                _ => CommitError::OtherError(err.into()),
            })?;

        Ok(ManifestLocation {
            version: manifest.version,
            path,
            size: Some(size),
            naming_scheme,
            e_tag: res.e_tag,
        })
    }
}

impl Debug for ConditionalPutCommitHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConditionalPutCommitHandler").finish()
    }
}

#[derive(Debug, Clone)]
pub struct CommitConfig {
    pub num_retries: u32,
    pub skip_auto_cleanup: bool,
    // TODO: add isolation_level
}

impl Default for CommitConfig {
    fn default() -> Self {
        Self {
            num_retries: 20,
            skip_auto_cleanup: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use lance_core::utils::tempfile::TempObjDir;

    use super::*;

    #[test]
    fn test_manifest_naming_scheme() {
        let v1 = ManifestNamingScheme::V1;
        let v2 = ManifestNamingScheme::V2;

        assert_eq!(
            v1.manifest_path(&Path::from("base"), 0),
            Path::from("base/_versions/0.manifest")
        );
        assert_eq!(
            v1.manifest_path(&Path::from("base"), 42),
            Path::from("base/_versions/42.manifest")
        );

        assert_eq!(
            v2.manifest_path(&Path::from("base"), 0),
            Path::from("base/_versions/18446744073709551615.manifest")
        );
        assert_eq!(
            v2.manifest_path(&Path::from("base"), 42),
            Path::from("base/_versions/18446744073709551573.manifest")
        );

        assert_eq!(v1.parse_version("0.manifest"), Some(0));
        assert_eq!(v1.parse_version("42.manifest"), Some(42));
        assert_eq!(
            v1.parse_version("42.manifest-cee4fbbb-eb19-4ea3-8ca7-54f5ec33dedc"),
            Some(42)
        );

        assert_eq!(v2.parse_version("18446744073709551615.manifest"), Some(0));
        assert_eq!(v2.parse_version("18446744073709551573.manifest"), Some(42));
        assert_eq!(
            v2.parse_version("18446744073709551573.manifest-cee4fbbb-eb19-4ea3-8ca7-54f5ec33dedc"),
            Some(42)
        );

        assert_eq!(ManifestNamingScheme::detect_scheme("0.manifest"), Some(v1));
        assert_eq!(
            ManifestNamingScheme::detect_scheme("18446744073709551615.manifest"),
            Some(v2)
        );
        assert_eq!(ManifestNamingScheme::detect_scheme("something else"), None);
    }

    #[tokio::test]
    async fn test_manifest_naming_migration() {
        let object_store = ObjectStore::memory();
        let base = Path::from("base");
        let versions_dir = base.child(VERSIONS_DIR);

        // Write two v1 files and one v1
        let original_files = vec![
            versions_dir.child("irrelevant"),
            ManifestNamingScheme::V1.manifest_path(&base, 0),
            ManifestNamingScheme::V2.manifest_path(&base, 1),
        ];
        for path in original_files {
            object_store.put(&path, b"".as_slice()).await.unwrap();
        }

        migrate_scheme_to_v2(&object_store, &base).await.unwrap();

        let expected_files = vec![
            ManifestNamingScheme::V2.manifest_path(&base, 1),
            ManifestNamingScheme::V2.manifest_path(&base, 0),
            versions_dir.child("irrelevant"),
        ];
        let actual_files = object_store
            .inner
            .list(Some(&versions_dir))
            .map_ok(|res| res.location)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(actual_files, expected_files);
    }

    #[tokio::test]
    #[rstest::rstest]
    async fn test_list_manifests_sorted(
        #[values(true, false)] lexical_list_store: bool,
        #[values(ManifestNamingScheme::V1, ManifestNamingScheme::V2)]
        naming_scheme: ManifestNamingScheme,
    ) {
        let tempdir;
        let (object_store, base) = if lexical_list_store {
            (Box::new(ObjectStore::memory()), Path::from("base"))
        } else {
            tempdir = TempObjDir::default();
            let path = tempdir.child("base");
            let store = Box::new(ObjectStore::local());
            assert!(!store.list_is_lexically_ordered);
            (store, path)
        };

        // Write 12 manifest files, latest first
        let mut expected_paths = Vec::new();
        for i in (0..12).rev() {
            let path = naming_scheme.manifest_path(&base, i);
            object_store.put(&path, b"".as_slice()).await.unwrap();
            expected_paths.push(path);
        }

        let actual_versions = ConditionalPutCommitHandler
            .list_manifest_locations(&base, &object_store, true)
            .map_ok(|location| location.path)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(actual_versions, expected_paths);
    }

    #[tokio::test]
    #[rstest::rstest]
    async fn test_current_manifest_path(
        #[values(true, false)] lexical_list_store: bool,
        #[values(ManifestNamingScheme::V1, ManifestNamingScheme::V2)]
        naming_scheme: ManifestNamingScheme,
    ) {
        // Use memory store for both cases to avoid local FS special codepath.
        // Modify list_is_lexically_ordered to simulate different object stores.
        let mut object_store = ObjectStore::memory();
        object_store.list_is_lexically_ordered = lexical_list_store;
        let object_store = Box::new(object_store);
        let base = Path::from("base");

        // Write 12 manifest files in non-sequential order
        for version in [5, 2, 11, 0, 8, 3, 10, 1, 7, 4, 9, 6] {
            let path = naming_scheme.manifest_path(&base, version);
            object_store.put(&path, b"".as_slice()).await.unwrap();
        }

        let location = current_manifest_path(&object_store, &base).await.unwrap();

        assert_eq!(location.version, 11);
        assert_eq!(location.naming_scheme, naming_scheme);
        assert_eq!(location.path, naming_scheme.manifest_path(&base, 11));
    }
}
