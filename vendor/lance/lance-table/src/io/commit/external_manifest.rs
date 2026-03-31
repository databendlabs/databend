// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Trait for external manifest handler.
//!
//! This trait abstracts an external storage with put_if_not_exists semantics.

use std::sync::Arc;

use async_trait::async_trait;
use lance_core::utils::tracing::{
    AUDIT_MODE_CREATE, AUDIT_MODE_DELETE, AUDIT_TYPE_MANIFEST, TRACE_FILE_AUDIT,
};
use lance_core::{Error, Result};
use lance_io::object_store::ObjectStore;
use log::warn;
use object_store::ObjectMeta;
use object_store::{path::Path, Error as ObjectStoreError, ObjectStore as OSObjectStore};
use snafu::location;
use tracing::info;

use super::{
    current_manifest_path, default_resolve_version, make_staging_manifest_path, ManifestLocation,
    ManifestNamingScheme, MANIFEST_EXTENSION,
};
use crate::format::{IndexMetadata, Manifest, Transaction};
use crate::io::commit::{CommitError, CommitHandler};

/// External manifest store
///
/// This trait abstracts an external storage for source of truth for manifests.
/// The storage is expected to remember (uri, version) -> manifest_path
/// and able to run transactions on the manifest_path.
///
/// This trait is called an **External** manifest store because the store is
/// expected to work in tandem with the object store. We are only leveraging
/// the external store for concurrent commit. Any manifest committed thru this
/// trait should ultimately be materialized in the object store.
/// For a visual explanation of the commit loop see
/// https://github.com/lance-format/lance/assets/12615154/b0822312-0826-432a-b554-3965f8d48d04
#[async_trait]
pub trait ExternalManifestStore: std::fmt::Debug + Send + Sync {
    /// Get the manifest path for a given base_uri and version
    async fn get(&self, base_uri: &str, version: u64) -> Result<String>;

    async fn get_manifest_location(
        &self,
        base_uri: &str,
        version: u64,
    ) -> Result<ManifestLocation> {
        let path = self.get(base_uri, version).await?;
        let path = Path::from(path);
        let naming_scheme = detect_naming_scheme_from_path(&path)?;
        Ok(ManifestLocation {
            version,
            path,
            size: None,
            naming_scheme,
            e_tag: None,
        })
    }

    /// Get the latest version of a dataset at the base_uri, and the path to the manifest.
    /// The path is provided as an optimization. The path is deterministic based on
    /// the version and the store should not customize it.
    async fn get_latest_version(&self, base_uri: &str) -> Result<Option<(u64, String)>>;

    /// Get the latest manifest location for a given base_uri.
    ///
    /// By default, this calls get_latest_version.  Impls should
    /// override this method if they store both the location and size
    /// of the latest manifest.
    async fn get_latest_manifest_location(
        &self,
        base_uri: &str,
    ) -> Result<Option<ManifestLocation>> {
        self.get_latest_version(base_uri).await.and_then(|res| {
            res.map(|(version, uri)| {
                let path = Path::from(uri);
                let naming_scheme = detect_naming_scheme_from_path(&path)?;
                Ok(ManifestLocation {
                    version,
                    path,
                    size: None,
                    naming_scheme,
                    e_tag: None,
                })
            })
            .transpose()
        })
    }

    /// Put the manifest path for a given base_uri and version, should fail if the version already exists
    async fn put_if_not_exists(
        &self,
        base_uri: &str,
        version: u64,
        path: &str,
        size: u64,
        e_tag: Option<String>,
    ) -> Result<()>;

    /// Put the manifest path for a given base_uri and version, should fail if the version **does not** already exist
    async fn put_if_exists(
        &self,
        base_uri: &str,
        version: u64,
        path: &str,
        size: u64,
        e_tag: Option<String>,
    ) -> Result<()>;

    /// Delete the manifest information for given base_uri from the store
    async fn delete(&self, _base_uri: &str) -> Result<()> {
        Ok(())
    }
}

pub(crate) fn detect_naming_scheme_from_path(path: &Path) -> Result<ManifestNamingScheme> {
    path.filename()
        .and_then(|name| {
            ManifestNamingScheme::detect_scheme(name)
                .or_else(|| Some(ManifestNamingScheme::detect_scheme_staging(name)))
        })
        .ok_or_else(|| {
            Error::corrupt_file(
                path.clone(),
                "Path does not follow known manifest naming convention.",
                location!(),
            )
        })
}

/// External manifest commit handler
/// This handler is used to commit a manifest to an external store
/// for detailed design, see https://github.com/lance-format/lance/issues/1183
#[derive(Debug)]
pub struct ExternalManifestCommitHandler {
    pub external_manifest_store: Arc<dyn ExternalManifestStore>,
}

impl ExternalManifestCommitHandler {
    /// The manifest is considered committed once the staging manifest is written
    /// to object store and that path is committed to the external store.
    ///
    /// However, to fully complete this, the staging manifest should be materialized
    /// into the final path, the final path should be committed to the external store
    /// and the staging manifest should be deleted. These steps may be completed
    /// by any number of readers or writers, so care should be taken to ensure
    /// that the manifest is not lost nor any errors occur due to duplicate
    /// operations.
    #[allow(clippy::too_many_arguments)]
    async fn finalize_manifest(
        &self,
        base_path: &Path,
        staging_manifest_path: &Path,
        version: u64,
        size: u64,
        e_tag: Option<String>,
        store: &dyn OSObjectStore,
        naming_scheme: ManifestNamingScheme,
    ) -> std::result::Result<ManifestLocation, Error> {
        // step 1: copy the manifest to the final location
        let final_manifest_path = naming_scheme.manifest_path(base_path, version);

        let copied = match store
            .copy(staging_manifest_path, &final_manifest_path)
            .await
        {
            Ok(_) => true,
            Err(ObjectStoreError::NotFound { .. }) => false, // Another writer beat us to it.
            Err(e) => return Err(e.into()),
        };
        if copied {
            info!(target: TRACE_FILE_AUDIT, mode=AUDIT_MODE_CREATE, r#type=AUDIT_TYPE_MANIFEST, path = final_manifest_path.as_ref());
        }

        // On S3, the etag can change if originally was MultipartUpload and later was Copy
        // https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html#AmazonS3-Type-Object-ETag
        // We only do MultipartUpload for > 5MB files, so we can skip this check
        // if size < 5MB. However, we need to double check the final_manifest_path
        // exists before we change the external store, otherwise we may point to a
        // non-existing manifest.
        let e_tag = if copied && size < 5 * 1024 * 1024 {
            e_tag
        } else {
            let meta = store.head(&final_manifest_path).await?;
            meta.e_tag
        };

        let location = ManifestLocation {
            version,
            path: final_manifest_path,
            size: Some(size),
            naming_scheme,
            e_tag,
        };

        if !copied {
            return Ok(location);
        }

        // step 2: flip the external store to point to the final location
        self.external_manifest_store
            .put_if_exists(
                base_path.as_ref(),
                version,
                location.path.as_ref(),
                size,
                location.e_tag.clone(),
            )
            .await?;

        // step 3: delete the staging manifest
        match store.delete(staging_manifest_path).await {
            Ok(_) => {}
            Err(ObjectStoreError::NotFound { .. }) => {}
            Err(e) => return Err(e.into()),
        }
        info!(target: TRACE_FILE_AUDIT, mode=AUDIT_MODE_DELETE, r#type=AUDIT_TYPE_MANIFEST, path = staging_manifest_path.as_ref());

        Ok(location)
    }
}

#[async_trait]
impl CommitHandler for ExternalManifestCommitHandler {
    async fn resolve_latest_location(
        &self,
        base_path: &Path,
        object_store: &ObjectStore,
    ) -> std::result::Result<ManifestLocation, Error> {
        let location = self
            .external_manifest_store
            .get_latest_manifest_location(base_path.as_ref())
            .await?;

        match location {
            Some(ManifestLocation {
                version,
                path,
                size,
                naming_scheme,
                e_tag,
            }) => {
                // The path is finalized, no need to check object store
                if path.extension() == Some(MANIFEST_EXTENSION) {
                    return Ok(ManifestLocation {
                        version,
                        path,
                        size,
                        naming_scheme,
                        e_tag,
                    });
                }

                let (size, e_tag) = if let Some(size) = size {
                    (size, e_tag)
                } else {
                    match object_store.inner.head(&path).await {
                        Ok(meta) => (meta.size, meta.e_tag),
                        Err(ObjectStoreError::NotFound { .. }) => {
                            // there may be other threads that have finished executing finalize_manifest.
                            let new_location = self
                                .external_manifest_store
                                .get_manifest_location(base_path.as_ref(), version)
                                .await?;
                            return Ok(new_location);
                        }
                        Err(e) => return Err(e.into()),
                    }
                };

                let final_location = self
                    .finalize_manifest(
                        base_path,
                        &path,
                        version,
                        size,
                        e_tag.clone(),
                        &object_store.inner,
                        naming_scheme,
                    )
                    .await?;

                Ok(final_location)
            }
            // Dataset not found in the external store, this could be because the dataset did not
            // use external store for commit before. In this case, we search for the latest manifest
            None => current_manifest_path(object_store, base_path).await,
        }
    }

    async fn resolve_version_location(
        &self,
        base_path: &Path,
        version: u64,
        object_store: &dyn OSObjectStore,
    ) -> std::result::Result<ManifestLocation, Error> {
        let location_res = self
            .external_manifest_store
            .get_manifest_location(base_path.as_ref(), version)
            .await;

        let location = match location_res {
            Ok(p) => p,
            // not board external manifest yet, direct to object store
            Err(Error::NotFound { .. }) => {
                let path = default_resolve_version(base_path, version, object_store)
                    .await
                    .map_err(|_| Error::NotFound {
                        uri: format!("{}@{}", base_path, version),
                        location: location!(),
                    })?
                    .path;
                match object_store.head(&path).await {
                    Ok(ObjectMeta { size, e_tag, .. }) => {
                        let res = self
                            .external_manifest_store
                            .put_if_not_exists(
                                base_path.as_ref(),
                                version,
                                path.as_ref(),
                                size,
                                e_tag.clone(),
                            )
                            .await;
                        if let Err(e) = res {
                            warn!(
                                "could not update external manifest store during load, with error: {}",
                                e
                            );
                        }
                        let naming_scheme =
                            ManifestNamingScheme::detect_scheme_staging(path.filename().unwrap());
                        return Ok(ManifestLocation {
                            version,
                            path,
                            size: Some(size),
                            naming_scheme,
                            e_tag,
                        });
                    }
                    Err(ObjectStoreError::NotFound { .. }) => {
                        return Err(Error::NotFound {
                            uri: path.to_string(),
                            location: location!(),
                        });
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            Err(e) => return Err(e),
        };

        // finalized path, just return
        if location.path.extension() == Some(MANIFEST_EXTENSION) {
            return Ok(location);
        }

        let naming_scheme =
            ManifestNamingScheme::detect_scheme_staging(location.path.filename().unwrap());

        let (size, e_tag) = if let Some(size) = location.size {
            (size, location.e_tag.clone())
        } else {
            let meta = object_store.head(&location.path).await?;
            (meta.size as u64, meta.e_tag)
        };

        self.finalize_manifest(
            base_path,
            &location.path,
            version,
            size,
            e_tag,
            object_store,
            naming_scheme,
        )
        .await
    }

    async fn commit(
        &self,
        manifest: &mut Manifest,
        indices: Option<Vec<IndexMetadata>>,
        base_path: &Path,
        object_store: &ObjectStore,
        manifest_writer: super::ManifestWriter,
        naming_scheme: ManifestNamingScheme,
        transaction: Option<Transaction>,
    ) -> std::result::Result<ManifestLocation, CommitError> {
        // path we get here is the path to the manifest we want to write
        // use object_store.base_path.as_ref() for getting the root of the dataset

        // step 1: Write the manifest we want to commit to object store with a temporary name
        let path = naming_scheme.manifest_path(base_path, manifest.version);
        let staging_path = make_staging_manifest_path(&path)?;
        let write_res =
            manifest_writer(object_store, manifest, indices, &staging_path, transaction).await?;

        // step 2 & 3: Try to commit this version to external store, return err on failure
        let res = self
            .external_manifest_store
            .put_if_not_exists(
                base_path.as_ref(),
                manifest.version,
                staging_path.as_ref(),
                write_res.size as u64,
                write_res.e_tag.clone(),
            )
            .await
            .map_err(|_| CommitError::CommitConflict {});

        if let Err(err) = res {
            // delete the staging manifest
            match object_store.inner.delete(&staging_path).await {
                Ok(_) => {}
                Err(ObjectStoreError::NotFound { .. }) => {}
                Err(e) => return Err(CommitError::OtherError(e.into())),
            }
            info!(target: TRACE_FILE_AUDIT, mode=AUDIT_MODE_DELETE, r#type=AUDIT_TYPE_MANIFEST, path = staging_path.as_ref());
            return Err(err);
        }

        Ok(self
            .finalize_manifest(
                base_path,
                &staging_path,
                manifest.version,
                write_res.size as u64,
                write_res.e_tag,
                &object_store.inner,
                naming_scheme,
            )
            .await?)
    }

    async fn delete(&self, base_path: &Path) -> Result<()> {
        self.external_manifest_store
            .delete(base_path.as_ref())
            .await
    }
}
