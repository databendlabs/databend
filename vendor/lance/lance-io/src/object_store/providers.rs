// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{
    collections::HashMap,
    sync::{Arc, RwLock, Weak},
};

use object_store::path::Path;
use snafu::location;
use url::Url;

use crate::object_store::uri_to_url;
use crate::object_store::WrappingObjectStore;

use super::{tracing::ObjectStoreTracingExt, ObjectStore, ObjectStoreParams};
use lance_core::error::{Error, LanceOptionExt, Result};

#[cfg(feature = "aws")]
pub mod aws;
#[cfg(feature = "azure")]
pub mod azure;
#[cfg(feature = "gcp")]
pub mod gcp;
#[cfg(feature = "huggingface")]
pub mod huggingface;
pub mod local;
pub mod memory;
#[cfg(feature = "oss")]
pub mod oss;

#[async_trait::async_trait]
pub trait ObjectStoreProvider: std::fmt::Debug + Sync + Send {
    async fn new_store(&self, base_path: Url, params: &ObjectStoreParams) -> Result<ObjectStore>;

    /// Extract the path relative to the base of the store.
    ///
    /// For example, in S3 the path is relative to the bucket. So a URL of
    /// `s3://bucket/path/to/file` would return `path/to/file`.
    ///
    /// Meanwhile, for a file store, the path is relative to the filesystem root.
    /// So a URL of `file:///path/to/file` would return `/path/to/file`.
    fn extract_path(&self, url: &Url) -> Result<Path> {
        Path::parse(url.path()).map_err(|_| {
            Error::invalid_input(format!("Invalid path in URL: {}", url.path()), location!())
        })
    }

    /// Calculate the unique prefix that should be used for this object store.
    ///
    /// For object stores that don't have the concept of buckets, this will just be something like
    /// 'file' or 'memory'.
    ///
    /// In object stores where all bucket names are unique, like s3, this will be
    /// simply 's3$my_bucket_name' or similar.
    ///
    /// In Azure, only the combination of (account name, container name) is unique, so
    /// this will be something like 'az$account_name@container'
    ///
    /// Providers should override this if they have special requirements like Azure's.
    fn calculate_object_store_prefix(
        &self,
        url: &Url,
        _storage_options: Option<&HashMap<String, String>>,
    ) -> Result<String> {
        Ok(format!("{}${}", url.scheme(), url.authority()))
    }
}

/// A registry of object store providers.
///
/// Use [`Self::default()`] to create one with the available default providers.
/// This includes (depending on features enabled):
/// - `memory`: An in-memory object store.
/// - `file`: A local file object store, with optimized code paths.
/// - `file-object-store`: A local file object store that uses the ObjectStore API,
///   for all operations. Used for testing with ObjectStore wrappers.
/// - `s3`: An S3 object store.
/// - `s3+ddb`: An S3 object store with DynamoDB for metadata.
/// - `az`: An Azure Blob Storage object store.
/// - `gs`: A Google Cloud Storage object store.
///
/// Use [`Self::empty()`] to create an empty registry, with no providers registered.
///
/// The registry also caches object stores that are currently in use. It holds
/// weak references to the object stores, so they are not held onto. If an object
/// store is no longer in use, it will be removed from the cache on the next
/// call to either [`Self::active_stores()`] or [`Self::get_store()`].
#[derive(Debug)]
pub struct ObjectStoreRegistry {
    providers: RwLock<HashMap<String, Arc<dyn ObjectStoreProvider>>>,
    // Cache of object stores currently in use. We use a weak reference so the
    // cache itself doesn't keep them alive if no object store is actually using
    // it.
    active_stores: RwLock<HashMap<(String, ObjectStoreParams), Weak<ObjectStore>>>,
}

impl ObjectStoreRegistry {
    /// Create a new registry with no providers registered.
    ///
    /// Typically, you want to use [`Self::default()`] instead, so you get the
    /// default providers.
    pub fn empty() -> Self {
        Self {
            providers: RwLock::new(HashMap::new()),
            active_stores: RwLock::new(HashMap::new()),
        }
    }

    /// Get the object store provider for a given scheme.
    pub fn get_provider(&self, scheme: &str) -> Option<Arc<dyn ObjectStoreProvider>> {
        self.providers
            .read()
            .expect("ObjectStoreRegistry lock poisoned")
            .get(scheme)
            .cloned()
    }

    /// Get a list of all active object stores.
    ///
    /// Calling this will also clean up any weak references to object stores that
    /// are no longer valid.
    pub fn active_stores(&self) -> Vec<Arc<ObjectStore>> {
        let mut found_inactive = false;
        let output = self
            .active_stores
            .read()
            .expect("ObjectStoreRegistry lock poisoned")
            .values()
            .filter_map(|weak| match weak.upgrade() {
                Some(store) => Some(store),
                None => {
                    found_inactive = true;
                    None
                }
            })
            .collect();

        if found_inactive {
            // Clean up the cache by removing any weak references that are no longer valid
            let mut cache_lock = self
                .active_stores
                .write()
                .expect("ObjectStoreRegistry lock poisoned");
            cache_lock.retain(|_, weak| weak.upgrade().is_some());
        }
        output
    }

    fn scheme_not_found_error(&self, scheme: &str) -> Error {
        let mut message = format!("No object store provider found for scheme: '{}'", scheme);
        if let Ok(providers) = self.providers.read() {
            let valid_schemes = providers.keys().cloned().collect::<Vec<_>>().join(", ");
            message.push_str(&format!("\nValid schemes: {}", valid_schemes));
        }
        Error::invalid_input(message, location!())
    }

    /// Get an object store for a given base path and parameters.
    ///
    /// If the object store is already in use, it will return a strong reference
    /// to the object store. If the object store is not in use, it will create a
    /// new object store and return a strong reference to it.
    pub async fn get_store(
        &self,
        base_path: Url,
        params: &ObjectStoreParams,
    ) -> Result<Arc<ObjectStore>> {
        let scheme = base_path.scheme();
        let Some(provider) = self.get_provider(scheme) else {
            return Err(self.scheme_not_found_error(scheme));
        };

        let cache_path =
            provider.calculate_object_store_prefix(&base_path, params.storage_options())?;
        let cache_key = (cache_path.clone(), params.clone());

        // Check if we have a cached store for this base path and params
        {
            let maybe_store = self
                .active_stores
                .read()
                .ok()
                .expect_ok()?
                .get(&cache_key)
                .cloned();
            if let Some(store) = maybe_store {
                if let Some(store) = store.upgrade() {
                    return Ok(store);
                } else {
                    // Remove the weak reference if it is no longer valid
                    let mut cache_lock = self
                        .active_stores
                        .write()
                        .expect("ObjectStoreRegistry lock poisoned");
                    if let Some(store) = cache_lock.get(&cache_key) {
                        if store.upgrade().is_none() {
                            // Remove the weak reference if it is no longer valid
                            cache_lock.remove(&cache_key);
                        }
                    }
                }
            }
        }

        let mut store = provider.new_store(base_path, params).await?;

        store.inner = store.inner.traced();

        if let Some(wrapper) = &params.object_store_wrapper {
            store.inner = wrapper.wrap(&cache_path, store.inner);
        }

        // Always wrap with IO tracking
        store.inner = store.io_tracker.wrap("", store.inner);

        let store = Arc::new(store);

        {
            // Insert the store into the cache
            let mut cache_lock = self.active_stores.write().ok().expect_ok()?;
            cache_lock.insert(cache_key, Arc::downgrade(&store));
        }

        Ok(store)
    }

    /// Calculate the datastore prefix based on the URI and the storage options.
    /// The data store prefix should uniquely identify the datastore.
    pub fn calculate_object_store_prefix(
        &self,
        uri: &str,
        storage_options: Option<&HashMap<String, String>>,
    ) -> Result<String> {
        let url = uri_to_url(uri)?;
        match self.get_provider(url.scheme()) {
            None => {
                if url.scheme() == "file" || url.scheme().len() == 1 {
                    Ok("file".to_string())
                } else {
                    Err(self.scheme_not_found_error(url.scheme()))
                }
            }
            Some(provider) => provider.calculate_object_store_prefix(&url, storage_options),
        }
    }
}

impl Default for ObjectStoreRegistry {
    fn default() -> Self {
        let mut providers: HashMap<String, Arc<dyn ObjectStoreProvider>> = HashMap::new();

        providers.insert("memory".into(), Arc::new(memory::MemoryStoreProvider));
        providers.insert("file".into(), Arc::new(local::FileStoreProvider));
        // The "file" scheme has special optimized code paths that bypass
        // the ObjectStore API for better performance. However, this can make it
        // hard to test when using ObjectStore wrappers, such as IOTrackingStore.
        // So we provide a "file-object-store" scheme that uses the ObjectStore API.
        // The specialized code paths are differentiated by the scheme name.
        providers.insert(
            "file-object-store".into(),
            Arc::new(local::FileStoreProvider),
        );

        #[cfg(feature = "aws")]
        {
            let aws = Arc::new(aws::AwsStoreProvider);
            providers.insert("s3".into(), aws.clone());
            providers.insert("s3+ddb".into(), aws);
        }
        #[cfg(feature = "azure")]
        providers.insert("az".into(), Arc::new(azure::AzureBlobStoreProvider));
        #[cfg(feature = "gcp")]
        providers.insert("gs".into(), Arc::new(gcp::GcsStoreProvider));
        #[cfg(feature = "oss")]
        providers.insert("oss".into(), Arc::new(oss::OssStoreProvider));
        #[cfg(feature = "huggingface")]
        providers.insert("hf".into(), Arc::new(huggingface::HuggingfaceStoreProvider));
        Self {
            providers: RwLock::new(providers),
            active_stores: RwLock::new(HashMap::new()),
        }
    }
}

impl ObjectStoreRegistry {
    /// Add a new object store provider to the registry. The provider will be used
    /// in [`Self::get_store()`] when a URL is passed with a matching scheme.
    pub fn insert(&self, scheme: &str, provider: Arc<dyn ObjectStoreProvider>) {
        self.providers
            .write()
            .expect("ObjectStoreRegistry lock poisoned")
            .insert(scheme.into(), provider);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct DummyProvider;

    #[async_trait::async_trait]
    impl ObjectStoreProvider for DummyProvider {
        async fn new_store(
            &self,
            _base_path: Url,
            _params: &ObjectStoreParams,
        ) -> Result<ObjectStore> {
            unreachable!("This test doesn't create stores")
        }
    }

    #[test]
    fn test_calculate_object_store_prefix() {
        let provider = DummyProvider;
        let url = Url::parse("dummy://blah/path").unwrap();
        assert_eq!(
            "dummy$blah",
            provider.calculate_object_store_prefix(&url, None).unwrap()
        );
    }

    #[test]
    fn test_calculate_object_store_scheme_not_found() {
        let registry = ObjectStoreRegistry::empty();
        registry.insert("dummy", Arc::new(DummyProvider));
        let s = "Invalid user input: No object store provider found for scheme: 'dummy2'\nValid schemes: dummy";
        let result = registry
            .calculate_object_store_prefix("dummy2://mybucket/my/long/path", None)
            .expect_err("expected error")
            .to_string();
        assert_eq!(s, &result[..s.len()]);
    }

    // Test that paths without a scheme get treated as local paths.
    #[test]
    fn test_calculate_object_store_prefix_for_local() {
        let registry = ObjectStoreRegistry::empty();
        assert_eq!(
            "file",
            registry
                .calculate_object_store_prefix("/tmp/foobar", None)
                .unwrap()
        );
    }

    // Test that paths with a single-letter scheme that is not registered for anything get treated as local paths.
    #[test]
    fn test_calculate_object_store_prefix_for_local_windows_path() {
        let registry = ObjectStoreRegistry::empty();
        assert_eq!(
            "file",
            registry
                .calculate_object_store_prefix("c://dos/path", None)
                .unwrap()
        );
    }

    // Test that paths with a given scheme get mapped to that storage provider.
    #[test]
    fn test_calculate_object_store_prefix_for_dummy_path() {
        let registry = ObjectStoreRegistry::empty();
        registry.insert("dummy", Arc::new(DummyProvider));
        assert_eq!(
            "dummy$mybucket",
            registry
                .calculate_object_store_prefix("dummy://mybucket/my/long/path", None)
                .unwrap()
        );
    }
}
