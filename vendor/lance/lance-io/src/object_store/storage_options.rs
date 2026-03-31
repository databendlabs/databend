// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Storage options provider and accessor for dynamic credential fetching
//!
//! This module provides:
//! - [`StorageOptionsProvider`] trait for fetching storage options from various sources
//!   (namespace servers, secret managers, etc.) with support for expiration tracking
//! - [`StorageOptionsAccessor`] for unified access to storage options with automatic
//!   caching and refresh

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

#[cfg(test)]
use mock_instant::thread_local::{SystemTime, UNIX_EPOCH};

#[cfg(not(test))]
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use lance_namespace::models::DescribeTableRequest;
use lance_namespace::LanceNamespace;
use snafu::location;
use tokio::sync::RwLock;

use crate::{Error, Result};

/// Key for the expiration timestamp in storage options HashMap
pub const EXPIRES_AT_MILLIS_KEY: &str = "expires_at_millis";

/// Key for the refresh offset in storage options HashMap (milliseconds before expiry to refresh)
pub const REFRESH_OFFSET_MILLIS_KEY: &str = "refresh_offset_millis";

/// Default refresh offset: 60 seconds before expiration
const DEFAULT_REFRESH_OFFSET_MILLIS: u64 = 60_000;

/// Trait for providing storage options with expiration tracking
///
/// Implementations can fetch storage options from various sources (namespace servers,
/// secret managers, etc.) and are usable from Python/Java.
///
/// # Current Use Cases
///
/// - **Temporary Credentials**: Fetch short-lived AWS temporary credentials that expire
///   after a set time period, with automatic refresh before expiration
///
/// # Future Possible Use Cases
///
/// - **Dynamic Storage Location Resolution**: Resolve logical names to actual storage
///   locations (bucket aliases, S3 Access Points, region-specific endpoints) that may
///   change based on region, tier, data migration, or failover scenarios
/// - **Runtime S3 Tags Assignment**: Inject cost allocation tags, security labels, or
///   compliance metadata into S3 requests based on the current execution context (user,
///   application, workspace, etc.)
/// - **Dynamic Endpoint Configuration**: Update storage endpoints for disaster recovery,
///   A/B testing, or gradual migration scenarios
/// - **Just-in-time Permission Elevation**: Request elevated permissions only when needed
///   for sensitive operations, then immediately revoke them
/// - **Secret Manager Integration**: Fetch encryption keys from AWS Secrets Manager,
///   Azure Key Vault, or Google Secret Manager with automatic rotation
/// - **OIDC/SAML Federation**: Integrate with identity providers to obtain storage
///   credentials based on user identity and group membership
///
/// # Equality and Hashing
///
/// Implementations must provide `provider_id()` which returns a unique identifier for
/// equality and hashing purposes. Two providers with the same ID are considered equal
/// and will share the same cached ObjectStore in the registry.
#[async_trait]
pub trait StorageOptionsProvider: Send + Sync + fmt::Debug {
    /// Fetch fresh storage options
    ///
    /// Returns None if no storage options are available, or Some(HashMap) with the options.
    /// If the [`EXPIRES_AT_MILLIS_KEY`] key is present in the HashMap, it should contain the
    /// epoch time in milliseconds when the options expire, and credentials will automatically
    /// refresh before expiration.
    /// If [`EXPIRES_AT_MILLIS_KEY`] is not provided, the options are considered to never expire.
    async fn fetch_storage_options(&self) -> Result<Option<HashMap<String, String>>>;

    /// Return a human-readable unique identifier for this provider instance
    ///
    /// This is used for equality comparison and hashing in the object store registry.
    /// Two providers with the same ID will be treated as equal and share the same cached
    /// ObjectStore.
    ///
    /// The ID should be human-readable for debugging and logging purposes.
    /// For example: `"namespace[dir(root=/data)],table[db$schema$table1]"`
    ///
    /// The ID should uniquely identify the provider's configuration.
    fn provider_id(&self) -> String;
}

/// StorageOptionsProvider implementation that fetches options from a LanceNamespace
pub struct LanceNamespaceStorageOptionsProvider {
    namespace: Arc<dyn LanceNamespace>,
    table_id: Vec<String>,
}

impl fmt::Debug for LanceNamespaceStorageOptionsProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.provider_id())
    }
}

impl fmt::Display for LanceNamespaceStorageOptionsProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.provider_id())
    }
}

impl LanceNamespaceStorageOptionsProvider {
    /// Create a new LanceNamespaceStorageOptionsProvider
    ///
    /// # Arguments
    /// * `namespace` - The namespace implementation to fetch storage options from
    /// * `table_id` - The table identifier
    pub fn new(namespace: Arc<dyn LanceNamespace>, table_id: Vec<String>) -> Self {
        Self {
            namespace,
            table_id,
        }
    }
}

#[async_trait]
impl StorageOptionsProvider for LanceNamespaceStorageOptionsProvider {
    async fn fetch_storage_options(&self) -> Result<Option<HashMap<String, String>>> {
        let request = DescribeTableRequest {
            id: Some(self.table_id.clone()),
            ..Default::default()
        };

        let response = self
            .namespace
            .describe_table(request)
            .await
            .map_err(|e| Error::IO {
                source: Box::new(std::io::Error::other(format!(
                    "Failed to fetch storage options: {}",
                    e
                ))),
                location: location!(),
            })?;

        Ok(response.storage_options)
    }

    fn provider_id(&self) -> String {
        format!(
            "LanceNamespaceStorageOptionsProvider {{ namespace: {}, table_id: {:?} }}",
            self.namespace.namespace_id(),
            self.table_id
        )
    }
}

/// Unified access to storage options with automatic caching and refresh
///
/// This struct bundles static storage options with an optional dynamic provider,
/// handling all caching and refresh logic internally. It provides a single entry point
/// for accessing storage options regardless of whether they're static or dynamic.
///
/// # Behavior
///
/// - If only static options are provided, returns those options
/// - If a provider is configured, fetches from provider and caches results
/// - Automatically refreshes cached options before expiration (based on refresh_offset)
/// - Uses `expires_at_millis` key to track expiration
///
/// # Thread Safety
///
/// The accessor is thread-safe and can be shared across multiple tasks.
/// Concurrent refresh attempts are deduplicated using a try-lock mechanism.
pub struct StorageOptionsAccessor {
    /// Initial/fallback static storage options
    initial_options: Option<HashMap<String, String>>,

    /// Optional dynamic provider for refreshing options
    provider: Option<Arc<dyn StorageOptionsProvider>>,

    /// Cached storage options with expiration tracking
    cache: Arc<RwLock<Option<CachedStorageOptions>>>,

    /// Duration before expiry to trigger refresh
    refresh_offset: Duration,
}

impl fmt::Debug for StorageOptionsAccessor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageOptionsAccessor")
            .field("has_initial_options", &self.initial_options.is_some())
            .field("has_provider", &self.provider.is_some())
            .field("refresh_offset", &self.refresh_offset)
            .finish()
    }
}

#[derive(Debug, Clone)]
struct CachedStorageOptions {
    options: HashMap<String, String>,
    expires_at_millis: Option<u64>,
}

impl StorageOptionsAccessor {
    /// Extract refresh offset from storage options, or use default
    fn extract_refresh_offset(options: &HashMap<String, String>) -> Duration {
        options
            .get(REFRESH_OFFSET_MILLIS_KEY)
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or(Duration::from_millis(DEFAULT_REFRESH_OFFSET_MILLIS))
    }

    /// Create an accessor with only static options (no refresh capability)
    ///
    /// The returned accessor will always return the provided options.
    /// This is useful when credentials don't expire or are managed externally.
    pub fn with_static_options(options: HashMap<String, String>) -> Self {
        let expires_at_millis = options
            .get(EXPIRES_AT_MILLIS_KEY)
            .and_then(|s| s.parse::<u64>().ok());
        let refresh_offset = Self::extract_refresh_offset(&options);

        Self {
            initial_options: Some(options.clone()),
            provider: None,
            cache: Arc::new(RwLock::new(Some(CachedStorageOptions {
                options,
                expires_at_millis,
            }))),
            refresh_offset,
        }
    }

    /// Create an accessor with a dynamic provider (no initial options)
    ///
    /// The accessor will fetch from the provider on first access and cache
    /// the results. Refresh happens automatically before expiration.
    /// Uses the default refresh offset (60 seconds) until options are fetched.
    ///
    /// # Arguments
    /// * `provider` - The storage options provider for fetching fresh options
    pub fn with_provider(provider: Arc<dyn StorageOptionsProvider>) -> Self {
        Self {
            initial_options: None,
            provider: Some(provider),
            cache: Arc::new(RwLock::new(None)),
            refresh_offset: Duration::from_millis(DEFAULT_REFRESH_OFFSET_MILLIS),
        }
    }

    /// Create an accessor with initial options and a dynamic provider
    ///
    /// Initial options are used until they expire, then the provider is called.
    /// This avoids an immediate fetch when initial credentials are still valid.
    /// The `refresh_offset_millis` key in initial_options controls refresh timing.
    ///
    /// # Arguments
    /// * `initial_options` - Initial storage options to cache
    /// * `provider` - The storage options provider for refreshing
    pub fn with_initial_and_provider(
        initial_options: HashMap<String, String>,
        provider: Arc<dyn StorageOptionsProvider>,
    ) -> Self {
        let expires_at_millis = initial_options
            .get(EXPIRES_AT_MILLIS_KEY)
            .and_then(|s| s.parse::<u64>().ok());
        let refresh_offset = Self::extract_refresh_offset(&initial_options);

        Self {
            initial_options: Some(initial_options.clone()),
            provider: Some(provider),
            cache: Arc::new(RwLock::new(Some(CachedStorageOptions {
                options: initial_options,
                expires_at_millis,
            }))),
            refresh_offset,
        }
    }

    /// Get current valid storage options
    ///
    /// - Returns cached options if not expired
    /// - Fetches from provider if expired or not cached
    /// - Falls back to initial_options if provider returns None
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The provider fails to fetch options
    /// - No options are available (no cache, no provider, no initial options)
    pub async fn get_storage_options(&self) -> Result<super::StorageOptions> {
        loop {
            match self.do_get_storage_options().await? {
                Some(options) => return Ok(options),
                None => {
                    // Lock was busy, wait 10ms before retrying
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
            }
        }
    }

    async fn do_get_storage_options(&self) -> Result<Option<super::StorageOptions>> {
        // Check if we have valid cached options with read lock
        {
            let cached = self.cache.read().await;
            if !self.needs_refresh(&cached) {
                if let Some(cached_opts) = &*cached {
                    return Ok(Some(super::StorageOptions(cached_opts.options.clone())));
                }
            }
        }

        // If no provider, return initial options or error
        let Some(provider) = &self.provider else {
            return if let Some(initial) = &self.initial_options {
                Ok(Some(super::StorageOptions(initial.clone())))
            } else {
                Err(Error::IO {
                    source: Box::new(std::io::Error::other("No storage options available")),
                    location: location!(),
                })
            };
        };

        // Try to acquire write lock - if it fails, return None and let caller retry
        let Ok(mut cache) = self.cache.try_write() else {
            return Ok(None);
        };

        // Double-check if options are still stale after acquiring write lock
        // (another thread might have refreshed them)
        if !self.needs_refresh(&cache) {
            if let Some(cached_opts) = &*cache {
                return Ok(Some(super::StorageOptions(cached_opts.options.clone())));
            }
        }

        log::debug!(
            "Refreshing storage options from provider: {}",
            provider.provider_id()
        );

        let storage_options_map =
            provider
                .fetch_storage_options()
                .await
                .map_err(|e| Error::IO {
                    source: Box::new(std::io::Error::other(format!(
                        "Failed to fetch storage options: {}",
                        e
                    ))),
                    location: location!(),
                })?;

        let Some(options) = storage_options_map else {
            // Provider returned None, fall back to initial options
            if let Some(initial) = &self.initial_options {
                return Ok(Some(super::StorageOptions(initial.clone())));
            }
            return Err(Error::IO {
                source: Box::new(std::io::Error::other(
                    "Provider returned no storage options",
                )),
                location: location!(),
            });
        };

        let expires_at_millis = options
            .get(EXPIRES_AT_MILLIS_KEY)
            .and_then(|s| s.parse::<u64>().ok());

        if let Some(expires_at) = expires_at_millis {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::from_secs(0))
                .as_millis() as u64;
            let expires_in_secs = (expires_at.saturating_sub(now_ms)) / 1000;
            log::debug!(
                "Successfully refreshed storage options from provider: {}, options expire in {} seconds",
                provider.provider_id(),
                expires_in_secs
            );
        } else {
            log::debug!(
                "Successfully refreshed storage options from provider: {} (no expiration)",
                provider.provider_id()
            );
        }

        *cache = Some(CachedStorageOptions {
            options: options.clone(),
            expires_at_millis,
        });

        Ok(Some(super::StorageOptions(options)))
    }

    fn needs_refresh(&self, cached: &Option<CachedStorageOptions>) -> bool {
        match cached {
            None => true,
            Some(cached_opts) => {
                if let Some(expires_at_millis) = cached_opts.expires_at_millis {
                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or(Duration::from_secs(0))
                        .as_millis() as u64;

                    // Refresh if we're within the refresh offset of expiration
                    let refresh_offset_millis = self.refresh_offset.as_millis() as u64;
                    now_ms + refresh_offset_millis >= expires_at_millis
                } else {
                    // No expiration means options never expire
                    false
                }
            }
        }
    }

    /// Get the initial storage options without refresh
    ///
    /// Returns the initial options that were provided when creating the accessor.
    /// This does not trigger any refresh, even if the options have expired.
    pub fn initial_storage_options(&self) -> Option<&HashMap<String, String>> {
        self.initial_options.as_ref()
    }

    /// Get the accessor ID for equality/hashing
    ///
    /// Returns the provider_id if a provider exists, otherwise generates
    /// a stable ID from the initial options hash.
    pub fn accessor_id(&self) -> String {
        if let Some(provider) = &self.provider {
            provider.provider_id()
        } else if let Some(initial) = &self.initial_options {
            // Generate a stable ID from initial options
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};

            let mut hasher = DefaultHasher::new();
            let mut keys: Vec<_> = initial.keys().collect();
            keys.sort();
            for key in keys {
                key.hash(&mut hasher);
                initial.get(key).hash(&mut hasher);
            }
            format!("static_options_{:x}", hasher.finish())
        } else {
            "empty_accessor".to_string()
        }
    }

    /// Check if this accessor has a dynamic provider
    pub fn has_provider(&self) -> bool {
        self.provider.is_some()
    }

    /// Get the refresh offset duration
    pub fn refresh_offset(&self) -> Duration {
        self.refresh_offset
    }

    /// Get the storage options provider, if any
    pub fn provider(&self) -> Option<&Arc<dyn StorageOptionsProvider>> {
        self.provider.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mock_instant::thread_local::MockClock;

    #[derive(Debug)]
    struct MockStorageOptionsProvider {
        call_count: Arc<RwLock<usize>>,
        expires_in_millis: Option<u64>,
    }

    impl MockStorageOptionsProvider {
        fn new(expires_in_millis: Option<u64>) -> Self {
            Self {
                call_count: Arc::new(RwLock::new(0)),
                expires_in_millis,
            }
        }

        async fn get_call_count(&self) -> usize {
            *self.call_count.read().await
        }
    }

    #[async_trait]
    impl StorageOptionsProvider for MockStorageOptionsProvider {
        async fn fetch_storage_options(&self) -> Result<Option<HashMap<String, String>>> {
            let count = {
                let mut c = self.call_count.write().await;
                *c += 1;
                *c
            };

            let mut options = HashMap::from([
                ("aws_access_key_id".to_string(), format!("AKID_{}", count)),
                (
                    "aws_secret_access_key".to_string(),
                    format!("SECRET_{}", count),
                ),
                ("aws_session_token".to_string(), format!("TOKEN_{}", count)),
            ]);

            if let Some(expires_in) = self.expires_in_millis {
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                let expires_at = now_ms + expires_in;
                options.insert(EXPIRES_AT_MILLIS_KEY.to_string(), expires_at.to_string());
            }

            Ok(Some(options))
        }

        fn provider_id(&self) -> String {
            let ptr = Arc::as_ptr(&self.call_count) as usize;
            format!("MockStorageOptionsProvider {{ id: {} }}", ptr)
        }
    }

    #[tokio::test]
    async fn test_static_options_only() {
        let options = HashMap::from([
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]);
        let accessor = StorageOptionsAccessor::with_static_options(options.clone());

        let result = accessor.get_storage_options().await.unwrap();
        assert_eq!(result.0, options);
        assert!(!accessor.has_provider());
        assert_eq!(accessor.initial_storage_options(), Some(&options));
    }

    #[tokio::test]
    async fn test_provider_only() {
        MockClock::set_system_time(Duration::from_secs(100_000));

        let mock_provider = Arc::new(MockStorageOptionsProvider::new(Some(600_000)));
        let accessor = StorageOptionsAccessor::with_provider(mock_provider.clone());

        let result = accessor.get_storage_options().await.unwrap();
        assert!(result.0.contains_key("aws_access_key_id"));
        assert_eq!(result.0.get("aws_access_key_id").unwrap(), "AKID_1");
        assert!(accessor.has_provider());
        assert_eq!(accessor.initial_storage_options(), None);
        assert_eq!(mock_provider.get_call_count().await, 1);
    }

    #[tokio::test]
    async fn test_initial_and_provider_uses_initial_first() {
        MockClock::set_system_time(Duration::from_secs(100_000));

        let now_ms = MockClock::system_time().as_millis() as u64;
        let expires_at = now_ms + 600_000; // 10 minutes from now

        let initial = HashMap::from([
            ("aws_access_key_id".to_string(), "INITIAL_KEY".to_string()),
            (
                "aws_secret_access_key".to_string(),
                "INITIAL_SECRET".to_string(),
            ),
            (EXPIRES_AT_MILLIS_KEY.to_string(), expires_at.to_string()),
        ]);
        let mock_provider = Arc::new(MockStorageOptionsProvider::new(Some(600_000)));

        let accessor = StorageOptionsAccessor::with_initial_and_provider(
            initial.clone(),
            mock_provider.clone(),
        );

        // First call uses initial
        let result = accessor.get_storage_options().await.unwrap();
        assert_eq!(result.0.get("aws_access_key_id").unwrap(), "INITIAL_KEY");
        assert_eq!(mock_provider.get_call_count().await, 0); // Provider not called yet
    }

    #[tokio::test]
    async fn test_caching_and_refresh() {
        MockClock::set_system_time(Duration::from_secs(100_000));

        let mock_provider = Arc::new(MockStorageOptionsProvider::new(Some(600_000))); // 10 min expiry
                                                                                      // Use with_initial_and_provider to set custom refresh_offset_millis (5 min = 300000ms)
        let now_ms = MockClock::system_time().as_millis() as u64;
        let expires_at = now_ms + 600_000; // 10 minutes from now
        let initial = HashMap::from([
            (EXPIRES_AT_MILLIS_KEY.to_string(), expires_at.to_string()),
            (REFRESH_OFFSET_MILLIS_KEY.to_string(), "300000".to_string()), // 5 min refresh offset
        ]);
        let accessor =
            StorageOptionsAccessor::with_initial_and_provider(initial, mock_provider.clone());

        // First call uses initial cached options
        let result = accessor.get_storage_options().await.unwrap();
        assert!(result.0.contains_key(EXPIRES_AT_MILLIS_KEY));
        assert_eq!(mock_provider.get_call_count().await, 0);

        // Advance time to 6 minutes - should trigger refresh (within 5 min refresh offset)
        MockClock::set_system_time(Duration::from_secs(100_000 + 360));
        let result = accessor.get_storage_options().await.unwrap();
        assert_eq!(result.0.get("aws_access_key_id").unwrap(), "AKID_1");
        assert_eq!(mock_provider.get_call_count().await, 1);
    }

    #[tokio::test]
    async fn test_expired_initial_triggers_refresh() {
        MockClock::set_system_time(Duration::from_secs(100_000));

        let now_ms = MockClock::system_time().as_millis() as u64;
        let expired_time = now_ms - 1_000; // Expired 1 second ago

        let initial = HashMap::from([
            ("aws_access_key_id".to_string(), "EXPIRED_KEY".to_string()),
            (EXPIRES_AT_MILLIS_KEY.to_string(), expired_time.to_string()),
        ]);
        let mock_provider = Arc::new(MockStorageOptionsProvider::new(Some(600_000)));

        let accessor =
            StorageOptionsAccessor::with_initial_and_provider(initial, mock_provider.clone());

        // Should fetch from provider since initial is expired
        let result = accessor.get_storage_options().await.unwrap();
        assert_eq!(result.0.get("aws_access_key_id").unwrap(), "AKID_1");
        assert_eq!(mock_provider.get_call_count().await, 1);
    }

    #[tokio::test]
    async fn test_accessor_id_with_provider() {
        let mock_provider = Arc::new(MockStorageOptionsProvider::new(None));
        let accessor = StorageOptionsAccessor::with_provider(mock_provider);

        let id = accessor.accessor_id();
        assert!(id.starts_with("MockStorageOptionsProvider"));
    }

    #[tokio::test]
    async fn test_accessor_id_static() {
        let options = HashMap::from([("key".to_string(), "value".to_string())]);
        let accessor = StorageOptionsAccessor::with_static_options(options);

        let id = accessor.accessor_id();
        assert!(id.starts_with("static_options_"));
    }

    #[tokio::test]
    async fn test_concurrent_access() {
        // Create a mock provider with far future expiration
        let mock_provider = Arc::new(MockStorageOptionsProvider::new(Some(9999999999999)));

        let accessor = Arc::new(StorageOptionsAccessor::with_provider(mock_provider.clone()));

        // Spawn 10 concurrent tasks that all try to get options at the same time
        let mut handles = vec![];
        for i in 0..10 {
            let acc = accessor.clone();
            let handle = tokio::spawn(async move {
                let result = acc.get_storage_options().await.unwrap();
                assert_eq!(result.0.get("aws_access_key_id").unwrap(), "AKID_1");
                i
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        let results: Vec<_> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // Verify all 10 tasks completed successfully
        assert_eq!(results.len(), 10);

        // The provider should have been called exactly once
        let call_count = mock_provider.get_call_count().await;
        assert_eq!(
            call_count, 1,
            "Provider should be called exactly once despite concurrent access"
        );
    }

    #[tokio::test]
    async fn test_no_expiration_never_refreshes() {
        MockClock::set_system_time(Duration::from_secs(100_000));

        let mock_provider = Arc::new(MockStorageOptionsProvider::new(None)); // No expiration
        let accessor = StorageOptionsAccessor::with_provider(mock_provider.clone());

        // First call fetches
        accessor.get_storage_options().await.unwrap();
        assert_eq!(mock_provider.get_call_count().await, 1);

        // Advance time significantly
        MockClock::set_system_time(Duration::from_secs(200_000));

        // Should still use cached options
        accessor.get_storage_options().await.unwrap();
        assert_eq!(mock_provider.get_call_count().await, 1);
    }
}
