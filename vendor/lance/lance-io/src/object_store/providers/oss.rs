// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashMap;
use std::sync::Arc;

use object_store_opendal::OpendalStore;
use opendal::{services::Oss, Operator};
use snafu::location;
use url::Url;

use crate::object_store::{
    ObjectStore, ObjectStoreParams, ObjectStoreProvider, StorageOptions, DEFAULT_CLOUD_BLOCK_SIZE,
    DEFAULT_CLOUD_IO_PARALLELISM, DEFAULT_MAX_IOP_SIZE,
};
use lance_core::error::{Error, Result};

#[derive(Default, Debug)]
pub struct OssStoreProvider;

#[async_trait::async_trait]
impl ObjectStoreProvider for OssStoreProvider {
    async fn new_store(&self, base_path: Url, params: &ObjectStoreParams) -> Result<ObjectStore> {
        let block_size = params.block_size.unwrap_or(DEFAULT_CLOUD_BLOCK_SIZE);
        let storage_options = StorageOptions(params.storage_options().cloned().unwrap_or_default());

        let bucket = base_path
            .host_str()
            .ok_or_else(|| Error::invalid_input("OSS URL must contain bucket name", location!()))?
            .to_string();

        let prefix = base_path.path().trim_start_matches('/').to_string();

        // Start with environment variables as base configuration
        let mut config_map: HashMap<String, String> = std::env::vars()
            .filter(|(k, _)| {
                k.starts_with("OSS_") || k.starts_with("AWS_") || k.starts_with("ALIBABA_CLOUD_")
            })
            .map(|(k, v)| {
                // Convert env var names to opendal config keys
                let key = k
                    .to_lowercase()
                    .replace("oss_", "")
                    .replace("aws_", "")
                    .replace("alibaba_cloud_", "");
                (key, v)
            })
            .collect();

        config_map.insert("bucket".to_string(), bucket);

        if !prefix.is_empty() {
            config_map.insert("root".to_string(), "/".to_string());
        }

        // Override with storage options if provided
        if let Some(endpoint) = storage_options.0.get("oss_endpoint") {
            config_map.insert("endpoint".to_string(), endpoint.clone());
        }

        if let Some(access_key_id) = storage_options.0.get("oss_access_key_id") {
            config_map.insert("access_key_id".to_string(), access_key_id.clone());
        }

        if let Some(secret_access_key) = storage_options.0.get("oss_secret_access_key") {
            config_map.insert("access_key_secret".to_string(), secret_access_key.clone());
        }

        if let Some(region) = storage_options.0.get("oss_region") {
            config_map.insert("region".to_string(), region.clone());
        }

        if !config_map.contains_key("endpoint") {
            return Err(Error::invalid_input(
                "OSS endpoint is required. Please provide 'oss_endpoint' in storage options or set OSS_ENDPOINT environment variable",
                location!(),
            ));
        }

        let operator = Operator::from_iter::<Oss>(config_map)
            .map_err(|e| {
                Error::invalid_input(
                    format!("Failed to create OSS operator: {:?}", e),
                    location!(),
                )
            })?
            .finish();

        let opendal_store = Arc::new(OpendalStore::new(operator));

        let mut url = base_path;
        if !url.path().ends_with('/') {
            url.set_path(&format!("{}/", url.path()));
        }

        Ok(ObjectStore {
            scheme: "oss".to_string(),
            inner: opendal_store,
            block_size,
            max_iop_size: *DEFAULT_MAX_IOP_SIZE,
            use_constant_size_upload_parts: params.use_constant_size_upload_parts,
            list_is_lexically_ordered: params.list_is_lexically_ordered.unwrap_or(true),
            io_parallelism: DEFAULT_CLOUD_IO_PARALLELISM,
            download_retry_count: storage_options.download_retry_count(),
            io_tracker: Default::default(),
            store_prefix: self.calculate_object_store_prefix(&url, params.storage_options())?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::OssStoreProvider;
    use crate::object_store::ObjectStoreProvider;
    use url::Url;

    #[test]
    fn test_oss_store_path() {
        let provider = OssStoreProvider;

        let url = Url::parse("oss://bucket/path/to/file").unwrap();
        let path = provider.extract_path(&url).unwrap();
        let expected_path = object_store::path::Path::from("path/to/file");
        assert_eq!(path, expected_path);
    }
}
