// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{collections::HashMap, sync::Arc};

use crate::object_store::{
    ObjectStore, ObjectStoreParams, ObjectStoreProvider, StorageOptions, DEFAULT_LOCAL_BLOCK_SIZE,
    DEFAULT_LOCAL_IO_PARALLELISM, DEFAULT_MAX_IOP_SIZE,
};
use lance_core::error::Result;
use lance_core::Error;
use object_store::{local::LocalFileSystem, path::Path};
use snafu::location;
use url::Url;

#[derive(Default, Debug)]
pub struct FileStoreProvider;

#[async_trait::async_trait]
impl ObjectStoreProvider for FileStoreProvider {
    async fn new_store(&self, base_path: Url, params: &ObjectStoreParams) -> Result<ObjectStore> {
        let block_size = params.block_size.unwrap_or(DEFAULT_LOCAL_BLOCK_SIZE);
        let storage_options = StorageOptions(params.storage_options().cloned().unwrap_or_default());
        let download_retry_count = storage_options.download_retry_count();
        Ok(ObjectStore {
            inner: Arc::new(LocalFileSystem::new()),
            scheme: base_path.scheme().to_owned(),
            block_size,
            max_iop_size: *DEFAULT_MAX_IOP_SIZE,
            use_constant_size_upload_parts: false,
            list_is_lexically_ordered: false,
            io_parallelism: DEFAULT_LOCAL_IO_PARALLELISM,
            download_retry_count,
            io_tracker: Default::default(),
            store_prefix: self
                .calculate_object_store_prefix(&base_path, params.storage_options())?,
        })
    }

    fn extract_path(&self, url: &Url) -> Result<Path> {
        if let Ok(file_path) = url.to_file_path() {
            if let Ok(path) = Path::from_absolute_path(&file_path) {
                return Ok(path);
            }
        }

        Path::parse(url.path()).map_err(|e| {
            Error::invalid_input(
                format!("Failed to parse path '{}': {}", url.path(), e),
                location!(),
            )
        })
    }

    fn calculate_object_store_prefix(
        &self,
        url: &Url,
        _storage_options: Option<&HashMap<String, String>>,
    ) -> Result<String> {
        Ok(url.scheme().to_string())
    }
}

#[cfg(test)]
mod tests {
    use crate::object_store::uri_to_url;

    use super::*;

    #[test]
    fn test_file_store_path() {
        let provider = FileStoreProvider;

        let cases = [
            ("file:///", ""),
            ("file:///usr/local/bin", "usr/local/bin"),
            ("file-object-store:///path/to/file", "path/to/file"),
            ("file:///path/to/foo/../bar", "path/to/bar"),
        ];

        for (uri, expected_path) in cases {
            let url = uri_to_url(uri).unwrap();
            let path = provider.extract_path(&url).unwrap();
            assert_eq!(path.as_ref(), expected_path, "uri: '{}'", uri);
        }
    }

    #[test]
    fn test_calculate_object_store_prefix() {
        let provider = FileStoreProvider;
        assert_eq!(
            "file",
            provider
                .calculate_object_store_prefix(&Url::parse("file:///etc").unwrap(), None)
                .unwrap()
        );
    }

    #[test]
    fn test_calculate_object_store_prefix_for_file_object_store() {
        let provider = FileStoreProvider;
        assert_eq!(
            "file-object-store",
            provider
                .calculate_object_store_prefix(
                    &Url::parse("file-object-store:///etc").unwrap(),
                    None
                )
                .unwrap()
        );
    }

    #[test]
    #[cfg(windows)]
    fn test_file_store_path_windows() {
        let provider = FileStoreProvider;

        let cases = [
            (
                "C:\\Users\\ADMINI~1\\AppData\\Local\\",
                "C:/Users/ADMINI~1/AppData/Local",
            ),
            (
                "C:\\Users\\ADMINI~1\\AppData\\Local\\..\\",
                "C:/Users/ADMINI~1/AppData",
            ),
        ];

        for (uri, expected_path) in cases {
            let url = uri_to_url(uri).unwrap();
            let path = provider.extract_path(&url).unwrap();
            assert_eq!(path.as_ref(), expected_path);
        }
    }
}
