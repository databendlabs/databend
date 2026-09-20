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

mod bundle;
mod bundle_builder;
mod cache;
mod debug_proxy;
mod directory;
mod output_directory;
mod search_pin;

pub use bundle::BundleExternalFiles;
pub use bundle::BundleFileRanges;
pub use bundle::BundleOpenSlice;
pub use bundle::ExternalFile;
pub use bundle::INVERTED_INDEX_BUNDLE_INITIAL_FOOTER_READ_SIZE;
pub use bundle::INVERTED_INDEX_BUNDLE_MAX_FOOTER_SIZE;
pub use bundle::INVERTED_INDEX_BUNDLE_OBJECT_SUFFIX;
pub use bundle::INVERTED_INDEX_BUNDLE_TRAILER_LEN;
pub use bundle::INVERTED_INDEX_FILE_FORMAT_VERSION;
pub use bundle::InvertedIndexBundleFooter;
pub use bundle::InvertedIndexBundleVersion;
pub use bundle::MANAGED_JSON_PATH;
pub use bundle::META_JSON_PATH;
pub use bundle_builder::BundleSizes;
pub use bundle_builder::InvertedIndexBundleBuilder;
pub use cache::InvertedIndexLookupBytes;
pub use cache::InvertedIndexMeta;
pub use cache::InvertedIndexPayloadBytes;
pub use cache::inverted_index_meta_cache_key;
pub use directory::FooterDirectory;
pub use directory::collect_index_open_slices;
pub use output_directory::INVERTED_INDEX_STREAM_THRESHOLD;
pub use output_directory::InvertedIndexOutputDirectory;
pub use search_pin::SearchPinDirectory;

macro_rules! read_only_directory {
    () => {
        fn atomic_write(&self, _path: &std::path::Path, _data: &[u8]) -> std::io::Result<()> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "directory is read-only",
            ))
        }

        fn delete(
            &self,
            path: &std::path::Path,
        ) -> Result<(), tantivy::directory::error::DeleteError> {
            Err(tantivy::directory::error::DeleteError::IoError {
                io_error: std::sync::Arc::new(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "directory is read-only",
                )),
                filepath: path.to_path_buf(),
            })
        }

        fn open_write(
            &self,
            path: &std::path::Path,
        ) -> Result<tantivy::directory::WritePtr, tantivy::directory::error::OpenWriteError> {
            Err(tantivy::directory::error::OpenWriteError::wrap_io_error(
                std::io::Error::new(std::io::ErrorKind::Unsupported, "directory is read-only"),
                path.to_path_buf(),
            ))
        }

        fn sync_directory(&self) -> std::io::Result<()> {
            Ok(())
        }

        fn watch(
            &self,
            _watch_callback: tantivy::directory::WatchCallback,
        ) -> tantivy::Result<tantivy::directory::WatchHandle> {
            Ok(tantivy::directory::WatchHandle::empty())
        }

        fn acquire_lock(
            &self,
            _lock: &tantivy::directory::Lock,
        ) -> Result<tantivy::directory::DirectoryLock, tantivy::directory::error::LockError> {
            Ok(tantivy::directory::DirectoryLock::from(Box::new(|| {})))
        }
    };
}

pub(crate) use read_only_directory;
