// Copyright 2022 Datafuse Labs.
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

//! The disk cache, as `src/disk`: `DiskCache` over the pools and writer, and
//! `DiskCachedStorage`, the read path in front of object storage. Files are checked on disk,
//! since the cache writes in background threads.

mod disk_cache;
mod disk_cached_storage;

use std::path::Path;
use std::path::PathBuf;

/// The cache files under `dir`, recursively.
pub fn files_under(dir: &Path) -> Vec<PathBuf> {
    let mut files = vec![];
    let Ok(entries) = std::fs::read_dir(dir) else {
        return files;
    };
    for entry in entries.filter_map(|e| e.ok()) {
        let path = entry.path();
        if path.is_dir() {
            files.extend(files_under(&path));
        } else {
            files.push(path);
        }
    }
    files
}
