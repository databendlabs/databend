// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

/// Maps an owner storage prefix to the owning table id.
///
/// Populated from `FuseTable::parse_storage_prefix_from_table_info()` for the base table
/// and its branches. Prefixes are expected to be non-overlapping owner roots.
///
/// Used by both vacuum2 (branch-aware GC) and virtual column orphan cleanup to resolve
/// which table owns a given file path.
pub type StoragePrefixes = HashMap<String, u64>;

/// Resolve which table owns a file by matching its path against known storage prefixes.
pub fn table_id_by_path(storage_prefixes: &StoragePrefixes, path: &str) -> Result<u64> {
    storage_prefixes
        .iter()
        .filter(|(prefix, _)| path.starts_with(prefix.as_str()))
        .max_by_key(|(prefix, _)| prefix.len())
        .map(|(_, &table_id)| table_id)
        .ok_or_else(|| {
            ErrorCode::Internal(format!(
                "cannot resolve table by path '{}', known prefixes: {:?}",
                path,
                storage_prefixes.keys().collect::<Vec<_>>()
            ))
        })
}

/// Like `table_id_by_path` but returns `None` instead of `Err` when no prefix matches.
/// Useful when unrecognized paths should be silently skipped rather than treated as errors.
pub fn owner_of_path(storage_prefixes: &StoragePrefixes, path: &str) -> Option<u64> {
    storage_prefixes
        .iter()
        .filter(|(prefix, _)| path.starts_with(prefix.as_str()))
        .max_by_key(|(prefix, _)| prefix.len())
        .map(|(_, &id)| id)
}
