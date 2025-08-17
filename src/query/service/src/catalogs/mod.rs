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

pub mod default;
mod iceberg;

pub use databend_common_catalog::catalog::Catalog;
pub use databend_common_storages_hive as hive;
pub use default::table_memory_meta::InMemoryMetas;
pub use default::DatabaseCatalog;
pub use iceberg::IcebergCreator;

/// Merges two iterators of Option<T>, preferring left (primary) values over right (fallback) values.
/// If both primary and fallback are None, returns None.
pub fn merge_option_iterators<T>(
    primary: impl IntoIterator<Item = Option<T>>,
    fallback: impl IntoIterator<Item = Option<T>>,
) -> Vec<Option<T>> {
    primary
        .into_iter()
        .zip(fallback.into_iter())
        .map(|(primary_result, fallback_result)| primary_result.or(fallback_result))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_option_iterators_primary_preferred() {
        let primary = vec![Some("a"), Some("b"), None];
        let fallback = vec![Some("x"), Some("y"), Some("z")];
        let result = merge_option_iterators(primary, fallback);
        assert_eq!(result, vec![Some("a"), Some("b"), Some("z")]);
    }

    #[test]
    fn test_merge_option_iterators_all_none() {
        let primary: Vec<Option<&str>> = vec![None, None, None];
        let fallback: Vec<Option<&str>> = vec![None, None, None];
        let result = merge_option_iterators(primary, fallback);
        assert_eq!(result, vec![None, None, None]);
    }

    #[test]
    fn test_merge_option_iterators_mixed() {
        let primary = vec![Some(1), None, Some(3), None];
        let fallback = vec![None, Some(2), None, Some(4)];
        let result = merge_option_iterators(primary, fallback);
        assert_eq!(result, vec![Some(1), Some(2), Some(3), Some(4)]);
    }

    #[test]
    fn test_merge_option_iterators_empty() {
        let primary: Vec<Option<i32>> = vec![];
        let fallback: Vec<Option<i32>> = vec![];
        let result = merge_option_iterators(primary, fallback);
        assert_eq!(result, vec![]);
    }
}
