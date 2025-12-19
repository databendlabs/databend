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
pub use default::DatabaseCatalog;
pub use default::table_memory_meta::InMemoryMetas;
pub use iceberg::IcebergCreator;

/// Merges two iterators of Option<T>, preferring left (primary) values over right (fallback) values.
/// If both primary and fallback are None, returns None.
///
/// The iterators are expected to have the same length. This function requires
/// ExactSizeIterator so we can assert lengths in debug builds and reserve capacity.
pub(crate) fn merge_options<T, P, F>(primary: P, fallback: F) -> Vec<Option<T>>
where
    P: IntoIterator<Item = Option<T>>,
    F: IntoIterator<Item = Option<T>>,
    P::IntoIter: ExactSizeIterator,
    F::IntoIter: ExactSizeIterator,
{
    let p = primary.into_iter();
    let f = fallback.into_iter();

    // In debug builds ensure lengths match to catch callers that might pass mismatched inputs.
    debug_assert_eq!(
        p.len(),
        f.len(),
        "merge_options expects same-length iterators"
    );

    p.zip(f).map(|(a, b)| a.or(b)).collect::<Vec<Option<T>>>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_options_primary_preferred() {
        let primary = vec![Some("a"), Some("b"), None];
        let fallback = vec![Some("x"), Some("y"), Some("z")];
        let result = merge_options(primary, fallback);
        assert_eq!(result, vec![Some("a"), Some("b"), Some("z")]);
    }

    #[test]
    fn test_merge_options_all_none() {
        let primary: Vec<Option<&str>> = vec![None, None, None];
        let fallback: Vec<Option<&str>> = vec![None, None, None];
        let result = merge_options(primary, fallback);
        assert_eq!(result, vec![None, None, None]);
    }

    #[test]
    fn test_merge_options_mixed() {
        let primary = vec![Some(1), None, Some(3), None];
        let fallback = vec![None, Some(2), None, Some(4)];
        let result = merge_options(primary, fallback);
        assert_eq!(result, vec![Some(1), Some(2), Some(3), Some(4)]);
    }

    #[test]
    fn test_merge_options_empty() {
        let primary: Vec<Option<i32>> = vec![];
        let fallback: Vec<Option<i32>> = vec![];
        let result = merge_options(primary, fallback);
        assert_eq!(result, vec![]);
    }
}
