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

use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::DiskKey;
use databend_storages_common_cache::kinds::column;

#[test]
fn test_range_and_whole_object_are_different_units() {
    let whole = DiskKey::whole("a/b");
    let range = DiskKey::range("a/b", 0..10);
    assert_eq!(whole.to_string(), "a/b");
    assert_eq!(range.to_string(), "a/b#0-10");
    let kind = CacheKind::of::<column::Data>();
    assert_ne!(whole.rel_path(kind), range.rel_path(kind));
}

#[test]
fn test_rel_path_is_kind_prefix_hash() {
    let rel = DiskKey::whole("a/b").rel_path(CacheKind::of::<column::Data>());
    let parts: Vec<_> = rel.split('/').collect();
    assert_eq!(parts.len(), 3);
    assert_eq!(parts[0], "memory_cache_column_data");
    assert_eq!(parts[2].len(), 32);
    assert!(parts[2].starts_with(parts[1]));
}
