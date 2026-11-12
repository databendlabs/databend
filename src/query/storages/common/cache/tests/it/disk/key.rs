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
fn test_rel_path_is_kind_prefix_object_unit() {
    let rel = DiskKey::whole("a/b").rel_path(CacheKind::of::<column::Data>());
    let parts: Vec<_> = rel.split('/').collect();
    assert_eq!(parts.len(), 3);
    assert_eq!(parts[0], "memory_cache_column_data");
    let (object, unit) = parts[2].split_once('.').unwrap();
    assert_eq!(object, DiskKey::object_id("a/b"));
    assert_eq!((object.len(), unit.len()), (32, 32));
    assert!(object.starts_with(parts[1]));
}

/// Units of one object share its id, as the start of their name, whatever their kind.
#[test]
fn test_units_of_an_object_share_its_id() {
    let object = DiskKey::object_id("a/b");
    assert_eq!(object.len(), 32);
    assert_ne!(object, DiskKey::object_id("a/c"));
    let data = CacheKind::of::<column::Data>();
    let filters = CacheKind::of::<databend_storages_common_cache::kinds::bloom::Filter>();
    for (kind, key) in [
        (data, DiskKey::whole("a/b")),
        (data, DiskKey::range("a/b", 0..10)),
        (filters, DiskKey::range("a/b", 10..20)),
    ] {
        let rel = key.rel_path(kind);
        let name = rel.rsplit('/').next().unwrap();
        assert_eq!(name.split_once('.').unwrap().0, object, "{rel}");
        assert!(rel.starts_with(&format!("{}/{}/", kind.name(), &object[0..3])));
    }
}
