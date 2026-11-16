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

use databend_storages_common_cache::TableDataCacheKey;

/// Deserialized arrays carry their logical type: another type is another key.
#[test]
fn test_array_cache_key_includes_data_type() {
    let old = TableDataCacheKey::new("block", 1, 2, 3, "Decimal(10, 2)");
    let new = TableDataCacheKey::new("block", 1, 2, 3, "Decimal(15, 2)");
    assert_ne!(old.as_ref(), new.as_ref());
    assert_eq!(
        new.as_ref(),
        TableDataCacheKey::new("block", 1, 2, 3, "Decimal(15, 2)").as_ref()
    );
    assert_eq!(String::from(new), "block-1-2-3-Decimal(15, 2)");
}
