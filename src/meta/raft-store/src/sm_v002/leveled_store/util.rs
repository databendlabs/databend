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

use std::fmt;

use crate::sm_v002::leveled_store::map_api::MapKV;
use crate::sm_v002::leveled_store::map_api::MapKey;
use crate::sm_v002::marked::Marked;

/// Sort by key and internal_seq.
/// Return `true` if `a` should be placed before `b`, e.g., `a` is smaller.
pub(in crate::sm_v002) fn by_key_seq<K>((k1, v1): &MapKV<K>, (k2, v2): &MapKV<K>) -> bool
where K: MapKey + Ord + fmt::Debug {
    assert_ne!((k1, v1.internal_seq()), (k2, v2.internal_seq()));

    // Put entries with the same key together, smaller internal-seq first
    // Tombstone is always greater.
    (k1, v1.internal_seq()) <= (k2, v2.internal_seq())
}

/// Return `true` if `a` should be placed before `b`, e.g., `a` is smaller.
#[allow(clippy::type_complexity)]
pub(in crate::sm_v002) fn choose_greater<K>(
    (k1, v1): MapKV<K>,
    (k2, v2): MapKV<K>,
) -> Result<MapKV<K>, (MapKV<K>, MapKV<K>)>
where
    K: MapKey + Ord,
{
    if k1 == k2 {
        Ok((k1, Marked::max(v1, v2)))
    } else {
        Err(((k1, v1), (k2, v2)))
    }
}
