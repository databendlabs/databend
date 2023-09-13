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

use crate::sm_v002::marked::Marked;

type Tuple<K, V> = (K, Marked<V>);

/// Sort by key and internal_seq.
/// Return `true` if `a` should be placed before `b`, e.g., `a` is smaller.
pub(in crate::sm_v002) fn by_key_seq<K, V>((k1, v1): &Tuple<K, V>, (k2, v2): &Tuple<K, V>) -> bool
where K: Ord + fmt::Debug {
    assert_ne!((k1, v1.internal_seq()), (k2, v2.internal_seq()));

    // Put entries with the same key together, smaller internal-seq first
    // Tombstone is always greater.
    (k1, v1.internal_seq()) <= (k2, v2.internal_seq())
}

/// Return `true` if `a` should be placed before `b`, e.g., `a` is smaller.
#[allow(clippy::type_complexity)]
pub(in crate::sm_v002) fn choose_greater<K, V>(
    (k1, v1): Tuple<K, V>,
    (k2, v2): Tuple<K, V>,
) -> Result<Tuple<K, V>, (Tuple<K, V>, Tuple<K, V>)>
where
    K: Ord,
{
    if k1 == k2 {
        Ok((k1, Marked::max(v1, v2)))
    } else {
        Err(((k1, v1), (k2, v2)))
    }
}
