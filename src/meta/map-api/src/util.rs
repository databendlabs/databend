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
use std::io;

use crate::MapKey;

/// Result type of key-value pair and io Error used in a map.
type KVResult<K, M> = Result<(K, crate::MarkedOf<K, M>), io::Error>;

/// Comparator function for sorting key-value results by key and internal sequence number.
///
/// This function is used to establish a total ordering of key-value pairs where:
/// 1. Entries are first ordered by their keys
/// 2. For the same key, entries are ordered by their internal sequence numbers
///
/// Returns `true` if `r1` should be placed before `r2` in the sorted order.
pub(crate) fn by_key_seq<K, M>(r1: &KVResult<K, M>, r2: &KVResult<K, M>) -> bool
where K: MapKey<M> + Ord + fmt::Debug {
    match (r1, r2) {
        (Ok((k1, v1)), Ok((k2, v2))) => {
            let iseq1 = v1.order_key();
            let iseq2 = v2.order_key();

            // Same (key, seq) is only allowed if they are both tombstone:
            // `MapApi::set(None)` when there is already a tombstone produces
            // another tombstone with the same internal_seq.
            assert!(
                (k1, iseq1) != (k2, iseq2) || (iseq1.is_tombstone() && iseq2.is_tombstone()),
                "by_key_seq: same (key, internal_seq) and not all tombstone: k1:{:?} v1.internal_seq:{:?} k2:{:?} v2.internal_seq:{:?}",
                k1,
                iseq1,
                k2,
                iseq2,
            );

            // Put entries with the same key together, smaller internal-seq first
            // Tombstone is always greater.
            (k1, v1.order_key()) <= (k2, v2.order_key())
        }
        // If there is an error, just yield them in order.
        // It's the caller's responsibility to handle the error.
        _ => true,
    }
}

/// Attempts to merge two consecutive key-value results with the same key.
///
/// If the keys are equal, returns `Ok(combined)` where the values are merged by taking the greater one.
/// Otherwise, returns `Err((r1, r2))` to indicate that the results should not be merged.
#[allow(clippy::type_complexity)]
pub(crate) fn merge_kv_results<K, M>(
    r1: KVResult<K, M>,
    r2: KVResult<K, M>,
) -> Result<KVResult<K, M>, (KVResult<K, M>, KVResult<K, M>)>
where
    K: MapKey<M> + Ord,
{
    match (r1, r2) {
        (Ok((k1, v1)), Ok((k2, v2))) if k1 == k2 => {
            Ok(Ok((k1, crate::marked::Marked::max(v1, v2))))
        }
        // If there is an error,
        // or k1 != k2
        // just yield them without change.
        (r1, r2) => Err((r1, r2)),
    }
}
