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

use std::io;

use rotbl::v001::SeqMarked;

/// Result type of key-value pair and io Error used in a map.
type RotblResult = Result<(String, SeqMarked), io::Error>;

/// Sort by key and internal_seq.
/// Return `true` if `a` should be placed before `b`, e.g., `a` is smaller.
pub(crate) fn rotbl_by_key_seq(r1: &RotblResult, r2: &RotblResult) -> bool {
    match (r1, r2) {
        (Ok((k1, v1)), Ok((k2, v2))) => {
            // Put entries with the same key together, newer last
            // Tombstone is always greater if the seq is the same.
            (k1, v1.order_key()) <= (k2, v2.order_key())
        }
        // If there is an error, just yield them in order.
        // It's the caller's responsibility to handle the error.
        _ => true,
    }
}

/// Return an Ok(combined) to merge two consecutive values,
/// otherwise return Err((x,y)) to not to merge.
#[allow(clippy::type_complexity)]
pub(crate) fn rotbl_choose_greater(
    r1: RotblResult,
    r2: RotblResult,
) -> Result<RotblResult, (RotblResult, RotblResult)> {
    match (r1, r2) {
        (Ok((k1, v1)), Ok((k2, v2))) if k1 == k2 => Ok(Ok((k1, seq_marked_max(v1, v2)))),
        // If there is an error,
        // or k1 != k2
        // just yield them without change.
        (r1, r2) => Err((r1, r2)),
    }
}

fn seq_marked_max(a: SeqMarked, b: SeqMarked) -> SeqMarked {
    if a.internal_seq() > b.internal_seq() {
        a
    } else {
        b
    }
}
