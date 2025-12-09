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

use std::borrow::Cow;

use databend_common_column::binary::BinaryColumn;
use databend_common_column::binary::BinaryColumnBuilder;
use databend_common_io::deserialize_bitmap;

// Hybrid bitmap encoding header copied from databend_common_io::bitmap.
const HYBRID_MAGIC: [u8; 2] = *b"HB";
const HYBRID_VERSION: u8 = 1;
const HYBRID_KIND_SMALL: u8 = 0;
const HYBRID_KIND_LARGE: u8 = 1;

#[inline]
fn is_hybrid_encoding(bytes: &[u8]) -> bool {
    if bytes.is_empty() {
        return true;
    }
    if bytes.len() < 4 {
        return false;
    }
    bytes[0] == HYBRID_MAGIC[0]
        && bytes[1] == HYBRID_MAGIC[1]
        && bytes[2] == HYBRID_VERSION
        && (bytes[3] == HYBRID_KIND_SMALL || bytes[3] == HYBRID_KIND_LARGE)
}

/// Ensure bitmap bytes are encoded in the current hybrid format.
/// - If all rows are already in the new format, returns a borrowed column (zero copy).
/// - Once a legacy row is seen, the whole column is rewritten with normalized bytes.
pub fn normalize_bitmap_column(column: &BinaryColumn) -> Cow<'_, BinaryColumn> {
    if column.is_empty() {
        return Cow::Borrowed(column);
    }

    let mut has_legacy = false;
    for value in column.iter() {
        if !is_hybrid_encoding(value) {
            has_legacy = true;
            break;
        }
    }

    if !has_legacy {
        return Cow::Borrowed(column);
    }

    let mut builder = BinaryColumnBuilder::with_capacity(column.len(), column.data().len());
    for value in column.iter() {
        if is_hybrid_encoding(value) {
            builder.put_slice(value);
            builder.commit_row();
            continue;
        }

        match deserialize_bitmap(value) {
            Ok(bitmap) => {
                // Safe unwrap: serialize_into writes into Vec<u8>.
                bitmap.serialize_into(&mut builder.data).unwrap();
                builder.commit_row();
            }
            Err(_) => {
                // Keep original bytes if they cannot be decoded; this preserves prior behavior.
                builder.put_slice(value);
                builder.commit_row();
            }
        }
    }

    Cow::Owned(builder.build())
}
