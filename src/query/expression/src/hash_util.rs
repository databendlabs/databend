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

use databend_common_exception::Result;
use databend_common_hashtable::BloomHash;
use databend_common_hashtable::FastHash;

use crate::HashMethod;
use crate::HashMethodKind;
use crate::ProjectedBlock;

/// Get row hash by [`HashMethod`] (uses [`FastHash`], i.e. the hashtable hash).
pub fn hash_by_method<T>(
    method: &HashMethodKind,
    columns: ProjectedBlock,
    num_rows: usize,
    hashes: &mut T,
) -> Result<()>
where
    T: Extend<u64>,
{
    match method {
        HashMethodKind::Serializer(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.fast_hash()),
            );
        }
        HashMethodKind::SingleBinary(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.fast_hash()),
            );
        }
        HashMethodKind::KeysU8(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.fast_hash()),
            );
        }
        HashMethodKind::KeysU16(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.fast_hash()),
            );
        }
        HashMethodKind::KeysU32(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.fast_hash()),
            );
        }
        HashMethodKind::KeysU64(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.fast_hash()),
            );
        }
        HashMethodKind::KeysU128(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.fast_hash()),
            );
        }
        HashMethodKind::KeysU256(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.fast_hash()),
            );
        }
    }
    Ok(())
}

/// Get row hash for Bloom filter by [`HashMethod`]. This always uses [`BloomHash`],
/// which is independent of SSE4.2 and provides a well-distributed 64-bit hash.
pub fn hash_by_method_for_bloom<T>(
    method: &HashMethodKind,
    columns: ProjectedBlock,
    num_rows: usize,
    hashes: &mut T,
) -> Result<()>
where
    T: Extend<u64>,
{
    match method {
        HashMethodKind::Serializer(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.bloom_hash()),
            );
        }
        HashMethodKind::SingleBinary(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.bloom_hash()),
            );
        }
        HashMethodKind::KeysU8(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.bloom_hash()),
            );
        }
        HashMethodKind::KeysU16(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.bloom_hash()),
            );
        }
        HashMethodKind::KeysU32(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.bloom_hash()),
            );
        }
        HashMethodKind::KeysU64(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.bloom_hash()),
            );
        }
        HashMethodKind::KeysU128(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.bloom_hash()),
            );
        }
        HashMethodKind::KeysU256(method) => {
            let keys_state = method.build_keys_state(columns, num_rows)?;
            hashes.extend(
                method
                    .build_keys_iter(&keys_state)?
                    .map(|key| key.bloom_hash()),
            );
        }
    }
    Ok(())
}
