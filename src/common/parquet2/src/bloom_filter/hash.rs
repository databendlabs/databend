// Copyright [2021] [Jorge C Leitao]
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

use xxhash_rust::xxh64::xxh64;

use crate::types::NativeType;

const SEED: u64 = 0;

/// (xxh64) hash of a [`NativeType`].
#[inline]
pub fn hash_native<T: NativeType>(value: T) -> u64 {
    xxh64(value.to_le_bytes().as_ref(), SEED)
}

/// (xxh64) hash of a sequence of bytes (e.g. ByteArray).
#[inline]
pub fn hash_byte<A: AsRef<[u8]>>(value: A) -> u64 {
    xxh64(value.as_ref(), SEED)
}
