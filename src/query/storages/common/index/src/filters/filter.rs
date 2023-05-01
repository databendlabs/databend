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

use std::hash::Hash;

// `len()` returns an Option thus `is_empty()` can not be provided.
#[allow(clippy::len_without_is_empty)]
pub trait Filter: Sized {
    type CodecError: std::error::Error;

    /// The number of **cardinal** keys built into the filter.
    ///
    /// An implementation that does not store keys count should return `None`.
    fn len(&self) -> Option<usize> {
        None
    }

    /// Check if the given key is in the filter.
    ///
    /// False positive: returning `true` only a key **probably** presents.
    fn contains<K: ?Sized + Hash>(&self, key: &K) -> bool;

    /// Check if the pre-computed digest is in the filter.
    fn contains_digest(&self, digest: u64) -> bool;

    /// Serialize the filter.
    fn to_bytes(&self) -> Result<Vec<u8>, Self::CodecError>;

    /// Deserialize the binary array to a filter.
    fn from_bytes(buf: &[u8]) -> Result<(Self, usize), Self::CodecError>;
}

pub trait FilterBuilder {
    type Filter: Filter;
    type Error: std::error::Error;

    /// Add a key into the filter for building.
    fn add_key<K: Hash>(&mut self, key: &K);

    /// Add several keys into the filter for building.
    ///
    /// This methods can be called more than once.
    fn add_keys<K: Hash>(&mut self, keys: &[K]);

    /// Populate with pre-compute collection of 64-bit digests.
    fn add_digests<'i, I: IntoIterator<Item = &'i u64>>(&mut self, digests: I);

    /// Build the filter with added keys.
    fn build(&mut self) -> Result<Self::Filter, Self::Error>;
}
