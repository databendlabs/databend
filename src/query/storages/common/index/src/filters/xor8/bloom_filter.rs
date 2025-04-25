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

use std::collections::HashSet;
use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::mem;

use anyerror::AnyError;
use bloomfilter::Bloom;
use bytes::Bytes;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::filters::Filter;
use crate::filters::FilterBuilder;

pub struct BloomBuilder {
    bitmap_size: usize,
    set: HashSet<u64>,
}

pub struct BloomFilter {
    pub filter: Bloom<u64>,
}

impl TryFrom<&BloomFilter> for Vec<u8> {
    type Error = ErrorCode;

    fn try_from(value: &BloomFilter) -> std::result::Result<Vec<u8>, ErrorCode> {
        Ok(value.filter.to_bytes())
    }
}

impl TryFrom<Bytes> for BloomFilter {
    type Error = ErrorCode;

    fn try_from(value: Bytes) -> std::result::Result<BloomFilter, Self::Error> {
        Ok(BloomFilter {
            filter: Bloom::<u64>::from_slice(value.as_ref()).map_err(|e| {
                ErrorCode::StorageOther(format!("failed to decode ngram index meta {}", e))
            })?,
        })
    }
}

#[derive(thiserror::Error, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[error("cause: {cause}")]
pub struct BloomCodecError {
    #[source]
    cause: AnyError,
}

#[derive(thiserror::Error, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[error("fail to build bloom filter; cause: {cause}")]
pub struct BloomBuildingError {
    #[source]
    cause: AnyError,
}

impl FilterBuilder for BloomBuilder {
    type Filter = BloomFilter;
    type Error = BloomBuildingError;

    fn add_key<K: Hash>(&mut self, key: &K) {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        self.set.insert(hasher.finish());
    }

    fn add_keys<K: Hash>(&mut self, keys: &[K]) {
        for key in keys {
            self.add_key::<K>(key);
        }
    }

    fn add_digests<'i, I: IntoIterator<Item = &'i u64>>(&mut self, digests: I) {
        for digest in digests {
            self.set.insert(*digest);
        }
    }

    fn build(&mut self) -> std::result::Result<Self::Filter, Self::Error> {
        let mut bloom =
            Bloom::new(self.bitmap_size, self.set.len()).map_err(BloomBuildingError::new)?;

        for hash in mem::take(&mut self.set) {
            bloom.set(&hash);
        }
        Ok(BloomFilter { filter: bloom })
    }
}

impl BloomBuilder {
    pub fn create(bitmap_size: usize) -> Result<Self> {
        Ok(Self {
            bitmap_size,
            set: HashSet::default(),
        })
    }
}

impl Filter for BloomFilter {
    type CodecError = BloomCodecError;

    fn len(&self) -> Option<usize> {
        Some(self.filter.len() as usize)
    }

    fn contains<K: ?Sized + Hash>(&self, key: &K) -> bool {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        self.filter.check(&hasher.finish())
    }

    fn contains_digest(&self, digest: u64) -> bool {
        self.filter.check(&digest)
    }

    fn to_bytes(&self) -> std::result::Result<Vec<u8>, Self::CodecError> {
        Ok(self.filter.to_bytes())
    }

    fn from_bytes(buf: &[u8]) -> std::result::Result<(Self, usize), Self::CodecError> {
        Ok((
            Self {
                filter: Bloom::from_slice(buf)
                    .map_err(|_| BloomCodecError::new("fail to decode bloom"))?,
            },
            buf.len(),
        ))
    }
}

impl BloomCodecError {
    pub fn new(cause: impl ToString) -> Self {
        Self {
            cause: AnyError::error(cause),
        }
    }
}

impl From<BloomCodecError> for ErrorCode {
    fn from(e: BloomCodecError) -> Self {
        ErrorCode::Internal(e.to_string())
    }
}

impl BloomBuildingError {
    pub fn new(cause: impl ToString) -> Self {
        Self {
            cause: AnyError::error(cause),
        }
    }
}

impl From<BloomBuildingError> for ErrorCode {
    fn from(e: BloomBuildingError) -> Self {
        ErrorCode::Internal(e.to_string())
    }
}
