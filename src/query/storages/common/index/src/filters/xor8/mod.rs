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

mod binary_fuse32_filter;
mod block_filter;
mod block_filter_versions;
mod xor8_filter;

use std::hash::Hash;

pub use binary_fuse32_filter::BinaryFuse32Builder;
pub use binary_fuse32_filter::BinaryFuse32Filter;
pub use block_filter::BlockFilter;
pub use block_filter_versions::BlockBloomFilterIndexVersion;
pub use block_filter_versions::V2BloomBlock;
use bytes::Bytes;
use databend_common_exception::ErrorCode;
pub use xor8_filter::Xor8Builder;
pub use xor8_filter::Xor8Filter;

use crate::filters::Filter;
use crate::filters::FilterBuilder;
pub use crate::filters::bloom_filter::BloomBuilder;
pub use crate::filters::bloom_filter::BloomFilter;

#[derive(PartialEq)]
pub enum FilterImpl {
    Xor(Xor8Filter),
    BinaryFuse32(BinaryFuse32Filter),
    Ngram(BloomFilter),
}

pub enum FilterImplBuilder {
    Xor(Xor8Builder),
    BinaryFuse32(BinaryFuse32Builder),
    Ngram(BloomBuilder),
}

impl TryFrom<&FilterImpl> for Vec<u8> {
    type Error = ErrorCode;

    fn try_from(value: &FilterImpl) -> std::result::Result<Vec<u8>, ErrorCode> {
        value.to_bytes()
    }
}

impl TryFrom<Bytes> for FilterImpl {
    type Error = ErrorCode;

    fn try_from(value: Bytes) -> std::result::Result<FilterImpl, Self::Error> {
        Ok(Self::from_bytes(value.as_ref())?.0)
    }
}

impl FilterImpl {
    pub fn mem_bytes(&self) -> usize {
        match self {
            FilterImpl::Xor(filter) => {
                std::mem::size_of::<Xor8Builder>() + filter.filter.finger_prints.len()
            }
            FilterImpl::BinaryFuse32(filter) => filter.mem_bytes(),
            FilterImpl::Ngram(_) => std::mem::size_of::<BloomFilter>(),
        }
    }
}

impl Filter for FilterImpl {
    type CodecError = ErrorCode;

    fn len(&self) -> Option<usize> {
        match self {
            FilterImpl::Xor(filter) => filter.len(),
            FilterImpl::BinaryFuse32(filter) => filter.len(),
            FilterImpl::Ngram(filter) => filter.len(),
        }
    }

    fn contains<K: ?Sized + Hash>(&self, key: &K) -> bool {
        match self {
            FilterImpl::Xor(filter) => filter.contains(key),
            FilterImpl::BinaryFuse32(filter) => filter.contains(key),
            FilterImpl::Ngram(filter) => filter.contains(key),
        }
    }

    fn contains_digest(&self, digest: u64) -> bool {
        match self {
            FilterImpl::Xor(filter) => filter.contains_digest(digest),
            FilterImpl::BinaryFuse32(filter) => filter.contains_digest(digest),
            FilterImpl::Ngram(filter) => filter.contains_digest(digest),
        }
    }

    fn to_bytes(&self) -> Result<Vec<u8>, Self::CodecError> {
        Ok(match self {
            FilterImpl::Xor(filter) => filter.to_bytes()?,
            FilterImpl::BinaryFuse32(filter) => {
                let mut bytes = filter.to_bytes()?;
                bytes.insert(0, b'f');
                bytes
            }
            FilterImpl::Ngram(filter) => {
                let mut bytes = filter.to_bytes()?;
                // major ranges from [0, 7] is Xor8Filter
                bytes.insert(0, b'n');
                bytes
            }
        })
    }

    fn from_bytes(buf: &[u8]) -> Result<(Self, usize), Self::CodecError> {
        Ok(if buf[0] == b'f' {
            BinaryFuse32Filter::from_bytes(&buf[1..])
                .map(|(filter, len)| (FilterImpl::BinaryFuse32(filter), len + 1))?
        } else if buf[0] == b'n' {
            BloomFilter::from_bytes(&buf[1..])
                .map(|(filter, len)| (FilterImpl::Ngram(filter), len + 1))?
        } else {
            Xor8Filter::from_bytes(buf).map(|(filter, len)| (FilterImpl::Xor(filter), len))?
        })
    }
}

impl FilterBuilder for FilterImplBuilder {
    type Filter = FilterImpl;
    type Error = ErrorCode;

    fn add_key<K: Hash>(&mut self, key: &K) {
        match self {
            FilterImplBuilder::Xor(filter) => filter.add_key(key),
            FilterImplBuilder::BinaryFuse32(filter) => filter.add_key(key),
            FilterImplBuilder::Ngram(filter) => filter.add_key(key),
        }
    }

    fn add_keys<K: Hash>(&mut self, keys: &[K]) {
        match self {
            FilterImplBuilder::Xor(filter) => filter.add_keys(keys),
            FilterImplBuilder::BinaryFuse32(filter) => filter.add_keys(keys),
            FilterImplBuilder::Ngram(filter) => filter.add_keys(keys),
        }
    }

    fn add_digests<'i, I: IntoIterator<Item = &'i u64>>(&mut self, digests: I) {
        match self {
            FilterImplBuilder::Xor(filter) => filter.add_digests(digests),
            FilterImplBuilder::BinaryFuse32(filter) => filter.add_digests(digests),
            FilterImplBuilder::Ngram(filter) => filter.add_digests(digests),
        }
    }

    fn build(&mut self) -> Result<Self::Filter, Self::Error> {
        match self {
            FilterImplBuilder::Xor(filter) => Ok(FilterImpl::Xor(filter.build()?)),
            FilterImplBuilder::BinaryFuse32(filter) => {
                Ok(FilterImpl::BinaryFuse32(filter.build()?))
            }
            FilterImplBuilder::Ngram(filter) => Ok(FilterImpl::Ngram(filter.build()?)),
        }
    }
}
