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

mod block_filter;
mod block_filter_versions;
mod bloom_filter;
mod xor8_filter;

use std::hash::Hash;

pub use block_filter::BlockFilter;
pub use block_filter_versions::BlockBloomFilterIndexVersion;
pub use block_filter_versions::V2BloomBlock;
pub use bloom_filter::BloomBuilder;
pub use bloom_filter::BloomBuildingError;
pub use bloom_filter::BloomCodecError;
pub use bloom_filter::BloomFilter;
use databend_common_exception::ErrorCode;
pub use xor8_filter::Xor8Builder;
pub use xor8_filter::Xor8BuildingError;
pub use xor8_filter::Xor8CodecError;
pub use xor8_filter::Xor8Filter;

use crate::filters::Filter;
use crate::filters::FilterBuilder;

pub enum FilterImpl {
    Xor(Xor8Filter),
    Bloom(BloomFilter),
}

pub enum FilterImplBuilder {
    Xor(Xor8Builder),
    Bloom(BloomBuilder),
}

impl TryFrom<&FilterImpl> for Vec<u8> {
    type Error = ErrorCode;

    fn try_from(value: &FilterImpl) -> std::result::Result<Vec<u8>, ErrorCode> {
        Ok(match value {
            FilterImpl::Xor(filter) => {
                let mut bytes = filter.to_bytes()?;
                bytes.push(0u8);
                bytes
            }
            FilterImpl::Bloom(filter) => {
                let mut bytes = filter.to_bytes()?;
                bytes.push(1u8);
                bytes
            }
        })
    }
}

impl TryFrom<&[u8]> for FilterImpl {
    type Error = ErrorCode;

    fn try_from(value: &[u8]) -> std::result::Result<FilterImpl, Self::Error> {
        Ok(match value[value.len() - 1] {
            0 => FilterImpl::Xor(Xor8Filter::try_from(&value[0..value.len() - 1])?),
            1 => FilterImpl::Bloom(BloomFilter::try_from(&value[0..value.len() - 1])?),
            _ => unreachable!(),
        })
    }
}

impl FilterImpl {
    pub fn mem_bytes(&self) -> usize {
        match self {
            FilterImpl::Xor(filter) => {
                std::mem::size_of::<Xor8Builder>() + filter.filter.finger_prints.len()
            }
            FilterImpl::Bloom(_) => std::mem::size_of::<BloomFilter>(),
        }
    }
}

impl Filter for FilterImpl {
    type CodecError = ErrorCode;

    fn contains<K: ?Sized + Hash>(&self, key: &K) -> bool {
        match self {
            FilterImpl::Xor(filter) => filter.contains(key),
            FilterImpl::Bloom(filter) => filter.contains(key),
        }
    }

    fn contains_digest(&self, digest: u64) -> bool {
        match self {
            FilterImpl::Xor(filter) => filter.contains_digest(digest),
            FilterImpl::Bloom(filter) => filter.contains_digest(digest),
        }
    }

    fn to_bytes(&self) -> Result<Vec<u8>, Self::CodecError> {
        Vec::<u8>::try_from(self)
    }

    fn from_bytes(buf: &[u8]) -> Result<(Self, usize), Self::CodecError> {
        Self::try_from(buf).map(|filter| (filter, buf.len()))
    }
}

impl FilterBuilder for FilterImplBuilder {
    type Filter = FilterImpl;
    type Error = ErrorCode;

    fn add_key<K: Hash>(&mut self, key: &K) {
        match self {
            FilterImplBuilder::Xor(filter) => filter.add_key(key),
            FilterImplBuilder::Bloom(filter) => filter.add_key(key),
        }
    }

    fn add_keys<K: Hash>(&mut self, keys: &[K]) {
        match self {
            FilterImplBuilder::Xor(filter) => filter.add_keys(keys),
            FilterImplBuilder::Bloom(filter) => filter.add_keys(keys),
        }
    }

    fn add_digests<'i, I: IntoIterator<Item = &'i u64>>(&mut self, digests: I) {
        match self {
            FilterImplBuilder::Xor(filter) => filter.add_digests(digests),
            FilterImplBuilder::Bloom(filter) => filter.add_digests(digests),
        }
    }

    fn build(&mut self) -> Result<Self::Filter, Self::Error> {
        match self {
            FilterImplBuilder::Xor(filter) => Ok(FilterImpl::Xor(filter.build()?)),
            FilterImplBuilder::Bloom(filter) => Ok(FilterImpl::Bloom(filter.build()?)),
        }
    }
}
