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

use std::cmp::max;
use std::collections::HashSet;
use std::hash::Hash;
use std::hash::Hasher;
use std::mem;

use anyerror::AnyError;
use databend_common_exception::ErrorCode;
use databend_common_expression::types::DataType;
use databend_common_functions::scalars::CityHasher64;

use crate::Index;
use crate::filters::Filter;
use crate::filters::FilterBuilder;

type UnderType = u64;

pub struct BloomBuilder {
    bloom_size: u64,
    seed: u64,
    inner: HashSet<u64>,
}

impl BloomBuilder {
    pub fn create(bloom_size: u64, seed: u64) -> Self {
        Self {
            bloom_size,
            seed,
            inner: Default::default(),
        }
    }
}

#[derive(thiserror::Error, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[error("{msg}")]
pub struct BloomCodecError {
    msg: String,
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
        let mut hasher64 = CityHasher64::with_seed(self.seed);
        key.hash(&mut hasher64);
        self.inner.insert(hasher64.finish());
    }

    fn add_keys<K: Hash>(&mut self, keys: &[K]) {
        for key in keys {
            self.add_key::<K>(key);
        }
    }

    fn add_digests<'i, I: IntoIterator<Item = &'i u64>>(&mut self, digests: I) {
        self.inner.extend(digests);
    }

    fn build(&mut self) -> Result<Self::Filter, Self::Error> {
        let item_count = self.inner.len();

        let mut filter = BloomFilter::with_item_count(self.bloom_size, item_count, self.seed);
        for hash in mem::take(&mut self.inner) {
            filter.add(hash);
        }
        Ok(filter)
    }

    fn peek_len(&self) -> Option<usize> {
        // Return the accurate NDV from the digest HashSet
        Some(self.inner.len())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BloomFilter {
    size: u64,
    hashes: u64,
    seed: u64,
    words: u64,
    filter: Vec<UnderType>,
}

impl Index for BloomFilter {
    fn supported_type(data_type: &DataType) -> bool {
        matches!(data_type.remove_nullable(), DataType::String)
    }
}

impl Filter for BloomFilter {
    type CodecError = BloomCodecError;

    fn len(&self) -> Option<usize> {
        None
    }

    fn contains<K: ?Sized + Hash>(&self, key: &K) -> bool {
        let mut hasher64 = CityHasher64::with_seed(self.seed);
        key.hash(&mut hasher64);
        self.find(hasher64.finish())
    }

    fn contains_digest(&self, digest: u64) -> bool {
        self.find(digest)
    }

    fn to_bytes(&self) -> Result<Vec<u8>, Self::CodecError> {
        let mut bytes = Vec::new();

        bytes.extend(&self.size.to_le_bytes());
        bytes.extend(&self.hashes.to_le_bytes());
        bytes.extend(&self.seed.to_le_bytes());
        bytes.extend(&self.words.to_le_bytes());

        let len = self.filter.len();
        bytes.extend(&(len as u64).to_le_bytes());

        for word in &self.filter {
            bytes.extend(&word.to_le_bytes());
        }
        Ok(bytes)
    }

    fn from_bytes(buf: &[u8]) -> Result<(Self, usize), Self::CodecError> {
        let mut offset = 0;

        fn read_u64(data: &[u8], offset: &mut usize) -> Result<u64, BloomCodecError> {
            if *offset + 8 > data.len() {
                return Err(BloomCodecError {
                    msg: "Unexpected end of data".into(),
                });
            }
            let value = u64::from_le_bytes(data[*offset..*offset + 8].try_into().unwrap());
            *offset += 8;
            Ok(value)
        }

        let size = read_u64(buf, &mut offset)?;
        let hashes = read_u64(buf, &mut offset)?;
        let seed = read_u64(buf, &mut offset)?;
        let words = read_u64(buf, &mut offset)?;
        let filter_len = read_u64(buf, &mut offset)? as usize;

        let mut filter = Vec::with_capacity(filter_len);
        for _ in 0..filter_len {
            filter.push(read_u64(buf, &mut offset)?);
        }

        Ok((
            BloomFilter {
                size,
                hashes,
                seed,
                words,
                filter,
            },
            buf.len(),
        ))
    }
}

impl BloomFilter {
    pub fn with_item_count(filter_size: u64, mut item_count: usize, seed: u64) -> Self {
        assert!(filter_size > 0, "filter_size must be > 0");
        item_count = max(item_count, 1);

        let k = Self::optimal_k(filter_size, item_count);

        Self::with_params(filter_size, k, seed)
    }

    #[inline]
    fn optimal_k(filter_size: u64, item_count: usize) -> u64 {
        let ln2 = std::f64::consts::LN_2;
        let k = ((filter_size as f64 / item_count as f64) * ln2).ceil() as u64;
        k.max(1)
    }

    pub fn with_params(size: u64, hashes: u64, seed: u64) -> Self {
        assert_ne!(size, 0);
        assert_ne!(hashes, 0);
        let words = size.div_ceil(std::mem::size_of::<UnderType>() as u64);
        Self {
            size,
            hashes,
            seed,
            words,
            filter: vec![0; words as usize],
        }
    }

    pub fn resize(&mut self, size: u64) {
        self.size = size;
        self.words = size.div_ceil(std::mem::size_of::<UnderType>() as u64);
        self.filter.resize(self.words as usize, 0);
    }

    pub fn find(&self, hash: u64) -> bool {
        for i in 0..self.hashes {
            let pos = hash.wrapping_add(i).wrapping_add(i * i) % (8 * self.size);
            let bit_pos = pos as usize % (8 * std::mem::size_of::<UnderType>());
            let word_index = pos as usize / (8 * std::mem::size_of::<UnderType>());
            if self.filter[word_index] & (1 << bit_pos) == 0 {
                return false;
            }
        }
        true
    }

    pub fn add(&mut self, hash: u64) {
        for i in 0..self.hashes {
            let pos = hash.wrapping_add(i).wrapping_add(i * i) % (8 * self.size);
            let bit_pos = pos as usize % (8 * std::mem::size_of::<UnderType>());
            let word_index = pos as usize / (8 * std::mem::size_of::<UnderType>());
            self.filter[word_index] |= 1 << bit_pos;
        }
    }

    pub fn clear(&mut self) {
        self.filter.fill(0);
    }

    pub fn is_empty(&self) -> bool {
        self.filter.iter().all(|&x| x == 0)
    }

    pub fn memory_usage_bytes(&self) -> usize {
        self.filter.capacity() * std::mem::size_of::<UnderType>()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_insert_and_check() {
        for i in 0..1_000_000 {
            let mut filter = BloomFilter::with_params(10, 1, 0);
            filter.add(i);
            assert!(filter.find(i));
        }
    }

    #[test]
    fn test_sbbf_insert_and_check() {
        let item_count = 1_000_000;
        let mut filter = BloomFilter::with_item_count(10 * 1024, item_count, 0);
        for i in 0..item_count as u64 {
            filter.add(i);
            assert!(filter.find(i));
        }
    }

    #[test]
    fn test_encode_and_decode() {
        let mut hashes = Vec::new();
        for i in 0..500000 {
            hashes.push(i);
        }
        let mut filter = BloomFilter::with_params(10 * 1024, 1, 0);
        for hash in hashes.iter() {
            filter.add(*hash);
        }
        assert!(hashes.iter().all(|hash| filter.find(*hash)));
        let buf = filter.to_bytes().unwrap();
        let (decode_filter, _) = BloomFilter::from_bytes(&buf).unwrap();
        filter
            .filter
            .iter()
            .zip(decode_filter.filter.iter())
            .for_each(|(a, b)| {
                assert_eq!(a, b);
            });
        assert!(hashes.iter().all(|hash| decode_filter.find(*hash)));
    }

    #[test]
    fn test_optimal_k() {
        assert_eq!(BloomFilter::optimal_k(1000, 100), 7); // (1000/100)*ln(2) ≈ 6.93 → ceil → 7
        assert_eq!(BloomFilter::optimal_k(1024, 128), 6); // (1024/128)*ln(2) ≈ 5.545 → ceil → 6
        assert_eq!(BloomFilter::optimal_k(100, 1000), 1); // (100/1000)*ln(2) ≈ 0.069 → ceil → 1
        assert_eq!(BloomFilter::optimal_k(100, 100), 1); // (100/100)*ln(2) ≈ 0.693 → ceil → 1
        assert_eq!(BloomFilter::optimal_k(1, 1), 1); // (1/1)*ln(2) ≈ 0.693 → ceil → 1
        assert_eq!(BloomFilter::optimal_k(1, 1000), 1); // (1/1000)*ln(2) ≈ 0.0007 → ceil → 1
        assert_eq!(BloomFilter::optimal_k(100, 50), 2); // (100/50)*ln(2) ≈ 1.386 → ceil → 2
        assert_eq!(BloomFilter::optimal_k(101, 50), 2); // (101/50)*ln(2) ≈ 1.400 → ceil → 2
        assert_eq!(BloomFilter::optimal_k(1_000_000, 10_000), 70); // (1e6/1e4)*ln(2) ≈ 69.31 → ceil → 70
    }
}
