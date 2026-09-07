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
use std::hash::Hasher;

use anyerror::AnyError;
use databend_common_exception::ErrorCode;
use databend_common_expression::types::DataType;
use databend_common_functions::scalars::CityHasher64;

use crate::Index;
use crate::filters::Filter;
use crate::filters::FilterBuilder;

type UnderType = u64;

// Bound adaptive folding to preserve pruning quality for low-cardinality blocks while still
// allowing the default 1 MiB bitmap to shrink by up to 32x.
const MIN_FOLDED_BLOOM_SIZE: u64 = 32 * 1024;

pub struct BloomBuilder {
    false_positive_rate: f64,
    filter: BloomFilter,
}

impl BloomBuilder {
    pub fn create(max_bloom_size: u64, false_positive_rate: f64, seed: u64) -> Self {
        // Folding requires the final bitmap size to divide the initial size. Rounding the cap down
        // to a power of two makes every smaller power-of-two candidate foldable.
        let bloom_size = BloomFilter::foldable_size(max_bloom_size);
        Self {
            false_positive_rate,
            filter: BloomFilter::with_false_positive_rate(bloom_size, false_positive_rate, seed),
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
        let mut hasher64 = CityHasher64::with_seed(self.filter.seed);
        key.hash(&mut hasher64);
        self.filter.add(hasher64.finish());
    }

    fn add_keys<K: Hash>(&mut self, keys: &[K]) {
        for key in keys {
            self.add_key::<K>(key);
        }
    }

    fn add_digest(&mut self, digest: u64) {
        self.filter.add(digest);
    }

    fn build(mut self) -> Result<Self::Filter, Self::Error> {
        self.filter
            .fold(self.false_positive_rate, MIN_FOLDED_BLOOM_SIZE);
        Ok(self.filter)
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
    fn foldable_size(max_filter_size: u64) -> u64 {
        assert!(max_filter_size >= std::mem::size_of::<UnderType>() as u64);
        // A power-of-two size lets every smaller candidate divide the original bit count, which
        // is required to preserve membership when corresponding bitmap positions are folded.
        1 << max_filter_size.ilog2()
    }

    pub fn with_false_positive_rate(filter_size: u64, false_positive_rate: f64, seed: u64) -> Self {
        assert!(
            false_positive_rate.is_finite()
                && false_positive_rate > 0.0
                && false_positive_rate < 1.0,
            "false_positive_rate must be finite and between 0 and 1"
        );
        let hashes = Self::hashes_for_false_positive_rate(false_positive_rate);
        Self::with_params(filter_size, hashes, seed)
    }

    #[inline]
    fn hashes_for_false_positive_rate(false_positive_rate: f64) -> u64 {
        (-false_positive_rate.ln() / std::f64::consts::LN_2)
            .ceil()
            .max(1.0) as u64
    }

    fn estimated_false_positive_rate(set_bits: u64, bit_count: u64, hashes: u64) -> f64 {
        let occupancy = set_bits as f64 / bit_count as f64;
        occupancy.powf(hashes as f64)
    }

    fn estimated_false_positive_rate_after_fold(&self) -> f64 {
        let target_words = self.filter.len() / 2;
        let set_bits = (0..target_words)
            .map(|i| (self.filter[i] | self.filter[i + target_words]).count_ones() as u64)
            .sum::<u64>();
        let bit_count = (target_words * UnderType::BITS as usize) as u64;
        Self::estimated_false_positive_rate(set_bits, bit_count, self.hashes)
    }

    fn fold(&mut self, target_false_positive_rate: f64, min_filter_size: u64) {
        // Evaluate the bitmap produced by each candidate fold directly. The lengths form a
        // geometric sequence, so all candidate scans together are linear in the original bitmap
        // size and require no additional bitmap allocation.
        while self.size / 2 >= min_filter_size
            && self.estimated_false_positive_rate_after_fold() <= target_false_positive_rate
        {
            let target_words = self.filter.len() / 2;
            // For a power-of-two bitmap, (position % old_bits) % new_bits equals
            // position % new_bits. OR-ing corresponding halves therefore preserves every bit
            // that add() would have set had the filter originally used the smaller size.
            for i in 0..target_words {
                self.filter[i] |= self.filter[i + target_words];
            }
            self.filter.truncate(target_words);
            self.size /= 2;
            self.words = target_words as u64;
        }
        self.filter.shrink_to_fit();
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

    #[inline]
    fn probe_position(hash: u64, probe: u64, bit_count: u64) -> u64 {
        let position = hash
            .wrapping_add(probe)
            .wrapping_add(probe.wrapping_mul(probe));
        if bit_count.is_power_of_two() {
            position & (bit_count - 1)
        } else {
            // Filters written before adaptive folding may have a non-power-of-two size.
            position % bit_count
        }
    }

    pub fn find(&self, hash: u64) -> bool {
        let bit_count = 8 * self.size;
        for i in 0..self.hashes {
            let pos = Self::probe_position(hash, i, bit_count);
            let bit_pos = pos as usize % (8 * std::mem::size_of::<UnderType>());
            let word_index = pos as usize / (8 * std::mem::size_of::<UnderType>());
            if self.filter[word_index] & (1 << bit_pos) == 0 {
                return false;
            }
        }
        true
    }

    pub fn add(&mut self, hash: u64) {
        let bit_count = 8 * self.size;
        for i in 0..self.hashes {
            let pos = Self::probe_position(hash, i, bit_count);
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
    fn test_false_positive_rate_insert_and_check() {
        let item_count = 1_000_000;
        let mut filter = BloomFilter::with_false_positive_rate(10 * 1024, 0.01, 0);
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
    fn test_decode_legacy_non_power_of_two_filter() {
        let digests = (0_u64..100)
            .map(|value| {
                let mut hasher = CityHasher64::with_seed(7);
                value.hash(&mut hasher);
                hasher.finish()
            })
            .collect::<Vec<_>>();
        let mut legacy_filter = BloomFilter::with_params(1000, 7, 7);
        for digest in &digests {
            legacy_filter.add(*digest);
        }

        let bytes = legacy_filter.to_bytes().unwrap();
        let (decoded, consumed) = BloomFilter::from_bytes(&bytes).unwrap();

        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded.size, 1000);
        assert!(digests.iter().all(|digest| decoded.find(*digest)));
    }

    #[test]
    fn test_hashes_for_false_positive_rate() {
        assert_eq!(
            BloomFilter::hashes_for_false_positive_rate(crate::DEFAULT_NGRAM_FALSE_POSITIVE_RATE),
            4
        );
        assert_eq!(BloomFilter::hashes_for_false_positive_rate(0.5), 1);
        assert_eq!(BloomFilter::hashes_for_false_positive_rate(0.01), 7);
        assert_eq!(BloomFilter::hashes_for_false_positive_rate(0.001), 10);
    }

    #[test]
    fn test_probe_position_uses_mask_with_legacy_fallback() {
        let hash = 12_345_u64;
        let probe = 7_u64;
        let position = hash
            .wrapping_add(probe)
            .wrapping_add(probe.wrapping_mul(probe));

        assert_eq!(
            BloomFilter::probe_position(hash, probe, 8192),
            position & 8191
        );
        assert_eq!(
            BloomFilter::probe_position(hash, probe, 8000),
            position % 8000
        );
    }

    #[test]
    fn test_fold_uses_candidate_bitmap_occupancy() {
        let mut above_target = BloomFilter::with_false_positive_rate(16, 0.01, 7);
        above_target.filter[0] = (1_u64 << 34) - 1;
        assert!(above_target.estimated_false_positive_rate_after_fold() > 0.01);
        above_target.fold(0.01, 8);
        assert_eq!(above_target.size, 16);

        let mut below_target = BloomFilter::with_false_positive_rate(16, 0.01, 7);
        below_target.filter[0] = (1_u64 << 33) - 1;
        assert!(below_target.estimated_false_positive_rate_after_fold() <= 0.01);
        below_target.fold(0.01, 8);
        assert_eq!(below_target.size, 8);
    }

    #[test]
    fn test_bloom_builder_writes_filter_directly() {
        let mut builder = BloomBuilder::create(1024, 0.01, 7);
        builder.add_digest(11);
        builder.add_digest(13);
        builder.add_digests([17, 19].iter());

        let filter = builder.build().unwrap();
        assert_eq!(filter.hashes, 7);
        assert_eq!(filter.size, 1024);
        assert!(filter.find(11));
        assert!(filter.find(13));
        assert!(filter.find(17));
        assert!(filter.find(19));
    }

    #[test]
    fn test_bloom_builder_respects_small_non_power_of_two_limit() {
        let mut builder = BloomBuilder::create(1000, 0.01, 7);
        builder.add_digest(11);

        let filter = builder.build().unwrap();
        assert_eq!(filter.size, 512);
        assert!(filter.find(11));
    }

    #[test]
    fn test_bloom_builder_folds_without_false_negatives() {
        let digests = (0_u64..100)
            .map(|value| {
                let mut hasher = CityHasher64::with_seed(7);
                value.hash(&mut hasher);
                hasher.finish()
            })
            .collect::<Vec<_>>();
        let mut builder = BloomBuilder::create(1024 * 1024, 0.01, 7);
        builder.add_digests(digests.iter());

        let filter = builder.build().unwrap();
        let set_bits = filter
            .filter
            .iter()
            .map(|word| word.count_ones() as u64)
            .sum::<u64>();
        let estimated_false_positive_rate =
            BloomFilter::estimated_false_positive_rate(set_bits, 8 * filter.size, filter.hashes);
        assert_eq!(filter.size, MIN_FOLDED_BLOOM_SIZE);
        assert!(estimated_false_positive_rate <= 0.01);
        assert!(digests.iter().all(|digest| filter.find(*digest)));
    }

    #[test]
    fn test_bloom_builder_keeps_max_size_when_occupancy_is_high() {
        let mut builder = BloomBuilder::create(1024 * 1024, 0.01, 7);
        for value in 0..1_000_000_u64 {
            let mut hasher = CityHasher64::with_seed(7);
            value.hash(&mut hasher);
            builder.add_digest(hasher.finish());
        }

        let filter = builder.build().unwrap();
        assert_eq!(filter.size, 1024 * 1024);
    }

    #[test]
    fn test_foldable_size_does_not_exceed_limit() {
        assert_eq!(BloomFilter::foldable_size(1024), 1024);
        assert_eq!(BloomFilter::foldable_size(1000), 512);
    }
}
