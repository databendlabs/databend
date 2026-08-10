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
use std::fmt::Display;
use std::hash::Hash;
use std::hash::Hasher;

use anyerror::AnyError;
use bytes::Bytes;
use databend_common_exception::ErrorCode;
use databend_common_expression::types::DataType;
use xorf::BinaryFuse32;
use xorf::BinaryFuse32Ref;
use xorf::DmaSerializable;
use xorf::Filter as XorFilter;
use xorf::FilterRef as XorFilterRef;

use crate::Index;
use crate::filters::Filter;
use crate::filters::FilterBuilder;

#[derive(Default)]
pub struct BinaryFuse32Builder {
    digests: HashSet<u64>,
}

// Databend-specific retry policy. The upstream xorf constructor does not expose
// capacity controls, so Databend uses these values only after the default xorf
// build path fails.
const BINARY_FUSE32_EXPANSION_FACTORS: &[f64] = &[1.25, 1.5, 2.0];
const BINARY_FUSE32_EXPANDED_MAX_ITER: usize = 128;

// Copied from xorf 0.12.0's binary-fuse implementation.
const BINARY_FUSE32_ARITY: u32 = 3;
const BINARY_FUSE32_MAX_SEGMENT_LENGTH: u32 = 262_144;

// Databend-specific wrapper for selecting the normal xorf build path, expanded
// capacity retries, and test-only failure injection.
#[derive(Clone, Copy)]
pub(super) struct BinaryFuse32BuildPolicy {
    expansion_factors: &'static [f64],
    expanded_max_iter: usize,
    #[cfg(test)]
    skip_default: bool,
    #[cfg(test)]
    force_expanded_failure: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BinaryFuse32Filter {
    pub descriptor: [u8; BinaryFuse32::DESCRIPTOR_LEN],
    pub fingerprints: Vec<u32>,
    pub len: usize,
}

struct BinaryFuse32BuildParts {
    descriptor: [u8; BinaryFuse32::DESCRIPTOR_LEN],
    fingerprints: Vec<u32>,
    report: BinaryFuse32ExpandedBuildReport,
}

struct BinaryFuse32BuildOutcome {
    filter: BinaryFuse32Filter,
    report: BinaryFuse32BuildReport,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct BinaryFuse32BuildReport {
    method: BinaryFuse32BuildMethod,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum BinaryFuse32BuildMethod {
    Default,
    Expanded(BinaryFuse32ExpandedBuildReport),
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct BinaryFuse32ExpandedBuildReport {
    capacity_factor_multiplier: f64,
    max_iter: usize,
    iterations: usize,
    fingerprint_count: usize,
    segment_length: u32,
    segment_count_length: u32,
}

impl BinaryFuse32BuildOutcome {
    fn log_final_if_needed(&self, distinct_digests: usize) {
        match self.report.method {
            BinaryFuse32BuildMethod::Default => {}
            BinaryFuse32BuildMethod::Expanded(report) => {
                log::info!(
                    "binary fuse32 filter built with expanded capacity; final_filter=binary_fuse32 distinct_digests={} capacity_factor_multiplier={:.2} max_iter={} iterations={} fingerprint_count={} segment_length={} segment_count_length={}",
                    distinct_digests,
                    report.capacity_factor_multiplier,
                    report.max_iter,
                    report.iterations,
                    report.fingerprint_count,
                    report.segment_length,
                    report.segment_count_length
                );
            }
        }
    }
}

#[derive(thiserror::Error, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[error("{msg}; cause: {cause}")]
pub struct BinaryFuse32CodecError {
    msg: String,
    #[source]
    cause: AnyError,
}

#[derive(thiserror::Error, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[error("fail to build binary fuse 32 filter; cause: {cause}")]
pub struct BinaryFuse32BuildingError {
    #[source]
    cause: AnyError,
}

impl BinaryFuse32Builder {
    pub fn create() -> Self {
        Self::default()
    }

    pub(super) fn take_digests(&mut self) -> Vec<u64> {
        std::mem::take(&mut self.digests).into_iter().collect()
    }

    pub(super) fn build_from_digests(
        digests: &[u64],
    ) -> Result<BinaryFuse32Filter, BinaryFuse32BuildingError> {
        Self::build_from_digests_with_policy(digests, BinaryFuse32BuildPolicy::default())
    }

    pub(super) fn build_from_digests_with_policy(
        digests: &[u64],
        policy: BinaryFuse32BuildPolicy,
    ) -> Result<BinaryFuse32Filter, BinaryFuse32BuildingError> {
        Self::build_from_digests_with_policy_and_report(digests, policy)
            .map(|outcome| outcome.filter)
    }

    fn build_from_digests_with_policy_and_report(
        digests: &[u64],
        policy: BinaryFuse32BuildPolicy,
    ) -> Result<BinaryFuse32BuildOutcome, BinaryFuse32BuildingError> {
        let len = digests.len();
        let mut last_error = None;

        // External crate path: keep the default xorf constructor unchanged.
        if policy.should_try_default() {
            match BinaryFuse32::try_from(digests) {
                Ok(filter) => {
                    let mut descriptor = [0; BinaryFuse32::DESCRIPTOR_LEN];
                    filter.dma_copy_descriptor_to(&mut descriptor);
                    return Ok(BinaryFuse32BuildOutcome {
                        filter: BinaryFuse32Filter {
                            descriptor,
                            fingerprints: filter.fingerprints.into_vec(),
                            len,
                        },
                        report: BinaryFuse32BuildReport {
                            method: BinaryFuse32BuildMethod::Default,
                        },
                    });
                }
                Err(err) => {
                    let err = err.to_string();
                    log::info!(
                        "binary fuse32 default build failed for {} distinct digests, retrying with expanded capacity; expansion_factors={:?}; error={}",
                        len,
                        policy.expansion_factors,
                        err
                    );
                    last_error = Some(err);
                }
            }
        }

        // Databend-specific retry path: use the local xorf-derived constructor
        // with additional capacity before giving up.
        for factor in policy.expansion_factors {
            if policy.should_force_expanded_failure() {
                let err = "forced binary fuse32 expanded build failure".to_string();
                log::info!(
                    "binary fuse32 expanded capacity build failed; distinct_digests={} capacity_factor_multiplier={:.2} max_iter={} error={}",
                    len,
                    factor,
                    policy.expanded_max_iter,
                    err
                );
                last_error = Some(err);
                continue;
            }

            match build_binary_fuse32_with_capacity_factor(
                digests,
                *factor,
                policy.expanded_max_iter,
            ) {
                Ok(parts) => {
                    let outcome = BinaryFuse32BuildOutcome {
                        filter: BinaryFuse32Filter {
                            descriptor: parts.descriptor,
                            fingerprints: parts.fingerprints,
                            len,
                        },
                        report: BinaryFuse32BuildReport {
                            method: BinaryFuse32BuildMethod::Expanded(parts.report),
                        },
                    };
                    outcome.log_final_if_needed(len);
                    return Ok(outcome);
                }
                Err(err) => {
                    log::info!(
                        "binary fuse32 expanded capacity build failed; distinct_digests={} capacity_factor_multiplier={:.2} max_iter={} error={}",
                        len,
                        factor,
                        policy.expanded_max_iter,
                        err
                    );
                    last_error = Some(err.to_string());
                }
            }
        }

        Err(BinaryFuse32BuildingError::new(last_error.unwrap_or_else(
            || "Failed to construct binary fuse filter.".to_string(),
        )))
    }
}

impl TryFrom<&BinaryFuse32Filter> for Vec<u8> {
    type Error = ErrorCode;

    fn try_from(value: &BinaryFuse32Filter) -> std::result::Result<Self, Self::Error> {
        value.to_bytes().map_err(ErrorCode::from)
    }
}

impl TryFrom<Bytes> for BinaryFuse32Filter {
    type Error = ErrorCode;

    fn try_from(value: Bytes) -> std::result::Result<Self, Self::Error> {
        BinaryFuse32Filter::from_bytes(value.as_ref())
            .map(|(v, len)| {
                assert_eq!(len, value.len());
                v
            })
            .map_err(ErrorCode::from)
    }
}

impl FilterBuilder for BinaryFuse32Builder {
    type Filter = BinaryFuse32Filter;
    type Error = BinaryFuse32BuildingError;

    fn add_key<K: Hash>(&mut self, key: &K) {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        self.digests.insert(hasher.finish());
    }

    fn add_keys<K: Hash>(&mut self, keys: &[K]) {
        for key in keys {
            self.add_key(key);
        }
    }

    fn add_digests<'i, I: IntoIterator<Item = &'i u64>>(&mut self, digests: I) {
        self.digests.extend(digests.into_iter().copied());
    }

    fn build(&mut self) -> Result<Self::Filter, Self::Error> {
        let digests = self.take_digests();
        Self::build_from_digests(digests.as_slice())
    }
}

impl Default for BinaryFuse32BuildPolicy {
    fn default() -> Self {
        Self {
            expansion_factors: BINARY_FUSE32_EXPANSION_FACTORS,
            expanded_max_iter: BINARY_FUSE32_EXPANDED_MAX_ITER,
            #[cfg(test)]
            skip_default: false,
            #[cfg(test)]
            force_expanded_failure: false,
        }
    }
}

impl BinaryFuse32BuildPolicy {
    #[cfg(test)]
    pub(super) fn for_test(skip_default: bool, force_expanded_failure: bool) -> Self {
        Self {
            expansion_factors: &[1.25],
            expanded_max_iter: BINARY_FUSE32_EXPANDED_MAX_ITER,
            skip_default,
            force_expanded_failure,
        }
    }

    fn should_try_default(&self) -> bool {
        #[cfg(test)]
        {
            !self.skip_default
        }
        #[cfg(not(test))]
        {
            true
        }
    }

    fn should_force_expanded_failure(&self) -> bool {
        #[cfg(test)]
        {
            self.force_expanded_failure
        }
        #[cfg(not(test))]
        {
            false
        }
    }
}

// Copied/adapted from xorf 0.12.0's binary-fuse constructor
// (MIT, Copyright (c) 2019 hafiz).
//
// Databend modifications:
// - capacity_factor_multiplier lets retry callers allocate more buckets than upstream xorf.
// - max_iter is caller-controlled because this path runs after xorf's default 1000 attempts.
// - the return type is BinaryFuse32BuildParts so Databend can keep its existing filter format.
fn build_binary_fuse32_with_capacity_factor(
    keys: &[u64],
    capacity_factor_multiplier: f64,
    max_iter: usize,
) -> Result<BinaryFuse32BuildParts, &'static str> {
    let size = keys.len();
    let segment_length = binary_fuse32_segment_length(BINARY_FUSE32_ARITY, size as u32)
        .min(BINARY_FUSE32_MAX_SEGMENT_LENGTH);
    let segment_length_mask = segment_length - 1;

    // Databend-specific capacity expansion: upstream xorf uses only
    // binary_fuse32_size_factor(...); retries multiply it to reduce graph load.
    let size_factor = binary_fuse32_size_factor(BINARY_FUSE32_ARITY, size as u32)
        * capacity_factor_multiplier.max(1.0);
    let capacity = if size > 1 {
        (size as f64 * size_factor).round() as u32
    } else {
        0
    };
    let init_segment_count = capacity.div_ceil(segment_length);
    let (fp_array_len, segment_count) = {
        let array_len = init_segment_count * segment_length;
        let proposed = array_len.div_ceil(segment_length);
        let segment_count = if proposed < BINARY_FUSE32_ARITY {
            1
        } else {
            proposed - (BINARY_FUSE32_ARITY - 1)
        };
        let array_len = (segment_count + BINARY_FUSE32_ARITY - 1) * segment_length;
        (array_len as usize, segment_count)
    };
    let segment_count_length = segment_count * segment_length;

    let mut fingerprints = random_u32_vec(fp_array_len);
    let capacity = fingerprints.len();
    let mut alone = vec![0_u32; capacity];
    let mut t2count = vec![0_u8; capacity];
    let mut t2hash = vec![0_u64; capacity];
    let mut reverse_h = vec![0_u8; size];
    let mut reverse_order = vec![0_u64; size + 1];
    reverse_order[size] = 1;

    let mut block_bits = 1_u32;
    while (1_u32 << block_bits) < segment_count {
        block_bits += 1;
    }
    let start_pos_len = 1_usize << block_bits;
    let mut start_pos = vec![0_usize; start_pos_len];
    let mut h012 = [0_u32; 6];

    let mut rng = 1;
    let mut seed = splitmix64(&mut rng);
    let mut ultimate_size = 0;
    let mut iterations = 0;
    let mut done = false;

    for iter in 0..max_iter {
        for (i, pos) in start_pos.iter_mut().enumerate() {
            *pos = (((i as u64) * (size as u64)) >> block_bits) as usize;
        }
        for key in keys {
            let hash = mix(*key, seed);
            let mut segment_index = hash >> (64 - block_bits);
            while reverse_order[start_pos[segment_index as usize]] != 0 {
                segment_index += 1;
                segment_index &= (1_u64 << block_bits) - 1;
            }
            reverse_order[start_pos[segment_index as usize]] = hash;
            start_pos[segment_index as usize] += 1;
        }

        let mut error = false;
        let mut duplicates = 0;
        for hash in reverse_order.iter().take(size).copied() {
            let (index1, index2, index3) = hash_of_hash(
                hash,
                segment_length,
                segment_length_mask,
                segment_count_length,
            );
            let (index1, index2, index3) = (index1 as usize, index2 as usize, index3 as usize);

            t2count[index1] = t2count[index1].wrapping_add(4);
            t2hash[index1] ^= hash;
            t2count[index2] = t2count[index2].wrapping_add(4);
            t2count[index2] ^= 1;
            t2hash[index2] ^= hash;
            t2count[index3] = t2count[index3].wrapping_add(4);
            t2count[index3] ^= 2;
            t2hash[index3] ^= hash;

            if t2hash[index1] & t2hash[index2] & t2hash[index3] == 0
                && (((t2hash[index1] == 0) && (t2count[index1] == 8))
                    || ((t2hash[index2] == 0) && (t2count[index2] == 8))
                    || ((t2hash[index3] == 0) && (t2count[index3] == 8)))
            {
                duplicates += 1;
                t2count[index1] = t2count[index1].wrapping_sub(4);
                t2hash[index1] ^= hash;
                t2count[index2] = t2count[index2].wrapping_sub(4);
                t2count[index2] ^= 1;
                t2hash[index2] ^= hash;
                t2count[index3] = t2count[index3].wrapping_sub(4);
                t2count[index3] ^= 2;
                t2hash[index3] ^= hash;
            }
            error = t2count[index1] < 4 || t2count[index2] < 4 || t2count[index3] < 4;
        }
        if error {
            reset_binary_fuse32_build_state(&mut reverse_order, &mut t2count, &mut t2hash, size);
            seed = splitmix64(&mut rng);
            continue;
        }

        let mut qsize = 0;
        for (i, count) in t2count.iter().enumerate().take(capacity) {
            alone[qsize] = i as u32;
            if (count >> 2) == 1 {
                qsize += 1;
            }
        }

        let mut stack_size = 0;
        while qsize > 0 {
            qsize -= 1;
            let index = alone[qsize] as usize;
            if (t2count[index] >> 2) == 1 {
                let hash = t2hash[index];
                let found = t2count[index] & 3;
                reverse_h[stack_size] = found;
                reverse_order[stack_size] = hash;
                stack_size += 1;

                let (index1, index2, index3) = hash_of_hash(
                    hash,
                    segment_length,
                    segment_length_mask,
                    segment_count_length,
                );
                h012[1] = index2;
                h012[2] = index3;
                h012[3] = index1;
                h012[4] = h012[1];

                let other_index1 = h012[(found + 1) as usize] as usize;
                alone[qsize] = other_index1 as u32;
                if (t2count[other_index1] >> 2) == 2 {
                    qsize += 1;
                }
                t2count[other_index1] = t2count[other_index1].wrapping_sub(4);
                t2count[other_index1] ^= mod3(found + 1);
                t2hash[other_index1] ^= hash;

                let other_index2 = h012[(found + 2) as usize] as usize;
                alone[qsize] = other_index2 as u32;
                if (t2count[other_index2] >> 2) == 2 {
                    qsize += 1;
                }
                t2count[other_index2] = t2count[other_index2].wrapping_sub(4);
                t2count[other_index2] ^= mod3(found + 2);
                t2hash[other_index2] ^= hash;
            }
        }

        if stack_size + duplicates == size {
            ultimate_size = stack_size;
            iterations = iter + 1;
            done = true;
            break;
        }

        reset_binary_fuse32_build_state(&mut reverse_order, &mut t2count, &mut t2hash, size);
        seed = splitmix64(&mut rng);
    }

    if !done {
        return Err("Failed to construct binary fuse filter.");
    }

    for i in (0..ultimate_size).rev() {
        let hash = reverse_order[i];
        let xor2 = fingerprint(hash);
        let (index1, index2, index3) = hash_of_hash(
            hash,
            segment_length,
            segment_length_mask,
            segment_count_length,
        );
        let found = reverse_h[i] as usize;
        h012[0] = index1;
        h012[1] = index2;
        h012[2] = index3;
        h012[3] = h012[0];
        h012[4] = h012[1];
        fingerprints[h012[found] as usize] =
            xor2 ^ fingerprints[h012[found + 1] as usize] ^ fingerprints[h012[found + 2] as usize];
    }

    Ok(BinaryFuse32BuildParts {
        descriptor: serialize_binary_fuse32_descriptor(
            seed,
            segment_length,
            segment_length_mask,
            segment_count_length,
        ),
        report: BinaryFuse32ExpandedBuildReport {
            capacity_factor_multiplier: capacity_factor_multiplier.max(1.0),
            max_iter,
            iterations,
            fingerprint_count: fingerprints.len(),
            segment_length,
            segment_count_length,
        },
        fingerprints,
    })
}

fn reset_binary_fuse32_build_state(
    reverse_order: &mut [u64],
    t2count: &mut [u8],
    t2hash: &mut [u64],
    size: usize,
) {
    for value in reverse_order.iter_mut().take(size) {
        *value = 0;
    }
    t2count.fill(0);
    t2hash.fill(0);
}

// Copied from xorf 0.12.0 prelude::bfuse::segment_length.
fn binary_fuse32_segment_length(arity: u32, size: u32) -> u32 {
    if size == 0 {
        return 4;
    }

    match arity {
        3 => 1 << ((size as f64).ln() / 3.33_f64.ln() + 2.25).floor() as u32,
        4 => 1 << ((size as f64).ln() / 2.91_f64.ln() - 0.5).floor() as u32,
        _ => 65_536,
    }
}

// Copied from xorf 0.12.0 prelude::bfuse::size_factor.
fn binary_fuse32_size_factor(arity: u32, size: u32) -> f64 {
    match arity {
        3 => 1.125_f64.max(0.875 + 0.25 * 1_000_000_f64.ln() / (size as f64).ln()),
        4 => 1.075_f64.max(0.77 + 0.305 * 600_000_f64.ln() / (size as f64).ln()),
        _ => 2.0,
    }
}

// Copied from xorf 0.12.0 prelude::bfuse::hash_of_hash.
const fn hash_of_hash(
    hash: u64,
    segment_length: u32,
    segment_length_mask: u32,
    segment_count_length: u32,
) -> (u32, u32, u32) {
    let hi = ((hash as u128 * segment_count_length as u128) >> 64) as u64;
    let h0 = hi as u32;
    let mut h1 = h0 + segment_length;
    let mut h2 = h1 + segment_length;
    h1 ^= ((hash >> 18) as u32) & segment_length_mask;
    h2 ^= (hash as u32) & segment_length_mask;
    (h0, h1, h2)
}

// Copied from xorf 0.12.0 prelude::mix.
const fn mix(key: u64, seed: u64) -> u64 {
    murmur3_mix64(key.overflowing_add(seed).0)
}

// Copied from xorf 0.12.0 murmur3::mix64.
const fn murmur3_mix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.overflowing_mul(0xff51_afd7_ed55_8ccd).0;
    k ^= k >> 33;
    k = k.overflowing_mul(0xc4ce_b9fe_1a85_ec53).0;
    k ^= k >> 33;
    k
}

// Copied from xorf 0.12.0 splitmix64::splitmix64.
const fn splitmix64(seed: &mut u64) -> u64 {
    *seed = (*seed).overflowing_add(0x9e37_79b9_7f4a_7c15).0;
    let mut z = *seed;
    z = (z ^ (z >> 30)).overflowing_mul(0xbf58_476d_1ce4_e5b9).0;
    z = (z ^ (z >> 27)).overflowing_mul(0x94d0_49bb_1331_11eb).0;
    z ^ (z >> 31)
}

// Copied from xorf 0.12.0 fingerprint! macro for u32 fingerprints.
const fn fingerprint(hash: u64) -> u32 {
    (hash ^ (hash >> 32)) as u32
}

// Copied from xorf 0.12.0 prelude::bfuse::mod3.
const fn mod3(x: u8) -> u8 {
    if x > 2 { x - 3 } else { x }
}

// Copied behavior from xorf 0.12.0 make_fp_block! with the default
// uniform-random feature enabled.
fn random_u32_vec(len: usize) -> Vec<u32> {
    use rand::Rng;

    let mut rng = rand::thread_rng();
    (0..len).map(|_| rng.r#gen()).collect()
}

// Copied from xorf 0.12.0 prelude::bfuse::serialize_bfuse_descriptor.
fn serialize_binary_fuse32_descriptor(
    seed: u64,
    segment_length: u32,
    segment_length_mask: u32,
    segment_count_length: u32,
) -> [u8; BinaryFuse32::DESCRIPTOR_LEN] {
    let mut descriptor = [0; BinaryFuse32::DESCRIPTOR_LEN];
    descriptor[0..8].copy_from_slice(&seed.to_le_bytes());
    descriptor[8..12].copy_from_slice(&segment_length.to_le_bytes());
    descriptor[12..16].copy_from_slice(&segment_length_mask.to_le_bytes());
    descriptor[16..20].copy_from_slice(&segment_count_length.to_le_bytes());
    descriptor
}

impl BinaryFuse32Filter {
    pub fn mem_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.fingerprints.len() * std::mem::size_of::<u32>()
    }
}

impl Filter for BinaryFuse32Filter {
    type CodecError = BinaryFuse32CodecError;

    fn len(&self) -> Option<usize> {
        Some(self.len)
    }

    fn contains<K: ?Sized + Hash>(&self, key: &K) -> bool {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        self.contains_digest(hasher.finish())
    }

    fn contains_digest(&self, digest: u64) -> bool {
        BinaryFuse32Ref::from_dma(&self.descriptor, bytemuck::cast_slice(&self.fingerprints))
            .contains(&digest)
    }

    fn to_bytes(&self) -> Result<Vec<u8>, Self::CodecError> {
        let mut bytes = Vec::with_capacity(
            std::mem::size_of::<u64>()
                + self.descriptor.len()
                + std::mem::size_of::<u64>()
                + self.fingerprints.len() * std::mem::size_of::<u32>(),
        );
        bytes.extend_from_slice(&(self.len as u64).to_le_bytes());
        bytes.extend_from_slice(&self.descriptor);
        bytes.extend_from_slice(&(self.fingerprints.len() as u64).to_le_bytes());
        bytes.extend_from_slice(bytemuck::cast_slice(&self.fingerprints));
        Ok(bytes)
    }

    fn from_bytes(buf: &[u8]) -> Result<(Self, usize), Self::CodecError> {
        let mut offset = 0;

        fn eof() -> BinaryFuse32CodecError {
            BinaryFuse32CodecError::new(
                "unexpected end of data",
                &std::io::Error::other("unexpected end of data"),
            )
        }

        fn read_u64(data: &[u8], offset: &mut usize) -> Result<u64, BinaryFuse32CodecError> {
            if *offset + 8 > data.len() {
                return Err(eof());
            }
            let value = u64::from_le_bytes(data[*offset..*offset + 8].try_into().unwrap());
            *offset += 8;
            Ok(value)
        }

        let len = read_u64(buf, &mut offset)? as usize;

        let mut descriptor = [0; BinaryFuse32::DESCRIPTOR_LEN];
        if offset + BinaryFuse32::DESCRIPTOR_LEN > buf.len() {
            return Err(eof());
        }
        descriptor.copy_from_slice(&buf[offset..offset + BinaryFuse32::DESCRIPTOR_LEN]);
        offset += BinaryFuse32::DESCRIPTOR_LEN;

        let fingerprint_len = read_u64(buf, &mut offset)? as usize;
        let byte_len = fingerprint_len * std::mem::size_of::<u32>();
        if offset + byte_len > buf.len() {
            return Err(eof());
        }
        let fingerprints = buf[offset..offset + byte_len]
            .chunks_exact(std::mem::size_of::<u32>())
            .map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap()))
            .collect();
        offset += byte_len;

        Ok((
            Self {
                descriptor,
                fingerprints,
                len,
            },
            offset,
        ))
    }
}

impl Index for BinaryFuse32Filter {
    fn supported_type(data_type: &DataType) -> bool {
        crate::filters::Xor8Filter::supported_type(data_type)
    }
}

impl BinaryFuse32CodecError {
    pub fn new(msg: impl Display, cause: &(impl std::error::Error + 'static)) -> Self {
        Self {
            msg: msg.to_string(),
            cause: AnyError::new(cause),
        }
    }
}

impl BinaryFuse32BuildingError {
    pub fn new(cause: impl Display) -> Self {
        Self {
            cause: AnyError::error(cause.to_string()),
        }
    }
}

impl From<BinaryFuse32CodecError> for ErrorCode {
    fn from(value: BinaryFuse32CodecError) -> Self {
        ErrorCode::Internal(value.to_string())
    }
}

impl From<BinaryFuse32BuildingError> for ErrorCode {
    fn from(value: BinaryFuse32BuildingError) -> Self {
        ErrorCode::Internal(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filters::Filter;

    #[test]
    fn test_binary_fuse32_expanded_capacity_recovers_default_capacity_failure() -> anyhow::Result<()>
    {
        let digests = vec![
            12100706573412259482,
            17858013903799158384,
            10152736786087156494,
            12170187926617211409,
            5284553034609843520,
            3211710694437752088,
            15694839755804416522,
            6391141964374214533,
            17626201369861675860,
            7626797314791856264,
            8567517852399908291,
            6444936252164222020,
            9673155920647281941,
            15894702437242958609,
            90128777876498152,
            14490824520645362361,
            14163394562527960091,
            6708180727249648600,
            14372662790589329629,
            14618819062068333240,
            9620166075153112809,
            12994877493529946390,
            16040861691064476301,
            8245063073441705875,
            2179470377530870222,
            16066067840546653594,
            4265101728311556601,
            11360246383933319891,
            10356058033158317956,
            3785617108824048717,
            1247669162467556207,
            10022990056860465042,
            1110477748206289262,
            678962376812795467,
            17831833484229510073,
            12161966210082697389,
            4819526261903045160,
            14885564733575625995,
            14924878448938284390,
            15064984116938632310,
            10127765685281267631,
            6092469470155110584,
            7681863527100079009,
            11015870799992868554,
            7134618193083055412,
            16467668817819039376,
            9928519277040420462,
            6893434167711646496,
            15572473738468915916,
            11315772967394156742,
            15268015117525993607,
            14575715293971198500,
            11495411569663237476,
            12444118524173595866,
            14601166832861057032,
            17642275503075678808,
            11185851304602332536,
            709252799161131540,
            16995486786927067363,
            274162950980966,
            7040072277512602530,
            5934435424632034030,
            17643569146426879563,
            5046090017088365665,
            16014883091215448974,
            883819158922431235,
            10196270488149429808,
            16910334359241066592,
            13490698011305250417,
            9517203424476796989,
            6863696478851308562,
            14417947448345495252,
            1571779993856074856,
            13278998155001809952,
            13361374239916316824,
            3837283065186354764,
            6680095640535184930,
            15588342311100669528,
            15924741661086806326,
            684459667952103007,
            16868037927113757844,
            6685867604996884404,
            13507247350073953511,
            3158590621289567134,
            2808056756228423657,
            8361707712099200938,
            10784383143483211449,
            477119357624403010,
            1911009254525574630,
            13632728370555614054,
            11342636802715993318,
            9862827257094527411,
            13299353238451455087,
            2581568334556385261,
            11414734225507378062,
            15307696201761468920,
            248366729594843930,
            11179289291902106648,
            6994260465887223064,
            14502631462809994788,
            18247160397703506436,
            17057838425721492455,
            14520256392420020792,
            15085264981589126584,
            10293093893062252943,
            13161074059329979574,
            14038977343094598345,
            14850852512134113083,
            10330068343636960212,
            4729983124433325678,
            17652684372743698114,
            15535070370897330016,
            3825171570508255433,
            8969825465492377628,
            16167872397109883820,
            1603489173358878440,
            7945632331782402465,
            6971186796201247995,
            1820773671547830035,
            8295330553601476933,
            17796666516457908256,
            6108601816583581531,
            10407093527012674639,
            6552574783561857517,
            9601667971705774556,
            85163704654460877,
            15480882177410309130,
            16770848825996339221,
        ];

        assert!(
            build_binary_fuse32_with_capacity_factor(digests.as_slice(), 1.0, 1).is_err(),
            "sample should fail with default capacity in a single construction attempt"
        );

        let outcome = BinaryFuse32Builder::build_from_digests_with_policy_and_report(
            digests.as_slice(),
            BinaryFuse32BuildPolicy::for_test(true, false),
        )?;
        let filter = outcome.filter;

        match outcome.report.method {
            BinaryFuse32BuildMethod::Expanded(report) => {
                assert_eq!(report.capacity_factor_multiplier, 1.25);
                assert_eq!(report.max_iter, BINARY_FUSE32_EXPANDED_MAX_ITER);
                assert_eq!(report.fingerprint_count, filter.fingerprints.len());
            }
            BinaryFuse32BuildMethod::Default => {
                panic!("expected expanded capacity build report")
            }
        }

        assert_eq!(filter.len(), Some(digests.len()));
        for digest in digests {
            assert!(
                filter.contains_digest(digest),
                "digest {} not present",
                digest
            );
        }

        let bytes = filter.to_bytes()?;
        let (decoded, len) = BinaryFuse32Filter::from_bytes(&bytes)?;
        assert_eq!(len, bytes.len());
        assert_eq!(decoded, filter);

        Ok(())
    }
}
