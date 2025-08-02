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

//! Codes are borrowed from [simple_hll](https://github.com/sundy-li/simple_hll)

use std::hash::Hash;

use ahash::RandomState;

const P: usize = 7_usize;
const Q: usize = 64 - P;
const M: usize = 1 << P;

/// Fixed seed
const SEED: RandomState = RandomState::with_seeds(
    0x355e438b4b1478c7_u64,
    0xd0e8453cd135b473_u64,
    0xf7b252066a57836a_u64,
    0xb8a829e3713c09bf_u64,
);

/// Note: We don't make MetaHLL as static struct by keeping `PhantomData<T>`
/// Callers should take care of its hash function to be unchanged.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetaHLL {
    pub(crate) registers: Vec<u8>,
}

impl Default for MetaHLL {
    fn default() -> Self {
        Self::new()
    }
}

impl MetaHLL {
    /// note that this method should not be invoked in untrusted environment
    pub fn new() -> Self {
        Self {
            registers: vec![0; M],
        }
    }

    /// Adds an hash to the MetaHLL.
    /// hash value is dertermined by caller
    #[inline]
    pub fn add_hash(&mut self, hash: u64) {
        let index = (hash & (M as u64 - 1)) as usize;
        let one_position = (((hash >> P) | (1_u64 << Q)).trailing_zeros() + 1) as u8;
        unsafe {
            let val = self.registers.get_unchecked_mut(index);
            if *val < one_position {
                *val = one_position;
            }
        }
    }

    /// Adds an object to the MetaHLL.
    /// Though we could pass different types into this method, caller should notice that
    pub fn add_object<T: Hash>(&mut self, obj: &T) {
        let hash = SEED.hash_one(obj);
        self.add_hash(hash);
    }

    /// Merge the other [`MetaHLL`] into this one
    pub fn merge(&mut self, other: &Self) {
        for i in 0..self.registers.len() {
            self.registers[i] = self.registers[i].max(other.registers[i]);
        }
    }

    /// Get the register histogram (each value in register index into
    /// the histogram
    #[inline]
    fn get_histogram(&self) -> [u32; 64] {
        let mut histogram = [0; 64];
        // hopefully this can be unrolled
        for r in &self.registers {
            histogram[*r as usize] += 1;
        }
        histogram
    }

    /// Guess the number of unique elements seen by the MetaHLL.
    #[inline]
    pub fn count(&self) -> usize {
        let histogram = self.get_histogram();
        let m = M as f64;
        let mut z = m * hll_tau((m - histogram[Q + 1] as f64) / m);
        for i in histogram[1..=Q].iter().rev() {
            z += *i as f64;
            z *= 0.5;
        }
        z += m * hll_sigma(histogram[0] as f64 / m);

        (0.5 / 2_f64.ln() * m * m / z).round() as usize
    }

    #[inline]
    pub fn num_empty_registers(&self) -> usize {
        self.registers.iter().filter(|x| **x == 0).count()
    }
}

/// Helper function sigma as defined in
/// "New cardinality estimation algorithms for MetaHLL sketches"
/// Otmar Ertl, https://arxiv.org/abs/1702.01284
#[inline]
fn hll_sigma(x: f64) -> f64 {
    if x == 1. {
        f64::INFINITY
    } else {
        let mut y = 1.0;
        let mut z = x;
        let mut x = x;
        loop {
            x *= x;
            let z_prime = z;
            z += x * y;
            y += y;

            if z_prime == z {
                break;
            }
        }
        z
    }
}

/// Helper function tau as defined in
/// "New cardinality estimation algorithms for MetaHLL sketches"
/// Otmar Ertl, https://arxiv.org/abs/1702.01284
#[inline]
fn hll_tau(x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        0.0
    } else {
        let mut y = 1.0;
        let mut z = 1.0 - x;
        let mut x = x;
        loop {
            x = x.sqrt();
            let z_prime = z;
            y *= 0.5;
            z -= (1.0 - x).powi(2) * y;
            if z_prime == z {
                break;
            }
        }
        z / 3.0
    }
}

#[derive(serde::Serialize, borsh::BorshSerialize)]
enum MetaHLLVariantRef<'a> {
    Empty,
    Sparse { indices: Vec<u8>, values: Vec<u8> },
    Full(&'a Vec<u8>),
}

#[derive(serde::Deserialize, borsh::BorshDeserialize)]
enum MetaHLLVariant {
    Empty,
    Sparse { indices: Vec<u8>, values: Vec<u8> },
    Full(Vec<u8>),
}

impl From<MetaHLLVariant> for MetaHLL {
    fn from(value: MetaHLLVariant) -> Self {
        match value {
            MetaHLLVariant::Empty => MetaHLL::new(),
            MetaHLLVariant::Sparse { indices, values } => {
                let mut registers = vec![0; 1 << P];
                for (i, v) in indices.into_iter().zip(values.into_iter()) {
                    registers[i as usize] = v;
                }
                MetaHLL { registers }
            }
            MetaHLLVariant::Full(registers) => MetaHLL { registers },
        }
    }
}

impl<'a> From<&'a MetaHLL> for MetaHLLVariantRef<'a> {
    fn from(hll: &'a MetaHLL) -> Self {
        let none_empty_registers = M - hll.num_empty_registers();

        if none_empty_registers == 0 {
            MetaHLLVariantRef::Empty
        } else if none_empty_registers * 3 <= M {
            // If the number of empty registers is larger enough, we can use sparse serialize to reduce the binary size
            // each register in sparse format will occupy 3 bytes, 2 for register index and 1 for register value.
            let mut indices = Vec::with_capacity(none_empty_registers);
            let mut values = Vec::with_capacity(none_empty_registers);
            for (index, &value) in hll.registers.iter().enumerate() {
                if value != 0 {
                    indices.push(index as u8);
                    values.push(value);
                }
            }
            MetaHLLVariantRef::Sparse { indices, values }
        } else {
            MetaHLLVariantRef::Full(&hll.registers)
        }
    }
}

impl serde::Serialize for MetaHLL {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        let v: MetaHLLVariantRef<'_> = self.into();
        v.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for MetaHLL {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        let v = MetaHLLVariant::deserialize(deserializer)?;
        Ok(v.into())
    }
}

impl borsh::BorshSerialize for MetaHLL {
    fn serialize<W: std::io::prelude::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let v: MetaHLLVariantRef<'_> = self.into();
        v.serialize(writer)
    }
}

impl borsh::BorshDeserialize for MetaHLL {
    fn deserialize_reader<R: std::io::prelude::Read>(reader: &mut R) -> std::io::Result<Self> {
        let v = MetaHLLVariant::deserialize_reader(reader)?;
        Ok(v.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compare_with_delta(got: usize, expected: usize) {
        let expected = expected as f64;
        let diff = (got as f64) - expected;
        let diff = diff.abs() / expected;
        // times 6 because we want the tests to be stable
        // so we allow a rather large margin of error
        // this is adopted from redis's unit test version as well
        let margin = 1.04 / ((M as f64).sqrt()) * 6.0;
        assert!(
            diff <= margin,
            "{} is not near {} percent of {} which is ({}, {})",
            got,
            margin,
            expected,
            expected * (1.0 - margin),
            expected * (1.0 + margin)
        );
    }

    macro_rules! sized_number_test {
        ($SIZE: expr, $T: tt) => {{
            let mut hll = MetaHLL::new();
            for i in 0..$SIZE {
                hll.add_object(&(i as $T));
            }
            compare_with_delta(hll.count(), $SIZE);
        }};
    }

    macro_rules! typed_large_number_test {
        ($SIZE: expr) => {{
            sized_number_test!($SIZE, u64);
            sized_number_test!($SIZE, u128);
            sized_number_test!($SIZE, i64);
            sized_number_test!($SIZE, i128);
        }};
    }

    macro_rules! typed_number_test {
        ($SIZE: expr) => {{
            sized_number_test!($SIZE, u16);
            sized_number_test!($SIZE, u32);
            sized_number_test!($SIZE, i16);
            sized_number_test!($SIZE, i32);
            typed_large_number_test!($SIZE);
        }};
    }

    #[test]
    fn test_empty() {
        let hll = MetaHLL::new();
        assert_eq!(hll.count(), 0);
    }

    #[test]
    fn test_one() {
        let mut hll = MetaHLL::new();
        hll.add_hash(1);
        assert_eq!(hll.count(), 1);
    }

    #[test]
    fn test_number_100() {
        typed_number_test!(100);
    }

    #[test]
    fn test_number_1k() {
        typed_number_test!(1_000);
    }

    #[test]
    fn test_number_10k() {
        typed_number_test!(10_000);
    }

    #[test]
    fn test_number_100k() {
        typed_large_number_test!(100_000);
    }

    #[test]
    fn test_number_1m() {
        typed_large_number_test!(1_000_000);
    }

    #[test]
    fn test_empty_merge() {
        let mut hll = MetaHLL::new();
        hll.merge(&MetaHLL::new());
        assert_eq!(hll.count(), 0);
    }

    #[test]
    fn test_merge_overlapped() {
        let mut hll = MetaHLL::new();
        for i in 0..1000 {
            hll.add_object(&i);
        }

        let other = MetaHLL::new();
        for i in 0..1000 {
            hll.add_object(&i);
        }

        hll.merge(&other);
        compare_with_delta(hll.count(), 1000);
    }

    #[test]
    fn test_repetition() {
        let mut hll = MetaHLL::new();
        for i in 0..1_000_000 {
            hll.add_object(&(i % 1000));
        }
        compare_with_delta(hll.count(), 1000);
    }
}
