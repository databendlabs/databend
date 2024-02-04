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

//! # HyperLogLog
//!
//! `hyperloglog` port from [redis's implementation](https://github.com/redis/redis/blob/4930d19e70c391750479951022e207e19111eb55/src/hyperloglog.c)
//! Some codes are borrowed from:
//! 1. https://github.com/crepererum/pdatastructs.rs/blob/3997ed50f6b6871c9e53c4c5e0f48f431405fc63/src/hyperloglog.rs
//! 2. https://github.com/apache/arrow-datafusion/blob/f203d863f5c8bc9f133f6dd9b2e34e57ac3cdddc/datafusion/physical-expr/src/aggregate/hyperloglog.rs

use std::hash::Hash;

use ahash::RandomState;

/// By default, we use 2**14 registers like redis
const REDIS_P: usize = 14_usize;

/// Fixed seed
const SEED: RandomState = RandomState::with_seeds(
    0x355e438b4b1478c7_u64,
    0xd0e8453cd135b473_u64,
    0xf7b252066a57836a_u64,
    0xb8a829e3713c09bf_u64,
);

/// Note: We don't make HyperLogLog as static struct by keeping `PhantomData<T>`
/// Callers should take care of its hash function to be unchanged.
/// P is the bucket number, must be [4, 18]
/// Q = 64 - P
/// Register num is 1 << P
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HyperLogLog<const P: usize = REDIS_P> {
    registers: Vec<u8>,
}

impl<const P: usize> Default for HyperLogLog<P> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const P: usize> HyperLogLog<P> {
    /// note that this method should not be invoked in untrusted environment
    pub fn new() -> Self {
        assert!(
            (P >= 4) & (P <= 18),
            "P ({}) must be larger or equal than 4 and smaller or equal than 18",
            P
        );

        Self {
            registers: vec![0; 1 << P],
        }
    }

    pub fn with_registers(registers: Vec<u8>) -> Self {
        assert_eq!(registers.len(), Self::number_registers());

        Self { registers }
    }

    /// Adds an element to the HyperLogLog.
    /// hash value is dertermined by caller
    #[inline]
    pub fn add_hash(&mut self, hash: u64) {
        let index = (hash & Self::register_mask()) as usize;
        let one_position = ((hash >> P) | (1_u64 << Self::q())).trailing_zeros() + 1;
        self.registers[index] = self.registers[index].max(one_position as u8);
    }

    /// Adds an element to the HyperLogLog.
    /// hash value is dertermined by caller
    pub fn add_object<T: Hash>(&mut self, obj: &T) {
        let hash = SEED.hash_one(obj);
        self.add_hash(hash);
    }

    /// Merge the other [`HyperLogLog`] into this one
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

    /// Guess the number of unique elements seen by the HyperLogLog.
    pub fn count(&self) -> usize {
        let histogram = self.get_histogram();
        let m = Self::number_registers() as f64;
        let q = Self::q();
        let mut z = m * hll_tau((m - histogram[q + 1] as f64) / m);
        for i in histogram[1..=q].iter().rev() {
            z += *i as f64;
            z *= 0.5;
        }
        z += m * hll_sigma(histogram[0] as f64 / m);

        (0.5 / 2_f64.ln() * m * m / z).round() as usize
    }

    #[inline]
    fn q() -> usize {
        64 - P
    }

    #[inline]
    fn register_mask() -> u64 {
        Self::number_registers() as u64 - 1
    }

    #[inline]
    pub fn number_registers() -> usize {
        1 << P
    }

    #[inline]
    pub fn error_rate() -> f64 {
        1.04f64 / (Self::number_registers() as f64).sqrt()
    }

    #[inline]
    pub fn max_byte_size() -> usize {
        Self::number_registers() * 8
    }

    #[inline]
    pub fn num_empty_registers(&self) -> usize {
        self.registers.iter().filter(|x| **x == 0).count()
    }
}

impl<const P: usize> Extend<u64> for HyperLogLog<P> {
    fn extend<S: IntoIterator<Item = u64>>(&mut self, iter: S) {
        for elem in iter {
            self.add_hash(elem);
        }
    }
}

impl<'a, const P: usize> Extend<&'a u64> for HyperLogLog<P> {
    fn extend<S: IntoIterator<Item = &'a u64>>(&mut self, iter: S) {
        for elem in iter {
            self.add_hash(*elem);
        }
    }
}

/// Helper function sigma as defined in
/// "New cardinality estimation algorithms for HyperLogLog sketches"
/// Otmar Ertl, arXiv:1702.01284
#[allow(dead_code)]
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
/// "New cardinality estimation algorithms for HyperLogLog sketches"
/// Otmar Ertl, arXiv:1702.01284
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
enum HyperLogLogVariantRef<'a, const P: usize> {
    Empty,
    Sparse { data: Vec<(u16, u8)> },
    Full(&'a Vec<u8>),
}

#[derive(serde::Deserialize, borsh::BorshDeserialize)]
enum HyperLogLogVariant<const P: usize> {
    Empty,
    Sparse { data: Vec<(u16, u8)> },
    Full(Vec<u8>),
}

impl<const P: usize> From<HyperLogLogVariant<P>> for HyperLogLog<P> {
    fn from(value: HyperLogLogVariant<P>) -> Self {
        match value {
            HyperLogLogVariant::Empty => HyperLogLog::<P>::new(),
            HyperLogLogVariant::Sparse { data } => {
                let mut registers = vec![0; 1 << P];
                for (index, val) in data {
                    registers[index as usize] = val;
                }

                HyperLogLog::<P> { registers }
            }
            HyperLogLogVariant::Full(registers) => HyperLogLog::<P> { registers },
        }
    }
}

impl<'a, const P: usize> From<&'a HyperLogLog<P>> for HyperLogLogVariantRef<'a, P> {
    fn from(hll: &'a HyperLogLog<P>) -> Self {
        let none_empty_registers = HyperLogLog::<P>::number_registers() - hll.num_empty_registers();

        if none_empty_registers == 0 {
            HyperLogLogVariantRef::Empty
        } else if none_empty_registers * 3 <= HyperLogLog::<P>::number_registers() {
            // If the number of empty registers is larger enough, we can use sparse serialize to reduce the binary size
            // each register in sparse format will occupy 3 bytes, 2 for register index and 1 for register value.
            let sparse_data: Vec<(u16, u8)> = hll
                .registers
                .iter()
                .enumerate()
                .filter_map(|(index, &value)| {
                    if value != 0 {
                        Some((index as u16, value))
                    } else {
                        None
                    }
                })
                .collect();

            HyperLogLogVariantRef::Sparse { data: sparse_data }
        } else {
            HyperLogLogVariantRef::Full(&hll.registers)
        }
    }
}

impl<const P: usize> serde::Serialize for HyperLogLog<P> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        let v: HyperLogLogVariantRef<'_, P> = self.into();
        v.serialize(serializer)
    }
}

impl<'de, const P: usize> serde::Deserialize<'de> for HyperLogLog<P> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        let v = HyperLogLogVariant::<P>::deserialize(deserializer)?;
        Ok(v.into())
    }
}

impl<const P: usize> borsh::BorshSerialize for HyperLogLog<P> {
    fn serialize<W: std::io::prelude::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let v: HyperLogLogVariantRef<'_, P> = self.into();
        v.serialize(writer)
    }
}

impl<const P: usize> borsh::BorshDeserialize for HyperLogLog<P> {
    fn deserialize_reader<R: std::io::prelude::Read>(reader: &mut R) -> std::io::Result<Self> {
        let v = HyperLogLogVariant::<P>::deserialize_reader(reader)?;
        Ok(v.into())
    }
}
