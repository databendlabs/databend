// Copyright 2021 Datafuse Labs.
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

use bincode;
use common_datavalues::fasthash::city;
use common_datavalues::fasthash::FastHasher;
use common_datavalues::prelude::DataColumn;
use common_datavalues::DFHasher;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_tracing::tracing;

use crate::storages::index::IndexSchemaVersion;

// The seed values are from clickhouse.
// See https://github.com/ClickHouse/ClickHouse/blob/1bf375e2b761db5b99b0f403b90c412a530f4d5c/src/Interpreters/BloomFilter.cpp#L18
const SEED_GEN_A: u64 = 845897321;
const SEED_GEN_B: u64 = 217728422;

/// Bloom filter for data skipping.
/// Most ideas/implementations are ported from Clickhouse.
/// https://github.com/ClickHouse/ClickHouse/blob/1bf375e2b761db5b99b0f403b90c412a530f4d5c/src/Interpreters/BloomFilter.cpp
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct BloomFilter {
    // container for bitmap
    container: Vec<u64>,

    // number of bits of the bitmap
    num_bits: usize,

    num_hashes: usize,

    version: IndexSchemaVersion,

    seed1: u64,

    seed2: u64,
}

impl BloomFilter {
    /// Create a bloom filter instance with estimated number of items and expected false positive rate.
    pub fn with_rate(num_items: u64, false_positive_rate: f64, seed: u64) -> Self {
        let num_bits = Self::optimal_num_bits(num_items, false_positive_rate);
        let num_hashes = Self::optimal_num_hashes(num_items, num_bits as u64);

        Self::with_size(num_bits, num_hashes, seed)
    }

    /// Create a bloom filter instance with specified bitmap length and number of hashes
    pub fn with_size(num_bits: usize, num_hashes: usize, seed: u64) -> Self {
        // calculate the number of u64 we need
        let num_u64 = (num_bits + 63) / 64;

        // Seed calculation is ported from clickhouse.
        // https://github.com/ClickHouse/ClickHouse/blob/1bf375e2b761db5b99b0f403b90c412a530f4d5c/src/Interpreters/BloomFilter.cpp#L50
        let seed1 = seed;
        let seed2 = SEED_GEN_A * seed + SEED_GEN_B;

        Self {
            container: vec![0; num_u64],
            seed1,
            seed2,
            num_hashes,
            num_bits,
            version: IndexSchemaVersion::V1,
        }
    }

    /// Calculate the number of bits for the bloom filter from the formula:
    /// m  = - n * ln(p) / (ln(2)^2)
    ///
    /// n: number of items
    ///
    /// p: false positive rate
    pub fn optimal_num_bits(num_items: u64, false_positive_rate: f64) -> usize {
        let power_of_ln2 = core::f32::consts::LN_2 as f64 * core::f32::consts::LN_2 as f64;
        let m = -(num_items as f64 * false_positive_rate.ln()) / power_of_ln2;
        let num_bits = m.ceil() as usize;
        tracing::info!("Bloom filter calculate optimal bits, num_bits: {}, num_items: {}, false_positive_rate: {}", num_bits, num_items, false_positive_rate);
        num_bits
    }

    /// Calculate the number of hashes for the bloom filter from the formula:
    /// k = m/n * ln(2)
    ///
    /// m: number of bits
    ///
    /// n: number of items
    pub fn optimal_num_hashes(num_items: u64, num_bits: u64) -> usize {
        let k = num_bits as f64 / num_items as f64 * core::f32::consts::LN_2 as f64;
        let num_hashes = std::cmp::max(2, k.ceil() as usize); // at least two hashes
        tracing::info!(
            "Bloom filter calculate optimal hashes, num_hashes: {}",
            num_hashes
        );
        num_hashes
    }

    /// Returns the number of bits of the bloom filter.
    pub fn num_bits(&self) -> usize {
        self.num_bits
    }

    /// Returns the number of hashes of the bloom filter.
    pub fn num_hashes(&self) -> usize {
        self.num_hashes
    }

    /// Returns whether the data type is supported by bloom filter.
    ///
    /// The supported types are most same as Databricks:
    /// https://docs.microsoft.com/en-us/azure/databricks/delta/optimizations/bloom-filters
    ///
    /// "Bloom filters support columns with the following (input) data types: byte, short, int,
    /// long, float, double, date, timestamp, and string."
    ///
    /// Nulls are not added to the Bloom
    /// filter, so any null related filter requires reading the data file. "
    pub fn is_supported_type(data_type: &DataType) -> bool {
        matches!(
            data_type,
            DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Date16
                | DataType::Date32
                | DataType::DateTime32(_)
                | DataType::DateTime64(_, _)
                | DataType::Interval(_)
                | DataType::String
        )
    }

    /// Return whether the data value is supported by bloom filter.
    /// Nulls are not supported.
    pub fn is_supported_value(value: &DataValue) -> bool {
        Self::is_supported_type(&value.data_type()) && !value.is_null()
    }

    #[inline(always)]
    fn create_city_hasher(seed: u64) -> DFHasher {
        let hasher = city::Hasher64::with_seed(seed);
        DFHasher::CityHasher64(hasher, seed)
    }

    #[inline(always)]
    // Set bits for bloom filter, ported from Clickhouse.
    // https://github.com/ClickHouse/ClickHouse/blob/1bf375e2b761db5b99b0f403b90c412a530f4d5c/src/Interpreters/BloomFilter.cpp#L67
    fn set_bits(&mut self, hash1: &u64, hash2: &u64) {
        let h1 = std::num::Wrapping(*hash1);
        let h2 = std::num::Wrapping(*hash2);

        for i in 0..self.num_hashes {
            let index = std::num::Wrapping(i as u64);
            let res = (h1 + index * h2 + index * index).0;
            let bit_pos = (res % self.num_bits as u64) as usize;
            self.container[bit_pos / 64] |= 1u64 << (bit_pos % 64);
        }
    }

    /// Add the column data into bloom filter, Nulls are skipped and not added.
    ///
    /// The design of skipping Nulls are arguably correct. For now we do the same as databricks.
    /// See the design of databricks https://docs.databricks.com/delta/optimizations/bloom-filters.html
    pub fn add(&mut self, column: &DataColumn) -> Result<()> {
        if !Self::is_supported_type(&column.data_type()) {
            return Err(ErrorCode::BadArguments(format!(
                "Unsupported data type: {} ",
                column.data_type()
            )));
        }

        let series = column.to_minimal_array()?;

        let city_hasher1 = Self::create_city_hasher(self.seed1);
        let hash1_arr = series.vec_hash(city_hasher1)?;

        let city_hasher2 = Self::create_city_hasher(self.seed2);
        let hash2_arr = series.vec_hash(city_hasher2)?;

        let validity = series.validity();
        let no_null = validity == None || series.null_count() == 0;
        let all_null = series.len() == series.null_count();

        if all_null {
            return Ok(());
        }

        if no_null {
            hash1_arr
                .into_no_null_iter()
                .zip(hash2_arr.into_no_null_iter())
                .for_each(|(h1, h2)| {
                    self.set_bits(h1, h2);
                });
        } else {
            let bitmap = validity.unwrap();
            bitmap
                .into_iter()
                .zip(hash1_arr.into_no_null_iter())
                .zip(hash2_arr.into_no_null_iter())
                .for_each(|((valid, h1), h2)| {
                    if valid {
                        self.set_bits(h1, h2);
                    }
                });
        }
        Ok(())
    }

    /// Check the existence of the data. The data value should not be Null.
    /// Use BloomFilter::is_supported_value to check before using this method.
    ///
    ///
    /// Notice: false positive may exist, e.g. return true doesn't guarantee the value exists.
    /// But returning false guarantees that it never ever showed up.
    ///
    /// Example:
    /// ```
    ///     let not_exist = BloomFilter::is_supported_value(data_value) && !bloom.find(data_value)?;
    ///
    /// ```
    pub fn find(&mut self, val: DataValue) -> Result<bool> {
        if !Self::is_supported_value(&val) {
            return Err(ErrorCode::BadArguments(format!(
                "Unsupported data value: {} ",
                val
            )));
        }

        let col = DataColumn::Constant(val, 1);
        let series = col.to_minimal_array()?;

        let city_hasher1 = Self::create_city_hasher(self.seed1);
        let hash1_arr = series.vec_hash(city_hasher1)?;
        let hash1 = hash1_arr.inner().value(0);

        let city_hasher2 = Self::create_city_hasher(self.seed2);
        let hash2_arr = series.vec_hash(city_hasher2)?;
        let hash2 = hash2_arr.inner().value(0);

        let h1 = std::num::Wrapping(hash1);
        let h2 = std::num::Wrapping(hash2);
        for i in 0..self.num_hashes {
            let index = std::num::Wrapping(i as u64);
            let res = (h1 + index * h2 + index * index).0;
            let bit_pos = (res % self.num_bits as u64) as usize;

            // If any bit is not 1 in bloom filter, it means the data never ever showed up before.
            let flag = self.container[bit_pos / 64] & (1 << (bit_pos % 64));
            if flag == 0 {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Serialize the bloom filter to byte vector.
    pub fn to_vec(&self) -> Result<Vec<u8>> {
        match bincode::serialize(self) {
            Ok(v) => Ok(v),
            Err(e) => Err(ErrorCode::SerdeError(format!(
                "bincode serialization error: {} ",
                e
            ))),
        }
    }

    /// Deserialize from a byte slice and return a bloom filter.
    pub fn from_vec(bytes: &[u8]) -> Result<Self> {
        match bincode::deserialize::<BloomFilter>(bytes) {
            Ok(bloom_filter) => Ok(bloom_filter),
            Err(e) => Err(ErrorCode::SerdeError(format!(
                "bincode deserialization error: {} ",
                e
            ))),
        }
    }
}
