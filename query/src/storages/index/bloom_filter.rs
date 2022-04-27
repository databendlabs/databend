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

use std::num::Wrapping;
use std::sync::Arc;

use bincode;
use bit_vec::BitVec;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::Expression;
use common_tracing::tracing;

use crate::pipelines::transforms::ExpressionExecutor;
use crate::sessions::QueryContext;
use crate::storages::index::IndexSchemaVersion;

/// BloomFilterExprEvalResult represents the evaluation result of an expression by bloom filter.
///
/// For example, expression of 'age = 12' should return false is the bloom filter are sure
/// of the nonexistent of value '12' in column 'age'. Otherwise should return 'Unknown'.
///
/// If the column is not applicable for bloom filter, like TypeID::struct, NotApplicable is used.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BloomFilterExprEvalResult {
    False,
    Unknown,
    NotApplicable,
}

/// BloomFilterIndex represents multiple  bloom filters (per column) in data block.
///
/// By default we create bloom filter per column for a parquet data file. For columns whose data_type
/// are not applicable for a bloom filter, we skip the creation.
/// That is to say, it is legal to have a BloomFilterBlock with zero columns.
///
/// For example, for the source data block as follows:
///```
///         +---name--+--age--+
///         | "Alice" |  20   |
///         | "Bob"   |  30   |
///         +---------+-------+
/// ```
/// We will create bloom filter table as follows:
///```
///         +---Bloom(name)--+--Bloom(age)--+
///         |  123456789abcd |  ac2345bcd   |
///         +----------------+--------------+
/// ```
pub struct BloomFilterIndexer {
    // The schema of the source table/block, which the bloom filter work for.
    pub source_schema: DataSchemaRef,

    // The bloom block contains bloom filters;
    pub bloom_block: DataBlock,

    pub ctx: Arc<QueryContext>,
}

const BLOOM_FILTER_MAX_NUM_BITS: usize = 20480; // 2.5KB, maybe too big?
const BLOOM_FILTER_DEFAULT_FALSE_POSITIVE_RATE: f64 = 0.05;
const BLOOM_FILTER_SEED_GEN_A: u64 = 845897321;
const BLOOM_FILTER_SEED_GEN_B: u64 = 217728422;

impl BloomFilterIndexer {
    /// For every applicable column, we will create a bloom filter.
    /// The bloom filter will be stored with field name 'Bloom(column_name)'
    pub fn to_bloom_column_name(column_name: &str) -> String {
        format!("Bloom({})", column_name)
    }

    #[inline(always)]
    fn create_seed() -> u64 {
        let seed: u64 = rand::random();
        seed
    }

    /// Load a bloom filter directly from the source table's schema and the corresponding bloom parquet file.
    pub fn from_bloom_block(
        source_table_schema: DataSchemaRef,
        bloom_block: DataBlock,
        ctx: Arc<QueryContext>,
    ) -> Result<Self> {
        Ok(Self {
            source_schema: source_table_schema,
            bloom_block,
            ctx,
        })
    }

    /// Create a bloom filter block from source data.
    ///
    /// All input blocks should be belong to a Parquet file, e.g. the block array represents the parquet file in memory.
    #[allow(dead_code)]
    pub fn try_create(source_data_blocks: &[DataBlock], ctx: Arc<QueryContext>) -> Result<Self> {
        let seed = Self::create_seed();
        Self::try_create_with_seed(source_data_blocks, seed, ctx)
    }

    /// Create a bloom filter block from source data blocks and seed(s).
    ///
    /// All input blocks should be belong to a Parquet file, e.g. the block array represents the parquet file in memory.
    pub fn try_create_with_seed(
        blocks: &[DataBlock],
        seed: u64,
        ctx: Arc<QueryContext>,
    ) -> Result<Self> {
        if blocks.is_empty() {
            return Err(ErrorCode::BadArguments("data blocks is empty"));
        }

        let source_schema = blocks[0].schema().clone();

        let total_num_rows = blocks.iter().map(|block| block.num_rows() as u64).sum();

        let mut bloom_columns = vec![];
        let mut bloom_fields = vec![];

        let fields = blocks[0].schema().fields();
        for (i, field) in fields.iter().enumerate() {
            if BloomFilter::is_supported_type(field.data_type()) {
                // create field for applicable ones
                let bloom_column_name = Self::to_bloom_column_name(field.name());
                let bloom_field = DataField::new(&bloom_column_name, Vu8::to_data_type());
                bloom_fields.push(bloom_field);

                // create bloom filter per column
                let mut bloom_filter = BloomFilter::with_rate_and_max_bits(
                    total_num_rows,
                    BLOOM_FILTER_DEFAULT_FALSE_POSITIVE_RATE,
                    BLOOM_FILTER_MAX_NUM_BITS,
                    seed,
                );

                // ingest the same column data from all blocks
                for block in blocks.iter() {
                    let col = block.column(i);
                    bloom_filter.add(col, ctx.clone())?;
                }

                // create bloom filter column
                let serialized_bytes = bloom_filter.to_vec()?;
                let bloom_value = DataValue::String(serialized_bytes);
                let bloom_column: ColumnRef = bloom_value.as_const_column(&StringType::arc(), 1)?;
                bloom_columns.push(bloom_column);
            }
        }

        let bloom_schema = Arc::new(DataSchema::new(bloom_fields));
        let bloom_block = DataBlock::create(bloom_schema, bloom_columns);
        Ok(Self {
            source_schema,
            bloom_block,
            ctx,
        })
    }

    fn find(
        &self,
        column_name: &str,
        target: DataValue,
        typ: DataTypePtr,
        ctx: Arc<QueryContext>,
    ) -> Result<BloomFilterExprEvalResult> {
        let bloom_column = Self::to_bloom_column_name(column_name);
        if !self.bloom_block.schema().has_field(&bloom_column)
            || !BloomFilter::is_supported_type(&typ)
            || target.is_null()
        {
            // The column doesn't have bloom filter bitmap
            return Ok(BloomFilterExprEvalResult::NotApplicable);
        }

        let bloom_bytes = self.bloom_block.first(&bloom_column)?.as_string()?;
        let bloom_filter = BloomFilter::from_vec(bloom_bytes.as_ref())?;

        if bloom_filter.find(target, typ, ctx)? {
            Ok(BloomFilterExprEvalResult::Unknown)
        } else {
            Ok(BloomFilterExprEvalResult::False)
        }
    }

    /// Returns false when the expression must be false, otherwise true.
    /// The 'true' doesn't really mean the expression is true, but 'maybe true'.
    /// That is to say, you still need the load all data and run the execution.
    #[allow(dead_code)]
    pub fn maybe_true(&self, expr: &Expression) -> Result<bool> {
        Ok(self.eval(expr)? != BloomFilterExprEvalResult::False)
    }

    /// Apply the predicate expression, return the result.
    /// If we are sure of skipping the scan, return false, e.g. the expression must be false.
    /// This happens when the data doesn't show up in bloom filter.
    ///
    /// Otherwise return either Unknown or NotApplicable.
    pub fn eval(&self, expr: &Expression) -> Result<BloomFilterExprEvalResult> {
        //TODO: support multiple columns and other ops like 'in' ...
        match expr {
            Expression::BinaryExpression { left, op, right } => match op.to_lowercase().as_str() {
                "=" => self.eval_equivalent_expression(left, right),
                "and" => self.eval_logical_and(left, right),
                "or" => self.eval_logical_or(left, right),
                _ => Ok(BloomFilterExprEvalResult::NotApplicable),
            },
            _ => Ok(BloomFilterExprEvalResult::NotApplicable),
        }
    }

    // Evaluate the equivalent expression like "name='Alice'"
    fn eval_equivalent_expression(
        &self,
        left: &Expression,
        right: &Expression,
    ) -> Result<BloomFilterExprEvalResult> {
        let schema: &DataSchemaRef = &self.source_schema;

        // For now only support single column like "name = 'Alice'"
        match (left, right) {
            // match the expression of 'column_name = literal constant'
            (Expression::Column(column), Expression::Literal { value, .. })
            | (Expression::Literal { value, .. }, Expression::Column(column)) => {
                // find the corresponding column from source table
                match schema.column_with_name(column) {
                    Some((_index, data_field)) => {
                        let data_type = data_field.data_type();
                        self.find(column, value.clone(), data_type.clone(), self.ctx.clone())
                    }
                    None => Err(ErrorCode::BadArguments(format!(
                        "Column '{}' not found in schema",
                        column
                    ))),
                }
            }
            _ => Ok(BloomFilterExprEvalResult::NotApplicable),
        }
    }

    // Evaluate the logical and expression
    fn eval_logical_and(
        &self,
        left: &Expression,
        right: &Expression,
    ) -> Result<BloomFilterExprEvalResult> {
        let left_result = self.eval(left)?;
        if left_result == BloomFilterExprEvalResult::False {
            return Ok(BloomFilterExprEvalResult::False);
        }

        let right_result = self.eval(right)?;
        if right_result == BloomFilterExprEvalResult::False {
            return Ok(BloomFilterExprEvalResult::False);
        }

        if left_result == BloomFilterExprEvalResult::NotApplicable
            || right_result == BloomFilterExprEvalResult::NotApplicable
        {
            Ok(BloomFilterExprEvalResult::NotApplicable)
        } else {
            Ok(BloomFilterExprEvalResult::Unknown)
        }
    }

    // Evaluate the logical or expression
    fn eval_logical_or(
        &self,
        left: &Expression,
        right: &Expression,
    ) -> Result<BloomFilterExprEvalResult> {
        let left_result = self.eval(left)?;
        let right_result = self.eval(right)?;
        match (&left_result, &right_result) {
            (&BloomFilterExprEvalResult::False, &BloomFilterExprEvalResult::False) => {
                Ok(BloomFilterExprEvalResult::False)
            }
            (&BloomFilterExprEvalResult::False, _) => Ok(right_result),
            (_, &BloomFilterExprEvalResult::False) => Ok(left_result),
            (&BloomFilterExprEvalResult::Unknown, &BloomFilterExprEvalResult::Unknown) => {
                Ok(BloomFilterExprEvalResult::Unknown)
            }
            _ => Ok(BloomFilterExprEvalResult::NotApplicable),
        }
    }

    /// Find and returns the bloom filter by name
    pub fn try_get_bloom(&self, column_name: &str) -> Result<BloomFilter> {
        let bloom_column = Self::to_bloom_column_name(column_name);
        let val = self.bloom_block.first(&bloom_column)?;
        let bloom_bytes = val.as_string()?;
        let bloom_filter = BloomFilter::from_vec(bloom_bytes.as_ref())?;
        Ok(bloom_filter)
    }
}

/// A bloom filter implementation for data column and values.
///
/// Most ideas/implementations are ported from Clickhouse.
/// https://github.com/ClickHouse/ClickHouse/blob/1bf375e2b761db5b99b0f403b90c412a530f4d5c/src/Interpreters/BloomFilter.cpp
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct BloomFilter {
    // Container for bitmap
    container: BitVec,

    // The number of hashes for the bloom filter. We use double hashing and mix the result
    // to achieve k hashes. The value doesn't really mean the number of hashing we actually compute.
    // For more details, see this paper: http://www.eecs.harvard.edu/~michaelm/postscripts/rsa2008.pdf
    num_hashes: usize,

    version: IndexSchemaVersion,

    // The seeding for hash function. For now we use CityHash64, that needs only one seed.
    seed: u64,
}

impl BloomFilter {
    /// Create a bloom filter instance with estimated number of items and expected false positive rate.
    pub fn with_rate(num_items: u64, false_positive_rate: f64, seed: u64) -> Self {
        let num_bits = Self::optimal_num_bits(num_items, false_positive_rate);
        let num_hashes = Self::optimal_num_hashes(num_items, num_bits as u64);

        Self::with_size(num_bits, num_hashes, seed)
    }

    /// Create a bloom filter instance with estimated number of items, expected false positive rate,
    /// and maximum number of bits.
    pub fn with_rate_and_max_bits(
        num_items: u64,
        false_positive_rate: f64,
        max_num_bits: usize,
        seed: u64,
    ) -> Self {
        let mut num_bits = Self::optimal_num_bits(num_items, false_positive_rate);
        let num_hashes = Self::optimal_num_hashes(num_items, num_bits as u64);

        num_bits = std::cmp::min(num_bits, max_num_bits);

        Self::with_size(num_bits, num_hashes, seed)
    }

    /// Create a bloom filter instance with specified bitmap length and number of hashes
    pub fn with_size(num_bits: usize, num_hashes: usize, seed: u64) -> Self {
        Self {
            container: BitVec::from_elem(num_bits, false),
            seed,
            num_hashes,
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
        self.container.len()
    }

    /// Returns the number of hashes of the bloom filter.
    pub fn num_hashes(&self) -> usize {
        self.num_hashes
    }

    /// Returns the reference of bitmap container
    pub fn bitmap(&self) -> &BitVec<u32> {
        &self.container
    }

    /// Returns true if the bitmap contains the other's bitmap.
    /// Notice: this function doesn't do any schema check, but only bits comparison.
    pub fn contains(&self, other: &BloomFilter) -> bool {
        if self.num_bits() != other.num_bits() {
            return false;
        }

        let mut copy = self.container.clone();
        !copy.or(other.bitmap())
    }

    /// Clone and return an empty bloom filter with same number of bits, seeds and hashes.
    /// All bits are set to false/zero, e.g. the hashed bits are not cloned.
    #[must_use]
    pub fn clone_empty(&self) -> Self {
        Self::with_size(self.num_bits(), self.num_hashes(), self.seed)
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
    pub fn is_supported_type(data_type: &DataTypePtr) -> bool {
        // we support nullable column but Nulls are not added into the bloom filter.
        let inner_type = remove_nullable(data_type);
        let data_type_id = inner_type.data_type_id();
        matches!(
            data_type_id,
            TypeID::UInt8
                | TypeID::UInt16
                | TypeID::UInt32
                | TypeID::UInt64
                | TypeID::Int8
                | TypeID::Int16
                | TypeID::Int32
                | TypeID::Int64
                | TypeID::Float32
                | TypeID::Float64
                | TypeID::Date
                | TypeID::Timestamp
                | TypeID::Interval
                | TypeID::String
        )
    }

    #[inline(always)]
    fn compute_hash_bit_pos(&self, index: usize, h1: Wrapping<u64>, h2: Wrapping<u64>) -> usize {
        let i = Wrapping(index as u64);
        let hash = (h1 + i * h2 + i * i).0;
        (hash % self.container.len() as u64) as usize
    }

    #[inline(always)]
    // Set bits for bloom filter, ported from Clickhouse.
    // https://github.com/ClickHouse/ClickHouse/blob/1bf375e2b761db5b99b0f403b90c412a530f4d5c/src/Interpreters/BloomFilter.cpp#L67
    fn set_bits(&mut self, hash1: &u64, hash2: &u64) {
        let h1 = Wrapping(*hash1);
        let h2 = Wrapping(*hash2);

        for i in 0..self.num_hashes {
            let bit_pos = self.compute_hash_bit_pos(i, h1, h2);
            self.container.set(bit_pos, true);
        }
    }

    #[inline(always)]
    fn generate_seed_2(&self) -> u64 {
        let seed1 = std::num::Wrapping(self.seed);
        let gen1 = std::num::Wrapping(BLOOM_FILTER_SEED_GEN_A);
        let gen2 = std::num::Wrapping(BLOOM_FILTER_SEED_GEN_B);
        let seed2 = seed1 * gen1 + gen2;
        seed2.0
    }

    fn compute_column_city_hash(
        seed: u64,
        column: &ColumnRef,
        ctx: Arc<QueryContext>,
    ) -> Result<ColumnRef> {
        let input_column = "input"; // create a dummy column name
        let input_field = DataField::new(input_column, column.data_type());
        let input_schema = Arc::new(DataSchema::new(vec![input_field]));
        let args = vec![
            Expression::Column(String::from(input_column)),
            Expression::create_literal_with_type(DataValue::UInt64(seed), UInt64Type::arc()),
        ];

        let output_column = "output";
        let output_data_type = if column.is_nullable() {
            wrap_nullable(&UInt64Type::arc())
        } else {
            UInt64Type::arc()
        };
        let output_field = DataField::new(output_column, output_data_type);
        let output_schema = DataSchemaRefExt::create(vec![output_field]);

        let expr: Expression = Expression::Alias(
            String::from(output_column),
            Box::new(Expression::create_scalar_function("city64WithSeed", args)),
        );
        let expr_executor = ExpressionExecutor::try_create(
            "calculate cityhash64",
            input_schema.clone(),
            output_schema,
            vec![expr],
            true,
            ctx,
        )?;

        let data_block = DataBlock::create(input_schema, vec![column.clone()]);
        let executor = Arc::new(expr_executor);
        let output = executor.execute(&data_block)?;
        Ok(output.column(0).clone())
    }

    fn compute_column_double_hashes(
        &self,
        column: &ColumnRef,
        ctx: Arc<QueryContext>,
    ) -> Result<(ColumnRef, ColumnRef)> {
        let hash1_column: ColumnRef =
            Self::compute_column_city_hash(self.seed, column, ctx.clone())?;
        let seed2 = self.generate_seed_2();
        let hash2_column: ColumnRef = Self::compute_column_city_hash(seed2, column, ctx)?;

        Ok((hash1_column, hash2_column))
    }

    /// Add the column data into bloom filter, Nulls are skipped and not added.
    ///
    /// The design of skipping Nulls is arguably correct. For now we do the same as databricks.
    /// See the design of databricks https://docs.databricks.com/delta/optimizations/bloom-filters.html
    pub fn add(&mut self, column: &ColumnRef, ctx: Arc<QueryContext>) -> Result<()> {
        if !Self::is_supported_type(&column.data_type()) {
            return Err(ErrorCode::BadArguments(format!(
                "Unsupported data type: {} ",
                column.data_type_id()
            )));
        }

        let (is_all_null, validity) = column.validity();
        if is_all_null {
            return Ok(());
        }

        let (hash1_column, hash2_column) = self.compute_column_double_hashes(column, ctx)?;

        // If the input is not nullable, we say all hashed values should be valid and we put them into bloom filter without checking validity.
        if !column.is_nullable() {
            let column1: &UInt64Column = Series::check_get(&hash1_column)?;
            let column2: &UInt64Column = Series::check_get(&hash2_column)?;

            column1.iter().zip(column2.iter()).for_each(|(h1, h2)| {
                self.set_bits(h1, h2);
            });
        } else {
            let bitmap = validity.unwrap();

            let column1 = Series::remove_nullable(&hash1_column);
            let column1: &UInt64Column = Series::check_get(&column1)?;

            let column2 = Series::remove_nullable(&hash2_column);
            let column2: &UInt64Column = Series::check_get(&column2)?;

            bitmap
                .into_iter()
                .zip(column1.iter())
                .zip(column2.iter())
                .for_each(|((valid, h1), h2)| {
                    if valid {
                        self.set_bits(h1, h2);
                    }
                });
        }
        Ok(())
    }

    fn compute_data_value_double_hashes(
        &self,
        data_value: DataValue,
        data_type: DataTypePtr,
        ctx: Arc<QueryContext>,
    ) -> Result<(u64, u64)> {
        let col = data_value.as_const_column(&data_type, 1)?;

        let hash1_column = Self::compute_column_city_hash(self.seed, &col, ctx.clone())?;
        let h1 = hash1_column.get_u64(0)?;

        let seed2 = self.generate_seed_2();
        let hash2_column = Self::compute_column_city_hash(seed2, &col, ctx)?;
        let h2 = hash2_column.get_u64(0)?;

        Ok((h1, h2))
    }

    /// Check the existence of the data. The data value should not be Null.
    /// Use BloomFilter::is_supported_type to check before using this method.
    ///
    /// The data_type is necessary here because hash(12_u8) is not always the same with hash(12_u64)
    /// for some hash functions.
    ///
    /// The data_value doesn't carry enough type information, which actually makes
    /// sense. For example, the where clause '<expr> = 12', value 12 is hard to tell the type unless we have
    /// type of <expr>.
    ///
    /// Notice: false positive may exist, e.g. return true doesn't guarantee the value exists.
    /// But returning false guarantees that it never ever showed up.
    ///
    /// Example:
    /// ```
    ///     let not_exist = BloomFilter::is_supported_type(data_type) && !bloom.find(data_value, data_type)?;
    ///
    /// ```
    pub fn find(&self, val: DataValue, typ: DataTypePtr, ctx: Arc<QueryContext>) -> Result<bool> {
        if !Self::is_supported_type(&typ) {
            return Err(ErrorCode::BadArguments(format!(
                "Unsupported data type: {:?} ",
                typ
            )));
        }

        if val.is_null() {
            return Err(ErrorCode::BadArguments("Null value is not supported"));
        }

        let (hash1, hash2) = self.compute_data_value_double_hashes(val, typ, ctx)?;
        let h1 = Wrapping(hash1);
        let h2 = Wrapping(hash2);
        for i in 0..self.num_hashes {
            let bit_pos = self.compute_hash_bit_pos(i, h1, h2);

            // If any bit is not 1 in bloom filter, it means the data never ever showed up before.
            // The 'unwrap' should be fine because 'bit_pos' was mod by container.len().
            let is_bit_set = self.container.get(bit_pos).unwrap();
            if !is_bit_set {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Serialize the bloom filter to byte vector.
    pub fn to_vec(&self) -> Result<Vec<u8>> {
        match bincode::serialize(self) {
            Ok(v) => Ok(v),
            Err(e) => Err(ErrorCode::StorageOther(format!(
                "bincode serialization error: {} ",
                e
            ))),
        }
    }

    /// Deserialize from a byte slice and return a bloom filter.
    pub fn from_vec(bytes: &[u8]) -> Result<Self> {
        match bincode::deserialize::<BloomFilter>(bytes) {
            Ok(bloom_filter) => Ok(bloom_filter),
            Err(e) => Err(ErrorCode::StorageOther(format!(
                "bincode deserialization error: {} ",
                e
            ))),
        }
    }
}
