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

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_legacy_expression::LegacyExpression;

use crate::bloom::Bloom;
use crate::bloom::XorBloom;
use crate::SupportedType;

/// BloomFilterIndex represents multiple  bloom filters (per column) in data block.
///
/// By default we create bloom filter per column for a parquet data file. For columns whose data_type
/// are not applicable for a bloom filter, we skip the creation.
/// That is to say, it is legal to have a BloomFilterBlock with zero columns.
///
/// For example, for the source data block as follows:
/// ```
///         +---name--+--age--+
///         | "Alice" |  20   |
///         | "Bob"   |  30   |
///         +---------+-------+
/// ```
/// We will create bloom filter table as follows:
/// ```
///         +---Bloom(name)--+--Bloom(age)--+
///         |  123456789abcd |  ac2345bcd   |
///         +----------------+--------------+
/// ```
pub struct BloomFilterIndexer {
    // The schema of the source table/block, which the bloom filter work for.
    pub source_schema: DataSchemaRef,

    // The schema of the bloom filter block
    pub bloom_schema: DataSchemaRef,

    // The bloom block contains bloom filters;
    pub bloom_block: DataBlock,
}

/// BloomFilterExprEvalResult represents the evaluation result of an expression by bloom filter.
///
/// For example, expression of 'age = 12' should return false is the bloom filter are sure
/// of the nonexistent of value '12' in column 'age'. Otherwise should return 'Unknown'.
///
/// If the column is not applicable for bloom filter, like TypeID::struct, NotApplicable is used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BloomFilterExprEvalResult {
    False,
    Unknown,
    NotApplicable,
}

impl BloomFilterIndexer {
    /// For every applicable column, we will create a bloom filter.
    /// The bloom filter will be stored with field name 'Bloom(column_name)'
    pub fn to_bloom_column_name(column_name: &str) -> String {
        format!("Bloom({})", column_name)
    }
    pub fn to_bloom_schema(data_schema: &DataSchema) -> DataSchema {
        let mut bloom_fields = vec![];
        let fields = data_schema.fields();
        for field in fields.iter() {
            if XorBloom::is_supported_type(field.data_type()) {
                // create field for applicable ones
                let bloom_column_name = Self::to_bloom_column_name(field.name());
                let bloom_field = DataField::new(&bloom_column_name, Vu8::to_data_type());
                bloom_fields.push(bloom_field);
            }
        }

        DataSchema::new(bloom_fields)
    }

    /// Load a bloom filter directly from the source table's schema and the corresponding bloom parquet file.
    #[tracing::instrument(level = "debug", skip_all)]
    pub fn from_bloom_block(
        source_table_schema: DataSchemaRef,
        bloom_block: DataBlock,
    ) -> Result<Self> {
        Ok(Self {
            source_schema: source_table_schema,
            bloom_schema: bloom_block.schema().clone(),
            bloom_block,
        })
    }

    /// Create a bloom filter block from source data.
    ///
    /// All input blocks should be belong to a Parquet file, e.g. the block array represents the parquet file in memory.

    pub fn try_create(blocks: &[&DataBlock]) -> Result<Self> {
        if blocks.is_empty() {
            return Err(ErrorCode::BadArguments("data blocks is empty"));
        }

        let source_schema = blocks[0].schema().clone();
        // let _total_num_rows = blocks.iter().map(|block| block.num_rows() as u64).sum();
        let mut bloom_columns = vec![];

        let fields = source_schema.fields();
        for (i, field) in fields.iter().enumerate() {
            if XorBloom::is_supported_type(field.data_type()) {
                // create bloom filter per column
                let mut bloom_filter = XorBloom::create();

                // ingest the same column data from all blocks
                for block in blocks.iter() {
                    let col = block.column(i);
                    bloom_filter.add_keys(&col.to_values());
                }

                // finalize the filter
                bloom_filter.build()?;

                // create bloom filter column
                let serialized_bytes = bloom_filter.to_bytes()?;
                let bloom_value = DataValue::String(serialized_bytes);
                let bloom_column: ColumnRef =
                    bloom_value.as_const_column(&StringType::new_impl(), 1)?;
                bloom_columns.push(bloom_column);
            }
        }

        let bloom_schema = Arc::new(Self::to_bloom_schema(source_schema.as_ref()));
        let bloom_block = DataBlock::create(bloom_schema.clone(), bloom_columns);
        Ok(Self {
            source_schema,
            bloom_schema,
            bloom_block,
        })
    }

    pub fn find(
        &self,
        column_name: &str,
        target: DataValue,
        typ: &DataTypeImpl,
    ) -> Result<BloomFilterExprEvalResult> {
        let bloom_column = Self::to_bloom_column_name(column_name);
        if !self.bloom_block.schema().has_field(&bloom_column)
            || !XorBloom::is_supported_type(typ)
            || target.is_null()
        {
            // The column doesn't have bloom filter bitmap
            return Ok(BloomFilterExprEvalResult::NotApplicable);
        }

        let bloom_bytes = self.bloom_block.first(&bloom_column)?.as_string()?;
        let (bloom_filter, _size) = XorBloom::from_bytes(&bloom_bytes)?;
        if bloom_filter.contains(&target) {
            Ok(BloomFilterExprEvalResult::Unknown)
        } else {
            Ok(BloomFilterExprEvalResult::False)
        }
    }

    /// Returns false when the expression must be false, otherwise true.
    /// The 'true' doesn't really mean the expression is true, but 'maybe true'.
    /// That is to say, you still need the load all data and run the execution.

    pub fn maybe_true(&self, expr: &LegacyExpression) -> Result<bool> {
        Ok(self.eval(expr)? != BloomFilterExprEvalResult::False)
    }

    /// Apply the predicate expression, return the result.
    /// If we are sure of skipping the scan, return false, e.g. the expression must be false.
    /// This happens when the data doesn't show up in bloom filter.
    ///
    /// Otherwise return either Unknown or NotApplicable.
    #[tracing::instrument(level = "debug", name = "bloom_filter_index_eval", skip_all)]
    pub fn eval(&self, expr: &LegacyExpression) -> Result<BloomFilterExprEvalResult> {
        // TODO: support multiple columns and other ops like 'in' ...
        match expr {
            LegacyExpression::BinaryExpression { left, op, right } => {
                match op.to_lowercase().as_str() {
                    "=" => self.eval_equivalent_expression(left, right),
                    "and" => self.eval_logical_and(left, right),
                    "or" => self.eval_logical_or(left, right),
                    _ => Ok(BloomFilterExprEvalResult::NotApplicable),
                }
            }
            _ => Ok(BloomFilterExprEvalResult::NotApplicable),
        }
    }

    // Evaluate the equivalent expression like "name='Alice'"
    fn eval_equivalent_expression(
        &self,
        left: &LegacyExpression,
        right: &LegacyExpression,
    ) -> Result<BloomFilterExprEvalResult> {
        let schema: &DataSchemaRef = &self.source_schema;

        // For now only support single column like "name = 'Alice'"
        match (left, right) {
            // match the expression of 'column_name = literal constant'
            (LegacyExpression::Column(column), LegacyExpression::Literal { value, .. })
            | (LegacyExpression::Literal { value, .. }, LegacyExpression::Column(column)) => {
                // find the corresponding column from source table
                match schema.column_with_name(column) {
                    Some((_index, data_field)) => {
                        let data_type = data_field.data_type();

                        // check if cast needed
                        let value = if &value.data_type() != data_type {
                            let col = value.as_const_column(data_type, 1)?;
                            col.get_checked(0)?
                        } else {
                            value.clone()
                        };
                        self.find(column, value, data_type)
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
        left: &LegacyExpression,
        right: &LegacyExpression,
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
        left: &LegacyExpression,
        right: &LegacyExpression,
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
    pub fn try_get_bloom(&self, column_name: &str) -> Result<XorBloom> {
        let bloom_column = Self::to_bloom_column_name(column_name);
        let val = self.bloom_block.first(&bloom_column)?;
        let bloom_bytes = val.as_string()?;
        let (bloom_filter, _size) = XorBloom::from_bytes(bloom_bytes.as_ref())?;
        Ok(bloom_filter)
    }
}
