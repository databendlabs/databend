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

//! Streaming single-row-group Parquet writers.
//!
//! Built on the low-level `parquet` API (exposed by the datafuse-extras arrow-rs fork) so the
//! serialized payload is a list of `Bytes` ([`SerializedParquet::payload`]) that the fuse write
//! path forwards straight to opendal with no consolidation copy. Two writers are provided:
//!
//! - [`BulkBlockParquetWriter`] (leaf-oriented, low-level — see the `bulk` module): drive it
//!   leaf by leaf; each `write` encodes an `ArrowLeafColumn` straight into the open leaf's page
//!   writer, flushing pages to the sink as they fill — no per-column chunk buffer. The
//!   vertical-merge path drives this directly, leaf-for-leaf against the reader.
//! - [`BlockParquetWriter`] (row-oriented, high-level — see the `block` module): receives
//!   `DataBlock`s and encodes+compresses each immediately into one column writer per leaf, so
//!   buffered memory is the compressed pages rather than the raw blocks; at `finish` it
//!   assembles the file from the closed column chunks with no data copy.
//!
//! Both are restricted to a single row group.
//!
//! NOTE: the leaf-value dispatch and the `ArrowColumnChunk::into_parts` accessor are reused from
//! arrow-rs via the fork's public API. Keep in sync with the pinned fork rev.

mod block;
mod bulk;

use bytes::Bytes;
use parquet::file::metadata::ParquetMetaData;

pub use self::block::BlockParquetWriter;
pub use self::bulk::BulkBlockParquetWriter;
pub use self::bulk::LeafColumnWriter;

/// Result of finishing a [`BulkBlockParquetWriter`] / [`BlockParquetWriter`]: the serialized
/// single-row-group Parquet bytes plus the file metadata.
///
/// `payload` is a list of chunks rather than one `Vec<u8>`: the fuse write path forwards it
/// straight to opendal (`Buffer::from(Vec<Bytes>)`) with no consolidation copy. Callers that
/// genuinely need one contiguous buffer should concat the chunks themselves (e.g.
/// `payload.concat()`) rather than relying on a helper, so the copy stays explicit at the call
/// site.
pub struct SerializedParquet {
    pub payload: Vec<Bytes>,
    pub metadata: ParquetMetaData,
}

impl SerializedParquet {
    /// Total byte length of the serialized parquet across all chunks.
    pub fn len(&self) -> usize {
        self.payload.iter().map(|c| c.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.payload.iter().all(|c| c.is_empty())
    }
}

/// Shared test fixtures for both writers' unit tests.
#[cfg(test)]
pub(crate) mod test_util {
    use std::sync::Arc;

    use arrow_schema::Schema;
    use bytes::Bytes;
    use databend_common_expression::Column;
    use databend_common_expression::DataBlock;
    use databend_common_expression::DataSchema;
    use databend_common_expression::FromData;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::types::ArgType;
    use databend_common_expression::types::BinaryType;
    use databend_common_expression::types::BooleanType;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::DateType;
    use databend_common_expression::types::Decimal128Type;
    use databend_common_expression::types::Float64Type;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::Int64Type;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::StringType;
    use databend_common_expression::types::TimestampType;
    use databend_common_expression::types::array::ArrayColumn;
    use databend_common_expression::types::array::ArrayColumnBuilder;
    use databend_common_expression::types::decimal::DecimalDataType;
    use databend_common_expression::types::decimal::DecimalSize;
    use databend_storages_common_table_meta::table::TableCompression;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::properties::WriterPropertiesPtr;

    use crate::build_parquet_writer_properties;

    // A wide schema covering byte-array (String), primitive (Int32/Int64), nullable, and
    // nested list (Array) leaves — exercising both encoder kinds and nested leaf ordering.
    pub fn sample_schema() -> TableSchema {
        TableSchema::new(vec![
            TableField::new("s", TableDataType::String),
            TableField::new("i32", TableDataType::Number(NumberDataType::Int32)),
            TableField::new(
                "i64_null",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::Int64))),
            ),
            TableField::new(
                "arr",
                TableDataType::Array(Box::new(TableDataType::Number(NumberDataType::Int32))),
            ),
            TableField::new(
                "s_null",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
        ])
    }

    pub fn sample_block() -> DataBlock {
        let mut array_builder = ArrayColumnBuilder::<Int32Type>::with_capacity(3, 3, &[]);
        {
            let mut arrays = array_builder.as_mut();
            arrays.put_item(1);
            arrays.put_item(2);
            arrays.commit_row();
            arrays.put_item(3);
            arrays.commit_row();
            arrays.push_default();
        }
        let array_column = Column::Array(Box::new(
            array_builder
                .build()
                .upcast(&DataType::Array(Int32Type::data_type().into())),
        ));

        DataBlock::new_from_columns(vec![
            StringType::from_data(vec!["alpha", "beta", "gamma"]),
            Int32Type::from_data(vec![10, 20, 30]),
            Int64Type::from_opt_data(vec![Some(100), None, Some(300)]),
            array_column,
            StringType::from_opt_data(vec![Some("x"), None, Some("z")]),
        ])
    }

    // A wide schema covering many leaf types and both encoder kinds: bool, float, decimal,
    // date, timestamp, binary (byte-array), string, nested array, map, tuple (struct).
    pub fn wide_schema() -> TableSchema {
        TableSchema::new(vec![
            TableField::new("b", TableDataType::Boolean),
            TableField::new("f64", TableDataType::Number(NumberDataType::Float64)),
            TableField::new(
                "dec",
                TableDataType::Decimal(DecimalDataType::Decimal128(
                    DecimalSize::new(20, 4).unwrap(),
                )),
            ),
            TableField::new("date", TableDataType::Date),
            TableField::new("ts", TableDataType::Timestamp),
            TableField::new("bin", TableDataType::Binary),
            TableField::new(
                "map",
                TableDataType::Map(Box::new(TableDataType::Tuple {
                    fields_name: vec!["key".to_string(), "value".to_string()],
                    fields_type: vec![TableDataType::String, TableDataType::String],
                })),
            ),
            TableField::new("tup", TableDataType::Tuple {
                fields_name: vec!["a".to_string(), "b".to_string()],
                fields_type: vec![
                    TableDataType::Number(NumberDataType::Int32),
                    TableDataType::String,
                ],
            }),
        ])
    }

    pub fn wide_block() -> DataBlock {
        let map = Column::Map(Box::new(ArrayColumn::new(
            Column::Tuple(vec![
                StringType::from_data(vec!["k1", "k2", "k3"]),
                StringType::from_data(vec!["v1", "v2", "v3"]),
            ]),
            vec![0_u64, 1, 2, 3].into(),
        )));
        let tup = Column::Tuple(vec![
            Int32Type::from_data(vec![1, 2, 3]),
            StringType::from_data(vec!["t1", "t2", "t3"]),
        ]);

        DataBlock::new_from_columns(vec![
            BooleanType::from_data(vec![true, false, true]),
            Float64Type::from_data(vec![1.5, 2.5, 3.5]),
            Decimal128Type::from_data_with_size(
                vec![12345i128, -67890, 0],
                Some(DecimalSize::new(20, 4).unwrap()),
            ),
            DateType::from_data(vec![18000, 18001, 18002]),
            TimestampType::from_data(vec![1_600_000_000_000_000, 1_600_000_001_000_000, 0]),
            BinaryType::from_data(vec![
                b"\x00\x01".as_slice(),
                b"ab".as_slice(),
                b"".as_slice(),
            ]),
            map,
            tup,
        ])
    }

    pub fn props(schema: &TableSchema) -> WriterPropertiesPtr {
        Arc::new(build_parquet_writer_properties(
            TableCompression::Zstd,
            true,
            None::<&databend_storages_common_table_meta::meta::StatisticsOfColumns>,
            None,
            0,
            schema,
            None,
            None,
        ))
    }

    pub fn read_back(bytes: Vec<u8>) -> (Vec<DataBlock>, usize) {
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
            .unwrap()
            .with_batch_size(usize::MAX);
        let num_row_groups = reader.metadata().num_row_groups();
        let mut reader = reader.build().unwrap();
        let mut blocks = Vec::new();
        for batch in reader.by_ref() {
            let batch = batch.unwrap();
            let table_schema: TableSchema = batch.schema().as_ref().try_into().unwrap();
            let data_schema = DataSchema::from(&table_schema);
            blocks.push(DataBlock::from_record_batch(&data_schema, &batch).unwrap());
        }
        (blocks, num_row_groups)
    }

    pub fn assert_blocks_eq(expected: &DataBlock, actual: &DataBlock) {
        assert_eq!(expected.num_rows(), actual.num_rows());
        assert_eq!(expected.num_columns(), actual.num_columns());
        for (e, a) in expected.columns().iter().zip(actual.columns()) {
            assert_eq!(e.to_column(), a.to_column());
        }
    }

    /// Drive the low-level leaf API directly: for each top-level field, leaf-expand its column
    /// from every block, then write each leaf (fed all fragments) and close it. Shared by the
    /// low-level writer's tests and the cross-writer equality test.
    pub fn bulk_write_blocks(
        arrow_schema: Arc<Schema>,
        props: WriterPropertiesPtr,
        blocks: &[DataBlock],
    ) -> (Vec<u8>, parquet::file::metadata::ParquetMetaData) {
        use arrow_array::ArrayRef;
        use parquet::arrow::arrow_writer::compute_leaves;

        use super::BulkBlockParquetWriter;
        use super::SerializedParquet;

        let mut writer = BulkBlockParquetWriter::new(arrow_schema.clone(), props).unwrap();
        for (field_idx, field) in arrow_schema.fields().iter().enumerate() {
            let leaves_per_fragment: Vec<_> = blocks
                .iter()
                .map(|block| {
                    let array = ArrayRef::from(&block.get_by_offset(field_idx).to_column());
                    compute_leaves(field, &array).unwrap()
                })
                .collect();
            let num_leaves = leaves_per_fragment[0].len();
            for leaf_idx in 0..num_leaves {
                let mut leaf = writer.next_leaf().unwrap();
                for fragment in &leaves_per_fragment {
                    leaf.write(&fragment[leaf_idx]).unwrap();
                }
                leaf.close().unwrap();
            }
        }
        let SerializedParquet { payload, metadata } = writer.finish().unwrap();
        (payload.concat(), metadata)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Schema;
    use databend_common_expression::DataBlock;

    use super::test_util::assert_blocks_eq;
    use super::test_util::bulk_write_blocks;
    use super::test_util::props;
    use super::test_util::read_back;
    use super::test_util::sample_block;
    use super::test_util::sample_schema;
    use super::*;

    // Both writers must produce semantically identical files: same decoded data when read back.
    #[test]
    fn test_both_writers_produce_equal_data() {
        let schema = sample_schema();
        let arrow_schema = Arc::new(Schema::from(&schema));

        let mut buffered = BlockParquetWriter::new(arrow_schema.clone(), props(&schema));
        buffered.write_block(sample_block()).unwrap();
        buffered.write_block(sample_block()).unwrap();
        let buffered_bytes = buffered.finish().unwrap().payload.concat();

        let blocks = [sample_block(), sample_block()];
        let (streaming_bytes, _) = bulk_write_blocks(arrow_schema, props(&schema), &blocks);

        let (buffered_blocks, _) = read_back(buffered_bytes);
        let (streaming_blocks, _) = read_back(streaming_bytes);
        let buffered_data = DataBlock::concat(&buffered_blocks).unwrap();
        let streaming_data = DataBlock::concat(&streaming_blocks).unwrap();
        assert_blocks_eq(&buffered_data, &streaming_data);
    }

    // Low-level leaf API roundtrips on its own.
    #[test]
    fn test_bulk_block_parquet_writer_roundtrip() {
        let schema = sample_schema();
        let arrow_schema = Arc::new(Schema::from(&schema));
        let blocks = [sample_block(), sample_block()];

        let (bytes, meta) = bulk_write_blocks(arrow_schema, props(&schema), &blocks);

        assert_eq!(meta.num_row_groups(), 1);
        let (read_blocks, num_rg) = read_back(bytes);
        assert_eq!(num_rg, 1);
        let got = DataBlock::concat(&read_blocks).unwrap();
        let expected = DataBlock::concat(&[sample_block(), sample_block()]).unwrap();
        assert_blocks_eq(&expected, &got);
    }
}
