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

//! High-level, row-oriented single-row-group Parquet writer.
//!
//! [`BlockParquetWriter`] receives data row-wise (block by block) but parquet needs columns
//! written sequentially, so it holds one [`ArrowColumnWriter`] per leaf and encodes+compresses
//! each block immediately into them. Buffered memory is therefore the compressed pages, not the
//! raw blocks. At `finish` each column closes into an already-encoded `Vec<Bytes>` which is
//! pushed into the payload with no copy (an `Arc` move), its page offsets remapped to absolute
//! file offsets, and the footer is appended — producing the `Vec<Bytes>` payload the fuse write
//! path forwards straight to opendal.

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::new_empty_array;
use arrow_schema::Schema;
use bytes::Bytes;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use parquet::arrow::ArrowSchemaConverter;
use parquet::arrow::arrow_writer::ArrowColumnWriter;
use parquet::arrow::arrow_writer::compute_leaves;
#[allow(deprecated)]
use parquet::arrow::arrow_writer::get_column_writers;
use parquet::file::metadata::ColumnChunkMetaData;
use parquet::file::metadata::FileMetaData;
use parquet::file::metadata::ParquetMetaDataBuilder;
use parquet::file::metadata::ParquetMetaDataWriter;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::properties::WriterPropertiesPtr;
use parquet::schema::types::SchemaDescPtr;

use super::BulkBlockParquetWriter;
use super::SerializedParquet;

/// Parquet file magic bytes (`PAR1`), written at the start of the file and before the footer.
const PARQUET_MAGIC: &[u8; 4] = b"PAR1";

/// High-level row-oriented writer: encodes+compresses each incoming `DataBlock` immediately
/// into per-leaf [`ArrowColumnWriter`]s, then at [`Self::finish`] assembles the single-row-group
/// file from the closed column chunks. Used by the insert / fuse `StreamBlockBuilder` path and
/// `blocks_to_parquet*` where blocks arrive incrementally or as a batch.
pub struct BlockParquetWriter {
    arrow_schema: Arc<Schema>,
    props: WriterPropertiesPtr,
    /// Lazily created on first `write_block` (one writer per parquet leaf column). Each
    /// `write_block` encodes+compresses immediately into these, so peak buffered memory is the
    /// compressed pages, not the raw blocks.
    state: Option<ColumnWriterState>,
    num_rows: usize,
}

struct ColumnWriterState {
    /// One per parquet leaf, in leaf order. Built from the arrow schema + props.
    column_writers: Vec<ArrowColumnWriter>,
    /// Parquet schema descriptor, kept for footer assembly at `finish`.
    parquet_schema: SchemaDescPtr,
}

impl BlockParquetWriter {
    pub fn new(arrow_schema: Arc<Schema>, props: WriterPropertiesPtr) -> Self {
        Self {
            arrow_schema,
            props,
            state: None,
            num_rows: 0,
        }
    }

    fn ensure_state(&mut self) -> Result<&mut ColumnWriterState> {
        if self.state.is_none() {
            let parquet_schema = ArrowSchemaConverter::new()
                .with_coerce_types(self.props.coerce_types())
                .convert(&self.arrow_schema)?;
            #[allow(deprecated)]
            let column_writers =
                get_column_writers(&parquet_schema, &self.props, &self.arrow_schema)?;
            self.state = Some(ColumnWriterState {
                column_writers,
                parquet_schema: Arc::new(parquet_schema),
            });
        }
        Ok(self.state.as_mut().unwrap())
    }

    /// Encode and compress the block's columns immediately into the per-leaf column writers.
    /// Returns an error if leaf expansion or encoding fails.
    pub fn write_block(&mut self, block: DataBlock) -> Result<()> {
        if block.is_empty() {
            return Ok(());
        }

        let num_rows = block.num_rows();
        let arrow_schema = self.arrow_schema.clone();
        let state = self.ensure_state()?;
        let mut leaf = state.column_writers.iter_mut();

        for (field_idx, field) in arrow_schema.fields().iter().enumerate() {
            let array = ArrayRef::from(&block.get_by_offset(field_idx).to_column());
            for leaf_column in compute_leaves(field, &array)? {
                let writer = leaf.next().ok_or_else(|| {
                    ErrorCode::Internal("more leaf columns than the schema declares")
                })?;
                writer.write(&leaf_column)?;
            }
        }
        self.num_rows += num_rows;
        Ok(())
    }

    pub fn write_blocks(&mut self, blocks: impl IntoIterator<Item = DataBlock>) -> Result<()> {
        for block in blocks {
            self.write_block(block)?;
        }
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.num_rows == 0
    }

    /// Estimated size of the compressed pages buffered so far, summed across leaves. Lets the
    /// caller make flush decisions on real compressed size rather than raw block size.
    pub fn compressed_size(&self) -> usize {
        self.state.as_ref().map_or(0, |state| {
            state
                .column_writers
                .iter()
                .map(|w| w.get_estimated_total_bytes())
                .sum()
        })
    }

    /// Close all column writers and assemble the single-row-group parquet file directly as a
    /// list of `Bytes`: the `PAR1` magic, each column chunk's already-encoded `Bytes` pushed
    /// with no copy (its page offsets remapped to absolute file offsets), then the footer.
    /// Returns the payload chunks + metadata.
    pub fn finish(self) -> Result<SerializedParquet> {
        let BlockParquetWriter {
            arrow_schema,
            props,
            state,
            num_rows,
        } = self;

        // No data written: fall back to the leaf writer to emit a valid empty-schema file.
        let Some(state) = state else {
            let mut writer = BulkBlockParquetWriter::new(arrow_schema.clone(), props)?;
            for field in arrow_schema.fields() {
                let leaves = compute_leaves(field, &new_empty_array(field.data_type()))?;
                for leaf_column in &leaves {
                    let mut leaf = writer.next_leaf()?;
                    leaf.write(leaf_column)?;
                    leaf.close()?;
                }
            }
            return writer.finish();
        };

        assemble_parquet(props, state, num_rows)
    }
}

/// Assemble a single-row-group parquet file from closed column chunks, pushing their compressed
/// bytes into the output with no copy and remapping page offsets — mirroring the fork's
/// `SerializedRowGroupWriter::append_column` offset logic, but writing owned `Bytes` directly.
fn assemble_parquet(
    props: WriterPropertiesPtr,
    state: ColumnWriterState,
    num_rows: usize,
) -> Result<SerializedParquet> {
    let ColumnWriterState {
        column_writers,
        parquet_schema,
    } = state;
    let schema_descr = parquet_schema;

    // The payload is assembled directly as a list of `Bytes`: the magic, then each column
    // chunk's already-encoded compressed `Bytes` (pushed with no copy — an `Arc` move), then
    // the footer. `file_offset` tracks the absolute write position for page-offset remapping.
    let mut payload: Vec<Bytes> = Vec::with_capacity(column_writers.len() + 2);
    payload.push(Bytes::from_static(PARQUET_MAGIC));
    let mut file_offset = PARQUET_MAGIC.len() as i64;

    let mut column_chunks = Vec::with_capacity(column_writers.len());
    let mut total_uncompressed: i64 = 0;
    for writer in column_writers {
        let (data, close) = writer.close()?.into_parts();
        let metadata = close.metadata;

        // A freshly closed chunk's offsets are relative to its own start (offset 0), so the
        // absolute file offset is simply the current write position.
        let write_offset = file_offset;
        let src_data_offset = metadata.data_page_offset();
        let src_dictionary_offset = metadata.dictionary_page_offset();
        let map_offset = |x: i64| x + write_offset;

        for chunk in data {
            file_offset += chunk.len() as i64;
            if !chunk.is_empty() {
                payload.push(chunk);
            }
        }

        total_uncompressed += metadata.uncompressed_size();
        let mut builder = ColumnChunkMetaData::builder(metadata.column_descr_ptr())
            .set_compression(metadata.compression())
            .set_encodings_mask(*metadata.encodings_mask())
            .set_total_compressed_size(metadata.compressed_size())
            .set_total_uncompressed_size(metadata.uncompressed_size())
            .set_num_values(metadata.num_values())
            .set_data_page_offset(map_offset(src_data_offset))
            .set_dictionary_page_offset(src_dictionary_offset.map(map_offset))
            .set_unencoded_byte_array_data_bytes(metadata.unencoded_byte_array_data_bytes());

        if let Some(rep_hist) = metadata.repetition_level_histogram() {
            builder = builder.set_repetition_level_histogram(Some(rep_hist.clone()));
        }

        if let Some(def_hist) = metadata.definition_level_histogram() {
            builder = builder.set_definition_level_histogram(Some(def_hist.clone()));
        }

        if let Some(statistics) = metadata.statistics() {
            builder = builder.set_statistics(statistics.clone());
        }

        if let Some(page_encoding_stats) = metadata.page_encoding_stats() {
            builder = builder.set_page_encoding_stats(page_encoding_stats.clone());
        }

        column_chunks.push(builder.build()?);
    }

    let row_group = RowGroupMetaData::builder(schema_descr.clone())
        .set_column_metadata(column_chunks)
        .set_total_byte_size(total_uncompressed)
        .set_num_rows(num_rows as i64)
        .set_ordinal(0)
        .build()?;

    let file_metadata = FileMetaData::new(
        props.writer_version().as_num(),
        num_rows as i64,
        Some(props.created_by().to_string()),
        props.key_value_metadata().cloned(),
        schema_descr.clone(),
        None,
    );

    let metadata = ParquetMetaDataBuilder::new(file_metadata)
        .set_row_groups(vec![row_group])
        .build();

    // Footer (thrift FileMetaData + 4-byte length + trailing PAR1) is a few KB; serialize it
    // into a small buffer and push as the final chunk. The column data above was zero-copy.
    let mut footer = Vec::new();
    ParquetMetaDataWriter::new(&mut footer, &metadata).finish()?;
    payload.push(Bytes::from(footer));

    Ok(SerializedParquet { payload, metadata })
}

#[cfg(test)]
mod tests {
    use databend_common_expression::DataBlock;
    use databend_common_expression::FromData;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::StringType;

    use super::*;
    use crate::parquet_writer::test_util::assert_blocks_eq;
    use crate::parquet_writer::test_util::props;
    use crate::parquet_writer::test_util::read_back;
    use crate::parquet_writer::test_util::sample_block;
    use crate::parquet_writer::test_util::sample_schema;
    use crate::parquet_writer::test_util::wide_block;
    use crate::parquet_writer::test_util::wide_schema;

    #[test]
    fn test_roundtrip_single_block() {
        let schema = sample_schema();
        let arrow_schema = Arc::new(Schema::from(&schema));
        let block = sample_block();

        let mut writer = BlockParquetWriter::new(arrow_schema, props(&schema));
        writer.write_block(block.clone()).unwrap();
        let serialized = writer.finish().unwrap();

        assert_eq!(serialized.metadata.num_row_groups(), 1);
        let (blocks, num_rg) = read_back(serialized.payload.concat());
        assert_eq!(num_rg, 1);
        let got = DataBlock::concat(&blocks).unwrap();
        assert_blocks_eq(&block, &got);
    }

    #[test]
    fn test_roundtrip_multi_block() {
        let schema = sample_schema();
        let arrow_schema = Arc::new(Schema::from(&schema));

        let mut writer = BlockParquetWriter::new(arrow_schema, props(&schema));
        writer.write_block(sample_block()).unwrap();
        writer.write_block(sample_block()).unwrap();
        writer.write_block(sample_block()).unwrap();
        let serialized = writer.finish().unwrap();

        assert_eq!(serialized.metadata.num_row_groups(), 1);
        let (blocks, num_rg) = read_back(serialized.payload.concat());
        assert_eq!(num_rg, 1);
        let got = DataBlock::concat(&blocks).unwrap();

        let expected =
            DataBlock::concat(&[sample_block(), sample_block(), sample_block()]).unwrap();
        assert_eq!(got.num_rows(), expected.num_rows());
        assert_blocks_eq(&expected, &got);
    }

    #[test]
    fn test_dictionary_string_uses_byte_array_path() {
        // High row count, low cardinality string → dictionary enabled + ByteArrayEncoder.
        let schema = TableSchema::new(vec![TableField::new("s", TableDataType::String)]);
        let arrow_schema = Arc::new(Schema::from(&schema));
        let values: Vec<String> = (0..1000).map(|i| format!("v{}", i % 5)).collect();
        let block = DataBlock::new_from_columns(vec![StringType::from_data(values)]);

        let mut writer = BlockParquetWriter::new(arrow_schema, props(&schema));
        writer.write_block(block.clone()).unwrap();
        let serialized = writer.finish().unwrap();
        assert_eq!(serialized.metadata.num_row_groups(), 1);

        let (blocks, _) = read_back(serialized.payload.concat());
        let got = DataBlock::concat(&blocks).unwrap();
        assert_blocks_eq(&block, &got);
    }

    // Compressed-page buffering means the estimate grows as blocks are written, and is far
    // below the raw uncompressed size for a low-cardinality column.
    #[test]
    fn test_compressed_size_tracks_buffered_pages() {
        let schema = TableSchema::new(vec![TableField::new(
            "i64",
            TableDataType::Number(NumberDataType::Int64),
        )]);
        let arrow_schema = Arc::new(Schema::from(&schema));
        let block = DataBlock::new_from_columns(vec![
            databend_common_expression::types::Int64Type::from_data(
                (0..10_000).map(|_| 7i64).collect::<Vec<_>>(),
            ),
        ]);

        let mut writer = BlockParquetWriter::new(arrow_schema, props(&schema));
        assert_eq!(writer.compressed_size(), 0);
        writer.write_block(block).unwrap();
        // 10k identical i64 values compress to far less than the 80 KB raw size.
        let estimated = writer.compressed_size();
        assert!(
            estimated < 80_000,
            "estimate {estimated} not below raw size"
        );
    }

    // Reads back through the arrow crate's Parquet reader across a wide set of leaf types,
    // confirming the zero-copy assembled file (with manually remapped offsets) is valid.
    #[test]
    fn test_wide_types_roundtrip_via_arrow_reader() {
        let schema = wide_schema();
        let arrow_schema = Arc::new(Schema::from(&schema));
        let expected = DataBlock::concat(&[wide_block(), wide_block()]).unwrap();

        let mut writer = BlockParquetWriter::new(arrow_schema, props(&schema));
        writer.write_block(wide_block()).unwrap();
        writer.write_block(wide_block()).unwrap();
        let serialized = writer.finish().unwrap();
        assert_eq!(serialized.metadata.num_row_groups(), 1);
        let (blocks, num_rg) = read_back(serialized.payload.concat());
        assert_eq!(num_rg, 1);
        assert_blocks_eq(&expected, &DataBlock::concat(&blocks).unwrap());
    }
}
