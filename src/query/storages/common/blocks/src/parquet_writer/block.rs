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

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use bytes::Bytes;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use parquet::arrow::ARROW_SCHEMA_META_KEY;
use parquet::arrow::ArrowSchemaConverter;
use parquet::arrow::arrow_writer::ArrowColumnWriter;
use parquet::arrow::arrow_writer::compute_leaves;
#[allow(deprecated)]
use parquet::arrow::arrow_writer::get_column_writers;
use parquet::arrow::encode_arrow_schema;
use parquet::file::metadata::ColumnChunkMetaData;
use parquet::file::metadata::FileMetaData;
use parquet::file::metadata::KeyValue;
use parquet::file::metadata::ParquetMetaDataBuilder;
use parquet::file::metadata::ParquetMetaDataWriter;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::properties::WriterPropertiesPtr;
use parquet::schema::types::SchemaDescPtr;

use super::SerializedParquet;
use crate::MAX_BATCH_MEMORY_SIZE;
use crate::PARQUET_PAGE_SIZE_HARD_LIMIT;

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
    ///
    /// Large batches are split into row-chunks under [`MAX_BATCH_MEMORY_SIZE`] before encoding:
    /// the column writers flush pages by their `data_page_*` limits, but a single oversized
    /// `write` of values that all land in one mini-batch can still produce one page exceeding
    /// parquet's ~2GB page-size hard limit (i32 overflow). Pre-splitting keeps each `write`
    /// bounded. Row size is assumed roughly uniform (`RecordBatch::slice` shares buffers, so a
    /// sliced chunk's true size cannot be measured) — see the `LIMITATION` on the old
    /// `write_batch_with_page_limit`.
    pub fn write_block(&mut self, block: DataBlock) -> Result<()> {
        if block.is_empty() {
            return Ok(());
        }

        let num_rows = block.num_rows();
        let arrow_schema = self.arrow_schema.clone();
        // Convert through the arrow schema so the columns line up with the parquet leaves by
        // schema (name/type/coercion), not by the block's positional column order.
        let batch = block.to_record_batch_with_arrow_schema(&arrow_schema)?;
        let batch_size = batch.get_array_memory_size();

        // Fast path: the whole batch fits within the soft limit, encode it in one go.
        if batch_size <= MAX_BATCH_MEMORY_SIZE {
            self.write_record_batch(&arrow_schema, &batch)?;
            self.num_rows += num_rows;
            return Ok(());
        }

        // A single row cannot be split across pages, so if one row exceeds the ~2GB hard limit
        // the encode would overflow the i32 page size. Reject it up front with a clear error.
        if num_rows == 1 {
            if batch_size > PARQUET_PAGE_SIZE_HARD_LIMIT {
                return Err(ErrorCode::Internal(format!(
                    "A single row requires {} bytes which exceeds Parquet's page size limit ({} bytes).",
                    batch_size, PARQUET_PAGE_SIZE_HARD_LIMIT
                )));
            }
            self.write_record_batch(&arrow_schema, &batch)?;
            self.num_rows += num_rows;
            return Ok(());
        }

        // Split into row-chunks sized to stay under the soft limit, assuming uniform row size.
        let rows_per_chunk = (((num_rows as f64) * (MAX_BATCH_MEMORY_SIZE as f64)
            / batch_size as f64)
            .ceil() as usize)
            .max(1);
        let mut offset = 0;
        while offset < num_rows {
            let length = rows_per_chunk.min(num_rows - offset);
            let chunk = batch.slice(offset, length);
            self.write_record_batch(&arrow_schema, &chunk)?;
            offset += length;
        }
        self.num_rows += num_rows;
        Ok(())
    }

    /// Leaf-expand each top-level column of `batch` and feed every leaf fragment into the
    /// matching column writer, in parquet leaf order.
    fn write_record_batch(&mut self, arrow_schema: &Schema, batch: &RecordBatch) -> Result<()> {
        let state = self.ensure_state()?;
        let mut writers = state.column_writers.iter_mut();
        for (field, column) in arrow_schema.fields().iter().zip(batch.columns()) {
            for leaf_column in compute_leaves(field, column)? {
                let writer = writers.next().ok_or_else(|| {
                    ErrorCode::Internal("more leaf columns than the schema declares")
                })?;
                writer.write(&leaf_column)?;
            }
        }
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
    pub fn finish(mut self) -> Result<SerializedParquet> {
        // Build the column writers even when no block was written, so the empty case takes the
        // same path: each leaf closes into a valid 0-row column chunk that `assemble_parquet`
        // stitches into an empty-schema file — no separate low-level fallback.
        self.ensure_state()?;
        let BlockParquetWriter {
            arrow_schema,
            props,
            state,
            num_rows,
        } = self;
        assemble_parquet(&arrow_schema, props, state.unwrap(), num_rows)
    }
}

/// Assemble a single-row-group parquet file from closed column chunks, pushing their compressed
/// bytes into the output with no copy and remapping page offsets — mirroring the fork's
/// `SerializedRowGroupWriter::append_column` offset logic, but writing owned `Bytes` directly.
fn assemble_parquet(
    arrow_schema: &Schema,
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

    // Replicate `ArrowWriter`: embed the IPC-encoded Arrow schema under `ARROW:schema` so
    // readers can reconstruct Databend extension-backed types (Variant, Bitmap, Geometry,
    // TimestampTz, ...) instead of inferring them as plain LargeBinary/Decimal. We assemble the
    // footer by hand, so this metadata must be added explicitly here. Any existing
    // `ARROW:schema` entry from the writer props is replaced to keep a single authoritative copy.
    let mut key_value_metadata: Vec<KeyValue> = props
        .key_value_metadata()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|kv| kv.key != ARROW_SCHEMA_META_KEY)
        .collect();

    key_value_metadata.push(KeyValue {
        key: ARROW_SCHEMA_META_KEY.to_string(),
        value: Some(encode_arrow_schema(arrow_schema)),
    });

    let file_metadata = FileMetaData::new(
        props.writer_version().as_num(),
        num_rows as i64,
        Some(props.created_by().to_string()),
        Some(key_value_metadata),
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
    use crate::parquet_writer::test_util::props_with_data_page_rows;
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

    // A batch far larger than the soft split limit must still round-trip exactly, and the
    // pre-splitting must produce multiple data pages (rather than one oversized page).
    #[test]
    fn test_large_batch_is_split_into_pages() {
        use parquet::file::reader::FileReader;
        use parquet::file::reader::SerializedFileReader;

        let schema = TableSchema::new(vec![TableField::new("s", TableDataType::String)]);
        let arrow_schema = Arc::new(Schema::from(&schema));
        // ~100 MiB of distinct 1 MiB strings (distinct so dictionary stays small and the data
        // pages carry the bulk), well above the 64 MiB soft limit.
        let values: Vec<String> = (0..100)
            .map(|i| format!("{i:07}").repeat(1 << 17))
            .collect();
        let block = DataBlock::new_from_columns(vec![StringType::from_data(values.clone())]);
        let raw_size = block
            .clone()
            .to_record_batch_with_arrow_schema(&arrow_schema)
            .unwrap()
            .get_array_memory_size();
        assert!(raw_size > MAX_BATCH_MEMORY_SIZE, "test setup too small");

        // Disable dictionary so the values land in data pages, exercising the split path.
        let mut writer = BlockParquetWriter::new(
            arrow_schema,
            Arc::new(crate::build_parquet_writer_properties(
                databend_storages_common_table_meta::table::TableCompression::None,
                false,
                None::<&databend_storages_common_table_meta::meta::StatisticsOfColumns>,
                None,
                0,
                &schema,
                None,
                None,
            )),
        );
        writer.write_block(block).unwrap();
        let serialized = writer.finish().unwrap();

        let bytes = bytes::Bytes::from(serialized.payload.concat());
        let reader = SerializedFileReader::new(bytes.clone()).unwrap();
        let mut page_reader = reader
            .get_row_group(0)
            .unwrap()
            .get_column_page_reader(0)
            .unwrap();
        let mut pages = 0;
        let mut total_values = 0;
        while let Some(page) = page_reader.get_next_page().unwrap() {
            pages += 1;
            total_values += page.num_values() as usize;
        }
        assert!(
            pages > 1,
            "expected multiple pages from splitting, got {pages}"
        );
        assert_eq!(total_values, 100);

        let (blocks, _) = read_back(bytes.to_vec());
        let got = DataBlock::concat(&blocks).unwrap();
        assert_eq!(got.num_rows(), 100);
        assert_eq!(got.columns()[0].to_column(), StringType::from_data(values));
    }

    // Finishing without writing any block (or after only empty blocks) must still emit a valid
    // single-row-group, 0-row file readable by the arrow reader — the unified `finish` path
    // builds the column writers on demand rather than falling back to a separate writer.
    #[test]
    fn test_finish_without_data_emits_valid_empty_file() {
        let schema = sample_schema();
        let arrow_schema = Arc::new(Schema::from(&schema));

        let mut writer = BlockParquetWriter::new(arrow_schema, props(&schema));
        assert!(writer.is_empty());
        // An explicitly empty block must not change the outcome.
        writer.write_block(DataBlock::empty()).unwrap();
        assert!(writer.is_empty());

        let serialized = writer.finish().unwrap();
        assert_eq!(serialized.metadata.num_row_groups(), 1);
        assert_eq!(serialized.metadata.file_metadata().num_rows(), 0);

        let (blocks, num_rg) = read_back(serialized.payload.concat());
        assert_eq!(num_rg, 1);
        let total_rows: usize = blocks.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);
    }

    // A single row above the soft split limit but under the ~2GB hard limit must be written
    // directly (a single row cannot be split across pages) and round-trip intact.
    #[test]
    fn test_single_large_row_under_hard_limit_succeeds() {
        let schema = TableSchema::new(vec![TableField::new("s", TableDataType::String)]);
        let arrow_schema = Arc::new(Schema::from(&schema));
        // One value larger than the 64 MiB soft limit, far below the ~2GB hard limit.
        let big = "x".repeat((MAX_BATCH_MEMORY_SIZE * 3) / 2);
        let block = DataBlock::new_from_columns(vec![StringType::from_data(vec![big.clone()])]);

        let mut writer = BlockParquetWriter::new(arrow_schema, props(&schema));
        writer.write_block(block).unwrap();
        let serialized = writer.finish().unwrap();
        assert_eq!(serialized.metadata.num_row_groups(), 1);

        let (blocks, _) = read_back(serialized.payload.concat());
        let got = DataBlock::concat(&blocks).unwrap();
        assert_eq!(got.num_rows(), 1);
        assert_eq!(
            got.columns()[0].to_column(),
            StringType::from_data(vec![big])
        );
    }

    #[test]
    fn test_arrow_schema_metadata_preserves_extension_types() {
        use databend_common_expression::types::BitmapType;
        use databend_common_expression::types::GeometryType;
        use databend_common_expression::types::VariantType;

        let schema = TableSchema::new(vec![
            TableField::new("v", TableDataType::Variant),
            TableField::new("bm", TableDataType::Bitmap),
            TableField::new("geo", TableDataType::Geometry),
            TableField::new(
                "v_null",
                TableDataType::Nullable(Box::new(TableDataType::Variant)),
            ),
        ]);
        let arrow_schema = Arc::new(Schema::from(&schema));

        // Opaque payloads: round-trip fidelity here is about types + bytes, not JSONB/WKB
        // validity, so raw bytes suffice.
        let block = DataBlock::new_from_columns(vec![
            VariantType::from_data(vec![b"\x20\x00".to_vec(), b"\x40\x01".to_vec()]),
            BitmapType::from_data(vec![b"\x01\x02".to_vec(), b"\x03".to_vec()]),
            GeometryType::from_data(vec![b"\x00\x00\x00".to_vec(), b"\xff".to_vec()]),
            VariantType::from_opt_data(vec![Some(b"\x10".to_vec()), None]),
        ]);

        let mut writer = BlockParquetWriter::new(arrow_schema.clone(), props(&schema));
        writer.write_block(block.clone()).unwrap();
        let serialized = writer.finish().unwrap();

        // The footer must carry the encoded Arrow schema under ARROW:schema, matching exactly
        // what the arrow writer would have embedded for this schema.
        let kvs = serialized
            .metadata
            .file_metadata()
            .key_value_metadata()
            .expect("file metadata must include key/value metadata");
        let arrow_meta = kvs
            .iter()
            .find(|kv| kv.key == ARROW_SCHEMA_META_KEY)
            .expect("ARROW:schema must be embedded in the footer");
        assert_eq!(
            arrow_meta.value.as_deref(),
            Some(encode_arrow_schema(&arrow_schema).as_str()),
            "embedded Arrow schema must match the writer's schema"
        );

        // The reader reconstructs the schema purely from the parquet file (mirroring the
        // result-cache reader). The extension types must survive the round-trip.
        let recovered_schema: TableSchema = {
            let bytes = bytes::Bytes::from(serialized.payload.concat());
            let reader =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(bytes)
                    .unwrap();
            reader.schema().as_ref().try_into().unwrap()
        };
        assert_eq!(
            recovered_schema.fields()[0].data_type(),
            &TableDataType::Variant
        );
        assert_eq!(
            recovered_schema.fields()[1].data_type(),
            &TableDataType::Bitmap
        );
        assert_eq!(
            recovered_schema.fields()[2].data_type(),
            &TableDataType::Geometry
        );
        assert_eq!(
            recovered_schema.fields()[3].data_type(),
            &TableDataType::Nullable(Box::new(TableDataType::Variant))
        );

        let (blocks, num_rg) = read_back(serialized.payload.concat());
        assert_eq!(num_rg, 1);
        let got = DataBlock::concat(&blocks).unwrap();
        // Compare through arrow arrays: the extension types are binary-backed and may differ in
        // internal column representation after round-trip even when semantically identical, so
        // RecordBatch equality (well-defined per arrow array) is the right comparison here.
        let expected_batch = block
            .to_record_batch_with_arrow_schema(&arrow_schema)
            .unwrap();
        let got_batch = got
            .to_record_batch_with_arrow_schema(&arrow_schema)
            .unwrap();
        assert_eq!(expected_batch, got_batch);
    }

    #[test]
    fn test_compatible_with_arrow_writer_for_extension_types() {
        use databend_common_expression::types::BitmapType;
        use databend_common_expression::types::GeometryType;
        use databend_common_expression::types::VariantType;
        use parquet::arrow::ArrowWriter;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let schema = TableSchema::new(vec![
            TableField::new("v", TableDataType::Variant),
            TableField::new("bm", TableDataType::Bitmap),
            TableField::new("geo", TableDataType::Geometry),
            TableField::new("i", TableDataType::Number(NumberDataType::Int64)),
            TableField::new(
                "v_null",
                TableDataType::Nullable(Box::new(TableDataType::Variant)),
            ),
        ]);
        let arrow_schema = Arc::new(Schema::from(&schema));
        let block = DataBlock::new_from_columns(vec![
            VariantType::from_data(vec![b"\x20\x00".to_vec(), b"\x40\x01".to_vec()]),
            BitmapType::from_data(vec![b"\x01\x02".to_vec(), b"\x03".to_vec()]),
            GeometryType::from_data(vec![b"\x00\x00".to_vec(), b"\xff".to_vec()]),
            databend_common_expression::types::Int64Type::from_data(vec![1i64, 2]),
            VariantType::from_opt_data(vec![Some(b"\x10".to_vec()), None]),
        ]);
        let batch = block
            .clone()
            .to_record_batch_with_arrow_schema(&arrow_schema)
            .unwrap();

        // Old path: ArrowWriter with the same writer properties.
        let writer_props = props(&schema);
        let mut old_bytes = Vec::new();
        let mut arrow_writer = ArrowWriter::try_new(
            &mut old_bytes,
            arrow_schema.clone(),
            Some((*writer_props).clone()),
        )
        .unwrap();
        arrow_writer.write(&batch).unwrap();
        arrow_writer.close().unwrap();

        // New path: BlockParquetWriter.
        let mut writer = BlockParquetWriter::new(arrow_schema.clone(), writer_props);
        writer.write_block(block).unwrap();
        let new_bytes = writer.finish().unwrap().payload.concat();

        // Both files must embed the identical `ARROW:schema` value.
        let arrow_meta = |bytes: &[u8]| -> String {
            let reader =
                ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(bytes))
                    .unwrap();
            reader
                .metadata()
                .file_metadata()
                .key_value_metadata()
                .unwrap()
                .iter()
                .find(|kv| kv.key == ARROW_SCHEMA_META_KEY)
                .unwrap()
                .value
                .clone()
                .unwrap()
        };
        assert_eq!(arrow_meta(&old_bytes), arrow_meta(&new_bytes));

        // The reader-visible Arrow schema (including per-field extension metadata) must match.
        let read_schema = |bytes: &[u8]| {
            ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(bytes))
                .unwrap()
                .schema()
                .clone()
        };
        assert_eq!(read_schema(&old_bytes), read_schema(&new_bytes));

        // And the decoded data must be identical.
        let (old_blocks, _) = read_back(old_bytes);
        let (new_blocks, _) = read_back(new_bytes);
        let old = DataBlock::concat(&old_blocks)
            .unwrap()
            .to_record_batch_with_arrow_schema(&arrow_schema)
            .unwrap();
        let new = DataBlock::concat(&new_blocks)
            .unwrap()
            .to_record_batch_with_arrow_schema(&arrow_schema)
            .unwrap();
        assert_eq!(old, new);
    }

    // Zero-copy footer assembly deliberately omits the per-column OffsetIndex (the old
    // ArrowWriter wrote one even with stats disabled). A multi-page column chunk must still be
    // fully readable by a standard parquet reader, which falls back to walking page headers
    // sequentially when no OffsetIndex is present. This locks in that the omission is safe.
    #[test]
    fn test_multi_page_chunk_without_offset_index_reads_back() {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use parquet::file::reader::FileReader;
        use parquet::file::reader::SerializedFileReader;

        let schema = TableSchema::new(vec![
            TableField::new("i", TableDataType::Number(NumberDataType::Int64)),
            TableField::new("s", TableDataType::String),
        ]);
        let arrow_schema = Arc::new(Schema::from(&schema));

        // 5000 rows, capped at 100 rows/page. The arrow column writer only checks page
        // boundaries at `write_batch_size` (1024) granularity, so the row count must exceed that
        // several times over to reliably span multiple pages.
        let n = 5000i64;
        let block = DataBlock::new_from_columns(vec![
            databend_common_expression::types::Int64Type::from_data((0..n).collect::<Vec<_>>()),
            StringType::from_data((0..n).map(|i| format!("row-{i}")).collect::<Vec<_>>()),
        ]);

        let mut writer = BlockParquetWriter::new(
            arrow_schema.clone(),
            props_with_data_page_rows(&schema, 100),
        );
        writer.write_block(block.clone()).unwrap();
        let serialized = writer.finish().unwrap();
        let bytes = bytes::Bytes::from(serialized.payload.concat());

        // The file must carry NO offset index (documents the zero-copy tradeoff).
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone()).unwrap();
        assert!(
            builder.metadata().offset_index().is_none(),
            "BlockParquetWriter is expected to omit the OffsetIndex"
        );

        // Each column chunk must actually span multiple pages, so the sequential page-header
        // walk on the read side is genuinely exercised.
        let reader = SerializedFileReader::new(bytes.clone()).unwrap();
        let rg = reader.get_row_group(0).unwrap();
        for col in 0..2 {
            let mut pages = 0;
            let mut page_reader = rg.get_column_page_reader(col).unwrap();
            while page_reader.get_next_page().unwrap().is_some() {
                pages += 1;
            }
            assert!(
                pages > 2,
                "column {col} should span several pages, got {pages}"
            );
        }

        // A standard reader (no offset index) reconstructs every row correctly.
        let (blocks, num_rg) = read_back(bytes.to_vec());
        assert_eq!(num_rg, 1);
        let got = DataBlock::concat(&blocks).unwrap();
        assert_eq!(got.num_rows(), n as usize);
        assert_blocks_eq(&block, &got);
    }
}
