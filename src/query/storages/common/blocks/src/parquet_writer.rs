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

//! Streaming single-row-group Parquet writer.
//!
//! Built on the low-level `parquet` API (`next_column_with_factory` +
//! `write_leaf_column`/`write_primitive_column`, exposed by the datafuse-extras
//! arrow-rs fork) so that compressed pages are flushed to the sink as they fill,
//! instead of buffering whole column chunks in memory like `ArrowWriter` does.
//!
//! Two writers are provided:
//! - [`BulkBlockParquetWriter`] (column-oriented): writes one full column at a time,
//!   peak memory ≈ one in-progress page. This is the core.
//! - [`BlockParquetWriter`] (row-oriented, high-level): buffers incoming `DataBlock`s,
//!   then at `finish` replays them column-by-column through a [`BulkBlockParquetWriter`].
//!
//! Both are restricted to a single row group. The underlying sink is a
//! [`ChunkedWriteBuffer`] (4 MiB chunks) rather than a single growing `Vec<u8>`, to avoid
//! repeated reallocation/copy as a large row group is serialized; at `finish` the chunks are
//! handed out as-is in [`SerializedParquet::payload`] (a `Vec<Bytes>`), so the fuse write
//! path can forward them straight to opendal with no consolidation copy.
//!
//! NOTE: the leaf-value dispatch is reused from arrow-rs via the fork's public
//! `write_leaf_column`/`write_byte_array_column`. Keep in sync with the pinned fork rev.

use std::io;
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Schema;
use bytes::Bytes;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use parquet::arrow::ArrowSchemaConverter;
use parquet::arrow::arrow_writer::ArrowLeafColumn;
use parquet::arrow::arrow_writer::ByteArrayEncoder;
use parquet::arrow::arrow_writer::compute_leaves;
use parquet::arrow::arrow_writer::write_byte_array_column;
use parquet::arrow::arrow_writer::write_leaf_column;
use parquet::column::writer::GenericColumnWriter;
use parquet::column::writer::get_column_writer;
use parquet::errors::ParquetError;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::WriterPropertiesPtr;
use parquet::file::writer::SerializedFileWriter;
use parquet::file::writer::SerializedRowGroupWriter;

/// Default chunk size for [`ChunkedWriteBuffer`]: 4 MiB.
const DEFAULT_CHUNK_SIZE: usize = 4 * 1024 * 1024;

/// A `Write` sink backed by a list of fixed-size chunks instead of one growing `Vec<u8>`.
///
/// `SerializedFileWriter` needs a `W: Write`. Backing it with a single `Vec<u8>` means every
/// time the vector outgrows its capacity it reallocates and copies *all* bytes written so
/// far — for a multi-hundred-MB row group that is repeated large memcpys plus transient 2x
/// peak memory. Appending into 4 MiB chunks avoids both: existing bytes are never moved, and
/// growth costs one chunk allocation. At finish the chunks are handed out as-is via
/// [`Self::into_chunks`] (each `Vec<u8>` becomes a `Bytes` with no copy), so the serialized
/// payload can travel to IO non-contiguously without ever being consolidated.
struct ChunkedWriteBuffer {
    chunk_size: usize,
    chunks: Vec<Vec<u8>>,
    len: usize,
}

impl ChunkedWriteBuffer {
    fn new(chunk_size: usize) -> Self {
        Self {
            chunk_size,
            chunks: Vec::new(),
            len: 0,
        }
    }

    /// Hand out the chunks as `Bytes` without copying their contents (each `Vec<u8>` is moved
    /// into a `Bytes`). The caller can write them to IO in order, or join them if a
    /// contiguous buffer is required.
    fn into_chunks(self) -> Vec<Bytes> {
        self.chunks.into_iter().map(Bytes::from).collect()
    }
}

impl io::Write for ChunkedWriteBuffer {
    fn write(&mut self, mut buf: &[u8]) -> io::Result<usize> {
        let total = buf.len();
        while !buf.is_empty() {
            let need_new_chunk = self
                .chunks
                .last()
                .is_none_or(|c| c.len() >= self.chunk_size);
            if need_new_chunk {
                let cap = self.chunk_size.max(buf.len());
                self.chunks.push(Vec::with_capacity(cap));
            }
            let chunk = self.chunks.last_mut().unwrap();
            let room = chunk.capacity() - chunk.len();
            let take = room.min(buf.len());
            chunk.extend_from_slice(&buf[..take]);
            buf = &buf[take..];
        }
        self.len += total;
        Ok(total)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Result of finishing a [`BulkBlockParquetWriter`] / [`BlockParquetWriter`]: the serialized
/// single-row-group Parquet bytes plus the file metadata.
///
/// `payload` is a list of contiguous chunks (~4 MiB each) rather than one `Vec<u8>`: the
/// fuse write path forwards it straight to opendal (`Buffer::from(Vec<Bytes>)`) with no
/// consolidation copy. Callers that genuinely need one contiguous buffer should concat the
/// chunks themselves (e.g. `payload.concat()`) rather than relying on a helper, so the copy
/// stays explicit at the call site.
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

/// Whether a parquet leaf column should be encoded with the specialized
/// [`ByteArrayEncoder`] (zero-copy byte arrays + dictionary) or the generic
/// column encoder, mirroring arrow-rs `get_arrow_column_writer`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LeafEncoderKind {
    ByteArray,
    Column,
}

/// Classify the leaf columns of an arrow schema in parquet leaf order, replicating
/// arrow-rs `ArrowColumnWriterFactory::get_arrow_column_writer` so we preserve the
/// `ByteArrayEncoder` optimization (including dictionaries of byte types). Also returns
/// the number of parquet leaves each top-level field expands to (`field_leaf_counts`),
/// so a per-field column writer can pick the right slice of `leaf_kinds`.
fn classify_schema(schema: &Schema) -> (Vec<LeafEncoderKind>, Vec<usize>) {
    let mut kinds = Vec::new();
    let mut field_leaf_counts = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let before = kinds.len();
        classify_data_type(field.data_type(), &mut kinds);
        field_leaf_counts.push(kinds.len() - before);
    }
    (kinds, field_leaf_counts)
}

fn classify_data_type(data_type: &ArrowDataType, out: &mut Vec<LeafEncoderKind>) {
    match data_type {
        ArrowDataType::LargeBinary
        | ArrowDataType::Binary
        | ArrowDataType::Utf8
        | ArrowDataType::LargeUtf8
        | ArrowDataType::BinaryView
        | ArrowDataType::Utf8View => out.push(LeafEncoderKind::ByteArray),
        ArrowDataType::List(f)
        | ArrowDataType::LargeList(f)
        | ArrowDataType::FixedSizeList(f, _)
        | ArrowDataType::ListView(f)
        | ArrowDataType::LargeListView(f) => classify_data_type(f.data_type(), out),
        ArrowDataType::Struct(fields) => {
            for field in fields {
                classify_data_type(field.data_type(), out);
            }
        }
        ArrowDataType::Map(f, _) => match f.data_type() {
            ArrowDataType::Struct(fields) => {
                classify_data_type(fields[0].data_type(), out);
                classify_data_type(fields[1].data_type(), out);
            }
            _ => unreachable!("invalid map type"),
        },
        ArrowDataType::Dictionary(_, value_type) => match value_type.as_ref() {
            ArrowDataType::Utf8
            | ArrowDataType::LargeUtf8
            | ArrowDataType::Binary
            | ArrowDataType::LargeBinary
            | ArrowDataType::Utf8View
            | ArrowDataType::BinaryView
            | ArrowDataType::FixedSizeBinary(_) => out.push(LeafEncoderKind::ByteArray),
            _ => out.push(LeafEncoderKind::Column),
        },
        // Primitives, FixedSizeBinary, Boolean, Null, etc.
        _ => out.push(LeafEncoderKind::Column),
    }
}

/// Low-level column-oriented single-row-group Parquet writer (writer style 2).
///
/// This writer *is* the (single) row group — there is no separate row-group level. Request
/// one [`BulkColumnParquetWriter`] per top-level field in schema order via
/// [`Self::next_column`], feed it one block's column at a time with
/// [`BulkColumnParquetWriter::write_column`] (loop over blocks), then
/// [`BulkColumnParquetWriter::close`] it before requesting the next column. Finally call
/// [`Self::finish`] for the bytes + metadata.
///
/// `write_column` buffers the cheap `ArrayRef` handles; the actual encoding runs at
/// `finish`, streaming one parquet leaf at a time with pages flushed to the sink as they
/// fill (no simultaneous encoded column-chunk buffering, unlike `ArrowWriter`). This is the
/// core primitive the future vertical-merge path drives directly.
pub struct BulkBlockParquetWriter {
    file_writer: SerializedFileWriter<ChunkedWriteBuffer>,
    arrow_schema: Arc<Schema>,
    leaf_kinds: Vec<LeafEncoderKind>,
    field_leaf_counts: Vec<usize>,
    /// Buffered fragments per top-level field (one entry per `write_column` call).
    columns: Vec<Vec<ArrayRef>>,
    next_field: usize,
}

impl BulkBlockParquetWriter {
    pub fn new(arrow_schema: Arc<Schema>, props: WriterPropertiesPtr) -> Result<Self> {
        let parquet_schema = ArrowSchemaConverter::new()
            .with_coerce_types(props.coerce_types())
            .convert(&arrow_schema)?;
        let root = parquet_schema.root_schema_ptr();
        let file_writer =
            SerializedFileWriter::new(ChunkedWriteBuffer::new(DEFAULT_CHUNK_SIZE), root, props)?;
        let (leaf_kinds, field_leaf_counts) = classify_schema(&arrow_schema);
        debug_assert_eq!(leaf_kinds.len(), parquet_schema.num_columns());
        let columns = vec![Vec::new(); arrow_schema.fields().len()];
        Ok(Self {
            file_writer,
            arrow_schema,
            leaf_kinds,
            field_leaf_counts,
            columns,
            next_field: 0,
        })
    }

    /// Number of top-level fields the writer expects, one column writer each.
    pub fn num_fields(&self) -> usize {
        self.arrow_schema.fields().len()
    }

    /// Open a column writer for the next top-level field (in schema order). Feed it one
    /// block's column at a time via [`BulkColumnParquetWriter::write_column`], then close it
    /// before requesting the next column.
    pub fn next_column(&mut self) -> Result<BulkColumnParquetWriter<'_>> {
        let idx = self.next_field;
        if idx >= self.arrow_schema.fields().len() {
            return Err(ErrorCode::Internal(
                "next_column called more times than the schema has fields",
            ));
        }
        self.next_field += 1;
        Ok(BulkColumnParquetWriter {
            fragments: &mut self.columns[idx],
        })
    }

    /// Encode the buffered columns into the single row group, then write the footer and
    /// return the serialized bytes together with the metadata. Errors if fewer columns were
    /// requested via [`Self::next_column`] than the schema declares.
    pub fn finish(mut self) -> Result<SerializedParquet> {
        let num_fields = self.arrow_schema.fields().len();
        if self.next_field != num_fields {
            return Err(ErrorCode::Internal(format!(
                "writer finished after {} columns, but schema declares {}",
                self.next_field, num_fields
            )));
        }
        {
            let mut row_group = self.file_writer.next_row_group()?;
            let mut leaf_idx = 0usize;
            for (field_idx, field) in self.arrow_schema.fields().iter().enumerate() {
                let leaves_per_fragment = self.columns[field_idx]
                    .iter()
                    .map(|array| compute_leaves(field, array))
                    .collect::<std::result::Result<Vec<_>, ParquetError>>()?;
                for j in 0..self.field_leaf_counts[field_idx] {
                    let kind = self.leaf_kinds[leaf_idx];
                    let frags: Vec<&ArrowLeafColumn> =
                        leaves_per_fragment.iter().map(|v| &v[j]).collect();
                    write_one_leaf(&mut row_group, kind, &frags)?;
                    leaf_idx += 1;
                }
            }
            row_group.close()?;
        }
        let metadata = self.file_writer.finish()?;
        let payload = std::mem::replace(
            self.file_writer.inner_mut(),
            ChunkedWriteBuffer::new(DEFAULT_CHUNK_SIZE),
        )
        .into_chunks();
        Ok(SerializedParquet { payload, metadata })
    }
}

/// Per-column writer for one top-level field, handed out by
/// [`BulkBlockParquetWriter::next_column`]. Call [`Self::write_column`] once per source
/// block (looping to assemble the full column from multiple blocks without concatenating
/// first), then [`Self::close`] before requesting the next column. Buffers the cheap
/// `ArrayRef` handles; encoding is deferred to [`BulkBlockParquetWriter::finish`].
pub struct BulkColumnParquetWriter<'a> {
    fragments: &'a mut Vec<ArrayRef>,
}

impl BulkColumnParquetWriter<'_> {
    /// Buffer one block's column for this field. `array` is this field's column from a
    /// single block; call repeatedly (once per block) to assemble the full column.
    pub fn write_column(&mut self, array: ArrayRef) -> Result<()> {
        self.fragments.push(array);
        Ok(())
    }

    /// Finish this column. The buffered fragments are encoded later at
    /// [`BulkBlockParquetWriter::finish`], so this is just a clarity marker for the
    /// per-column scope.
    pub fn close(self) -> Result<()> {
        Ok(())
    }
}

/// Open the next parquet leaf column, stream all `fragments` into it, then close it.
/// Pages are flushed to the file sink as they fill (no column-chunk buffering).
fn write_one_leaf(
    row_group: &mut SerializedRowGroupWriter<'_, ChunkedWriteBuffer>,
    kind: LeafEncoderKind,
    fragments: &[&ArrowLeafColumn],
) -> Result<()> {
    let written =
        row_group.next_column_with_factory(|descr, props, page_writer, on_close| match kind {
            LeafEncoderKind::ByteArray => {
                let mut writer =
                    GenericColumnWriter::<ByteArrayEncoder>::new(descr, props, page_writer);
                for frag in fragments {
                    write_byte_array_column(&mut writer, frag)?;
                }
                on_close(writer.close()?)
            }
            LeafEncoderKind::Column => {
                let mut writer = get_column_writer(descr, props, page_writer);
                for frag in fragments {
                    write_leaf_column(&mut writer, frag)?;
                }
                on_close(writer.close()?)
            }
        })?;
    if written.is_none() {
        return Err(ErrorCode::Internal(
            "parquet writer produced more leaf columns than the schema declares",
        ));
    }
    Ok(())
}

/// High-level row-oriented buffered writer (writer style 1): buffers incoming
/// `DataBlock`s, then at [`Self::finish`] replays them column-by-column through a
/// [`BulkBlockParquetWriter`]. Used by the insert / fuse `StreamBlockBuilder` path and
/// `blocks_to_parquet*` where blocks arrive incrementally or as a batch.
pub struct BlockParquetWriter {
    arrow_schema: Arc<Schema>,
    props: WriterPropertiesPtr,
    blocks: Vec<DataBlock>,
}

impl BlockParquetWriter {
    pub fn new(arrow_schema: Arc<Schema>, props: WriterPropertiesPtr) -> Self {
        Self {
            arrow_schema,
            props,
            blocks: Vec::new(),
        }
    }

    pub fn write_block(&mut self, block: DataBlock) {
        if !block.is_empty() {
            self.blocks.push(block);
        }
    }

    pub fn write_blocks(&mut self, blocks: impl IntoIterator<Item = DataBlock>) {
        for block in blocks {
            self.write_block(block);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.blocks.iter().all(|b| b.is_empty())
    }

    /// Encode all buffered blocks into a single row group and return the bytes plus
    /// metadata. Each top-level field is replayed as one fragment per buffered block.
    pub fn finish(self) -> Result<SerializedParquet> {
        let mut writer =
            BulkBlockParquetWriter::new(self.arrow_schema.clone(), self.props.clone())?;
        let num_fields = writer.num_fields();
        let blocks = self.blocks;
        for field_idx in 0..num_fields {
            let mut column = writer.next_column()?;
            for block in &blocks {
                column.write_column(ArrayRef::from(&block.get_by_offset(field_idx).to_column()))?;
            }
            column.close()?;
        }
        writer.finish()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use databend_common_expression::Column;
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

    use super::*;
    use crate::build_parquet_writer_properties;

    // A wide schema covering byte-array (String), primitive (Int32/Int64), nullable, and
    // nested list (Array) leaves — exercising both encoder kinds and nested leaf ordering.
    fn sample_schema() -> TableSchema {
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

    fn sample_block() -> DataBlock {
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

    fn props(schema: &TableSchema) -> WriterPropertiesPtr {
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

    fn read_back(bytes: Vec<u8>) -> (Vec<DataBlock>, usize) {
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

    fn assert_blocks_eq(expected: &DataBlock, actual: &DataBlock) {
        assert_eq!(expected.num_rows(), actual.num_rows());
        assert_eq!(expected.num_columns(), actual.num_columns());
        for (e, a) in expected.columns().iter().zip(actual.columns()) {
            assert_eq!(e.to_column(), a.to_column());
        }
    }

    /// Drive the low-level `BulkBlockParquetWriter` API: one column writer per field, each
    /// fed its column from every block (looping `write_column`) then closed.
    fn bulk_write_blocks(
        arrow_schema: Arc<Schema>,
        props: WriterPropertiesPtr,
        blocks: &[DataBlock],
    ) -> (Vec<u8>, ParquetMetaData) {
        let mut writer = BulkBlockParquetWriter::new(arrow_schema, props).unwrap();
        let num_fields = writer.num_fields();
        for field_idx in 0..num_fields {
            let mut column = writer.next_column().unwrap();
            for b in blocks {
                column
                    .write_column(ArrayRef::from(&b.get_by_offset(field_idx).to_column()))
                    .unwrap();
            }
            column.close().unwrap();
        }
        let SerializedParquet { payload, metadata } = writer.finish().unwrap();
        (payload.concat(), metadata)
    }

    #[test]
    fn test_block_parquet_writer_roundtrip_single_block() {
        let schema = sample_schema();
        let arrow_schema = Arc::new(Schema::from(&schema));
        let block = sample_block();

        let mut writer = BlockParquetWriter::new(arrow_schema, props(&schema));
        writer.write_block(block.clone());
        let serialized = writer.finish().unwrap();

        assert_eq!(serialized.metadata.num_row_groups(), 1);
        let (blocks, num_rg) = read_back(serialized.payload.concat());
        assert_eq!(num_rg, 1);
        let got = DataBlock::concat(&blocks).unwrap();
        assert_blocks_eq(&block, &got);
    }

    #[test]
    fn test_block_parquet_writer_roundtrip_multi_block() {
        let schema = sample_schema();
        let arrow_schema = Arc::new(Schema::from(&schema));

        let mut writer = BlockParquetWriter::new(arrow_schema, props(&schema));
        writer.write_block(sample_block());
        writer.write_block(sample_block());
        writer.write_block(sample_block());
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
    fn test_bulk_block_parquet_writer_roundtrip() {
        let schema = sample_schema();
        let arrow_schema = Arc::new(Schema::from(&schema));
        let blocks = [sample_block(), sample_block()];

        // Drive the low-level API column-by-column: each field's fragments are its
        // column from every block.
        let (bytes, meta) = bulk_write_blocks(arrow_schema, props(&schema), &blocks);

        assert_eq!(meta.num_row_groups(), 1);
        let (read_blocks, num_rg) = read_back(bytes);
        assert_eq!(num_rg, 1);
        let got = DataBlock::concat(&read_blocks).unwrap();
        let expected = DataBlock::concat(&[sample_block(), sample_block()]).unwrap();
        assert_blocks_eq(&expected, &got);
    }

    #[test]
    fn test_both_writers_produce_equal_data() {
        let schema = sample_schema();
        let arrow_schema = Arc::new(Schema::from(&schema));

        let mut buffered = BlockParquetWriter::new(arrow_schema.clone(), props(&schema));
        buffered.write_block(sample_block());
        buffered.write_block(sample_block());
        let buffered_bytes = buffered.finish().unwrap().payload.concat();

        let blocks = [sample_block(), sample_block()];
        let (streaming_bytes, _) = bulk_write_blocks(arrow_schema, props(&schema), &blocks);

        let (buffered_blocks, _) = read_back(buffered_bytes);
        let (streaming_blocks, _) = read_back(streaming_bytes);
        let buffered_data = DataBlock::concat(&buffered_blocks).unwrap();
        let streaming_data = DataBlock::concat(&streaming_blocks).unwrap();
        assert_blocks_eq(&buffered_data, &streaming_data);
    }

    #[test]
    fn test_dictionary_string_uses_byte_array_path() {
        // High row count, low cardinality string → dictionary enabled + ByteArrayEncoder.
        let schema = TableSchema::new(vec![TableField::new("s", TableDataType::String)]);
        let arrow_schema = Arc::new(Schema::from(&schema));
        let values: Vec<String> = (0..1000).map(|i| format!("v{}", i % 5)).collect();
        let block = DataBlock::new_from_columns(vec![StringType::from_data(values)]);

        let mut writer = BlockParquetWriter::new(arrow_schema, props(&schema));
        writer.write_block(block.clone());
        let serialized = writer.finish().unwrap();
        assert_eq!(serialized.metadata.num_row_groups(), 1);

        let (blocks, _) = read_back(serialized.payload.concat());
        let got = DataBlock::concat(&blocks).unwrap();
        assert_blocks_eq(&block, &got);
    }

    // A wide schema covering many leaf types and both encoder kinds: bool, float, decimal,
    // date, timestamp, binary (byte-array), string, nested array, map, tuple (struct).
    fn wide_schema() -> TableSchema {
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

    fn wide_block() -> DataBlock {
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

    // Verifies that data written by both writers reads back correctly through the arrow
    // crate's Parquet reader (`ParquetRecordBatchReaderBuilder`), across a wide set of leaf
    // types and both the high-level (`BlockParquetWriter`) and low-level
    // (`BulkBlockParquetWriter`) APIs.
    #[test]
    fn test_wide_types_roundtrip_via_arrow_reader() {
        let schema = wide_schema();
        let arrow_schema = Arc::new(Schema::from(&schema));
        let expected = DataBlock::concat(&[wide_block(), wide_block()]).unwrap();

        // High-level writer: buffer two blocks, finish, read back through arrow.
        let mut high = BlockParquetWriter::new(arrow_schema.clone(), props(&schema));
        high.write_block(wide_block());
        high.write_block(wide_block());
        let high_serialized = high.finish().unwrap();
        assert_eq!(high_serialized.metadata.num_row_groups(), 1);
        let (high_blocks, num_rg) = read_back(high_serialized.payload.concat());
        assert_eq!(num_rg, 1);
        assert_blocks_eq(&expected, &DataBlock::concat(&high_blocks).unwrap());

        // Low-level writer: same two blocks driven column-by-column, read back through arrow.
        let (low_bytes, low_meta) =
            bulk_write_blocks(arrow_schema, props(&schema), &[wide_block(), wide_block()]);
        assert_eq!(low_meta.num_row_groups(), 1);
        let (low_blocks, num_rg) = read_back(low_bytes);
        assert_eq!(num_rg, 1);
        assert_blocks_eq(&expected, &DataBlock::concat(&low_blocks).unwrap());
    }

    #[test]
    fn test_chunked_write_buffer() {
        use std::io::Write;

        // Chunk size 4: writes that span chunk boundaries must still reassemble exactly,
        // and a single write larger than the chunk size must be accepted in one chunk.
        let mut buf = ChunkedWriteBuffer::new(4);
        buf.write_all(b"ab").unwrap(); // partial first chunk
        buf.write_all(b"cde").unwrap(); // spills into a second chunk
        buf.write_all(b"fghijklm").unwrap(); // larger than chunk_size in one write
        assert_eq!(buf.len, 13);
        assert!(
            buf.chunks.len() > 1,
            "expected data to span multiple chunks"
        );
        assert_eq!(buf.into_chunks().concat(), b"abcdefghijklm");

        // Single write under chunk size yields a single chunk holding all bytes.
        let mut single = ChunkedWriteBuffer::new(4);
        single.write_all(b"xy").unwrap();
        let chunks = single.into_chunks();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks.concat(), b"xy");

        // Empty buffer yields no chunks.
        assert!(ChunkedWriteBuffer::new(4).into_chunks().is_empty());
    }
}
