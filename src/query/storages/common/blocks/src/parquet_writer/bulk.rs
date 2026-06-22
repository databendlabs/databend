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

//! Low-level, leaf-oriented single-row-group Parquet writer.
//!
//! Built on the `parquet` low-level API (`next_column_with_factory` +
//! `write_leaf_column`/`write_byte_array_column`, exposed by the datafuse-extras arrow-rs
//! fork) so compressed pages are flushed to the sink as they fill, instead of buffering whole
//! column chunks in memory like `ArrowWriter` does. The caller supplies the sink (any
//! `io::Write`); [`BulkBlockParquetWriter::finish`] writes the footer and hands the sink back
//! alongside the metadata, so the caller reads the serialized bytes from its own sink. The
//! provided [`ChunkedWriteBuffer`] sink keeps the bytes as 4 MiB chunks that the fuse write
//! path can forward straight to opendal with no consolidation copy.

use std::io;
use std::sync::Arc;

use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Schema;
use bytes::Bytes;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use parquet::arrow::ArrowSchemaConverter;
use parquet::arrow::add_encoded_arrow_schema_to_metadata;
use parquet::arrow::arrow_writer::ArrowLeafColumn;
use parquet::arrow::arrow_writer::ByteArrayEncoder;
use parquet::arrow::arrow_writer::write_byte_array_column;
use parquet::arrow::arrow_writer::write_leaf_column;
use parquet::column::writer::ColumnWriter;
use parquet::column::writer::GenericColumnWriter;
use parquet::column::writer::get_column_writer;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::WriterPropertiesPtr;
use parquet::file::writer::OnCloseColumnChunk;
use parquet::file::writer::SerializedFileWriter;
use parquet::file::writer::SerializedRowGroupWriter;

/// Default chunk size for [`ChunkedWriteBuffer`]: 4 MiB.
pub const DEFAULT_CHUNK_SIZE: usize = 4 * 1024 * 1024;

/// A `Write` sink backed by a list of fixed-size chunks instead of one growing `Vec<u8>`.
///
/// `SerializedFileWriter` needs a `W: Write`. Backing it with a single `Vec<u8>` means every
/// time the vector outgrows its capacity it reallocates and copies *all* bytes written so
/// far — for a multi-hundred-MB row group that is repeated large memcpys plus transient 2x
/// peak memory. Appending into 4 MiB chunks avoids both: existing bytes are never moved, and
/// growth costs one chunk allocation. At finish the chunks are handed out as-is via
/// [`Self::into_chunks`] (each `Vec<u8>` becomes a `Bytes` with no copy), so the serialized
/// payload can travel to IO non-contiguously without ever being consolidated.
pub struct ChunkedWriteBuffer {
    chunk_size: usize,
    chunks: Vec<Vec<u8>>,
    total_bytes: usize,
}

impl ChunkedWriteBuffer {
    pub fn new(chunk_size: usize) -> Self {
        Self {
            chunk_size,
            chunks: Vec::new(),
            total_bytes: 0,
        }
    }

    /// Hand out the chunks as `Bytes` without copying their contents (each `Vec<u8>` is moved
    /// into a `Bytes`). The caller can write them to IO in order, or join them if a
    /// contiguous buffer is required.
    pub fn into_chunks(self) -> Vec<Bytes> {
        self.chunks.into_iter().map(Bytes::from).collect()
    }
}

impl Default for ChunkedWriteBuffer {
    fn default() -> Self {
        Self::new(DEFAULT_CHUNK_SIZE)
    }
}

impl io::Write for ChunkedWriteBuffer {
    fn write(&mut self, mut remaining: &[u8]) -> io::Result<usize> {
        let bytes_written = remaining.len();

        while !remaining.is_empty() {
            if self.total_bytes.is_multiple_of(self.chunk_size) {
                self.chunks.push(Vec::with_capacity(self.chunk_size));
            }

            let current_chunk = self.chunks.last_mut().unwrap();
            let current_remaining = current_chunk.capacity() - current_chunk.len();

            let written = current_remaining.min(remaining.len());
            current_chunk.extend_from_slice(&remaining[..written]);
            remaining = &remaining[written..];
            self.total_bytes += written;
        }

        Ok(bytes_written)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
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
/// `ByteArrayEncoder` optimization (including dictionaries of byte types).
fn classify_schema(schema: &Schema) -> Vec<LeafEncoderKind> {
    let mut kinds = Vec::new();
    for field in schema.fields() {
        classify_data_type(field.data_type(), &mut kinds);
    }
    kinds
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

/// Low-level **leaf-oriented** single-row-group Parquet writer.
///
/// This writer *is* the (single) row group — it opens one implicitly on construction; there is
/// no separate row-group level to manage. It owns the parquet schema + per-leaf encoder
/// classification and the page-streaming encode, and is positional over parquet *leaf* columns
/// (the physical columns: a nested top-level field expands to several leaves), mirroring the
/// read side's `RowGroupReader::get_column_*`.
///
/// Drive it leaf by leaf: [`Self::next_leaf`] yields the next leaf (in parquet leaf order), feed
/// it its `ArrowLeafColumn`s via [`LeafColumnWriter::write`], then [`LeafColumnWriter::close`] it
/// before the next; finally [`Self::finish`] for the bytes + metadata. Each `write` encodes
/// straight into the open leaf's page writer, flushing pages to the sink as they fill — no
/// per-column chunk buffer (unlike `ArrowWriter`). Only one leaf may be open at a time; the
/// borrow checker enforces it (the leaf borrows `&mut self`). Leaf expansion (`compute_leaves`)
/// is the caller's job, so the caller fully controls how/where each leaf's arrays come from —
/// this is the primitive the vertical-merge path drives directly, leaf-for-leaf against the
/// reader.
///
/// SAFETY invariant: `row_group` borrows `*file_writer`. `file_writer` is boxed so its address
/// is stable across moves of this struct, and is declared after `row_group` so it is dropped
/// last (Rust drops fields top-to-bottom). While `row_group` is `Some`, nothing else may touch
/// `*file_writer`; [`Self::finish`] takes and closes `row_group` before reclaiming the file
/// writer.
///
/// Generic over the sink `W`: callers pass any `io::Write` (e.g. an in-memory buffer, or a
/// streaming IO writer). [`Self::new`] defaults to the chunked in-memory buffer;
/// [`Self::create`] takes an arbitrary sink. `W: Default` lets [`Self::finish`] move the
/// finished sink out via `mem::take` after the footer is written.
pub struct BulkBlockParquetWriter<W: io::Write + Send + Default + 'static = ChunkedWriteBuffer> {
    leaf_kinds: Vec<LeafEncoderKind>,
    next_leaf: usize,
    row_group: Option<SerializedRowGroupWriter<'static, W>>,
    file_writer: Box<SerializedFileWriter<W>>,
}

impl BulkBlockParquetWriter<ChunkedWriteBuffer> {
    /// Construct a writer backed by the in-memory [`ChunkedWriteBuffer`]. [`Self::finish`] returns
    /// the buffer, whose bytes the caller reads via [`ChunkedWriteBuffer::into_chunks`].
    pub fn new(arrow_schema: Arc<Schema>, props: WriterPropertiesPtr) -> Result<Self> {
        Self::create(
            ChunkedWriteBuffer::new(DEFAULT_CHUNK_SIZE),
            arrow_schema,
            props,
        )
    }
}

impl<W: io::Write + Send + Default + 'static> BulkBlockParquetWriter<W> {
    /// Construct a writer that streams the serialized parquet into the caller-provided `sink`.
    /// The footer is written on [`Self::finish`], which returns the metadata plus the sink moved
    /// back out.
    pub fn create(sink: W, arrow_schema: Arc<Schema>, props: WriterPropertiesPtr) -> Result<Self> {
        let parquet_schema = ArrowSchemaConverter::new()
            .with_coerce_types(props.coerce_types())
            .convert(&arrow_schema)?;
        let root = parquet_schema.root_schema_ptr();

        // Embed the IPC-encoded Arrow schema under `ARROW:schema`, mirroring `ArrowWriter::try_new`.
        // `SerializedFileWriter` does not inject it, so without this Databend extension-backed
        // types (Variant, Bitmap, Geometry, ...) — stored as plain `LargeBinary`/`Decimal128` —
        // would be unrecoverable for any reader that reconstructs types from the file's own schema.
        let mut props = (*props).clone();
        add_encoded_arrow_schema_to_metadata(&arrow_schema, &mut props);

        let mut file_writer = Box::new(SerializedFileWriter::new(sink, root, Arc::new(props))?);
        let leaf_kinds = classify_schema(&arrow_schema);
        debug_assert_eq!(leaf_kinds.len(), parquet_schema.num_columns());

        // Open the single row group up front and store it as a self-reference into the boxed
        // file writer. SAFETY: the `&mut` is taken through a raw pointer so its borrow is not
        // tracked against `file_writer` (letting us move the box into the struct below). The
        // borrow is really bounded by `*file_writer`, which is heap-stable and outlives
        // `row_group` (drop order); `row_group` is the sole accessor until `finish` closes it.
        let fw_ptr: *mut SerializedFileWriter<W> = &mut *file_writer;
        let row_group = unsafe { (*fw_ptr).next_row_group() }?;

        Ok(Self {
            leaf_kinds,
            next_leaf: 0,
            row_group: Some(row_group),
            file_writer,
        })
    }

    /// Number of parquet leaf columns this writer expects, one [`LeafColumnWriter`] each.
    pub fn num_leaves(&self) -> usize {
        self.leaf_kinds.len()
    }

    /// Open the next leaf column, or `None` once all declared leaves have been written. The
    /// returned writer borrows `&mut self`, so it must be closed/dropped before calling again —
    /// which is exactly the parquet "one open leaf at a time" rule, enforced at compile time.
    ///
    /// Errors if called more than [`Self::num_leaves`] times — the caller drives a known number
    /// of leaves, so overrunning is a programming error rather than a normal end-of-iteration.
    pub fn next_leaf(&mut self) -> Result<LeafColumnWriter<'_>> {
        if self.next_leaf >= self.leaf_kinds.len() {
            return Err(ErrorCode::Internal(format!(
                "next_leaf called {} times but the schema declares only {} leaf columns",
                self.next_leaf + 1,
                self.leaf_kinds.len()
            )));
        }

        let kind = self.leaf_kinds[self.next_leaf];
        let row_group = self
            .row_group
            .as_mut()
            .expect("row group stays open until finish");

        let writer = row_group
            .next_column_with_factory(move |descr, props, page_writer, on_close| {
                Ok(match kind {
                    LeafEncoderKind::ByteArray => LeafColumnWriter::ByteArray {
                        writer: GenericColumnWriter::<ByteArrayEncoder>::new(
                            descr,
                            props,
                            page_writer,
                        ),
                        on_close,
                    },
                    LeafEncoderKind::Column => LeafColumnWriter::Column {
                        writer: get_column_writer(descr, props, page_writer),
                        on_close,
                    },
                })
            })?
            .ok_or_else(|| {
                ErrorCode::Internal(
                    "parquet row group exhausted its leaf columns ahead of the schema",
                )
            })?;

        self.next_leaf += 1;
        Ok(writer)
    }

    /// Close the row group, write the footer, and return the parquet metadata plus the sink moved
    /// back out. The sink now holds the complete serialized parquet file; the caller reads the
    /// bytes from it (e.g. [`ChunkedWriteBuffer::into_chunks`]).
    pub fn finish(mut self) -> Result<(ParquetMetaData, W)> {
        // Close the row group first: this ends its borrow of `*file_writer` (and flushes the
        // row group metadata into it), making the file writer safe to access again.
        if let Some(row_group) = self.row_group.take() {
            row_group.close()?;
        }

        let metadata = self.file_writer.finish()?;
        let sink = std::mem::take(self.file_writer.inner_mut());
        Ok((metadata, sink))
    }
}

/// A single open parquet leaf column. [`Self::write`] encodes an `ArrowLeafColumn` directly
/// into the leaf's page writer (pages flushed to the sink as they fill, no chunk buffer);
/// [`Self::close`] finalizes the column chunk. Picks the specialized [`ByteArrayEncoder`] or
/// the generic column encoder per the schema's leaf classification.
pub enum LeafColumnWriter<'a> {
    ByteArray {
        writer: GenericColumnWriter<'a, ByteArrayEncoder>,
        on_close: OnCloseColumnChunk<'a>,
    },
    Column {
        writer: ColumnWriter<'a>,
        on_close: OnCloseColumnChunk<'a>,
    },
}

impl LeafColumnWriter<'_> {
    /// Encode one `ArrowLeafColumn` fragment into this leaf. Call repeatedly to stream multiple
    /// fragments (e.g. one per source block) into the same column.
    pub fn write(&mut self, leaf: &ArrowLeafColumn) -> Result<()> {
        match self {
            LeafColumnWriter::ByteArray { writer, .. } => {
                write_byte_array_column(writer, leaf)?;
            }
            LeafColumnWriter::Column { writer, .. } => {
                write_leaf_column(writer, leaf)?;
            }
        }
        Ok(())
    }

    /// Close this leaf, flushing its column chunk into the row group.
    pub fn close(self) -> Result<()> {
        match self {
            LeafColumnWriter::ByteArray { writer, on_close } => on_close(writer.close()?)?,
            LeafColumnWriter::Column { writer, on_close } => on_close(writer.close()?)?,
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunked_write_buffer() {
        use std::io::Write;

        // Chunk size 4: writes that span chunk boundaries must still reassemble exactly,
        // and a single write larger than the chunk size must be accepted in one chunk.
        let mut buf = ChunkedWriteBuffer::new(4);
        buf.write_all(b"ab").unwrap(); // partial first chunk
        buf.write_all(b"cde").unwrap(); // spills into a second chunk
        buf.write_all(b"fghijklm").unwrap(); // larger than chunk_size in one write
        assert_eq!(buf.total_bytes, 13);
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
