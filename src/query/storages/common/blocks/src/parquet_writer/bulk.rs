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
//! fork) so compressed pages are flushed to the writer as they fill, instead of buffering whole
//! column chunks in memory like `ArrowWriter` does. The caller supplies any [`BlockingWrite`];
//! [`BulkParquetFileWriter::finish`] writes the footer, closes the writer, and returns it alongside
//! the metadata so callers can inspect writer-specific output.

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

/// Default chunk size for [`MemoryBlockingWrite`]: 4 MiB.
pub const DEFAULT_CHUNK_SIZE: usize = 4 * 1024 * 1024;

/// An in-memory `Write` implementation backed by fixed-size chunks instead of one growing `Vec<u8>`.
///
/// `SerializedFileWriter` needs a `W: Write`. Backing it with a single `Vec<u8>` means every
/// time the vector outgrows its capacity it reallocates and copies *all* bytes written so
/// far — for a multi-hundred-MB row group that is repeated large memcpys plus transient 2x
/// peak memory. Appending into 4 MiB chunks avoids both: existing bytes are never moved, and
/// growth costs one chunk allocation. At finish the chunks are handed out as-is via
/// [`Self::into_chunks`] (each `Vec<u8>` becomes a `Bytes` with no copy), so the serialized
/// payload can travel to IO non-contiguously without ever being consolidated.
pub struct MemoryBlockingWrite {
    chunk_size: usize,
    chunks: Vec<Vec<u8>>,
    total_bytes: usize,
}

impl MemoryBlockingWrite {
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

impl Default for MemoryBlockingWrite {
    fn default() -> Self {
        Self::new(DEFAULT_CHUNK_SIZE)
    }
}

impl io::Write for MemoryBlockingWrite {
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

impl BlockingWrite for MemoryBlockingWrite {
    fn close(&mut self) -> Result<()> {
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

/// A blocking writer whose output must be explicitly committed after all bytes are written.
pub trait BlockingWrite: io::Write + Send {
    fn close(&mut self) -> Result<()>;
}

/// Private holder that lets the Parquet writer return `W` without requiring `W: Default`.
struct WriteSlot<W>(Option<W>);

impl<W> WriteSlot<W> {
    fn new(writer: W) -> Self {
        Self(Some(writer))
    }

    fn close_and_take(&mut self) -> Result<W>
    where W: BlockingWrite {
        let Some(writer) = self.0.as_mut() else {
            return Err(ErrorCode::Internal(
                "Parquet output writer has already been taken",
            ));
        };
        writer.close()?;
        Ok(self
            .0
            .take()
            .expect("Parquet output writer exists after close"))
    }
}

impl<W: io::Write> io::Write for WriteSlot<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let Some(writer) = self.0.as_mut() else {
            return Err(io::ErrorKind::BrokenPipe.into());
        };
        writer.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        let Some(writer) = self.0.as_mut() else {
            return Err(io::ErrorKind::BrokenPipe.into());
        };
        writer.flush()
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
/// Drive it leaf by leaf: [`Self::next_leaf`] consumes the file writer and returns an owning
/// [`BulkParquetLeafWriter`]. Feed it `ArrowLeafColumn`s via
/// [`BulkParquetLeafWriter::write`], then finish the leaf to recover the file writer before opening
/// the next; finally [`Self::finish`] writes the footer. Each write encodes straight into the open
/// leaf's page writer, flushing pages to the output writer as they fill — no per-column chunk buffer
/// (unlike `ArrowWriter`). The consuming API encodes the "one open leaf at a time" rule in the
/// type state and lets an active leaf live across caller method boundaries.
///
/// SAFETY invariant: `row_group` borrows `*file_writer`. Both are boxed so their addresses remain
/// stable when the owning state moves. While `row_group` is present, nothing else may touch
/// `*file_writer`; [`Self::finish`] closes it before reclaiming the file writer. An active
/// [`BulkParquetLeafWriter`] additionally borrows the boxed row group and is always dropped or
/// finished before its parent, preserving `leaf -> row_group -> file_writer` destruction order.
///
/// Generic over `W`: callers pass any [`BlockingWrite`] implementation, such as an in-memory chunk
/// writer or a streaming IO writer. [`Self::finish`] closes the writer before taking it from the
/// private writer slot; the returned writer exposes implementation-specific completed output.
pub struct BulkParquetFileWriter<W: BlockingWrite + 'static = MemoryBlockingWrite> {
    leaf_kinds: Vec<LeafEncoderKind>,
    next_leaf: usize,
    row_group: Option<Box<SerializedRowGroupWriter<'static, WriteSlot<W>>>>,
    file_writer: Box<SerializedFileWriter<WriteSlot<W>>>,
}

impl BulkParquetFileWriter<MemoryBlockingWrite> {
    /// Construct a writer backed by the in-memory [`MemoryBlockingWrite`]. [`Self::finish`]
    /// returns the writer, whose bytes the caller reads via [`MemoryBlockingWrite::into_chunks`].
    pub fn new(arrow_schema: Arc<Schema>, props: WriterPropertiesPtr) -> Result<Self> {
        Self::create(
            MemoryBlockingWrite::new(DEFAULT_CHUNK_SIZE),
            arrow_schema,
            props,
        )
    }
}

impl<W: BlockingWrite + 'static> BulkParquetFileWriter<W> {
    /// Construct a Parquet file writer backed by the caller-provided output writer. The footer is
    /// written on [`Self::finish`], which closes and returns the output writer with the metadata.
    pub fn create(
        writer: W,
        arrow_schema: Arc<Schema>,
        props: WriterPropertiesPtr,
    ) -> Result<Self> {
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

        let mut file_writer = Box::new(SerializedFileWriter::new(
            WriteSlot::new(writer),
            root,
            Arc::new(props),
        )?);
        let leaf_kinds = classify_schema(&arrow_schema);
        debug_assert_eq!(leaf_kinds.len(), parquet_schema.num_columns());

        // Open the single row group up front and store it as a self-reference into the boxed
        // file writer. SAFETY: the `&mut` is taken through a raw pointer so its borrow is not
        // tracked against `file_writer` (letting us move the box into the struct below). The
        // borrow is really bounded by `*file_writer`, which is heap-stable and outlives
        // `row_group` (drop order); `row_group` is the sole accessor until `finish` closes it.
        let fw_ptr: *mut SerializedFileWriter<WriteSlot<W>> = &mut *file_writer;
        let row_group = unsafe { (*fw_ptr).next_row_group() }?;

        Ok(Self {
            leaf_kinds,
            next_leaf: 0,
            row_group: Some(Box::new(row_group)),
            file_writer,
        })
    }

    /// Number of parquet leaf columns this writer expects, one [`LeafColumnWriter`] each.
    pub fn num_leaves(&self) -> usize {
        self.leaf_kinds.len()
    }

    /// Consume this file state and open the next leaf column. Finishing the returned leaf hands the
    /// file state back, making it impossible to open another leaf or finish the file while a leaf is
    /// active.
    pub fn next_leaf(mut self) -> Result<BulkParquetLeafWriter<W>> {
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

        let leaf = row_group
            .next_column_with_factory(move |descr, props, page_writer, on_close| {
                Ok(match kind {
                    LeafEncoderKind::ByteArray => RawLeafColumnWriter::ByteArray {
                        writer: GenericColumnWriter::<ByteArrayEncoder>::new(
                            descr,
                            props,
                            page_writer,
                        ),
                        on_close,
                    },
                    LeafEncoderKind::Column => RawLeafColumnWriter::Column {
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

        // SAFETY: `leaf` borrows the boxed row group (and, through its page writer, the boxed file
        // writer). Both boxes move with `self` without moving their allocations. The owning leaf
        // state declares `leaf` before `parent`, so the borrow is destroyed first; its public API
        // cannot expose either owner while the leaf is alive.
        let leaf = unsafe {
            std::mem::transmute::<RawLeafColumnWriter<'_>, RawLeafColumnWriter<'static>>(leaf)
        };
        self.next_leaf += 1;
        Ok(BulkParquetLeafWriter {
            leaf: Some(leaf),
            parent: Some(self),
        })
    }

    /// Close the row group, write the footer, close the output writer, and return both the Parquet
    /// metadata and completed writer. Metadata is returned only after close succeeds.
    pub fn finish(mut self) -> Result<(ParquetMetaData, W)> {
        if self.next_leaf != self.leaf_kinds.len() {
            return Err(ErrorCode::Internal(format!(
                "cannot finish parquet file: wrote {} of {} leaf columns",
                self.next_leaf,
                self.leaf_kinds.len()
            )));
        }

        // Close the row group first: this ends its borrow of `*file_writer` (and flushes the
        // row group metadata into it), making the file writer safe to access again.
        if let Some(row_group) = self.row_group.take() {
            row_group.close()?;
        }

        let metadata = self.file_writer.finish()?;
        let writer = self.file_writer.inner_mut().close_and_take()?;
        Ok((metadata, writer))
    }
}

/// Internal borrowed leaf state. It is wrapped by [`BulkParquetLeafWriter`] so callers interact
/// with an owning typestate rather than a self-referential borrow.
enum RawLeafColumnWriter<'a> {
    ByteArray {
        writer: GenericColumnWriter<'a, ByteArrayEncoder>,
        on_close: OnCloseColumnChunk<'a>,
    },
    Column {
        writer: ColumnWriter<'a>,
        on_close: OnCloseColumnChunk<'a>,
    },
}

impl RawLeafColumnWriter<'_> {
    fn write(&mut self, leaf: &ArrowLeafColumn) -> Result<()> {
        match self {
            RawLeafColumnWriter::ByteArray { writer, .. } => {
                write_byte_array_column(writer, leaf)?;
            }
            RawLeafColumnWriter::Column { writer, .. } => {
                write_leaf_column(writer, leaf)?;
            }
        }
        Ok(())
    }

    fn flush_page(&mut self) -> Result<()> {
        match self {
            RawLeafColumnWriter::ByteArray { writer, .. } => writer.flush_data_page()?,
            RawLeafColumnWriter::Column { writer, .. } => writer.flush_data_page()?,
        }
        Ok(())
    }

    fn finish(self) -> Result<()> {
        match self {
            RawLeafColumnWriter::ByteArray { writer, on_close } => on_close(writer.close()?)?,
            RawLeafColumnWriter::Column { writer, on_close } => on_close(writer.close()?)?,
        }
        Ok(())
    }
}

/// Owning active-leaf state returned by [`BulkParquetFileWriter::next_leaf`]. Finish it to recover
/// the parent file writer. This allows an incremental producer to retain the active leaf and flush
/// explicit page boundaries across calls without exposing self-referential lifetimes.
pub struct BulkParquetLeafWriter<W: BlockingWrite + 'static = MemoryBlockingWrite> {
    // Drop order is significant: the active leaf borrows allocations owned by `parent`.
    leaf: Option<RawLeafColumnWriter<'static>>,
    parent: Option<BulkParquetFileWriter<W>>,
}

// SAFETY: `RawLeafColumnWriter`'s erased close callback only mutates row-group metadata owned by the
// boxed parent. The leaf has exclusive access, both borrowed allocations are stable across moves,
// and field/drop order destroys the leaf before the parent. All other components are `Send` when W
// is `Send`.
unsafe impl<W: BlockingWrite + 'static> Send for BulkParquetLeafWriter<W> {}

impl<W: BlockingWrite + 'static> BulkParquetLeafWriter<W> {
    /// Encode one `ArrowLeafColumn` fragment into this leaf.
    pub fn write(&mut self, leaf: &ArrowLeafColumn) -> Result<()> {
        self.leaf.as_mut().expect("active leaf").write(leaf)
    }

    /// Force the values buffered since the previous boundary into one data page. No-op when empty.
    pub fn flush_page(&mut self) -> Result<()> {
        self.leaf.as_mut().expect("active leaf").flush_page()
    }

    /// Close this column chunk and return the parent file state.
    pub fn finish(mut self) -> Result<BulkParquetFileWriter<W>> {
        self.leaf.take().expect("active leaf").finish()?;
        Ok(self.parent.take().expect("parent writer"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct CloseTrackingWrite {
        bytes: Vec<u8>,
        closed: Arc<std::sync::atomic::AtomicBool>,
    }

    impl io::Write for CloseTrackingWrite {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.bytes.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl BlockingWrite for CloseTrackingWrite {
        fn close(&mut self) -> Result<()> {
            self.closed.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn test_finish_closes_writer() {
        let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let output = CloseTrackingWrite {
            bytes: Vec::new(),
            closed: closed.clone(),
        };
        let schema = Arc::new(Schema::empty());
        let props = Arc::new(parquet::file::properties::WriterProperties::builder().build());
        let writer = BulkParquetFileWriter::create(output, schema, props).unwrap();

        let (metadata, output) = writer.finish().unwrap();

        assert!(closed.load(std::sync::atomic::Ordering::SeqCst));
        assert!(!output.bytes.is_empty());
        assert!(!metadata.row_groups().is_empty());
    }

    #[derive(Debug, Default)]
    struct FailingCloseWrite(Vec<u8>);

    impl io::Write for FailingCloseWrite {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl BlockingWrite for FailingCloseWrite {
        fn close(&mut self) -> Result<()> {
            Err(ErrorCode::Internal("close failed"))
        }
    }

    #[test]
    fn test_finish_propagates_close_failure() {
        let schema = Arc::new(Schema::empty());
        let props = Arc::new(parquet::file::properties::WriterProperties::builder().build());
        let writer =
            BulkParquetFileWriter::create(FailingCloseWrite::default(), schema, props).unwrap();

        let error = writer.finish().unwrap_err();

        assert!(error.message().contains("close failed"));
    }

    #[test]
    fn test_memory_blocking_write() {
        use std::io::Write;

        // Chunk size 4: writes that span chunk boundaries must still reassemble exactly,
        // and a single write larger than the chunk size must be accepted in one chunk.
        let mut buf = MemoryBlockingWrite::new(4);
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
        let mut single = MemoryBlockingWrite::new(4);
        single.write_all(b"xy").unwrap();
        let chunks = single.into_chunks();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks.concat(), b"xy");

        // Empty buffer yields no chunks.
        assert!(MemoryBlockingWrite::new(4).into_chunks().is_empty());
    }
}
