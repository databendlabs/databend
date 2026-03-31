// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use core::panic;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use arrow_array::RecordBatch;

use arrow_data::ArrayData;
use bytes::{BufMut, Bytes, BytesMut};
use futures::stream::FuturesOrdered;
use futures::StreamExt;
use lance_core::datatypes::{Field, Schema as LanceSchema};
use lance_core::utils::bit::pad_bytes;
use lance_core::{Error, Result};
use lance_encoding::decoder::PageEncoding;
use lance_encoding::encoder::{
    default_encoding_strategy, BatchEncoder, EncodeTask, EncodedBatch, EncodedPage,
    EncodingOptions, FieldEncoder, FieldEncodingStrategy, OutOfLineBuffers,
};
use lance_encoding::repdef::RepDefBuilder;
use lance_encoding::version::LanceFileVersion;
use lance_io::object_store::ObjectStore;
use lance_io::object_writer::ObjectWriter;
use lance_io::traits::Writer;
use log::{debug, warn};
use object_store::path::Path;
use prost::Message;
use prost_types::Any;
use snafu::location;
use tokio::io::AsyncWriteExt;
use tracing::instrument;

use crate::datatypes::FieldsWithMeta;
use crate::format::pb;
use crate::format::pbfile;
use crate::format::pbfile::DirectEncoding;
use crate::format::MAGIC;

/// Pages buffers are aligned to 64 bytes
pub(crate) const PAGE_BUFFER_ALIGNMENT: usize = 64;
const PAD_BUFFER: [u8; PAGE_BUFFER_ALIGNMENT] = [72; PAGE_BUFFER_ALIGNMENT];
// In 2.1+, we split large pages on read instead of write to avoid empty pages
// and small pages issues. However, we keep the write-time limit at 32MB to avoid
// potential regressions in 2.0 format readers.
//
// This limit is not applied in the 2.1 writer
const MAX_PAGE_BYTES: usize = 32 * 1024 * 1024;
const ENV_LANCE_FILE_WRITER_MAX_PAGE_BYTES: &str = "LANCE_FILE_WRITER_MAX_PAGE_BYTES";

#[derive(Debug, Clone, Default)]
pub struct FileWriterOptions {
    /// How many bytes to use for buffering column data
    ///
    /// When data comes in small batches the writer will buffer column data so that
    /// larger pages can be created.  This value will be divided evenly across all of the
    /// columns.  Generally you want this to be at least large enough to match your
    /// filesystem's ideal read size per column.
    ///
    /// In some cases you might want this value to be even larger if you have highly
    /// compressible data.  However, if this is too large, then the writer could require
    /// a lot of memory and write performance may suffer if the CPU-expensive encoding
    /// falls behind and can't be interleaved with the I/O expensive flushing.
    ///
    /// The default will use 8MiB per column which should be reasonable for most cases.
    // TODO: Do we need to be able to set this on a per-column basis?
    pub data_cache_bytes: Option<u64>,
    /// A hint to indicate the max size of a page
    ///
    /// This hint can't always be respected.  A single value could be larger than this value
    /// and we never slice single values.  In addition, there are some cases where it can be
    /// difficult to know size up-front and so we might not be able to respect this value.
    pub max_page_bytes: Option<u64>,
    /// The file writer buffers columns until enough data has arrived to flush a page
    /// to disk.
    ///
    /// Some columns with small data types may not flush very often.  These arrays can
    /// stick around for a long time.  These arrays might also be keeping larger data
    /// structures alive.  By default, the writer will make a deep copy of this array
    /// to avoid any potential memory leaks.  However, this can be disabled for a
    /// (probably minor) performance boost if you are sure that arrays are not keeping
    /// any sibling structures alive (this typically means the array was allocated in
    /// the same language / runtime as the writer)
    ///
    /// Do not enable this if your data is arriving from the C data interface.
    /// Data typically arrives one "batch" at a time (encoded in the C data interface
    /// as a struct array).  Each array in that batch keeps the entire batch alive.
    /// This means a small boolean array (which we will buffer in memory for quite a
    /// while) might keep a much larger record batch around in memory (even though most
    /// of that batch's data has been written to disk)
    pub keep_original_array: Option<bool>,
    pub encoding_strategy: Option<Arc<dyn FieldEncodingStrategy>>,
    /// The format version to use when writing the file
    ///
    /// This controls which encodings will be used when encoding the data.  Newer
    /// versions may have more efficient encodings.  However, newer format versions will
    /// require more up-to-date readers to read the data.
    pub format_version: Option<LanceFileVersion>,
}

pub struct FileWriter {
    writer: ObjectWriter,
    schema: Option<LanceSchema>,
    column_writers: Vec<Box<dyn FieldEncoder>>,
    column_metadata: Vec<pbfile::ColumnMetadata>,
    field_id_to_column_indices: Vec<(u32, u32)>,
    num_columns: u32,
    rows_written: u64,
    global_buffers: Vec<(u64, u64)>,
    schema_metadata: HashMap<String, String>,
    options: FileWriterOptions,
}

fn initial_column_metadata() -> pbfile::ColumnMetadata {
    pbfile::ColumnMetadata {
        pages: Vec::new(),
        buffer_offsets: Vec::new(),
        buffer_sizes: Vec::new(),
        encoding: None,
    }
}

static WARNED_ON_UNSTABLE_API: AtomicBool = AtomicBool::new(false);

impl FileWriter {
    /// Create a new FileWriter with a desired output schema
    pub fn try_new(
        object_writer: ObjectWriter,
        schema: LanceSchema,
        options: FileWriterOptions,
    ) -> Result<Self> {
        let mut writer = Self::new_lazy(object_writer, options);
        writer.initialize(schema)?;
        Ok(writer)
    }

    /// Create a new FileWriter without a desired output schema
    ///
    /// The output schema will be set based on the first batch of data to arrive.
    /// If no data arrives and the writer is finished then the write will fail.
    pub fn new_lazy(object_writer: ObjectWriter, options: FileWriterOptions) -> Self {
        if let Some(format_version) = options.format_version {
            if format_version.is_unstable()
                && WARNED_ON_UNSTABLE_API
                    .compare_exchange(
                        false,
                        true,
                        std::sync::atomic::Ordering::Relaxed,
                        std::sync::atomic::Ordering::Relaxed,
                    )
                    .is_ok()
            {
                warn!("You have requested an unstable format version.  Files written with this format version may not be readable in the future!  This is a development feature and should only be used for experimentation and never for production data.");
            }
        }
        Self {
            writer: object_writer,
            schema: None,
            column_writers: Vec::new(),
            column_metadata: Vec::new(),
            num_columns: 0,
            rows_written: 0,
            field_id_to_column_indices: Vec::new(),
            global_buffers: Vec::new(),
            schema_metadata: HashMap::new(),
            options,
        }
    }

    /// Write a series of record batches to a new file
    ///
    /// Returns the number of rows written
    pub async fn create_file_with_batches(
        store: &ObjectStore,
        path: &Path,
        schema: lance_core::datatypes::Schema,
        batches: impl Iterator<Item = RecordBatch> + Send,
        options: FileWriterOptions,
    ) -> Result<usize> {
        let writer = store.create(path).await?;
        let mut writer = Self::try_new(writer, schema, options)?;
        for batch in batches {
            writer.write_batch(&batch).await?;
        }
        Ok(writer.finish().await? as usize)
    }

    async fn do_write_buffer(writer: &mut ObjectWriter, buf: &[u8]) -> Result<()> {
        writer.write_all(buf).await?;
        let pad_bytes = pad_bytes::<PAGE_BUFFER_ALIGNMENT>(buf.len());
        writer.write_all(&PAD_BUFFER[..pad_bytes]).await?;
        Ok(())
    }

    /// Returns the format version that will be used when writing the file
    pub fn version(&self) -> LanceFileVersion {
        self.options.format_version.unwrap_or_default()
    }

    async fn write_page(&mut self, encoded_page: EncodedPage) -> Result<()> {
        let buffers = encoded_page.data;
        let mut buffer_offsets = Vec::with_capacity(buffers.len());
        let mut buffer_sizes = Vec::with_capacity(buffers.len());
        for buffer in buffers {
            buffer_offsets.push(self.writer.tell().await? as u64);
            buffer_sizes.push(buffer.len() as u64);
            Self::do_write_buffer(&mut self.writer, &buffer).await?;
        }
        let encoded_encoding = match encoded_page.description {
            PageEncoding::Legacy(array_encoding) => Any::from_msg(&array_encoding)?.encode_to_vec(),
            PageEncoding::Structural(page_layout) => Any::from_msg(&page_layout)?.encode_to_vec(),
        };
        let page = pbfile::column_metadata::Page {
            buffer_offsets,
            buffer_sizes,
            encoding: Some(pbfile::Encoding {
                location: Some(pbfile::encoding::Location::Direct(DirectEncoding {
                    encoding: encoded_encoding,
                })),
            }),
            length: encoded_page.num_rows,
            priority: encoded_page.row_number,
        };
        self.column_metadata[encoded_page.column_idx as usize]
            .pages
            .push(page);
        Ok(())
    }

    #[instrument(skip_all, level = "debug")]
    async fn write_pages(&mut self, mut encoding_tasks: FuturesOrdered<EncodeTask>) -> Result<()> {
        // As soon as an encoding task is done we write it.  There is no parallelism
        // needed here because "writing" is really just submitting the buffer to the
        // underlying write scheduler (either the OS or object_store's scheduler for
        // cloud writes).  The only time we might truly await on write_page is if the
        // scheduler's write queue is full.
        //
        // Also, there is no point in trying to make write_page parallel anyways
        // because we wouldn't want buffers getting mixed up across pages.
        while let Some(encoding_task) = encoding_tasks.next().await {
            let encoded_page = encoding_task?;
            self.write_page(encoded_page).await?;
        }
        // It's important to flush here, we don't know when the next batch will arrive
        // and the underlying cloud store could have writes in progress that won't advance
        // until we interact with the writer again.  These in-progress writes will time out
        // if we don't flush.
        self.writer.flush().await?;
        Ok(())
    }

    /// Schedule batches of data to be written to the file
    pub async fn write_batches(
        &mut self,
        batches: impl Iterator<Item = &RecordBatch>,
    ) -> Result<()> {
        for batch in batches {
            self.write_batch(batch).await?;
        }
        Ok(())
    }

    fn verify_field_nullability(arr: &ArrayData, field: &Field) -> Result<()> {
        if !field.nullable && arr.null_count() > 0 {
            return Err(Error::invalid_input(format!("The field `{}` contained null values even though the field is marked non-null in the schema", field.name), location!()));
        }

        for (child_field, child_arr) in field.children.iter().zip(arr.child_data()) {
            Self::verify_field_nullability(child_arr, child_field)?;
        }

        Ok(())
    }

    fn verify_nullability_constraints(&self, batch: &RecordBatch) -> Result<()> {
        for (col, field) in batch
            .columns()
            .iter()
            .zip(self.schema.as_ref().unwrap().fields.iter())
        {
            Self::verify_field_nullability(&col.to_data(), field)?;
        }
        Ok(())
    }

    fn initialize(&mut self, mut schema: LanceSchema) -> Result<()> {
        let cache_bytes_per_column = if let Some(data_cache_bytes) = self.options.data_cache_bytes {
            data_cache_bytes / schema.fields.len() as u64
        } else {
            8 * 1024 * 1024
        };

        let max_page_bytes = self.options.max_page_bytes.unwrap_or_else(|| {
            std::env::var(ENV_LANCE_FILE_WRITER_MAX_PAGE_BYTES)
                .map(|s| {
                    s.parse::<u64>().unwrap_or_else(|e| {
                        warn!(
                            "Failed to parse {}: {}, using default",
                            ENV_LANCE_FILE_WRITER_MAX_PAGE_BYTES, e
                        );
                        MAX_PAGE_BYTES as u64
                    })
                })
                .unwrap_or(MAX_PAGE_BYTES as u64)
        });

        schema.validate()?;

        let keep_original_array = self.options.keep_original_array.unwrap_or(false);
        let encoding_strategy = self.options.encoding_strategy.clone().unwrap_or_else(|| {
            let version = self.version();
            default_encoding_strategy(version).into()
        });

        let encoding_options = EncodingOptions {
            cache_bytes_per_column,
            max_page_bytes,
            keep_original_array,
            buffer_alignment: PAGE_BUFFER_ALIGNMENT as u64,
        };
        let encoder =
            BatchEncoder::try_new(&schema, encoding_strategy.as_ref(), &encoding_options)?;
        self.num_columns = encoder.num_columns();

        self.column_writers = encoder.field_encoders;
        self.column_metadata = vec![initial_column_metadata(); self.num_columns as usize];
        self.field_id_to_column_indices = encoder.field_id_to_column_index;
        self.schema_metadata
            .extend(std::mem::take(&mut schema.metadata));
        self.schema = Some(schema);
        Ok(())
    }

    fn ensure_initialized(&mut self, batch: &RecordBatch) -> Result<&LanceSchema> {
        if self.schema.is_none() {
            let schema = LanceSchema::try_from(batch.schema().as_ref())?;
            self.initialize(schema)?;
        }
        Ok(self.schema.as_ref().unwrap())
    }

    #[instrument(skip_all, level = "debug")]
    fn encode_batch(
        &mut self,
        batch: &RecordBatch,
        external_buffers: &mut OutOfLineBuffers,
    ) -> Result<Vec<Vec<EncodeTask>>> {
        self.schema
            .as_ref()
            .unwrap()
            .fields
            .iter()
            .zip(self.column_writers.iter_mut())
            .map(|(field, column_writer)| {
                let array = batch
                    .column_by_name(&field.name)
                    .ok_or(Error::InvalidInput {
                        source: format!(
                            "Cannot write batch.  The batch was missing the column `{}`",
                            field.name
                        )
                        .into(),
                        location: location!(),
                    })?;
                let repdef = RepDefBuilder::default();
                let num_rows = array.len() as u64;
                column_writer.maybe_encode(
                    array.clone(),
                    external_buffers,
                    repdef,
                    self.rows_written,
                    num_rows,
                )
            })
            .collect::<Result<Vec<_>>>()
    }

    /// Schedule a batch of data to be written to the file
    ///
    /// Note: the future returned by this method may complete before the data has been fully
    /// flushed to the file (some data may be in the data cache or the I/O cache)
    pub async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        debug!(
            "write_batch called with {} rows, {} columns, and {} bytes of data",
            batch.num_rows(),
            batch.num_columns(),
            batch.get_array_memory_size()
        );
        self.ensure_initialized(batch)?;
        self.verify_nullability_constraints(batch)?;
        let num_rows = batch.num_rows() as u64;
        if num_rows == 0 {
            return Ok(());
        }
        if num_rows > u32::MAX as u64 {
            return Err(Error::InvalidInput {
                source: "cannot write Lance files with more than 2^32 rows".into(),
                location: location!(),
            });
        }
        // First we push each array into its column writer.  This may or may not generate enough
        // data to trigger an encoding task.  We collect any encoding tasks into a queue.
        let mut external_buffers =
            OutOfLineBuffers::new(self.tell().await?, PAGE_BUFFER_ALIGNMENT as u64);
        let encoding_tasks = self.encode_batch(batch, &mut external_buffers)?;
        // Next, write external buffers
        for external_buffer in external_buffers.take_buffers() {
            Self::do_write_buffer(&mut self.writer, &external_buffer).await?;
        }

        let encoding_tasks = encoding_tasks
            .into_iter()
            .flatten()
            .collect::<FuturesOrdered<_>>();

        self.rows_written = match self.rows_written.checked_add(batch.num_rows() as u64) {
            Some(rows_written) => rows_written,
            None => {
                return Err(Error::InvalidInput { source: format!("cannot write batch with {} rows because {} rows have already been written and Lance files cannot contain more than 2^64 rows", num_rows, self.rows_written).into(), location: location!() });
            }
        };

        self.write_pages(encoding_tasks).await?;

        Ok(())
    }

    async fn write_column_metadata(
        &mut self,
        metadata: pbfile::ColumnMetadata,
    ) -> Result<(u64, u64)> {
        let metadata_bytes = metadata.encode_to_vec();
        let position = self.writer.tell().await? as u64;
        let len = metadata_bytes.len() as u64;
        self.writer.write_all(&metadata_bytes).await?;
        Ok((position, len))
    }

    async fn write_column_metadatas(&mut self) -> Result<Vec<(u64, u64)>> {
        let mut metadatas = Vec::new();
        std::mem::swap(&mut self.column_metadata, &mut metadatas);
        let mut metadata_positions = Vec::with_capacity(metadatas.len());
        for metadata in metadatas {
            metadata_positions.push(self.write_column_metadata(metadata).await?);
        }
        Ok(metadata_positions)
    }

    fn make_file_descriptor(
        schema: &lance_core::datatypes::Schema,
        num_rows: u64,
    ) -> Result<pb::FileDescriptor> {
        let fields_with_meta = FieldsWithMeta::from(schema);
        Ok(pb::FileDescriptor {
            schema: Some(pb::Schema {
                fields: fields_with_meta.fields.0,
                metadata: fields_with_meta.metadata,
            }),
            length: num_rows,
        })
    }

    async fn write_global_buffers(&mut self) -> Result<Vec<(u64, u64)>> {
        let schema = self.schema.as_mut().ok_or(Error::invalid_input("No schema provided on writer open and no data provided.  Schema is unknown and file cannot be created", location!()))?;
        schema.metadata = std::mem::take(&mut self.schema_metadata);
        let file_descriptor = Self::make_file_descriptor(schema, self.rows_written)?;
        let file_descriptor_bytes = file_descriptor.encode_to_vec();
        let file_descriptor_len = file_descriptor_bytes.len() as u64;
        let file_descriptor_position = self.writer.tell().await? as u64;
        self.writer.write_all(&file_descriptor_bytes).await?;
        let mut gbo_table = Vec::with_capacity(1 + self.global_buffers.len());
        gbo_table.push((file_descriptor_position, file_descriptor_len));
        gbo_table.append(&mut self.global_buffers);
        Ok(gbo_table)
    }

    /// Add a metadata entry to the schema
    ///
    /// This method is useful because sometimes the metadata is not known until after the
    /// data has been written.  This method allows you to alter the schema metadata.  It
    /// must be called before `finish` is called.
    pub fn add_schema_metadata(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.schema_metadata.insert(key.into(), value.into());
    }

    /// Adds a global buffer to the file
    ///
    /// The global buffer can contain any arbitrary bytes.  It will be written to the disk
    /// immediately.  This method returns the index of the global buffer (this will always
    /// start at 1 and increment by 1 each time this method is called)
    pub async fn add_global_buffer(&mut self, buffer: Bytes) -> Result<u32> {
        let position = self.writer.tell().await? as u64;
        let len = buffer.len() as u64;
        Self::do_write_buffer(&mut self.writer, &buffer).await?;
        self.global_buffers.push((position, len));
        Ok(self.global_buffers.len() as u32)
    }

    async fn finish_writers(&mut self) -> Result<()> {
        let mut col_idx = 0;
        for mut writer in std::mem::take(&mut self.column_writers) {
            let mut external_buffers =
                OutOfLineBuffers::new(self.tell().await?, PAGE_BUFFER_ALIGNMENT as u64);
            let columns = writer.finish(&mut external_buffers).await?;
            for buffer in external_buffers.take_buffers() {
                self.writer.write_all(&buffer).await?;
            }
            debug_assert_eq!(
                columns.len(),
                writer.num_columns() as usize,
                "Expected {} columns from column at index {} and got {}",
                writer.num_columns(),
                col_idx,
                columns.len()
            );
            for column in columns {
                for page in column.final_pages {
                    self.write_page(page).await?;
                }
                let column_metadata = &mut self.column_metadata[col_idx];
                let mut buffer_pos = self.writer.tell().await? as u64;
                for buffer in column.column_buffers {
                    column_metadata.buffer_offsets.push(buffer_pos);
                    let mut size = 0;
                    Self::do_write_buffer(&mut self.writer, &buffer).await?;
                    size += buffer.len() as u64;
                    buffer_pos += size;
                    column_metadata.buffer_sizes.push(size);
                }
                let encoded_encoding = Any::from_msg(&column.encoding)?.encode_to_vec();
                column_metadata.encoding = Some(pbfile::Encoding {
                    location: Some(pbfile::encoding::Location::Direct(pbfile::DirectEncoding {
                        encoding: encoded_encoding,
                    })),
                });
                col_idx += 1;
            }
        }
        if col_idx != self.column_metadata.len() {
            panic!(
                "Column writers finished with {} columns but we expected {}",
                col_idx,
                self.column_metadata.len()
            );
        }
        Ok(())
    }

    /// Converts self.version (which is a mix of "software version" and
    /// "format version" into a format version)
    fn version_to_numbers(&self) -> (u16, u16) {
        let version = self.options.format_version.unwrap_or_default();
        match version.resolve() {
            LanceFileVersion::V2_0 => (0, 3),
            LanceFileVersion::V2_1 => (2, 1),
            LanceFileVersion::V2_2 => (2, 2),
            _ => panic!("Unsupported version: {}", version),
        }
    }

    /// Finishes writing the file
    ///
    /// This method will wait until all data has been flushed to the file.  Then it
    /// will write the file metadata and the footer.  It will not return until all
    /// data has been flushed and the file has been closed.
    ///
    /// Returns the total number of rows written
    pub async fn finish(&mut self) -> Result<u64> {
        // 1. flush any remaining data and write out those pages
        let mut external_buffers =
            OutOfLineBuffers::new(self.tell().await?, PAGE_BUFFER_ALIGNMENT as u64);
        let encoding_tasks = self
            .column_writers
            .iter_mut()
            .map(|writer| writer.flush(&mut external_buffers))
            .collect::<Result<Vec<_>>>()?;
        for external_buffer in external_buffers.take_buffers() {
            Self::do_write_buffer(&mut self.writer, &external_buffer).await?;
        }
        let encoding_tasks = encoding_tasks
            .into_iter()
            .flatten()
            .collect::<FuturesOrdered<_>>();
        self.write_pages(encoding_tasks).await?;

        self.finish_writers().await?;

        // 3. write global buffers (we write the schema here)
        let global_buffer_offsets = self.write_global_buffers().await?;
        let num_global_buffers = global_buffer_offsets.len() as u32;

        // 4. write the column metadatas
        let column_metadata_start = self.writer.tell().await? as u64;
        let metadata_positions = self.write_column_metadatas().await?;

        // 5. write the column metadata offset table
        let cmo_table_start = self.writer.tell().await? as u64;
        for (meta_pos, meta_len) in metadata_positions {
            self.writer.write_u64_le(meta_pos).await?;
            self.writer.write_u64_le(meta_len).await?;
        }

        // 6. write global buffers offset table
        let gbo_table_start = self.writer.tell().await? as u64;
        for (gbo_pos, gbo_len) in global_buffer_offsets {
            self.writer.write_u64_le(gbo_pos).await?;
            self.writer.write_u64_le(gbo_len).await?;
        }

        let (major, minor) = self.version_to_numbers();
        // 7. write the footer
        self.writer.write_u64_le(column_metadata_start).await?;
        self.writer.write_u64_le(cmo_table_start).await?;
        self.writer.write_u64_le(gbo_table_start).await?;
        self.writer.write_u32_le(num_global_buffers).await?;
        self.writer.write_u32_le(self.num_columns).await?;
        self.writer.write_u16_le(major).await?;
        self.writer.write_u16_le(minor).await?;
        self.writer.write_all(MAGIC).await?;

        // 7. close the writer
        self.writer.shutdown().await?;
        Ok(self.rows_written)
    }

    pub async fn abort(&mut self) {
        self.writer.abort().await;
    }

    pub async fn tell(&mut self) -> Result<u64> {
        Ok(self.writer.tell().await? as u64)
    }

    pub fn field_id_to_column_indices(&self) -> &[(u32, u32)] {
        &self.field_id_to_column_indices
    }
}

/// Utility trait for converting EncodedBatch to Bytes using the
/// lance file format
pub trait EncodedBatchWriteExt {
    /// Serializes into a lance file, including the schema
    fn try_to_self_described_lance(&self, version: LanceFileVersion) -> Result<Bytes>;
    /// Serializes into a lance file, without the schema.
    ///
    /// The schema must be provided to deserialize the buffer
    fn try_to_mini_lance(&self, version: LanceFileVersion) -> Result<Bytes>;
}

// Creates a lance footer and appends it to the encoded data
//
// The logic here is very similar to logic in the FileWriter except we
// are using BufMut (put_xyz) instead of AsyncWrite (write_xyz).
fn concat_lance_footer(
    batch: &EncodedBatch,
    write_schema: bool,
    version: LanceFileVersion,
) -> Result<Bytes> {
    // Estimating 1MiB for file footer
    let mut data = BytesMut::with_capacity(batch.data.len() + 1024 * 1024);
    data.put(batch.data.clone());
    // write global buffers (we write the schema here)
    let global_buffers = if write_schema {
        let schema_start = data.len() as u64;
        let lance_schema = lance_core::datatypes::Schema::try_from(batch.schema.as_ref())?;
        let descriptor = FileWriter::make_file_descriptor(&lance_schema, batch.num_rows)?;
        let descriptor_bytes = descriptor.encode_to_vec();
        let descriptor_len = descriptor_bytes.len() as u64;
        data.put(descriptor_bytes.as_slice());

        vec![(schema_start, descriptor_len)]
    } else {
        vec![]
    };
    let col_metadata_start = data.len() as u64;

    let mut col_metadata_positions = Vec::new();
    // Write column metadata
    for col in &batch.page_table {
        let position = data.len() as u64;
        let pages = col
            .page_infos
            .iter()
            .map(|page_info| {
                let encoded_encoding = match &page_info.encoding {
                    PageEncoding::Legacy(array_encoding) => {
                        Any::from_msg(array_encoding)?.encode_to_vec()
                    }
                    PageEncoding::Structural(page_layout) => {
                        Any::from_msg(page_layout)?.encode_to_vec()
                    }
                };
                let (buffer_offsets, buffer_sizes): (Vec<_>, Vec<_>) = page_info
                    .buffer_offsets_and_sizes
                    .as_ref()
                    .iter()
                    .cloned()
                    .unzip();
                Ok(pbfile::column_metadata::Page {
                    buffer_offsets,
                    buffer_sizes,
                    encoding: Some(pbfile::Encoding {
                        location: Some(pbfile::encoding::Location::Direct(DirectEncoding {
                            encoding: encoded_encoding,
                        })),
                    }),
                    length: page_info.num_rows,
                    priority: page_info.priority,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let (buffer_offsets, buffer_sizes): (Vec<_>, Vec<_>) =
            col.buffer_offsets_and_sizes.iter().cloned().unzip();
        let encoded_col_encoding = Any::from_msg(&col.encoding)?.encode_to_vec();
        let column = pbfile::ColumnMetadata {
            pages,
            buffer_offsets,
            buffer_sizes,
            encoding: Some(pbfile::Encoding {
                location: Some(pbfile::encoding::Location::Direct(pbfile::DirectEncoding {
                    encoding: encoded_col_encoding,
                })),
            }),
        };
        let column_bytes = column.encode_to_vec();
        col_metadata_positions.push((position, column_bytes.len() as u64));
        data.put(column_bytes.as_slice());
    }
    // Write column metadata offsets table
    let cmo_table_start = data.len() as u64;
    for (meta_pos, meta_len) in col_metadata_positions {
        data.put_u64_le(meta_pos);
        data.put_u64_le(meta_len);
    }
    // Write global buffers offsets table
    let gbo_table_start = data.len() as u64;
    let num_global_buffers = global_buffers.len() as u32;
    for (gbo_pos, gbo_len) in global_buffers {
        data.put_u64_le(gbo_pos);
        data.put_u64_le(gbo_len);
    }

    let (major, minor) = version.to_numbers();

    // write the footer
    data.put_u64_le(col_metadata_start);
    data.put_u64_le(cmo_table_start);
    data.put_u64_le(gbo_table_start);
    data.put_u32_le(num_global_buffers);
    data.put_u32_le(batch.page_table.len() as u32);
    data.put_u16_le(major as u16);
    data.put_u16_le(minor as u16);
    data.put(MAGIC.as_slice());

    Ok(data.freeze())
}

impl EncodedBatchWriteExt for EncodedBatch {
    fn try_to_self_described_lance(&self, version: LanceFileVersion) -> Result<Bytes> {
        concat_lance_footer(self, true, version)
    }

    fn try_to_mini_lance(&self, version: LanceFileVersion) -> Result<Bytes> {
        concat_lance_footer(self, false, version)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::reader::{describe_encoding, FileReader, FileReaderOptions};
    use crate::testing::FsFixture;
    use crate::writer::{FileWriter, FileWriterOptions, ENV_LANCE_FILE_WRITER_MAX_PAGE_BYTES};
    use arrow_array::builder::{Float32Builder, Int32Builder};
    use arrow_array::{types::Float64Type, RecordBatchReader, StringArray};
    use arrow_array::{Int32Array, RecordBatch, UInt64Array};
    use arrow_schema::{DataType, Field, Field as ArrowField, Schema, Schema as ArrowSchema};
    use lance_core::cache::LanceCache;
    use lance_core::datatypes::Schema as LanceSchema;
    use lance_core::utils::tempfile::TempObjFile;
    use lance_datagen::{array, gen_batch, BatchCount, RowCount};
    use lance_encoding::compression_config::{CompressionFieldParams, CompressionParams};
    use lance_encoding::decoder::DecoderPlugins;
    use lance_encoding::version::LanceFileVersion;
    use lance_io::object_store::ObjectStore;
    use lance_io::utils::CachedFileSize;

    #[tokio::test]
    async fn test_basic_write() {
        let tmp_path = TempObjFile::default();
        let obj_store = Arc::new(ObjectStore::local());

        let reader = gen_batch()
            .col("score", array::rand::<Float64Type>())
            .into_reader_rows(RowCount::from(1000), BatchCount::from(10));

        let writer = obj_store.create(&tmp_path).await.unwrap();

        let lance_schema =
            lance_core::datatypes::Schema::try_from(reader.schema().as_ref()).unwrap();

        let mut file_writer =
            FileWriter::try_new(writer, lance_schema, FileWriterOptions::default()).unwrap();

        for batch in reader {
            file_writer.write_batch(&batch.unwrap()).await.unwrap();
        }
        file_writer.add_schema_metadata("foo", "bar");
        file_writer.finish().await.unwrap();
        // Tests asserting the contents of the written file are in reader.rs
    }

    #[tokio::test]
    async fn test_write_empty() {
        let tmp_path = TempObjFile::default();
        let obj_store = Arc::new(ObjectStore::local());

        let reader = gen_batch()
            .col("score", array::rand::<Float64Type>())
            .into_reader_rows(RowCount::from(0), BatchCount::from(0));

        let writer = obj_store.create(&tmp_path).await.unwrap();

        let lance_schema =
            lance_core::datatypes::Schema::try_from(reader.schema().as_ref()).unwrap();

        let mut file_writer =
            FileWriter::try_new(writer, lance_schema, FileWriterOptions::default()).unwrap();

        for batch in reader {
            file_writer.write_batch(&batch.unwrap()).await.unwrap();
        }
        file_writer.add_schema_metadata("foo", "bar");
        file_writer.finish().await.unwrap();
    }

    #[tokio::test]
    async fn test_max_page_bytes_enforced() {
        let arrow_field = Field::new("data", DataType::UInt64, false);
        let arrow_schema = Schema::new(vec![arrow_field]);
        let lance_schema = LanceSchema::try_from(&arrow_schema).unwrap();

        // 8MiB
        let data: Vec<u64> = (0..1_000_000).collect();
        let array = UInt64Array::from(data);
        let batch =
            RecordBatch::try_new(arrow_schema.clone().into(), vec![Arc::new(array)]).unwrap();

        let options = FileWriterOptions {
            max_page_bytes: Some(1024 * 1024), // 1MB
            // This is a 2.0 only test because 2.1+ splits large pages on read instead of write
            format_version: Some(LanceFileVersion::V2_0),
            ..Default::default()
        };

        let path = TempObjFile::default();
        let object_store = ObjectStore::local();
        let mut writer = FileWriter::try_new(
            object_store.create(&path).await.unwrap(),
            lance_schema,
            options,
        )
        .unwrap();

        writer.write_batch(&batch).await.unwrap();
        writer.finish().await.unwrap();

        let fs = FsFixture::default();
        let file_scheduler = fs
            .scheduler
            .open_file(&path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let file_reader = FileReader::try_open(
            file_scheduler,
            None,
            Arc::<DecoderPlugins>::default(),
            &LanceCache::no_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        let column_meta = file_reader.metadata();

        let mut total_page_num: u32 = 0;
        for (col_idx, col_metadata) in column_meta.column_metadatas.iter().enumerate() {
            assert!(
                !col_metadata.pages.is_empty(),
                "Column {} has no pages",
                col_idx
            );

            for (page_idx, page) in col_metadata.pages.iter().enumerate() {
                total_page_num += 1;
                let total_size: u64 = page.buffer_sizes.iter().sum();
                assert!(
                    total_size <= 1024 * 1024,
                    "Column {} Page {} size {} exceeds 1MB limit",
                    col_idx,
                    page_idx,
                    total_size
                );
            }
        }

        assert_eq!(total_page_num, 8)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_max_page_bytes_env_var() {
        let arrow_field = Field::new("data", DataType::UInt64, false);
        let arrow_schema = Schema::new(vec![arrow_field]);
        let lance_schema = LanceSchema::try_from(&arrow_schema).unwrap();
        // 4MiB
        let data: Vec<u64> = (0..500_000).collect();
        let array = UInt64Array::from(data);
        let batch =
            RecordBatch::try_new(arrow_schema.clone().into(), vec![Arc::new(array)]).unwrap();

        // 2MiB
        std::env::set_var(ENV_LANCE_FILE_WRITER_MAX_PAGE_BYTES, "2097152");

        let options = FileWriterOptions {
            max_page_bytes: None, // enforce env
            ..Default::default()
        };

        let path = TempObjFile::default();
        let object_store = ObjectStore::local();
        let mut writer = FileWriter::try_new(
            object_store.create(&path).await.unwrap(),
            lance_schema.clone(),
            options,
        )
        .unwrap();

        writer.write_batch(&batch).await.unwrap();
        writer.finish().await.unwrap();

        let fs = FsFixture::default();
        let file_scheduler = fs
            .scheduler
            .open_file(&path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let file_reader = FileReader::try_open(
            file_scheduler,
            None,
            Arc::<DecoderPlugins>::default(),
            &LanceCache::no_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        for col_metadata in file_reader.metadata().column_metadatas.iter() {
            for page in col_metadata.pages.iter() {
                let total_size: u64 = page.buffer_sizes.iter().sum();
                assert!(
                    total_size <= 2 * 1024 * 1024,
                    "Page size {} exceeds 2MB limit",
                    total_size
                );
            }
        }

        std::env::set_var(ENV_LANCE_FILE_WRITER_MAX_PAGE_BYTES, "");
    }

    #[tokio::test]
    async fn test_compression_overrides_end_to_end() {
        // Create test schema with different column types
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("customer_id", DataType::Int32, false),
            ArrowField::new("product_id", DataType::Int32, false),
            ArrowField::new("quantity", DataType::Int32, false),
            ArrowField::new("price", DataType::Float32, false),
            ArrowField::new("description", DataType::Utf8, false),
        ]));

        let lance_schema = LanceSchema::try_from(arrow_schema.as_ref()).unwrap();

        // Create test data with patterns suitable for different compression
        let mut customer_ids = Int32Builder::new();
        let mut product_ids = Int32Builder::new();
        let mut quantities = Int32Builder::new();
        let mut prices = Float32Builder::new();
        let mut descriptions = Vec::new();

        // Generate data with specific patterns:
        // - customer_id: highly repetitive (good for RLE)
        // - product_id: moderately repetitive (good for RLE)
        // - quantity: random values (not good for RLE)
        // - price: some repetition
        // - description: long strings (good for Zstd)
        for i in 0..10000 {
            // Customer ID repeats every 100 rows (100 unique customers)
            // This creates runs of 100 identical values
            customer_ids.append_value(i / 100);

            // Product ID has only 5 unique values with long runs
            product_ids.append_value(i / 2000);

            // Quantity is mostly 1 with occasional other values
            quantities.append_value(if i % 10 == 0 { 5 } else { 1 });

            // Price has only 3 unique values
            prices.append_value(match i % 3 {
                0 => 9.99,
                1 => 19.99,
                _ => 29.99,
            });

            // Descriptions are repetitive but we'll keep them simple
            descriptions.push(format!("Product {}", i / 2000));
        }

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(customer_ids.finish()),
                Arc::new(product_ids.finish()),
                Arc::new(quantities.finish()),
                Arc::new(prices.finish()),
                Arc::new(StringArray::from(descriptions)),
            ],
        )
        .unwrap();

        // Configure compression parameters
        let mut params = CompressionParams::new();

        // RLE for ID columns (ends with _id)
        params.columns.insert(
            "*_id".to_string(),
            CompressionFieldParams {
                rle_threshold: Some(0.5), // Lower threshold to trigger RLE more easily
                compression: None,        // Will use default compression if any
                compression_level: None,
                bss: Some(lance_encoding::compression_config::BssMode::Off), // Explicitly disable BSS to ensure RLE is used
            },
        );

        // For now, we'll skip Zstd compression since it's not imported
        // In a real implementation, you could add other compression types here

        // Build encoding strategy with compression parameters
        let encoding_strategy = lance_encoding::encoder::default_encoding_strategy_with_params(
            LanceFileVersion::V2_1,
            params,
        )
        .unwrap();

        // Configure file writer options
        let options = FileWriterOptions {
            encoding_strategy: Some(Arc::from(encoding_strategy)),
            format_version: Some(LanceFileVersion::V2_1),
            max_page_bytes: Some(64 * 1024), // 64KB pages
            ..Default::default()
        };

        // Write the file
        let path = TempObjFile::default();
        let object_store = ObjectStore::local();

        let mut writer = FileWriter::try_new(
            object_store.create(&path).await.unwrap(),
            lance_schema.clone(),
            options,
        )
        .unwrap();

        writer.write_batch(&batch).await.unwrap();
        writer.add_schema_metadata("compression_test", "configured_compression");
        writer.finish().await.unwrap();

        // Now write the same data without compression overrides for comparison
        let path_no_compression = TempObjFile::default();
        let default_options = FileWriterOptions {
            format_version: Some(LanceFileVersion::V2_1),
            max_page_bytes: Some(64 * 1024),
            ..Default::default()
        };

        let mut writer_no_compression = FileWriter::try_new(
            object_store.create(&path_no_compression).await.unwrap(),
            lance_schema.clone(),
            default_options,
        )
        .unwrap();

        writer_no_compression.write_batch(&batch).await.unwrap();
        writer_no_compression.finish().await.unwrap();

        // Note: With our current data patterns and RLE compression, the compressed file
        // might actually be slightly larger due to compression metadata overhead.
        // This is expected and the test is mainly to verify the system works end-to-end.

        // Read back the compressed file and verify data integrity
        let fs = FsFixture::default();
        let file_scheduler = fs
            .scheduler
            .open_file(&path, &CachedFileSize::unknown())
            .await
            .unwrap();

        let file_reader = FileReader::try_open(
            file_scheduler,
            None,
            Arc::<DecoderPlugins>::default(),
            &LanceCache::no_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        // Verify metadata
        let metadata = file_reader.metadata();
        assert_eq!(metadata.major_version, 2);
        assert_eq!(metadata.minor_version, 1);

        let schema = file_reader.schema();
        assert_eq!(
            schema.metadata.get("compression_test"),
            Some(&"configured_compression".to_string())
        );

        // Verify the actual encodings used
        let column_metadatas = &metadata.column_metadatas;

        // Check customer_id column (index 0) - should use RLE due to our configuration
        assert!(!column_metadatas[0].pages.is_empty());
        let customer_id_encoding = describe_encoding(&column_metadatas[0].pages[0]);
        assert!(
            customer_id_encoding.contains("RLE") || customer_id_encoding.contains("Rle"),
            "customer_id column should use RLE encoding due to '*_id' pattern match, but got: {}",
            customer_id_encoding
        );

        // Check product_id column (index 1) - should use RLE due to our configuration
        assert!(!column_metadatas[1].pages.is_empty());
        let product_id_encoding = describe_encoding(&column_metadatas[1].pages[0]);
        assert!(
            product_id_encoding.contains("RLE") || product_id_encoding.contains("Rle"),
            "product_id column should use RLE encoding due to '*_id' pattern match, but got: {}",
            product_id_encoding
        );
    }

    #[tokio::test]
    async fn test_field_metadata_compression() {
        // Test that field metadata compression settings are respected
        let mut metadata = HashMap::new();
        metadata.insert(
            lance_encoding::constants::COMPRESSION_META_KEY.to_string(),
            "zstd".to_string(),
        );
        metadata.insert(
            lance_encoding::constants::COMPRESSION_LEVEL_META_KEY.to_string(),
            "6".to_string(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int32, false),
            ArrowField::new("text", DataType::Utf8, false).with_metadata(metadata.clone()),
            ArrowField::new("data", DataType::Int32, false).with_metadata(HashMap::from([(
                lance_encoding::constants::COMPRESSION_META_KEY.to_string(),
                "none".to_string(),
            )])),
        ]));

        let lance_schema = LanceSchema::try_from(arrow_schema.as_ref()).unwrap();

        // Create test data
        let id_array = Int32Array::from_iter_values(0..1000);
        let text_array = StringArray::from_iter_values(
            (0..1000).map(|i| format!("test string {} repeated text", i)),
        );
        let data_array = Int32Array::from_iter_values((0..1000).map(|i| i * 2));

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(id_array),
                Arc::new(text_array),
                Arc::new(data_array),
            ],
        )
        .unwrap();

        let path = TempObjFile::default();
        let object_store = ObjectStore::local();

        // Create encoding strategy that will read from field metadata
        let params = CompressionParams::new();
        let encoding_strategy = lance_encoding::encoder::default_encoding_strategy_with_params(
            LanceFileVersion::V2_1,
            params,
        )
        .unwrap();

        let options = FileWriterOptions {
            encoding_strategy: Some(Arc::from(encoding_strategy)),
            format_version: Some(LanceFileVersion::V2_1),
            ..Default::default()
        };
        let mut writer = FileWriter::try_new(
            object_store.create(&path).await.unwrap(),
            lance_schema.clone(),
            options,
        )
        .unwrap();

        writer.write_batch(&batch).await.unwrap();
        writer.finish().await.unwrap();

        // Read back metadata
        let fs = FsFixture::default();
        let file_scheduler = fs
            .scheduler
            .open_file(&path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let file_reader = FileReader::try_open(
            file_scheduler,
            None,
            Arc::<DecoderPlugins>::default(),
            &LanceCache::no_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        let column_metadatas = &file_reader.metadata().column_metadatas;

        // The text column (index 1) should use zstd compression based on metadata
        let text_encoding = describe_encoding(&column_metadatas[1].pages[0]);
        // For string columns, we expect Binary encoding with zstd compression
        assert!(
            text_encoding.contains("Zstd"),
            "text column should use zstd compression from field metadata, but got: {}",
            text_encoding
        );

        // The data column (index 2) should use no compression based on metadata
        let data_encoding = describe_encoding(&column_metadatas[2].pages[0]);
        // For Int32 columns with "none" compression, we expect Flat encoding without compression
        assert!(
            data_encoding.contains("Flat") && data_encoding.contains("compression: None"),
            "data column should use no compression from field metadata, but got: {}",
            data_encoding
        );
    }

    #[tokio::test]
    async fn test_field_metadata_rle_threshold() {
        // Test that RLE threshold from field metadata is respected
        let mut metadata = HashMap::new();
        metadata.insert(
            lance_encoding::constants::RLE_THRESHOLD_META_KEY.to_string(),
            "0.9".to_string(),
        );
        // Also set compression to ensure RLE is used
        metadata.insert(
            lance_encoding::constants::COMPRESSION_META_KEY.to_string(),
            "lz4".to_string(),
        );
        // Explicitly disable BSS to ensure RLE is tested
        metadata.insert(
            lance_encoding::constants::BSS_META_KEY.to_string(),
            "off".to_string(),
        );

        let arrow_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "status",
            DataType::Int32,
            false,
        )
        .with_metadata(metadata)]));

        let lance_schema = LanceSchema::try_from(arrow_schema.as_ref()).unwrap();

        // Create data with very high repetition (3 runs for 10000 values = 0.0003 ratio)
        let status_array = Int32Array::from_iter_values(
            std::iter::repeat_n(200, 8000)
                .chain(std::iter::repeat_n(404, 1500))
                .chain(std::iter::repeat_n(500, 500)),
        );

        let batch =
            RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(status_array)]).unwrap();

        let path = TempObjFile::default();
        let object_store = ObjectStore::local();

        // Create encoding strategy that will read from field metadata
        let params = CompressionParams::new();
        let encoding_strategy = lance_encoding::encoder::default_encoding_strategy_with_params(
            LanceFileVersion::V2_1,
            params,
        )
        .unwrap();

        let options = FileWriterOptions {
            encoding_strategy: Some(Arc::from(encoding_strategy)),
            format_version: Some(LanceFileVersion::V2_1),
            ..Default::default()
        };
        let mut writer = FileWriter::try_new(
            object_store.create(&path).await.unwrap(),
            lance_schema.clone(),
            options,
        )
        .unwrap();

        writer.write_batch(&batch).await.unwrap();
        writer.finish().await.unwrap();

        // Read back and check encoding
        let fs = FsFixture::default();
        let file_scheduler = fs
            .scheduler
            .open_file(&path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let file_reader = FileReader::try_open(
            file_scheduler,
            None,
            Arc::<DecoderPlugins>::default(),
            &LanceCache::no_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        let column_metadatas = &file_reader.metadata().column_metadatas;
        let status_encoding = describe_encoding(&column_metadatas[0].pages[0]);
        assert!(
            status_encoding.contains("RLE") || status_encoding.contains("Rle"),
            "status column should use RLE encoding due to metadata threshold, but got: {}",
            status_encoding
        );
    }

    #[tokio::test]
    async fn test_large_page_split_on_read() {
        use arrow_array::Array;
        use futures::TryStreamExt;
        use lance_encoding::decoder::FilterExpression;
        use lance_io::ReadBatchParams;

        // Test that large pages written with relaxed limits can be split during read

        let arrow_field = ArrowField::new("data", DataType::Binary, false);
        let arrow_schema = ArrowSchema::new(vec![arrow_field]);
        let lance_schema = LanceSchema::try_from(&arrow_schema).unwrap();

        // Create a large binary value (40MB) to trigger large page creation
        let large_value = vec![42u8; 40 * 1024 * 1024];
        let array = arrow_array::BinaryArray::from(vec![
            Some(large_value.as_slice()),
            Some(b"small value"),
        ]);
        let batch = RecordBatch::try_new(Arc::new(arrow_schema), vec![Arc::new(array)]).unwrap();

        // Write with relaxed page size limit (128MB)
        let options = FileWriterOptions {
            max_page_bytes: Some(128 * 1024 * 1024),
            format_version: Some(LanceFileVersion::V2_1),
            ..Default::default()
        };

        let fs = FsFixture::default();
        let path = fs.tmp_path;

        let mut writer = FileWriter::try_new(
            fs.object_store.create(&path).await.unwrap(),
            lance_schema.clone(),
            options,
        )
        .unwrap();

        writer.write_batch(&batch).await.unwrap();
        let num_rows = writer.finish().await.unwrap();
        assert_eq!(num_rows, 2);

        // Read back with split configuration
        let file_scheduler = fs
            .scheduler
            .open_file(&path, &CachedFileSize::unknown())
            .await
            .unwrap();

        // Configure reader to split pages larger than 10MB into chunks
        let reader_options = FileReaderOptions {
            read_chunk_size: 10 * 1024 * 1024, // 10MB chunks
            ..Default::default()
        };

        let file_reader = FileReader::try_open(
            file_scheduler,
            None,
            Arc::<DecoderPlugins>::default(),
            &LanceCache::no_cache(),
            reader_options,
        )
        .await
        .unwrap();

        // Read the data back
        let stream = file_reader
            .read_stream(
                ReadBatchParams::RangeFull,
                1024,
                10, // batch_readahead
                FilterExpression::no_filter(),
            )
            .unwrap();

        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        assert_eq!(batches.len(), 1);

        // Verify the data is correctly read despite splitting
        let read_array = batches[0].column(0);
        let read_binary = read_array
            .as_any()
            .downcast_ref::<arrow_array::BinaryArray>()
            .unwrap();

        assert_eq!(read_binary.len(), 2);
        assert_eq!(read_binary.value(0).len(), 40 * 1024 * 1024);
        assert_eq!(read_binary.value(1), b"small value");

        // Verify first value matches what we wrote
        assert!(read_binary.value(0).iter().all(|&b| b == 42u8));
    }
}
