// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{
    collections::{BTreeMap, BTreeSet},
    io::Cursor,
    ops::Range,
    pin::Pin,
    sync::Arc,
};

use arrow_array::RecordBatchReader;
use arrow_schema::Schema as ArrowSchema;
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
use bytes::{Bytes, BytesMut};
use deepsize::{Context, DeepSizeOf};
use futures::{stream::BoxStream, Stream, StreamExt};
use lance_encoding::{
    decoder::{
        schedule_and_decode, schedule_and_decode_blocking, ColumnInfo, DecoderConfig,
        DecoderPlugins, FilterExpression, PageEncoding, PageInfo, ReadBatchTask, RequestedRows,
        SchedulerDecoderConfig,
    },
    encoder::EncodedBatch,
    version::LanceFileVersion,
    EncodingsIo,
};
use log::debug;
use object_store::path::Path;
use prost::{Message, Name};
use snafu::location;

use lance_core::{
    cache::LanceCache,
    datatypes::{Field, Schema},
    Error, Result,
};
use lance_encoding::format::pb as pbenc;
use lance_encoding::format::pb21 as pbenc21;
use lance_io::{
    scheduler::FileScheduler,
    stream::{RecordBatchStream, RecordBatchStreamAdapter},
    ReadBatchParams,
};

use crate::{
    datatypes::{Fields, FieldsWithMeta},
    format::{pb, pbfile, MAGIC, MAJOR_VERSION, MINOR_VERSION},
    io::LanceEncodingsIo,
    writer::PAGE_BUFFER_ALIGNMENT,
};

/// Default chunk size for reading large pages (8MiB)
/// Pages larger than this will be split into multiple chunks during read
pub const DEFAULT_READ_CHUNK_SIZE: u64 = 8 * 1024 * 1024;

// For now, we don't use global buffers for anything other than schema.  If we
// use these later we should make them lazily loaded and then cached once loaded.
//
// We store their position / length for debugging purposes
#[derive(Debug, DeepSizeOf)]
pub struct BufferDescriptor {
    pub position: u64,
    pub size: u64,
}

/// Statistics summarize some of the file metadata for quick summary info
#[derive(Debug)]
pub struct FileStatistics {
    /// Statistics about each of the columns in the file
    pub columns: Vec<ColumnStatistics>,
}

/// Summary information describing a column
#[derive(Debug)]
pub struct ColumnStatistics {
    /// The number of pages in the column
    pub num_pages: usize,
    /// The total number of data & metadata bytes in the column
    ///
    /// This is the compressed on-disk size
    pub size_bytes: u64,
}

// TODO: Caching
#[derive(Debug)]
pub struct CachedFileMetadata {
    /// The schema of the file
    pub file_schema: Arc<Schema>,
    /// The column metadatas
    pub column_metadatas: Vec<pbfile::ColumnMetadata>,
    pub column_infos: Vec<Arc<ColumnInfo>>,
    /// The number of rows in the file
    pub num_rows: u64,
    pub file_buffers: Vec<BufferDescriptor>,
    /// The number of bytes contained in the data page section of the file
    pub num_data_bytes: u64,
    /// The number of bytes contained in the column metadata (not including buffers
    /// referenced by the metadata)
    pub num_column_metadata_bytes: u64,
    /// The number of bytes contained in global buffers
    pub num_global_buffer_bytes: u64,
    /// The number of bytes contained in the CMO and GBO tables
    pub num_footer_bytes: u64,
    pub major_version: u16,
    pub minor_version: u16,
}

impl DeepSizeOf for CachedFileMetadata {
    // TODO: include size for `column_metadatas` and `column_infos`.
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        self.file_schema.deep_size_of_children(context)
            + self
                .file_buffers
                .iter()
                .map(|file_buffer| file_buffer.deep_size_of_children(context))
                .sum::<usize>()
    }
}

impl CachedFileMetadata {
    pub fn version(&self) -> LanceFileVersion {
        match (self.major_version, self.minor_version) {
            (0, 3) => LanceFileVersion::V2_0,
            (2, 0) => LanceFileVersion::V2_0,
            (2, 1) => LanceFileVersion::V2_1,
            (2, 2) => LanceFileVersion::V2_2,
            _ => panic!(
                "Unsupported version: {}.{}",
                self.major_version, self.minor_version
            ),
        }
    }
}

/// Selecting columns from a lance file requires specifying both the
/// index of the column and the data type of the column
///
/// Partly, this is because it is not strictly required that columns
/// be read into the same type.  For example, a string column may be
/// read as a string, large_string or string_view type.
///
/// A read will only succeed if the decoder for a column is capable
/// of decoding into the requested type.
///
/// Note that this should generally be limited to different in-memory
/// representations of the same semantic type.  An encoding could
/// theoretically support "casting" (e.g. int to string, etc.) but
/// there is little advantage in doing so here.
///
/// Note: in order to specify a projection the user will need some way
/// to figure out the column indices.  In the table format we do this
/// using field IDs and keeping track of the field id->column index mapping.
///
/// If users are not using the table format then they will need to figure
/// out some way to do this themselves.
#[derive(Debug, Clone)]
pub struct ReaderProjection {
    /// The data types (schema) of the selected columns.  The names
    /// of the schema are arbitrary and ignored.
    pub schema: Arc<Schema>,
    /// The indices of the columns to load.
    ///
    /// The content of this vector depends on the file version.
    ///
    /// In Lance File Version 2.0 we need ids for structural fields as
    /// well as leaf fields:
    ///
    ///   - Primitive: the index of the column in the schema
    ///   - List: the index of the list column in the schema
    ///     followed by the column indices of the children
    ///   - FixedSizeList (of primitive): the index of the column in the schema
    ///     (this case is not nested)
    ///   - FixedSizeList (of non-primitive): not yet implemented
    ///   - Dictionary: same as primitive
    ///   - Struct: the index of the struct column in the schema
    ///     followed by the column indices of the children
    ///
    ///   In other words, this should be a DFS listing of the desired schema.
    ///
    /// In Lance File Version 2.1 we only need ids for leaf fields.  Any structural
    /// fields are completely transparent.
    ///
    /// For example, if the goal is to load:
    ///
    ///   x: int32
    ///   y: struct<z: int32, w: string>
    ///   z: list<int32>
    ///
    /// and the schema originally used to store the data was:
    ///
    ///   a: struct<x: int32>
    ///   b: int64
    ///   y: struct<z: int32, c: int64, w: string>
    ///   z: list<int32>
    ///
    /// Then the column_indices should be:
    ///
    /// - 2.0: [1, 3, 4, 6, 7, 8]
    /// - 2.1: [0, 2, 4, 5]
    pub column_indices: Vec<u32>,
}

impl ReaderProjection {
    fn from_field_ids_helper<'a>(
        file_version: LanceFileVersion,
        fields: impl Iterator<Item = &'a Field>,
        field_id_to_column_index: &BTreeMap<u32, u32>,
        column_indices: &mut Vec<u32>,
    ) -> Result<()> {
        for field in fields {
            let is_structural = file_version >= LanceFileVersion::V2_1;
            // In the 2.0 system we needed ids for intermediate fields.  In 2.1+
            // we only need ids for leaf fields.
            if !is_structural
                || field.children.is_empty()
                || field.is_blob()
                || field.is_packed_struct()
            {
                if let Some(column_idx) = field_id_to_column_index.get(&(field.id as u32)).copied()
                {
                    column_indices.push(column_idx);
                }
            }
            // Don't recurse into children if the field is a blob or packed struct in 2.1
            if !is_structural || (!field.is_blob() && !field.is_packed_struct()) {
                Self::from_field_ids_helper(
                    file_version,
                    field.children.iter(),
                    field_id_to_column_index,
                    column_indices,
                )?;
            }
        }
        Ok(())
    }

    /// Creates a projection using a mapping from field IDs to column indices
    ///
    /// You can obtain such a mapping when the file is written using the
    /// [`crate::writer::FileWriter::field_id_to_column_indices`] method.
    pub fn from_field_ids(
        file_version: LanceFileVersion,
        schema: &Schema,
        field_id_to_column_index: &BTreeMap<u32, u32>,
    ) -> Result<Self> {
        let mut column_indices = Vec::new();
        Self::from_field_ids_helper(
            file_version,
            schema.fields.iter(),
            field_id_to_column_index,
            &mut column_indices,
        )?;
        Ok(Self {
            schema: Arc::new(schema.clone()),
            column_indices,
        })
    }

    /// Creates a projection that reads the entire file
    ///
    /// If the schema provided is not the schema of the entire file then
    /// the projection will be invalid and the read will fail.
    /// If the field is a `struct datatype` with `packed` set to true in the field metadata,
    /// the whole struct has one column index.
    /// To support nested `packed-struct encoding`, this method need to be further adjusted.
    pub fn from_whole_schema(schema: &Schema, version: LanceFileVersion) -> Self {
        let schema = Arc::new(schema.clone());
        let is_structural = version >= LanceFileVersion::V2_1;
        let mut column_indices = vec![];
        let mut curr_column_idx = 0;
        let mut packed_struct_fields_num = 0;
        for field in schema.fields_pre_order() {
            if packed_struct_fields_num > 0 {
                packed_struct_fields_num -= 1;
                continue;
            }
            if field.is_packed_struct() {
                column_indices.push(curr_column_idx);
                curr_column_idx += 1;
                packed_struct_fields_num = field.children.len();
            } else if field.children.is_empty() || !is_structural {
                column_indices.push(curr_column_idx);
                curr_column_idx += 1;
            }
        }
        Self {
            schema,
            column_indices,
        }
    }

    /// Creates a projection that reads the specified columns provided by name
    ///
    /// The syntax for column names is the same as [`lance_core::datatypes::Schema::project`]
    ///
    /// If the schema provided is not the schema of the entire file then
    /// the projection will be invalid and the read will fail.
    pub fn from_column_names(
        file_version: LanceFileVersion,
        schema: &Schema,
        column_names: &[&str],
    ) -> Result<Self> {
        let field_id_to_column_index = schema
            .fields_pre_order()
            // In the 2.0 system we needed ids for intermediate fields.  In 2.1+
            // we only need ids for leaf fields.
            .filter(|field| {
                file_version < LanceFileVersion::V2_1 || field.is_leaf() || field.is_packed_struct()
            })
            .enumerate()
            .map(|(idx, field)| (field.id as u32, idx as u32))
            .collect::<BTreeMap<_, _>>();
        let projected = schema.project(column_names)?;
        let mut column_indices = Vec::new();
        Self::from_field_ids_helper(
            file_version,
            projected.fields.iter(),
            &field_id_to_column_index,
            &mut column_indices,
        )?;
        Ok(Self {
            schema: Arc::new(projected),
            column_indices,
        })
    }
}

/// File Reader Options that can control reading behaviors, such as whether to enable caching on repetition indices
#[derive(Clone, Debug)]
pub struct FileReaderOptions {
    pub decoder_config: DecoderConfig,
    /// Size of chunks when reading large pages. Pages larger than this
    /// will be read in multiple chunks to control memory usage.
    /// Default: 8MB (DEFAULT_READ_CHUNK_SIZE)
    pub read_chunk_size: u64,
}

impl Default for FileReaderOptions {
    fn default() -> Self {
        Self {
            decoder_config: DecoderConfig::default(),
            read_chunk_size: DEFAULT_READ_CHUNK_SIZE,
        }
    }
}

#[derive(Debug)]
pub struct FileReader {
    scheduler: Arc<dyn EncodingsIo>,
    // The default projection to be applied to all reads
    base_projection: ReaderProjection,
    num_rows: u64,
    metadata: Arc<CachedFileMetadata>,
    decoder_plugins: Arc<DecoderPlugins>,
    cache: Arc<LanceCache>,
    options: FileReaderOptions,
}
#[derive(Debug)]
struct Footer {
    #[allow(dead_code)]
    column_meta_start: u64,
    // We don't use this today because we always load metadata for every column
    // and don't yet support "metadata projection"
    #[allow(dead_code)]
    column_meta_offsets_start: u64,
    global_buff_offsets_start: u64,
    num_global_buffers: u32,
    num_columns: u32,
    major_version: u16,
    minor_version: u16,
}

const FOOTER_LEN: usize = 40;

impl FileReader {
    pub fn with_scheduler(&self, scheduler: Arc<dyn EncodingsIo>) -> Self {
        Self {
            scheduler,
            base_projection: self.base_projection.clone(),
            cache: self.cache.clone(),
            decoder_plugins: self.decoder_plugins.clone(),
            metadata: self.metadata.clone(),
            options: self.options.clone(),
            num_rows: self.num_rows,
        }
    }

    pub fn num_rows(&self) -> u64 {
        self.num_rows
    }

    pub fn metadata(&self) -> &Arc<CachedFileMetadata> {
        &self.metadata
    }

    pub fn file_statistics(&self) -> FileStatistics {
        let column_metadatas = &self.metadata().column_metadatas;

        let column_stats = column_metadatas
            .iter()
            .map(|col_metadata| {
                let num_pages = col_metadata.pages.len();
                let size_bytes = col_metadata
                    .pages
                    .iter()
                    .map(|page| page.buffer_sizes.iter().sum::<u64>())
                    .sum::<u64>();
                ColumnStatistics {
                    num_pages,
                    size_bytes,
                }
            })
            .collect();

        FileStatistics {
            columns: column_stats,
        }
    }

    pub async fn read_global_buffer(&self, index: u32) -> Result<Bytes> {
        let buffer_desc = self.metadata.file_buffers.get(index as usize).ok_or_else(||Error::invalid_input(format!("request for global buffer at index {} but there were only {} global buffers in the file", index, self.metadata.file_buffers.len()), location!()))?;
        self.scheduler
            .submit_single(
                buffer_desc.position..buffer_desc.position + buffer_desc.size,
                0,
            )
            .await
    }

    async fn read_tail(scheduler: &FileScheduler) -> Result<(Bytes, u64)> {
        let file_size = scheduler.reader().size().await? as u64;
        let begin = if file_size < scheduler.reader().block_size() as u64 {
            0
        } else {
            file_size - scheduler.reader().block_size() as u64
        };
        let tail_bytes = scheduler.submit_single(begin..file_size, 0).await?;
        Ok((tail_bytes, file_size))
    }

    // Checks to make sure the footer is written correctly and returns the
    // position of the file descriptor (which comes from the footer)
    fn decode_footer(footer_bytes: &Bytes) -> Result<Footer> {
        let len = footer_bytes.len();
        if len < FOOTER_LEN {
            return Err(Error::io(
                format!(
                    "does not have sufficient data, len: {}, bytes: {:?}",
                    len, footer_bytes
                ),
                location!(),
            ));
        }
        let mut cursor = Cursor::new(footer_bytes.slice(len - FOOTER_LEN..));

        let column_meta_start = cursor.read_u64::<LittleEndian>()?;
        let column_meta_offsets_start = cursor.read_u64::<LittleEndian>()?;
        let global_buff_offsets_start = cursor.read_u64::<LittleEndian>()?;
        let num_global_buffers = cursor.read_u32::<LittleEndian>()?;
        let num_columns = cursor.read_u32::<LittleEndian>()?;
        let major_version = cursor.read_u16::<LittleEndian>()?;
        let minor_version = cursor.read_u16::<LittleEndian>()?;

        if major_version == MAJOR_VERSION as u16 && minor_version == MINOR_VERSION as u16 {
            return Err(Error::version_conflict(
                "Attempt to use the lance v2 reader to read a legacy file".to_string(),
                major_version,
                minor_version,
                location!(),
            ));
        }

        let magic_bytes = footer_bytes.slice(len - 4..);
        if magic_bytes.as_ref() != MAGIC {
            return Err(Error::io(
                format!(
                    "file does not appear to be a Lance file (invalid magic: {:?})",
                    MAGIC
                ),
                location!(),
            ));
        }
        Ok(Footer {
            column_meta_start,
            column_meta_offsets_start,
            global_buff_offsets_start,
            num_global_buffers,
            num_columns,
            major_version,
            minor_version,
        })
    }

    // TODO: Once we have coalesced I/O we should only read the column metadatas that we need
    fn read_all_column_metadata(
        column_metadata_bytes: Bytes,
        footer: &Footer,
    ) -> Result<Vec<pbfile::ColumnMetadata>> {
        let column_metadata_start = footer.column_meta_start;
        // cmo == column_metadata_offsets
        let cmo_table_size = 16 * footer.num_columns as usize;
        let cmo_table = column_metadata_bytes.slice(column_metadata_bytes.len() - cmo_table_size..);

        (0..footer.num_columns)
            .map(|col_idx| {
                let offset = (col_idx * 16) as usize;
                let position = LittleEndian::read_u64(&cmo_table[offset..offset + 8]);
                let length = LittleEndian::read_u64(&cmo_table[offset + 8..offset + 16]);
                let normalized_position = (position - column_metadata_start) as usize;
                let normalized_end = normalized_position + (length as usize);
                Ok(pbfile::ColumnMetadata::decode(
                    &column_metadata_bytes[normalized_position..normalized_end],
                )?)
            })
            .collect::<Result<Vec<_>>>()
    }

    async fn optimistic_tail_read(
        data: &Bytes,
        start_pos: u64,
        scheduler: &FileScheduler,
        file_len: u64,
    ) -> Result<Bytes> {
        let num_bytes_needed = (file_len - start_pos) as usize;
        if data.len() >= num_bytes_needed {
            Ok(data.slice((data.len() - num_bytes_needed)..))
        } else {
            let num_bytes_missing = (num_bytes_needed - data.len()) as u64;
            let start = file_len - num_bytes_needed as u64;
            let missing_bytes = scheduler
                .submit_single(start..start + num_bytes_missing, 0)
                .await?;
            let mut combined = BytesMut::with_capacity(data.len() + num_bytes_missing as usize);
            combined.extend(missing_bytes);
            combined.extend(data);
            Ok(combined.freeze())
        }
    }

    fn do_decode_gbo_table(
        gbo_bytes: &Bytes,
        footer: &Footer,
        version: LanceFileVersion,
    ) -> Result<Vec<BufferDescriptor>> {
        let mut global_bufs_cursor = Cursor::new(gbo_bytes);

        let mut global_buffers = Vec::with_capacity(footer.num_global_buffers as usize);
        for _ in 0..footer.num_global_buffers {
            let buf_pos = global_bufs_cursor.read_u64::<LittleEndian>()?;
            assert!(
                version < LanceFileVersion::V2_1 || buf_pos % PAGE_BUFFER_ALIGNMENT as u64 == 0
            );
            let buf_size = global_bufs_cursor.read_u64::<LittleEndian>()?;
            global_buffers.push(BufferDescriptor {
                position: buf_pos,
                size: buf_size,
            });
        }

        Ok(global_buffers)
    }

    async fn decode_gbo_table(
        tail_bytes: &Bytes,
        file_len: u64,
        scheduler: &FileScheduler,
        footer: &Footer,
        version: LanceFileVersion,
    ) -> Result<Vec<BufferDescriptor>> {
        // This could, in theory, trigger another IOP but the GBO table should never be large
        // enough for that to happen
        let gbo_bytes = Self::optimistic_tail_read(
            tail_bytes,
            footer.global_buff_offsets_start,
            scheduler,
            file_len,
        )
        .await?;
        Self::do_decode_gbo_table(&gbo_bytes, footer, version)
    }

    fn decode_schema(schema_bytes: Bytes) -> Result<(u64, lance_core::datatypes::Schema)> {
        let file_descriptor = pb::FileDescriptor::decode(schema_bytes)?;
        let pb_schema = file_descriptor.schema.unwrap();
        let num_rows = file_descriptor.length;
        let fields_with_meta = FieldsWithMeta {
            fields: Fields(pb_schema.fields),
            metadata: pb_schema.metadata,
        };
        let schema = lance_core::datatypes::Schema::from(fields_with_meta);
        Ok((num_rows, schema))
    }

    // TODO: Support late projection.  Currently, if we want to perform a
    // projected read of a file, we load all of the column metadata, and then
    // only read the column data that is requested.  This is fine for most cases.
    //
    // However, if there are many columns then loading all of the column metadata
    // may be expensive.  We should support a mode where we only load the column
    // metadata for the columns that are requested (the file format supports this).
    //
    // The main challenge is that we either need to ignore the column metadata cache
    // or have a more sophisticated cache that can cache per-column metadata.
    //
    // Also, if the number of columns is fairly small, it's faster to read them as a
    // single IOP, but we can fix this through coalescing.
    pub async fn read_all_metadata(scheduler: &FileScheduler) -> Result<CachedFileMetadata> {
        // 1. read the footer
        let (tail_bytes, file_len) = Self::read_tail(scheduler).await?;
        let footer = Self::decode_footer(&tail_bytes)?;

        let file_version = LanceFileVersion::try_from_major_minor(
            footer.major_version as u32,
            footer.minor_version as u32,
        )?;

        let gbo_table =
            Self::decode_gbo_table(&tail_bytes, file_len, scheduler, &footer, file_version).await?;
        if gbo_table.is_empty() {
            return Err(Error::Internal {
                message: "File did not contain any global buffers, schema expected".to_string(),
                location: location!(),
            });
        }
        let schema_start = gbo_table[0].position;
        let schema_size = gbo_table[0].size;

        let num_footer_bytes = file_len - schema_start;

        // By default we read all column metadatas.  We do NOT read the column metadata buffers
        // at this point.  We only want to read the column metadata for columns we are actually loading.
        let all_metadata_bytes =
            Self::optimistic_tail_read(&tail_bytes, schema_start, scheduler, file_len).await?;

        let schema_bytes = all_metadata_bytes.slice(0..schema_size as usize);
        let (num_rows, schema) = Self::decode_schema(schema_bytes)?;

        // Next, read the metadata for the columns
        // This is both the column metadata and the CMO table
        let column_metadata_start = (footer.column_meta_start - schema_start) as usize;
        let column_metadata_end = (footer.global_buff_offsets_start - schema_start) as usize;
        let column_metadata_bytes =
            all_metadata_bytes.slice(column_metadata_start..column_metadata_end);
        let column_metadatas = Self::read_all_column_metadata(column_metadata_bytes, &footer)?;

        let num_global_buffer_bytes = gbo_table.iter().map(|buf| buf.size).sum::<u64>();
        let num_data_bytes = footer.column_meta_start - num_global_buffer_bytes;
        let num_column_metadata_bytes = footer.global_buff_offsets_start - footer.column_meta_start;

        let column_infos = Self::meta_to_col_infos(column_metadatas.as_slice(), file_version);

        Ok(CachedFileMetadata {
            file_schema: Arc::new(schema),
            column_metadatas,
            column_infos,
            num_rows,
            num_data_bytes,
            num_column_metadata_bytes,
            num_global_buffer_bytes,
            num_footer_bytes,
            file_buffers: gbo_table,
            major_version: footer.major_version,
            minor_version: footer.minor_version,
        })
    }

    fn fetch_encoding<M: Default + Name + Sized>(encoding: &pbfile::Encoding) -> M {
        match &encoding.location {
            Some(pbfile::encoding::Location::Indirect(_)) => todo!(),
            Some(pbfile::encoding::Location::Direct(encoding)) => {
                let encoding_buf = Bytes::from(encoding.encoding.clone());
                let encoding_any = prost_types::Any::decode(encoding_buf).unwrap();
                encoding_any.to_msg::<M>().unwrap()
            }
            Some(pbfile::encoding::Location::None(_)) => panic!(),
            None => panic!(),
        }
    }

    fn meta_to_col_infos(
        column_metadatas: &[pbfile::ColumnMetadata],
        file_version: LanceFileVersion,
    ) -> Vec<Arc<ColumnInfo>> {
        column_metadatas
            .iter()
            .enumerate()
            .map(|(col_idx, col_meta)| {
                let page_infos = col_meta
                    .pages
                    .iter()
                    .map(|page| {
                        let num_rows = page.length;
                        let encoding = match file_version {
                            LanceFileVersion::V2_0 => {
                                PageEncoding::Legacy(Self::fetch_encoding::<pbenc::ArrayEncoding>(
                                    page.encoding.as_ref().unwrap(),
                                ))
                            }
                            _ => PageEncoding::Structural(Self::fetch_encoding::<
                                pbenc21::PageLayout,
                            >(
                                page.encoding.as_ref().unwrap()
                            )),
                        };
                        let buffer_offsets_and_sizes = Arc::from(
                            page.buffer_offsets
                                .iter()
                                .zip(page.buffer_sizes.iter())
                                .map(|(offset, size)| {
                                    // Starting with version 2.1 we can assert that page buffers are aligned
                                    assert!(
                                        file_version < LanceFileVersion::V2_1
                                            || offset % PAGE_BUFFER_ALIGNMENT as u64 == 0
                                    );
                                    (*offset, *size)
                                })
                                .collect::<Vec<_>>(),
                        );
                        PageInfo {
                            buffer_offsets_and_sizes,
                            encoding,
                            num_rows,
                            priority: page.priority,
                        }
                    })
                    .collect::<Vec<_>>();
                let buffer_offsets_and_sizes = Arc::from(
                    col_meta
                        .buffer_offsets
                        .iter()
                        .zip(col_meta.buffer_sizes.iter())
                        .map(|(offset, size)| (*offset, *size))
                        .collect::<Vec<_>>(),
                );
                Arc::new(ColumnInfo {
                    index: col_idx as u32,
                    page_infos: Arc::from(page_infos),
                    buffer_offsets_and_sizes,
                    encoding: Self::fetch_encoding(col_meta.encoding.as_ref().unwrap()),
                })
            })
            .collect::<Vec<_>>()
    }

    fn validate_projection(
        projection: &ReaderProjection,
        metadata: &CachedFileMetadata,
    ) -> Result<()> {
        if projection.schema.fields.is_empty() {
            return Err(Error::invalid_input(
                "Attempt to read zero columns from the file, at least one column must be specified"
                    .to_string(),
                location!(),
            ));
        }
        let mut column_indices_seen = BTreeSet::new();
        for column_index in &projection.column_indices {
            if !column_indices_seen.insert(*column_index) {
                return Err(Error::invalid_input(
                    format!(
                        "The projection specified the column index {} more than once",
                        column_index
                    ),
                    location!(),
                ));
            }
            if *column_index >= metadata.column_infos.len() as u32 {
                return Err(Error::invalid_input(format!("The projection specified the column index {} but there are only {} columns in the file", column_index, metadata.column_infos.len()), location!()));
            }
        }
        Ok(())
    }

    /// Opens a new file reader without any pre-existing knowledge
    ///
    /// This will read the file schema from the file itself and thus requires a bit more I/O
    ///
    /// A `base_projection` can also be provided.  If provided, then the projection will apply
    /// to all reads from the file that do not specify their own projection.
    pub async fn try_open(
        scheduler: FileScheduler,
        base_projection: Option<ReaderProjection>,
        decoder_plugins: Arc<DecoderPlugins>,
        cache: &LanceCache,
        options: FileReaderOptions,
    ) -> Result<Self> {
        let file_metadata = Arc::new(Self::read_all_metadata(&scheduler).await?);
        let path = scheduler.reader().path().clone();

        // Create LanceEncodingsIo with read chunk size from options
        let encodings_io =
            LanceEncodingsIo::new(scheduler).with_read_chunk_size(options.read_chunk_size);

        Self::try_open_with_file_metadata(
            Arc::new(encodings_io),
            path,
            base_projection,
            decoder_plugins,
            file_metadata,
            cache,
            options,
        )
        .await
    }

    /// Same as `try_open` but with the file metadata already loaded.
    ///
    /// This method also can accept any kind of `EncodingsIo` implementation allowing
    /// for custom strategies to be used for I/O scheduling (e.g. for takes on fast
    /// disks it may be better to avoid asynchronous overhead).
    pub async fn try_open_with_file_metadata(
        scheduler: Arc<dyn EncodingsIo>,
        path: Path,
        base_projection: Option<ReaderProjection>,
        decoder_plugins: Arc<DecoderPlugins>,
        file_metadata: Arc<CachedFileMetadata>,
        cache: &LanceCache,
        options: FileReaderOptions,
    ) -> Result<Self> {
        let cache = Arc::new(cache.with_key_prefix(path.as_ref()));

        if let Some(base_projection) = base_projection.as_ref() {
            Self::validate_projection(base_projection, &file_metadata)?;
        }
        let num_rows = file_metadata.num_rows;
        Ok(Self {
            scheduler,
            base_projection: base_projection.unwrap_or(ReaderProjection::from_whole_schema(
                file_metadata.file_schema.as_ref(),
                file_metadata.version(),
            )),
            num_rows,
            metadata: file_metadata,
            decoder_plugins,
            cache,
            options,
        })
    }

    // The actual decoder needs all the column infos that make up a type.  In other words, if
    // the first type in the schema is Struct<i32, i32> then the decoder will need 3 column infos.
    //
    // This is a file reader concern because the file reader needs to support late projection of columns
    // and so it will need to figure this out anyways.
    //
    // It's a bit of a tricky process though because the number of column infos may depend on the
    // encoding.  Considering the above example, if we wrote it with a packed encoding, then there would
    // only be a single column in the file (and not 3).
    //
    // At the moment this method words because our rules are simple and we just repeat them here.  See
    // Self::default_projection for a similar problem.  In the future this is something the encodings
    // registry will need to figure out.
    fn collect_columns_from_projection(
        &self,
        _projection: &ReaderProjection,
    ) -> Result<Vec<Arc<ColumnInfo>>> {
        Ok(self.metadata.column_infos.to_vec())
    }

    #[allow(clippy::too_many_arguments)]
    fn do_read_range(
        column_infos: Vec<Arc<ColumnInfo>>,
        io: Arc<dyn EncodingsIo>,
        cache: Arc<LanceCache>,
        num_rows: u64,
        decoder_plugins: Arc<DecoderPlugins>,
        range: Range<u64>,
        batch_size: u32,
        projection: ReaderProjection,
        filter: FilterExpression,
        decoder_config: DecoderConfig,
    ) -> Result<BoxStream<'static, ReadBatchTask>> {
        debug!(
            "Reading range {:?} with batch_size {} from file with {} rows and {} columns into schema with {} columns",
            range,
            batch_size,
            num_rows,
            column_infos.len(),
            projection.schema.fields.len(),
        );

        let config = SchedulerDecoderConfig {
            batch_size,
            cache,
            decoder_plugins,
            io,
            decoder_config,
        };

        let requested_rows = RequestedRows::Ranges(vec![range]);

        Ok(schedule_and_decode(
            column_infos,
            requested_rows,
            filter,
            projection.column_indices,
            projection.schema,
            config,
        ))
    }

    fn read_range(
        &self,
        range: Range<u64>,
        batch_size: u32,
        projection: ReaderProjection,
        filter: FilterExpression,
    ) -> Result<BoxStream<'static, ReadBatchTask>> {
        // Create and initialize the stream
        Self::do_read_range(
            self.collect_columns_from_projection(&projection)?,
            self.scheduler.clone(),
            self.cache.clone(),
            self.num_rows,
            self.decoder_plugins.clone(),
            range,
            batch_size,
            projection,
            filter,
            self.options.decoder_config.clone(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn do_take_rows(
        column_infos: Vec<Arc<ColumnInfo>>,
        io: Arc<dyn EncodingsIo>,
        cache: Arc<LanceCache>,
        decoder_plugins: Arc<DecoderPlugins>,
        indices: Vec<u64>,
        batch_size: u32,
        projection: ReaderProjection,
        filter: FilterExpression,
        decoder_config: DecoderConfig,
    ) -> Result<BoxStream<'static, ReadBatchTask>> {
        debug!(
            "Taking {} rows spread across range {}..{} with batch_size {} from columns {:?}",
            indices.len(),
            indices[0],
            indices[indices.len() - 1],
            batch_size,
            column_infos.iter().map(|ci| ci.index).collect::<Vec<_>>()
        );

        let config = SchedulerDecoderConfig {
            batch_size,
            cache,
            decoder_plugins,
            io,
            decoder_config,
        };

        let requested_rows = RequestedRows::Indices(indices);

        Ok(schedule_and_decode(
            column_infos,
            requested_rows,
            filter,
            projection.column_indices,
            projection.schema,
            config,
        ))
    }

    fn take_rows(
        &self,
        indices: Vec<u64>,
        batch_size: u32,
        projection: ReaderProjection,
    ) -> Result<BoxStream<'static, ReadBatchTask>> {
        // Create and initialize the stream
        Self::do_take_rows(
            self.collect_columns_from_projection(&projection)?,
            self.scheduler.clone(),
            self.cache.clone(),
            self.decoder_plugins.clone(),
            indices,
            batch_size,
            projection,
            FilterExpression::no_filter(),
            self.options.decoder_config.clone(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn do_read_ranges(
        column_infos: Vec<Arc<ColumnInfo>>,
        io: Arc<dyn EncodingsIo>,
        cache: Arc<LanceCache>,
        decoder_plugins: Arc<DecoderPlugins>,
        ranges: Vec<Range<u64>>,
        batch_size: u32,
        projection: ReaderProjection,
        filter: FilterExpression,
        decoder_config: DecoderConfig,
    ) -> Result<BoxStream<'static, ReadBatchTask>> {
        let num_rows = ranges.iter().map(|r| r.end - r.start).sum::<u64>();
        debug!(
            "Taking {} ranges ({} rows) spread across range {}..{} with batch_size {} from columns {:?}",
            ranges.len(),
            num_rows,
            ranges[0].start,
            ranges[ranges.len() - 1].end,
            batch_size,
            column_infos.iter().map(|ci| ci.index).collect::<Vec<_>>()
        );

        let config = SchedulerDecoderConfig {
            batch_size,
            cache,
            decoder_plugins,
            io,
            decoder_config,
        };

        let requested_rows = RequestedRows::Ranges(ranges);

        Ok(schedule_and_decode(
            column_infos,
            requested_rows,
            filter,
            projection.column_indices,
            projection.schema,
            config,
        ))
    }

    fn read_ranges(
        &self,
        ranges: Vec<Range<u64>>,
        batch_size: u32,
        projection: ReaderProjection,
        filter: FilterExpression,
    ) -> Result<BoxStream<'static, ReadBatchTask>> {
        Self::do_read_ranges(
            self.collect_columns_from_projection(&projection)?,
            self.scheduler.clone(),
            self.cache.clone(),
            self.decoder_plugins.clone(),
            ranges,
            batch_size,
            projection,
            filter,
            self.options.decoder_config.clone(),
        )
    }

    /// Creates a stream of "read tasks" to read the data from the file
    ///
    /// The arguments are similar to [`Self::read_stream_projected`] but instead of returning a stream
    /// of record batches it returns a stream of "read tasks".
    ///
    /// The tasks should be consumed with some kind of `buffered` argument if CPU parallelism is desired.
    ///
    /// Note that "read task" is probably a bit imprecise.  The tasks are actually "decode tasks".  The
    /// reading happens asynchronously in the background.  In other words, a single read task may map to
    /// multiple I/O operations or a single I/O operation may map to multiple read tasks.
    pub fn read_tasks(
        &self,
        params: ReadBatchParams,
        batch_size: u32,
        projection: Option<ReaderProjection>,
        filter: FilterExpression,
    ) -> Result<Pin<Box<dyn Stream<Item = ReadBatchTask> + Send>>> {
        let projection = projection.unwrap_or_else(|| self.base_projection.clone());
        Self::validate_projection(&projection, &self.metadata)?;
        let verify_bound = |params: &ReadBatchParams, bound: u64, inclusive: bool| {
            if bound > self.num_rows || bound == self.num_rows && inclusive {
                Err(Error::invalid_input(
                    format!(
                        "cannot read {:?} from file with {} rows",
                        params, self.num_rows
                    ),
                    location!(),
                ))
            } else {
                Ok(())
            }
        };
        match &params {
            ReadBatchParams::Indices(indices) => {
                for idx in indices {
                    match idx {
                        None => {
                            return Err(Error::invalid_input(
                                "Null value in indices array",
                                location!(),
                            ));
                        }
                        Some(idx) => {
                            verify_bound(&params, idx as u64, true)?;
                        }
                    }
                }
                let indices = indices.iter().map(|idx| idx.unwrap() as u64).collect();
                self.take_rows(indices, batch_size, projection)
            }
            ReadBatchParams::Range(range) => {
                verify_bound(&params, range.end as u64, false)?;
                self.read_range(
                    range.start as u64..range.end as u64,
                    batch_size,
                    projection,
                    filter,
                )
            }
            ReadBatchParams::Ranges(ranges) => {
                let mut ranges_u64 = Vec::with_capacity(ranges.len());
                for range in ranges.as_ref() {
                    verify_bound(&params, range.end, false)?;
                    ranges_u64.push(range.start..range.end);
                }
                self.read_ranges(ranges_u64, batch_size, projection, filter)
            }
            ReadBatchParams::RangeFrom(range) => {
                verify_bound(&params, range.start as u64, true)?;
                self.read_range(
                    range.start as u64..self.num_rows,
                    batch_size,
                    projection,
                    filter,
                )
            }
            ReadBatchParams::RangeTo(range) => {
                verify_bound(&params, range.end as u64, false)?;
                self.read_range(0..range.end as u64, batch_size, projection, filter)
            }
            ReadBatchParams::RangeFull => {
                self.read_range(0..self.num_rows, batch_size, projection, filter)
            }
        }
    }

    /// Reads data from the file as a stream of record batches
    ///
    /// * `params` - Specifies the range (or indices) of data to read
    /// * `batch_size` - The maximum size of a single batch.  A batch may be smaller
    ///   if it is the last batch or if it is not possible to create a batch of the
    ///   requested size.
    ///
    ///   For example, if the batch size is 1024 and one of the columns is a string
    ///   column then there may be some ranges of 1024 rows that contain more than
    ///   2^31 bytes of string data (which is the maximum size of a string column
    ///   in Arrow).  In this case smaller batches may be emitted.
    /// * `batch_readahead` - The number of batches to read ahead.  This controls the
    ///   amount of CPU parallelism of the read.  In other words it controls how many
    ///   batches will be decoded in parallel.  It has no effect on the I/O parallelism
    ///   of the read (how many I/O requests are in flight at once).
    ///
    ///   This parameter also is also related to backpressure.  If the consumer of the
    ///   stream is slow then the reader will build up RAM.
    /// * `projection` - A projection to apply to the read.  This controls which columns
    ///   are read from the file.  The projection is NOT applied on top of the base
    ///   projection.  The projection is applied directly to the file schema.
    pub fn read_stream_projected(
        &self,
        params: ReadBatchParams,
        batch_size: u32,
        batch_readahead: u32,
        projection: ReaderProjection,
        filter: FilterExpression,
    ) -> Result<Pin<Box<dyn RecordBatchStream>>> {
        let arrow_schema = Arc::new(ArrowSchema::from(projection.schema.as_ref()));
        let tasks_stream = self.read_tasks(params, batch_size, Some(projection), filter)?;
        let batch_stream = tasks_stream
            .map(|task| task.task)
            .buffered(batch_readahead as usize)
            .boxed();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            arrow_schema,
            batch_stream,
        )))
    }

    fn take_rows_blocking(
        &self,
        indices: Vec<u64>,
        batch_size: u32,
        projection: ReaderProjection,
        filter: FilterExpression,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let column_infos = self.collect_columns_from_projection(&projection)?;
        debug!(
            "Taking {} rows spread across range {}..{} with batch_size {} from columns {:?}",
            indices.len(),
            indices[0],
            indices[indices.len() - 1],
            batch_size,
            column_infos.iter().map(|ci| ci.index).collect::<Vec<_>>()
        );

        let config = SchedulerDecoderConfig {
            batch_size,
            cache: self.cache.clone(),
            decoder_plugins: self.decoder_plugins.clone(),
            io: self.scheduler.clone(),
            decoder_config: self.options.decoder_config.clone(),
        };

        let requested_rows = RequestedRows::Indices(indices);

        schedule_and_decode_blocking(
            column_infos,
            requested_rows,
            filter,
            projection.column_indices,
            projection.schema,
            config,
        )
    }

    fn read_ranges_blocking(
        &self,
        ranges: Vec<Range<u64>>,
        batch_size: u32,
        projection: ReaderProjection,
        filter: FilterExpression,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let column_infos = self.collect_columns_from_projection(&projection)?;
        let num_rows = ranges.iter().map(|r| r.end - r.start).sum::<u64>();
        debug!(
            "Taking {} ranges ({} rows) spread across range {}..{} with batch_size {} from columns {:?}",
            ranges.len(),
            num_rows,
            ranges[0].start,
            ranges[ranges.len() - 1].end,
            batch_size,
            column_infos.iter().map(|ci| ci.index).collect::<Vec<_>>()
        );

        let config = SchedulerDecoderConfig {
            batch_size,
            cache: self.cache.clone(),
            decoder_plugins: self.decoder_plugins.clone(),
            io: self.scheduler.clone(),
            decoder_config: self.options.decoder_config.clone(),
        };

        let requested_rows = RequestedRows::Ranges(ranges);

        schedule_and_decode_blocking(
            column_infos,
            requested_rows,
            filter,
            projection.column_indices,
            projection.schema,
            config,
        )
    }

    fn read_range_blocking(
        &self,
        range: Range<u64>,
        batch_size: u32,
        projection: ReaderProjection,
        filter: FilterExpression,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let column_infos = self.collect_columns_from_projection(&projection)?;
        let num_rows = self.num_rows;

        debug!(
            "Reading range {:?} with batch_size {} from file with {} rows and {} columns into schema with {} columns",
            range,
            batch_size,
            num_rows,
            column_infos.len(),
            projection.schema.fields.len(),
        );

        let config = SchedulerDecoderConfig {
            batch_size,
            cache: self.cache.clone(),
            decoder_plugins: self.decoder_plugins.clone(),
            io: self.scheduler.clone(),
            decoder_config: self.options.decoder_config.clone(),
        };

        let requested_rows = RequestedRows::Ranges(vec![range]);

        schedule_and_decode_blocking(
            column_infos,
            requested_rows,
            filter,
            projection.column_indices,
            projection.schema,
            config,
        )
    }

    /// Read data from the file as an iterator of record batches
    ///
    /// This is a blocking variant of [`Self::read_stream_projected`] that runs entirely in the
    /// calling thread.  It will block on I/O if the decode is faster than the I/O.  It is useful
    /// for benchmarking and potentially from "take"ing small batches from fast disks.
    ///
    /// Large scans of in-memory data will still benefit from threading (and should therefore not
    /// use this method) because we can parallelize the decode.
    ///
    /// Note: calling this from within a tokio runtime will panic.  It is acceptable to call this
    /// from a spawn_blocking context.
    pub fn read_stream_projected_blocking(
        &self,
        params: ReadBatchParams,
        batch_size: u32,
        projection: Option<ReaderProjection>,
        filter: FilterExpression,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let projection = projection.unwrap_or_else(|| self.base_projection.clone());
        Self::validate_projection(&projection, &self.metadata)?;
        let verify_bound = |params: &ReadBatchParams, bound: u64, inclusive: bool| {
            if bound > self.num_rows || bound == self.num_rows && inclusive {
                Err(Error::invalid_input(
                    format!(
                        "cannot read {:?} from file with {} rows",
                        params, self.num_rows
                    ),
                    location!(),
                ))
            } else {
                Ok(())
            }
        };
        match &params {
            ReadBatchParams::Indices(indices) => {
                for idx in indices {
                    match idx {
                        None => {
                            return Err(Error::invalid_input(
                                "Null value in indices array",
                                location!(),
                            ));
                        }
                        Some(idx) => {
                            verify_bound(&params, idx as u64, true)?;
                        }
                    }
                }
                let indices = indices.iter().map(|idx| idx.unwrap() as u64).collect();
                self.take_rows_blocking(indices, batch_size, projection, filter)
            }
            ReadBatchParams::Range(range) => {
                verify_bound(&params, range.end as u64, false)?;
                self.read_range_blocking(
                    range.start as u64..range.end as u64,
                    batch_size,
                    projection,
                    filter,
                )
            }
            ReadBatchParams::Ranges(ranges) => {
                let mut ranges_u64 = Vec::with_capacity(ranges.len());
                for range in ranges.as_ref() {
                    verify_bound(&params, range.end, false)?;
                    ranges_u64.push(range.start..range.end);
                }
                self.read_ranges_blocking(ranges_u64, batch_size, projection, filter)
            }
            ReadBatchParams::RangeFrom(range) => {
                verify_bound(&params, range.start as u64, true)?;
                self.read_range_blocking(
                    range.start as u64..self.num_rows,
                    batch_size,
                    projection,
                    filter,
                )
            }
            ReadBatchParams::RangeTo(range) => {
                verify_bound(&params, range.end as u64, false)?;
                self.read_range_blocking(0..range.end as u64, batch_size, projection, filter)
            }
            ReadBatchParams::RangeFull => {
                self.read_range_blocking(0..self.num_rows, batch_size, projection, filter)
            }
        }
    }

    /// Reads data from the file as a stream of record batches
    ///
    /// This is similar to [`Self::read_stream_projected`] but uses the base projection
    /// provided when the file was opened (or reads all columns if the file was
    /// opened without a base projection)
    pub fn read_stream(
        &self,
        params: ReadBatchParams,
        batch_size: u32,
        batch_readahead: u32,
        filter: FilterExpression,
    ) -> Result<Pin<Box<dyn RecordBatchStream>>> {
        self.read_stream_projected(
            params,
            batch_size,
            batch_readahead,
            self.base_projection.clone(),
            filter,
        )
    }

    pub fn schema(&self) -> &Arc<Schema> {
        &self.metadata.file_schema
    }
}

/// Inspects a page and returns a String describing the page's encoding
pub fn describe_encoding(page: &pbfile::column_metadata::Page) -> String {
    if let Some(encoding) = &page.encoding {
        if let Some(style) = &encoding.location {
            match style {
                pbfile::encoding::Location::Indirect(indirect) => {
                    format!(
                        "IndirectEncoding(pos={},size={})",
                        indirect.buffer_location, indirect.buffer_length
                    )
                }
                pbfile::encoding::Location::Direct(direct) => {
                    let encoding_any =
                        prost_types::Any::decode(Bytes::from(direct.encoding.clone()))
                            .expect("failed to deserialize encoding as protobuf");
                    if encoding_any.type_url == "/lance.encodings.ArrayEncoding" {
                        let encoding = encoding_any.to_msg::<pbenc::ArrayEncoding>();
                        match encoding {
                            Ok(encoding) => {
                                format!("{:#?}", encoding)
                            }
                            Err(err) => {
                                format!("Unsupported(decode_err={})", err)
                            }
                        }
                    } else if encoding_any.type_url == "/lance.encodings21.PageLayout" {
                        let encoding = encoding_any.to_msg::<pbenc21::PageLayout>();
                        match encoding {
                            Ok(encoding) => {
                                format!("{:#?}", encoding)
                            }
                            Err(err) => {
                                format!("Unsupported(decode_err={})", err)
                            }
                        }
                    } else {
                        format!("Unrecognized(type_url={})", encoding_any.type_url)
                    }
                }
                pbfile::encoding::Location::None(_) => "NoEncodingDescription".to_string(),
            }
        } else {
            "MISSING STYLE".to_string()
        }
    } else {
        "MISSING".to_string()
    }
}

pub trait EncodedBatchReaderExt {
    fn try_from_mini_lance(
        bytes: Bytes,
        schema: &Schema,
        version: LanceFileVersion,
    ) -> Result<Self>
    where
        Self: Sized;
    fn try_from_self_described_lance(bytes: Bytes) -> Result<Self>
    where
        Self: Sized;
}

impl EncodedBatchReaderExt for EncodedBatch {
    fn try_from_mini_lance(
        bytes: Bytes,
        schema: &Schema,
        file_version: LanceFileVersion,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        let projection = ReaderProjection::from_whole_schema(schema, file_version);
        let footer = FileReader::decode_footer(&bytes)?;

        // Next, read the metadata for the columns
        // This is both the column metadata and the CMO table
        let column_metadata_start = footer.column_meta_start as usize;
        let column_metadata_end = footer.global_buff_offsets_start as usize;
        let column_metadata_bytes = bytes.slice(column_metadata_start..column_metadata_end);
        let column_metadatas =
            FileReader::read_all_column_metadata(column_metadata_bytes, &footer)?;

        let file_version = LanceFileVersion::try_from_major_minor(
            footer.major_version as u32,
            footer.minor_version as u32,
        )?;

        let page_table = FileReader::meta_to_col_infos(&column_metadatas, file_version);

        Ok(Self {
            data: bytes,
            num_rows: page_table
                .first()
                .map(|col| col.page_infos.iter().map(|page| page.num_rows).sum::<u64>())
                .unwrap_or(0),
            page_table,
            top_level_columns: projection.column_indices,
            schema: Arc::new(schema.clone()),
        })
    }

    fn try_from_self_described_lance(bytes: Bytes) -> Result<Self>
    where
        Self: Sized,
    {
        let footer = FileReader::decode_footer(&bytes)?;
        let file_version = LanceFileVersion::try_from_major_minor(
            footer.major_version as u32,
            footer.minor_version as u32,
        )?;

        let gbo_table = FileReader::do_decode_gbo_table(
            &bytes.slice(footer.global_buff_offsets_start as usize..),
            &footer,
            file_version,
        )?;
        if gbo_table.is_empty() {
            return Err(Error::Internal {
                message: "File did not contain any global buffers, schema expected".to_string(),
                location: location!(),
            });
        }
        let schema_start = gbo_table[0].position as usize;
        let schema_size = gbo_table[0].size as usize;

        let schema_bytes = bytes.slice(schema_start..(schema_start + schema_size));
        let (_, schema) = FileReader::decode_schema(schema_bytes)?;
        let projection = ReaderProjection::from_whole_schema(&schema, file_version);

        // Next, read the metadata for the columns
        // This is both the column metadata and the CMO table
        let column_metadata_start = footer.column_meta_start as usize;
        let column_metadata_end = footer.global_buff_offsets_start as usize;
        let column_metadata_bytes = bytes.slice(column_metadata_start..column_metadata_end);
        let column_metadatas =
            FileReader::read_all_column_metadata(column_metadata_bytes, &footer)?;

        let page_table = FileReader::meta_to_col_infos(&column_metadatas, file_version);

        Ok(Self {
            data: bytes,
            num_rows: page_table
                .first()
                .map(|col| col.page_infos.iter().map(|page| page.num_rows).sum::<u64>())
                .unwrap_or(0),
            page_table,
            top_level_columns: projection.column_indices,
            schema: Arc::new(schema),
        })
    }
}

#[cfg(test)]
pub mod tests {
    use std::{collections::BTreeMap, pin::Pin, sync::Arc};

    use arrow_array::{
        types::{Float64Type, Int32Type},
        RecordBatch, UInt32Array,
    };
    use arrow_schema::{DataType, Field, Fields, Schema as ArrowSchema};
    use bytes::Bytes;
    use futures::{prelude::stream::TryStreamExt, StreamExt};
    use lance_arrow::RecordBatchExt;
    use lance_core::{datatypes::Schema, ArrowResult};
    use lance_datagen::{array, gen_batch, BatchCount, ByteCount, RowCount};
    use lance_encoding::{
        decoder::{decode_batch, DecodeBatchScheduler, DecoderPlugins, FilterExpression},
        encoder::{default_encoding_strategy, encode_batch, EncodedBatch, EncodingOptions},
        version::LanceFileVersion,
    };
    use lance_io::{stream::RecordBatchStream, utils::CachedFileSize};
    use log::debug;
    use rstest::rstest;
    use tokio::sync::mpsc;

    use crate::reader::{EncodedBatchReaderExt, FileReader, FileReaderOptions, ReaderProjection};
    use crate::testing::{test_cache, write_lance_file, FsFixture, WrittenFile};
    use crate::writer::{EncodedBatchWriteExt, FileWriter, FileWriterOptions};
    use lance_encoding::decoder::DecoderConfig;

    async fn create_some_file(fs: &FsFixture, version: LanceFileVersion) -> WrittenFile {
        let location_type = DataType::Struct(Fields::from(vec![
            Field::new("x", DataType::Float64, true),
            Field::new("y", DataType::Float64, true),
        ]));
        let categories_type = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));

        let mut reader = gen_batch()
            .col("score", array::rand::<Float64Type>())
            .col("location", array::rand_type(&location_type))
            .col("categories", array::rand_type(&categories_type))
            .col("binary", array::rand_type(&DataType::Binary));
        if version <= LanceFileVersion::V2_0 {
            reader = reader.col("large_bin", array::rand_type(&DataType::LargeBinary));
        }
        let reader = reader.into_reader_rows(RowCount::from(1000), BatchCount::from(100));

        write_lance_file(
            reader,
            fs,
            FileWriterOptions {
                format_version: Some(version),
                ..Default::default()
            },
        )
        .await
    }

    type Transformer = Box<dyn Fn(&RecordBatch) -> RecordBatch>;

    async fn verify_expected(
        expected: &[RecordBatch],
        mut actual: Pin<Box<dyn RecordBatchStream>>,
        read_size: u32,
        transform: Option<Transformer>,
    ) {
        let mut remaining = expected.iter().map(|batch| batch.num_rows()).sum::<usize>() as u32;
        let mut expected_iter = expected.iter().map(|batch| {
            if let Some(transform) = &transform {
                transform(batch)
            } else {
                batch.clone()
            }
        });
        let mut next_expected = expected_iter.next().unwrap().clone();
        while let Some(actual) = actual.next().await {
            let mut actual = actual.unwrap();
            let mut rows_to_verify = actual.num_rows() as u32;
            let expected_length = remaining.min(read_size);
            assert_eq!(expected_length, rows_to_verify);

            while rows_to_verify > 0 {
                let next_slice_len = (next_expected.num_rows() as u32).min(rows_to_verify);
                assert_eq!(
                    next_expected.slice(0, next_slice_len as usize),
                    actual.slice(0, next_slice_len as usize)
                );
                remaining -= next_slice_len;
                rows_to_verify -= next_slice_len;
                if remaining > 0 {
                    if next_slice_len == next_expected.num_rows() as u32 {
                        next_expected = expected_iter.next().unwrap().clone();
                    } else {
                        next_expected = next_expected.slice(
                            next_slice_len as usize,
                            next_expected.num_rows() - next_slice_len as usize,
                        );
                    }
                }
                if rows_to_verify > 0 {
                    actual = actual.slice(
                        next_slice_len as usize,
                        actual.num_rows() - next_slice_len as usize,
                    );
                }
            }
        }
        assert_eq!(remaining, 0);
    }

    #[tokio::test]
    async fn test_round_trip() {
        let fs = FsFixture::default();

        let WrittenFile { data, .. } = create_some_file(&fs, LanceFileVersion::V2_0).await;

        for read_size in [32, 1024, 1024 * 1024] {
            let file_scheduler = fs
                .scheduler
                .open_file(&fs.tmp_path, &CachedFileSize::unknown())
                .await
                .unwrap();
            let file_reader = FileReader::try_open(
                file_scheduler,
                None,
                Arc::<DecoderPlugins>::default(),
                &test_cache(),
                FileReaderOptions::default(),
            )
            .await
            .unwrap();

            let schema = file_reader.schema();
            assert_eq!(schema.metadata.get("foo").unwrap(), "bar");

            let batch_stream = file_reader
                .read_stream(
                    lance_io::ReadBatchParams::RangeFull,
                    read_size,
                    16,
                    FilterExpression::no_filter(),
                )
                .unwrap();

            verify_expected(&data, batch_stream, read_size, None).await;
        }
    }

    #[rstest]
    #[test_log::test(tokio::test)]
    async fn test_encoded_batch_round_trip(
        // TODO: Add V2_1 (currently fails)
        #[values(LanceFileVersion::V2_0)] version: LanceFileVersion,
    ) {
        let data = gen_batch()
            .col("x", array::rand::<Int32Type>())
            .col("y", array::rand_utf8(ByteCount::from(16), false))
            .into_batch_rows(RowCount::from(10000))
            .unwrap();

        let lance_schema = Arc::new(Schema::try_from(data.schema().as_ref()).unwrap());

        let encoding_options = EncodingOptions {
            cache_bytes_per_column: 4096,
            max_page_bytes: 32 * 1024 * 1024,
            keep_original_array: true,
            buffer_alignment: 64,
        };

        let encoding_strategy = default_encoding_strategy(version);

        let encoded_batch = encode_batch(
            &data,
            lance_schema.clone(),
            encoding_strategy.as_ref(),
            &encoding_options,
        )
        .await
        .unwrap();

        // Test self described
        let bytes = encoded_batch.try_to_self_described_lance(version).unwrap();

        let decoded_batch = EncodedBatch::try_from_self_described_lance(bytes).unwrap();

        let decoded = decode_batch(
            &decoded_batch,
            &FilterExpression::no_filter(),
            Arc::<DecoderPlugins>::default(),
            false,
            version,
            None,
        )
        .await
        .unwrap();

        assert_eq!(data, decoded);

        // Test mini
        let bytes = encoded_batch.try_to_mini_lance(version).unwrap();
        let decoded_batch =
            EncodedBatch::try_from_mini_lance(bytes, lance_schema.as_ref(), LanceFileVersion::V2_0)
                .unwrap();
        let decoded = decode_batch(
            &decoded_batch,
            &FilterExpression::no_filter(),
            Arc::<DecoderPlugins>::default(),
            false,
            version,
            None,
        )
        .await
        .unwrap();

        assert_eq!(data, decoded);
    }

    #[rstest]
    #[test_log::test(tokio::test)]
    async fn test_projection(
        #[values(LanceFileVersion::V2_0, LanceFileVersion::V2_1)] version: LanceFileVersion,
    ) {
        let fs = FsFixture::default();

        let written_file = create_some_file(&fs, version).await;
        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();

        let field_id_mapping = written_file
            .field_id_mapping
            .iter()
            .copied()
            .collect::<BTreeMap<_, _>>();

        let empty_projection = ReaderProjection {
            column_indices: Vec::default(),
            schema: Arc::new(Schema::default()),
        };

        for columns in [
            vec!["score"],
            vec!["location"],
            vec!["categories"],
            vec!["score.x"],
            vec!["score", "categories"],
            vec!["score", "location"],
            vec!["location", "categories"],
            vec!["score.y", "location", "categories"],
        ] {
            debug!("Testing round trip with projection {:?}", columns);
            for use_field_ids in [true, false] {
                // We can specify the projection as part of the read operation via read_stream_projected
                let file_reader = FileReader::try_open(
                    file_scheduler.clone(),
                    None,
                    Arc::<DecoderPlugins>::default(),
                    &test_cache(),
                    FileReaderOptions::default(),
                )
                .await
                .unwrap();

                let projected_schema = written_file.schema.project(&columns).unwrap();
                let projection = if use_field_ids {
                    ReaderProjection::from_field_ids(
                        file_reader.metadata.version(),
                        &projected_schema,
                        &field_id_mapping,
                    )
                    .unwrap()
                } else {
                    ReaderProjection::from_column_names(
                        file_reader.metadata.version(),
                        &written_file.schema,
                        &columns,
                    )
                    .unwrap()
                };

                let batch_stream = file_reader
                    .read_stream_projected(
                        lance_io::ReadBatchParams::RangeFull,
                        1024,
                        16,
                        projection.clone(),
                        FilterExpression::no_filter(),
                    )
                    .unwrap();

                let projection_arrow = ArrowSchema::from(projection.schema.as_ref());
                verify_expected(
                    &written_file.data,
                    batch_stream,
                    1024,
                    Some(Box::new(move |batch: &RecordBatch| {
                        batch.project_by_schema(&projection_arrow).unwrap()
                    })),
                )
                .await;

                // We can also specify the projection as a base projection when we open the file
                let file_reader = FileReader::try_open(
                    file_scheduler.clone(),
                    Some(projection.clone()),
                    Arc::<DecoderPlugins>::default(),
                    &test_cache(),
                    FileReaderOptions::default(),
                )
                .await
                .unwrap();

                let batch_stream = file_reader
                    .read_stream(
                        lance_io::ReadBatchParams::RangeFull,
                        1024,
                        16,
                        FilterExpression::no_filter(),
                    )
                    .unwrap();

                let projection_arrow = ArrowSchema::from(projection.schema.as_ref());
                verify_expected(
                    &written_file.data,
                    batch_stream,
                    1024,
                    Some(Box::new(move |batch: &RecordBatch| {
                        batch.project_by_schema(&projection_arrow).unwrap()
                    })),
                )
                .await;

                assert!(file_reader
                    .read_stream_projected(
                        lance_io::ReadBatchParams::RangeFull,
                        1024,
                        16,
                        empty_projection.clone(),
                        FilterExpression::no_filter(),
                    )
                    .is_err());
            }
        }

        assert!(FileReader::try_open(
            file_scheduler.clone(),
            Some(empty_projection),
            Arc::<DecoderPlugins>::default(),
            &test_cache(),
            FileReaderOptions::default(),
        )
        .await
        .is_err());

        let arrow_schema = ArrowSchema::new(vec![
            Field::new("x", DataType::Int32, true),
            Field::new("y", DataType::Int32, true),
        ]);
        let schema = Schema::try_from(&arrow_schema).unwrap();

        let projection_with_dupes = ReaderProjection {
            column_indices: vec![0, 0],
            schema: Arc::new(schema),
        };

        assert!(FileReader::try_open(
            file_scheduler.clone(),
            Some(projection_with_dupes),
            Arc::<DecoderPlugins>::default(),
            &test_cache(),
            FileReaderOptions::default(),
        )
        .await
        .is_err());
    }

    #[test_log::test(tokio::test)]
    async fn test_compressing_buffer() {
        let fs = FsFixture::default();

        let written_file = create_some_file(&fs, LanceFileVersion::V2_0).await;
        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();

        // We can specify the projection as part of the read operation via read_stream_projected
        let file_reader = FileReader::try_open(
            file_scheduler.clone(),
            None,
            Arc::<DecoderPlugins>::default(),
            &test_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        let mut projection = written_file.schema.project(&["score"]).unwrap();
        for field in projection.fields.iter_mut() {
            field
                .metadata
                .insert("lance:compression".to_string(), "zstd".to_string());
        }
        let projection = ReaderProjection {
            column_indices: projection.fields.iter().map(|f| f.id as u32).collect(),
            schema: Arc::new(projection),
        };

        let batch_stream = file_reader
            .read_stream_projected(
                lance_io::ReadBatchParams::RangeFull,
                1024,
                16,
                projection.clone(),
                FilterExpression::no_filter(),
            )
            .unwrap();

        let projection_arrow = Arc::new(ArrowSchema::from(projection.schema.as_ref()));
        verify_expected(
            &written_file.data,
            batch_stream,
            1024,
            Some(Box::new(move |batch: &RecordBatch| {
                batch.project_by_schema(&projection_arrow).unwrap()
            })),
        )
        .await;
    }

    #[tokio::test]
    async fn test_read_all() {
        let fs = FsFixture::default();
        let WrittenFile { data, .. } = create_some_file(&fs, LanceFileVersion::V2_0).await;
        let total_rows = data.iter().map(|batch| batch.num_rows()).sum::<usize>();

        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let file_reader = FileReader::try_open(
            file_scheduler.clone(),
            None,
            Arc::<DecoderPlugins>::default(),
            &test_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        let batches = file_reader
            .read_stream(
                lance_io::ReadBatchParams::RangeFull,
                total_rows as u32,
                16,
                FilterExpression::no_filter(),
            )
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), total_rows);
    }

    #[rstest]
    #[tokio::test]
    async fn test_blocking_take(
        #[values(LanceFileVersion::V2_0, LanceFileVersion::V2_1)] version: LanceFileVersion,
    ) {
        let fs = FsFixture::default();
        let WrittenFile { data, schema, .. } = create_some_file(&fs, version).await;
        let total_rows = data.iter().map(|batch| batch.num_rows()).sum::<usize>();

        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let file_reader = FileReader::try_open(
            file_scheduler.clone(),
            Some(ReaderProjection::from_column_names(version, &schema, &["score"]).unwrap()),
            Arc::<DecoderPlugins>::default(),
            &test_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        let batches = tokio::task::spawn_blocking(move || {
            file_reader
                .read_stream_projected_blocking(
                    lance_io::ReadBatchParams::Indices(UInt32Array::from(vec![0, 1, 2, 3, 4])),
                    total_rows as u32,
                    None,
                    FilterExpression::no_filter(),
                )
                .unwrap()
                .collect::<ArrowResult<Vec<_>>>()
                .unwrap()
        })
        .await
        .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 5);
        assert_eq!(batches[0].num_columns(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_drop_in_progress() {
        let fs = FsFixture::default();
        let WrittenFile { data, .. } = create_some_file(&fs, LanceFileVersion::V2_0).await;
        let total_rows = data.iter().map(|batch| batch.num_rows()).sum::<usize>();

        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let file_reader = FileReader::try_open(
            file_scheduler.clone(),
            None,
            Arc::<DecoderPlugins>::default(),
            &test_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        let mut batches = file_reader
            .read_stream(
                lance_io::ReadBatchParams::RangeFull,
                (total_rows / 10) as u32,
                16,
                FilterExpression::no_filter(),
            )
            .unwrap();

        drop(file_reader);

        let batch = batches.next().await.unwrap().unwrap();
        assert!(batch.num_rows() > 0);

        // Drop in-progress scan
        drop(batches);
    }

    #[tokio::test]
    async fn drop_while_scheduling() {
        // This is a bit of a white-box test, pokes at the internals.  We want to
        // test the case where the read stream is dropped before the scheduling
        // thread finishes.  We can't do that in a black-box fashion because the
        // scheduling thread runs in the background and there is no easy way to
        // pause / gate it.

        // It's a regression for a bug where the scheduling thread would panic
        // if the stream was dropped before it finished.

        let fs = FsFixture::default();
        let written_file = create_some_file(&fs, LanceFileVersion::V2_0).await;
        let total_rows = written_file
            .data
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>();

        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let file_reader = FileReader::try_open(
            file_scheduler.clone(),
            None,
            Arc::<DecoderPlugins>::default(),
            &test_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        let projection =
            ReaderProjection::from_whole_schema(&written_file.schema, LanceFileVersion::V2_0);
        let column_infos = file_reader
            .collect_columns_from_projection(&projection)
            .unwrap();
        let mut decode_scheduler = DecodeBatchScheduler::try_new(
            &projection.schema,
            &projection.column_indices,
            &column_infos,
            &vec![],
            total_rows as u64,
            Arc::<DecoderPlugins>::default(),
            file_reader.scheduler.clone(),
            test_cache(),
            &FilterExpression::no_filter(),
            &DecoderConfig::default(),
        )
        .await
        .unwrap();

        let range = 0..total_rows as u64;

        let (tx, rx) = mpsc::unbounded_channel();

        // Simulate the stream / decoder being dropped
        drop(rx);

        // Scheduling should not panic
        decode_scheduler.schedule_range(
            range,
            &FilterExpression::no_filter(),
            tx,
            file_reader.scheduler.clone(),
        )
    }

    #[tokio::test]
    async fn test_read_empty_range() {
        let fs = FsFixture::default();
        create_some_file(&fs, LanceFileVersion::V2_0).await;

        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let file_reader = FileReader::try_open(
            file_scheduler.clone(),
            None,
            Arc::<DecoderPlugins>::default(),
            &test_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        // All ranges empty, no data
        let batches = file_reader
            .read_stream(
                lance_io::ReadBatchParams::Range(0..0),
                1024,
                16,
                FilterExpression::no_filter(),
            )
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(batches.len(), 0);

        // Some ranges empty
        let batches = file_reader
            .read_stream(
                lance_io::ReadBatchParams::Ranges(Arc::new([0..1, 2..2])),
                1024,
                16,
                FilterExpression::no_filter(),
            )
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);
    }

    #[tokio::test]
    async fn test_global_buffers() {
        let fs = FsFixture::default();

        let lance_schema =
            lance_core::datatypes::Schema::try_from(&ArrowSchema::new(vec![Field::new(
                "foo",
                DataType::Int32,
                true,
            )]))
            .unwrap();

        let mut file_writer = FileWriter::try_new(
            fs.object_store.create(&fs.tmp_path).await.unwrap(),
            lance_schema.clone(),
            FileWriterOptions::default(),
        )
        .unwrap();

        let test_bytes = Bytes::from_static(b"hello");

        let buf_index = file_writer
            .add_global_buffer(test_bytes.clone())
            .await
            .unwrap();

        assert_eq!(buf_index, 1);

        file_writer.finish().await.unwrap();

        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let file_reader = FileReader::try_open(
            file_scheduler.clone(),
            None,
            Arc::<DecoderPlugins>::default(),
            &test_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        let buf = file_reader.read_global_buffer(1).await.unwrap();
        assert_eq!(buf, test_bytes);
    }
}
