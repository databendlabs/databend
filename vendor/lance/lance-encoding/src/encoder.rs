// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! The top-level encoding module for Lance files.
//!
//! Lance files are encoded using a [`FieldEncodingStrategy`] which choose
//! what encoder to use for each field.
//!
//! The current strategy is the [`StructuralEncodingStrategy`] which uses "structural"
//! encoding.  A tree of encoders is built up for each field.  The struct & list encoders
//! simply pull off the validity and offsets and collect them.  Then, in the primitive leaf
//! encoder the validity, offsets, and values are accumulated in an accumulation buffer.  Once
//! enough data has been collected the primitive encoder will either use a miniblock encoding
//! or a full zip encoding to create a page of data from the accumulation buffer.

use std::{collections::HashMap, sync::Arc};

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::DataType;
use bytes::{Bytes, BytesMut};
use futures::future::BoxFuture;
use lance_core::datatypes::{Field, Schema};
use lance_core::utils::bit::{is_pwr_two, pad_bytes_to};
use lance_core::{Error, Result};
use snafu::location;

use crate::buffer::LanceBuffer;
use crate::compression::{CompressionStrategy, DefaultCompressionStrategy};
use crate::compression_config::CompressionParams;
use crate::decoder::PageEncoding;
use crate::encodings::logical::blob::{BlobStructuralEncoder, BlobV2StructuralEncoder};
use crate::encodings::logical::list::ListStructuralEncoder;
use crate::encodings::logical::primitive::PrimitiveStructuralEncoder;
use crate::encodings::logical::r#struct::StructStructuralEncoder;
use crate::repdef::RepDefBuilder;
use crate::version::LanceFileVersion;
use crate::{
    decoder::{ColumnInfo, PageInfo},
    format::pb,
};

/// The minimum alignment for a page buffer.  Writers must respect this.
pub const MIN_PAGE_BUFFER_ALIGNMENT: u64 = 8;

/// An encoded page of data
///
/// Maps to a top-level array
///
/// For example, FixedSizeList<Int32> will have two EncodedArray instances and one EncodedPage
#[derive(Debug)]
pub struct EncodedPage {
    // The encoded page buffers
    pub data: Vec<LanceBuffer>,
    // A description of the encoding used to encode the page
    pub description: PageEncoding,
    /// The number of rows in the encoded page
    pub num_rows: u64,
    /// The top-level row number of the first row in the page
    ///
    /// Generally the number of "top-level" rows and the number of rows are the same.  However,
    /// when there is repetition (list/fixed-size-list) there will be more or less items than rows.
    ///
    /// A top-level row can never be split across a page boundary.
    pub row_number: u64,
    /// The index of the column
    pub column_idx: u32,
}

pub struct EncodedColumn {
    pub column_buffers: Vec<LanceBuffer>,
    pub encoding: pb::ColumnEncoding,
    pub final_pages: Vec<EncodedPage>,
}

impl Default for EncodedColumn {
    fn default() -> Self {
        Self {
            column_buffers: Default::default(),
            encoding: pb::ColumnEncoding {
                column_encoding: Some(pb::column_encoding::ColumnEncoding::Values(())),
            },
            final_pages: Default::default(),
        }
    }
}

/// A tool to reserve space for buffers that are not in-line with the data
///
/// In most cases, buffers are stored in the page and referred to in the encoding
/// metadata by their index in the page.  This keeps all buffers within a page together.
/// As a result, most encoders should not need to use this structure.
///
/// In some cases (currently only the large binary encoding) there is a need to access
/// buffers that are not in the page (because storing the position / offset of every page
/// in the page metadata would be too expensive).
///
/// To do this you can add a buffer with `add_buffer` and then use the returned position
/// in some way (in the large binary encoding the returned position is stored in the page
/// data as a position / size array).
pub struct OutOfLineBuffers {
    position: u64,
    buffer_alignment: u64,
    buffers: Vec<LanceBuffer>,
}

impl OutOfLineBuffers {
    pub fn new(base_position: u64, buffer_alignment: u64) -> Self {
        Self {
            position: base_position,
            buffer_alignment,
            buffers: Vec::new(),
        }
    }

    pub fn add_buffer(&mut self, buffer: LanceBuffer) -> u64 {
        let position = self.position;
        self.position += buffer.len() as u64;
        self.position += pad_bytes_to(buffer.len(), self.buffer_alignment as usize) as u64;
        self.buffers.push(buffer);
        position
    }

    pub fn take_buffers(self) -> Vec<LanceBuffer> {
        self.buffers
    }

    pub fn reset_position(&mut self, position: u64) {
        self.position = position;
    }
}

/// A task to create a page of data
pub type EncodeTask = BoxFuture<'static, Result<EncodedPage>>;

/// Top level encoding trait to code any Arrow array type into one or more pages.
///
/// The field encoder implements buffering and encoding of a single input column
/// but it may map to multiple output columns.  For example, a list array or struct
/// array will be encoded into multiple columns.
///
/// Also, fields may be encoded at different speeds.  For example, given a struct
/// column with three fields (a boolean field, an int32 field, and a 4096-dimension
/// tensor field) the tensor field is likely to emit encoded pages much more frequently
/// than the boolean field.
pub trait FieldEncoder: Send {
    /// Buffer the data and, if there is enough data in the buffer to form a page, return
    /// an encoding task to encode the data.
    ///
    /// This may return more than one task because a single column may be mapped to multiple
    /// output columns.  For example, if encoding a struct column with three children then
    /// up to three tasks may be returned from each call to maybe_encode.
    ///
    /// It may also return multiple tasks for a single column if the input array is larger
    /// than a single disk page.
    ///
    /// It could also return an empty Vec if there is not enough data yet to encode any pages.
    ///
    /// The `row_number` must be passed which is the top-level row number currently being encoded
    /// This is stored in any pages produced by this call so that we can know the priority of the
    /// page.
    ///
    /// The `num_rows` is the number of top level rows.  It is initially the same as `array.len()`
    /// however it is passed seprately because array will become flattened over time (if there is
    /// repetition) and we need to know the original number of rows for various purposes.
    fn maybe_encode(
        &mut self,
        array: ArrayRef,
        external_buffers: &mut OutOfLineBuffers,
        repdef: RepDefBuilder,
        row_number: u64,
        num_rows: u64,
    ) -> Result<Vec<EncodeTask>>;
    /// Flush any remaining data from the buffers into encoding tasks
    ///
    /// Each encode task produces a single page.  The order of these pages will be maintained
    /// in the file (we do not worry about order between columns but all pages in the same
    /// column should maintain order)
    ///
    /// This may be called intermittently throughout encoding but will always be called
    /// once at the end of encoding just before calling finish
    fn flush(&mut self, external_buffers: &mut OutOfLineBuffers) -> Result<Vec<EncodeTask>>;
    /// Finish encoding and return column metadata
    ///
    /// This is called only once, after all encode tasks have completed
    ///
    /// This returns a Vec because a single field may have created multiple columns
    fn finish(
        &mut self,
        external_buffers: &mut OutOfLineBuffers,
    ) -> BoxFuture<'_, Result<Vec<EncodedColumn>>>;

    /// The number of output columns this encoding will create
    fn num_columns(&self) -> u32;
}

/// Keeps track of the current column index and makes a mapping
/// from field id to column index
#[derive(Debug, Default)]
pub struct ColumnIndexSequence {
    current_index: u32,
    mapping: Vec<(u32, u32)>,
}

impl ColumnIndexSequence {
    pub fn next_column_index(&mut self, field_id: u32) -> u32 {
        let idx = self.current_index;
        self.current_index += 1;
        self.mapping.push((field_id, idx));
        idx
    }

    pub fn skip(&mut self) {
        self.current_index += 1;
    }
}

/// Options that control the encoding process
pub struct EncodingOptions {
    /// How much data (in bytes) to cache in-memory before writing a page
    ///
    /// This cache is applied on a per-column basis
    pub cache_bytes_per_column: u64,
    /// The maximum size of a page in bytes, if a single array would create
    /// a page larger than this then it will be split into multiple pages
    pub max_page_bytes: u64,
    /// If false (the default) then arrays will be copied (deeply) before
    /// being cached.  This ensures any data kept alive by the array can
    /// be discarded safely and helps avoid writer accumulation.  However,
    /// there is an associated cost.
    pub keep_original_array: bool,
    /// The alignment that the writer is applying to buffers
    ///
    /// The encoder needs to know this so it figures the position of out-of-line
    /// buffers correctly
    pub buffer_alignment: u64,
}

impl Default for EncodingOptions {
    fn default() -> Self {
        Self {
            cache_bytes_per_column: 8 * 1024 * 1024,
            max_page_bytes: 32 * 1024 * 1024,
            keep_original_array: true,
            buffer_alignment: 64,
        }
    }
}

/// A trait to pick which kind of field encoding to use for a field
///
/// Unlike the ArrayEncodingStrategy, the field encoding strategy is
/// chosen before any data is generated and the same field encoder is
/// used for all data in the field.
pub trait FieldEncodingStrategy: Send + Sync + std::fmt::Debug {
    /// Choose and create an appropriate field encoder for the given
    /// field.
    ///
    /// The field encoder can be chosen on the data type as well as
    /// any metadata that is attached to the field.
    ///
    /// The `encoding_strategy_root` is the encoder that should be
    /// used to encode any inner data in struct / list / etc. fields.
    ///
    /// Initially it is the same as `self` and generally should be
    /// forwarded to any inner encoding strategy.
    fn create_field_encoder(
        &self,
        encoding_strategy_root: &dyn FieldEncodingStrategy,
        field: &Field,
        column_index: &mut ColumnIndexSequence,
        options: &EncodingOptions,
    ) -> Result<Box<dyn FieldEncoder>>;
}

pub fn default_encoding_strategy(version: LanceFileVersion) -> Box<dyn FieldEncodingStrategy> {
    match version.resolve() {
        LanceFileVersion::Legacy => panic!(),
        LanceFileVersion::V2_0 => Box::new(
            crate::previous::encoder::CoreFieldEncodingStrategy::new(version),
        ),
        _ => Box::new(StructuralEncodingStrategy::with_version(version)),
    }
}

/// Create an encoding strategy with user-configured compression parameters
pub fn default_encoding_strategy_with_params(
    version: LanceFileVersion,
    params: CompressionParams,
) -> Result<Box<dyn FieldEncodingStrategy>> {
    match version.resolve() {
        LanceFileVersion::Legacy | LanceFileVersion::V2_0 => Err(Error::invalid_input(
            "Compression parameters are only supported in Lance file version 2.1 and later",
            location!(),
        )),
        _ => {
            let compression_strategy =
                Arc::new(DefaultCompressionStrategy::with_params(params).with_version(version));
            Ok(Box::new(StructuralEncodingStrategy {
                compression_strategy,
                version,
            }))
        }
    }
}

/// An encoding strategy used for 2.1+ files
#[derive(Debug)]
pub struct StructuralEncodingStrategy {
    pub compression_strategy: Arc<dyn CompressionStrategy>,
    pub version: LanceFileVersion,
}

// For some reason, clippy thinks we can add Default to the above derive but
// rustc doesn't agree (no default for Arc<dyn Trait>)
#[allow(clippy::derivable_impls)]
impl Default for StructuralEncodingStrategy {
    fn default() -> Self {
        Self {
            compression_strategy: Arc::new(DefaultCompressionStrategy::new()),
            version: LanceFileVersion::default(),
        }
    }
}

impl StructuralEncodingStrategy {
    pub fn with_version(version: LanceFileVersion) -> Self {
        Self {
            compression_strategy: Arc::new(DefaultCompressionStrategy::new().with_version(version)),
            version,
        }
    }

    fn is_primitive_type(data_type: &DataType) -> bool {
        matches!(
            data_type,
            DataType::Boolean
                | DataType::Date32
                | DataType::Date64
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
                | DataType::Duration(_)
                | DataType::Float16
                | DataType::Float32
                | DataType::Float64
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Int8
                | DataType::Interval(_)
                | DataType::Null
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Timestamp(_, _)
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::UInt8
                | DataType::FixedSizeBinary(_)
                | DataType::FixedSizeList(_, _)
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::Utf8
                | DataType::LargeUtf8,
        )
    }

    fn do_create_field_encoder(
        &self,
        _encoding_strategy_root: &dyn FieldEncodingStrategy,
        field: &Field,
        column_index: &mut ColumnIndexSequence,
        options: &EncodingOptions,
        root_field_metadata: &HashMap<String, String>,
    ) -> Result<Box<dyn FieldEncoder>> {
        let data_type = field.data_type();

        // Check if field is marked as blob
        if field.is_blob() {
            match data_type {
                DataType::Binary | DataType::LargeBinary => {
                    return Ok(Box::new(BlobStructuralEncoder::new(
                        field,
                        column_index.next_column_index(field.id as u32),
                        options,
                        self.compression_strategy.clone(),
                    )?));
                }
                DataType::Struct(_) if self.version >= LanceFileVersion::V2_2 => {
                    return Ok(Box::new(BlobV2StructuralEncoder::new(
                        field,
                        column_index.next_column_index(field.id as u32),
                        options,
                        self.compression_strategy.clone(),
                    )?));
                }
                DataType::Struct(_) => {
                    return Err(Error::InvalidInput {
                        source: "Blob v2 struct input requires file version >= 2.2".into(),
                        location: location!(),
                    });
                }
                _ => {
                    return Err(Error::InvalidInput {
                        source: format!(
                            "Blob encoding only supports Binary/LargeBinary or v2 Struct, got {}",
                            data_type
                        )
                        .into(),
                        location: location!(),
                    });
                }
            }
        }

        if Self::is_primitive_type(&data_type) {
            Ok(Box::new(PrimitiveStructuralEncoder::try_new(
                options,
                self.compression_strategy.clone(),
                column_index.next_column_index(field.id as u32),
                field.clone(),
                Arc::new(root_field_metadata.clone()),
            )?))
        } else {
            match data_type {
                DataType::List(_) | DataType::LargeList(_) => {
                    let child = field.children.first().expect("List should have a child");
                    let child_encoder = self.do_create_field_encoder(
                        _encoding_strategy_root,
                        child,
                        column_index,
                        options,
                        root_field_metadata,
                    )?;
                    Ok(Box::new(ListStructuralEncoder::new(
                        options.keep_original_array,
                        child_encoder,
                    )))
                }
                DataType::Struct(fields) => {
                    if field.is_packed_struct() || fields.is_empty() {
                        // Both packed structs and empty structs are encoded as primitive
                        Ok(Box::new(PrimitiveStructuralEncoder::try_new(
                            options,
                            self.compression_strategy.clone(),
                            column_index.next_column_index(field.id as u32),
                            field.clone(),
                            Arc::new(root_field_metadata.clone()),
                        )?))
                    } else {
                        let children_encoders = field
                            .children
                            .iter()
                            .map(|field| {
                                self.do_create_field_encoder(
                                    _encoding_strategy_root,
                                    field,
                                    column_index,
                                    options,
                                    root_field_metadata,
                                )
                            })
                            .collect::<Result<Vec<_>>>()?;
                        Ok(Box::new(StructStructuralEncoder::new(
                            options.keep_original_array,
                            children_encoders,
                        )))
                    }
                }
                DataType::Dictionary(_, value_type) => {
                    // A dictionary of primitive is, itself, primitive
                    if Self::is_primitive_type(&value_type) {
                        Ok(Box::new(PrimitiveStructuralEncoder::try_new(
                            options,
                            self.compression_strategy.clone(),
                            column_index.next_column_index(field.id as u32),
                            field.clone(),
                            Arc::new(root_field_metadata.clone()),
                        )?))
                    } else {
                        // A dictionary of logical is, itself, logical and we don't support that today
                        // It could be possible (e.g. store indices in one column and values in remaining columns)
                        // but would be a significant amount of work
                        //
                        // An easier fallback implementation would be to decode-on-write and encode-on-read
                        Err(Error::NotSupported { source: format!("cannot encode a dictionary column whose value type is a logical type ({})", value_type).into(), location: location!() })
                    }
                }
                _ => todo!("Implement encoding for field {}", field),
            }
        }
    }
}

impl FieldEncodingStrategy for StructuralEncodingStrategy {
    fn create_field_encoder(
        &self,
        encoding_strategy_root: &dyn FieldEncodingStrategy,
        field: &Field,
        column_index: &mut ColumnIndexSequence,
        options: &EncodingOptions,
    ) -> Result<Box<dyn FieldEncoder>> {
        self.do_create_field_encoder(
            encoding_strategy_root,
            field,
            column_index,
            options,
            &field.metadata,
        )
    }
}

/// A batch encoder that encodes RecordBatch objects by delegating
/// to field encoders for each top-level field in the batch.
pub struct BatchEncoder {
    pub field_encoders: Vec<Box<dyn FieldEncoder>>,
    pub field_id_to_column_index: Vec<(u32, u32)>,
}

impl BatchEncoder {
    pub fn try_new(
        schema: &Schema,
        strategy: &dyn FieldEncodingStrategy,
        options: &EncodingOptions,
    ) -> Result<Self> {
        let mut col_idx = 0;
        let mut col_idx_sequence = ColumnIndexSequence::default();
        let field_encoders = schema
            .fields
            .iter()
            .map(|field| {
                let encoder = strategy.create_field_encoder(
                    strategy,
                    field,
                    &mut col_idx_sequence,
                    options,
                )?;
                col_idx += encoder.as_ref().num_columns();
                Ok(encoder)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            field_encoders,
            field_id_to_column_index: col_idx_sequence.mapping,
        })
    }

    pub fn num_columns(&self) -> u32 {
        self.field_encoders
            .iter()
            .map(|field_encoder| field_encoder.num_columns())
            .sum::<u32>()
    }
}

/// An encoded batch of data and a page table describing it
///
/// This is returned by [`crate::encoder::encode_batch`]
#[derive(Debug)]
pub struct EncodedBatch {
    pub data: Bytes,
    pub page_table: Vec<Arc<ColumnInfo>>,
    pub schema: Arc<Schema>,
    pub top_level_columns: Vec<u32>,
    pub num_rows: u64,
}

fn write_page_to_data_buffer(page: EncodedPage, data_buffer: &mut BytesMut) -> PageInfo {
    let buffers = page.data;
    let mut buffer_offsets_and_sizes = Vec::with_capacity(buffers.len());
    for buffer in buffers {
        let buffer_offset = data_buffer.len() as u64;
        data_buffer.extend_from_slice(&buffer);
        let size = data_buffer.len() as u64 - buffer_offset;
        buffer_offsets_and_sizes.push((buffer_offset, size));
    }

    PageInfo {
        buffer_offsets_and_sizes: Arc::from(buffer_offsets_and_sizes),
        encoding: page.description,
        num_rows: page.num_rows,
        priority: page.row_number,
    }
}

/// Helper method to encode a batch of data into memory
///
/// This is primarily for testing and benchmarking but could be useful in other
/// niche situations like IPC.
pub async fn encode_batch(
    batch: &RecordBatch,
    schema: Arc<Schema>,
    encoding_strategy: &dyn FieldEncodingStrategy,
    options: &EncodingOptions,
) -> Result<EncodedBatch> {
    if !is_pwr_two(options.buffer_alignment) || options.buffer_alignment < MIN_PAGE_BUFFER_ALIGNMENT
    {
        return Err(Error::InvalidInput {
            source: format!(
                "buffer_alignment must be a power of two and at least {}",
                MIN_PAGE_BUFFER_ALIGNMENT
            )
            .into(),
            location: location!(),
        });
    }

    let mut data_buffer = BytesMut::new();
    let lance_schema = Schema::try_from(batch.schema().as_ref())?;
    let options = EncodingOptions {
        keep_original_array: true,
        ..*options
    };
    let batch_encoder = BatchEncoder::try_new(&lance_schema, encoding_strategy, &options)?;
    let mut page_table = Vec::new();
    let mut col_idx_offset = 0;
    for (arr, mut encoder) in batch.columns().iter().zip(batch_encoder.field_encoders) {
        let mut external_buffers =
            OutOfLineBuffers::new(data_buffer.len() as u64, options.buffer_alignment);
        let repdef = RepDefBuilder::default();
        let encoder = encoder.as_mut();
        let num_rows = arr.len() as u64;
        let mut tasks =
            encoder.maybe_encode(arr.clone(), &mut external_buffers, repdef, 0, num_rows)?;
        tasks.extend(encoder.flush(&mut external_buffers)?);
        for buffer in external_buffers.take_buffers() {
            data_buffer.extend_from_slice(&buffer);
        }
        let mut pages = HashMap::<u32, Vec<PageInfo>>::new();
        for task in tasks {
            let encoded_page = task.await?;
            // Write external buffers first
            pages
                .entry(encoded_page.column_idx)
                .or_default()
                .push(write_page_to_data_buffer(encoded_page, &mut data_buffer));
        }
        let mut external_buffers =
            OutOfLineBuffers::new(data_buffer.len() as u64, options.buffer_alignment);
        let encoded_columns = encoder.finish(&mut external_buffers).await?;
        for buffer in external_buffers.take_buffers() {
            data_buffer.extend_from_slice(&buffer);
        }
        let num_columns = encoded_columns.len();
        for (col_idx, encoded_column) in encoded_columns.into_iter().enumerate() {
            let col_idx = col_idx + col_idx_offset;
            let mut col_buffer_offsets_and_sizes = Vec::new();
            for buffer in encoded_column.column_buffers {
                let buffer_offset = data_buffer.len() as u64;
                data_buffer.extend_from_slice(&buffer);
                let size = data_buffer.len() as u64 - buffer_offset;
                col_buffer_offsets_and_sizes.push((buffer_offset, size));
            }
            for page in encoded_column.final_pages {
                pages
                    .entry(page.column_idx)
                    .or_default()
                    .push(write_page_to_data_buffer(page, &mut data_buffer));
            }
            let col_pages = std::mem::take(pages.entry(col_idx as u32).or_default());
            page_table.push(Arc::new(ColumnInfo {
                index: col_idx as u32,
                buffer_offsets_and_sizes: Arc::from(
                    col_buffer_offsets_and_sizes.into_boxed_slice(),
                ),
                page_infos: Arc::from(col_pages.into_boxed_slice()),
                encoding: encoded_column.encoding,
            }))
        }
        col_idx_offset += num_columns;
    }
    let top_level_columns = batch_encoder
        .field_id_to_column_index
        .iter()
        .map(|(_, idx)| *idx)
        .collect();
    Ok(EncodedBatch {
        data: data_buffer.freeze(),
        top_level_columns,
        page_table,
        schema,
        num_rows: batch.num_rows() as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression_config::{CompressionFieldParams, CompressionParams};

    #[test]
    fn test_configured_encoding_strategy() {
        // Create test parameters
        let mut params = CompressionParams::new();
        params.columns.insert(
            "*_id".to_string(),
            CompressionFieldParams {
                rle_threshold: Some(0.5),
                compression: Some("lz4".to_string()),
                compression_level: None,
                bss: None,
            },
        );

        // Test with V2.1 - should succeed
        let strategy =
            default_encoding_strategy_with_params(LanceFileVersion::V2_1, params.clone())
                .expect("Should succeed for V2.1");

        // Verify it's a StructuralEncodingStrategy
        assert!(format!("{:?}", strategy).contains("StructuralEncodingStrategy"));
        assert!(format!("{:?}", strategy).contains("DefaultCompressionStrategy"));

        // Test with V2.0 - should fail
        let err = default_encoding_strategy_with_params(LanceFileVersion::V2_0, params.clone())
            .expect_err("Should fail for V2.0");
        assert!(err
            .to_string()
            .contains("only supported in Lance file version 2.1"));

        // Test with Legacy - should fail
        let err = default_encoding_strategy_with_params(LanceFileVersion::Legacy, params)
            .expect_err("Should fail for Legacy");
        assert!(err
            .to_string()
            .contains("only supported in Lance file version 2.1"));
    }
}
