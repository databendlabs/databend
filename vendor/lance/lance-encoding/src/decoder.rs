// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Utilities and traits for scheduling & decoding data
//!
//! Reading data involves two steps: scheduling and decoding.  The
//! scheduling step is responsible for figuring out what data is needed
//! and issuing the appropriate I/O requests.  The decoding step is
//! responsible for taking the loaded data and turning it into Arrow
//! arrays.
//!
//! # Scheduling
//!
//! Scheduling is split into [`self::FieldScheduler`] and [`self::PageScheduler`].
//! There is one field scheduler for each output field, which may map to many
//! columns of actual data.  A field scheduler is responsible for figuring out
//! the order in which pages should be scheduled.  Field schedulers then delegate
//! to page schedulers to figure out the I/O requests that need to be made for
//! the page.
//!
//! Page schedulers also create the decoders that will be used to decode the
//! scheduled data.
//!
//! # Decoding
//!
//! Decoders are split into [`self::PhysicalPageDecoder`] and
//! [`self::LogicalPageDecoder`].  Note that both physical and logical decoding
//! happens on a per-page basis.  There is no concept of a "field decoder" or
//! "column decoder".
//!
//! The physical decoders handle lower level encodings.  They have a few advantages:
//!
//!  * They do not need to decode into an Arrow array and so they don't need
//!    to be enveloped into the Arrow filesystem (e.g. Arrow doesn't have a
//!    bit-packed type.  We can use variable-length binary but that is kind
//!    of awkward)
//!  * They can decode into an existing allocation.  This can allow for "page
//!    bridging".  If we are trying to decode into a batch of 1024 rows and
//!    the rows 0..1024 are spread across two pages then we can avoid a memory
//!    copy by allocating once and decoding each page into the outer allocation.
//!    (note: page bridging is not actually implemented yet)
//!
//! However, there are some limitations for physical decoders:
//!
//!  * They are constrained to a single column
//!  * The API is more complex
//!
//! The logical decoders are designed to map one or more columns of Lance
//! data into an Arrow array.
//!
//! Typically, a "logical encoding" will have both a logical decoder and a field scheduler.
//! Meanwhile, a "physical encoding" will have a physical decoder but no corresponding field
//! scheduler.
//!
//!
//! # General notes
//!
//! Encodings are typically nested into each other to form a tree.  The top of the tree is
//! the user requested schema.  Each field in that schema is assigned to one top-level logical
//! encoding.  That encoding can then contain other logical encodings or physical encodings.
//! Physical encodings can also contain other physical encodings.
//!
//! So, for example, a single field in the Arrow schema might have the type List<UInt32>
//!
//! The encoding tree could then be:
//!
//! root: List (logical encoding)
//!  - indices: Primitive (logical encoding)
//!    - column: Basic (physical encoding)
//!      - validity: Bitmap (physical encoding)
//!      - values: RLE (physical encoding)
//!        - runs: Value (physical encoding)
//!        - values: Value (physical encoding)
//!  - items: Primitive (logical encoding)
//!    - column: Basic (physical encoding)
//!      - values: Value (physical encoding)
//!
//! Note that, in this example, root.items.column does not have a validity because there were
//! no nulls in the page.
//!
//! ## Multiple buffers or multiple columns?
//!
//! Note that there are many different ways we can write encodings.  For example, we might
//! store primitive fields in a single column with two buffers (one for validity and one for
//! values)
//!
//! On the other hand, we could also store a primitive field as two different columns.  One
//! that yields a non-nullable boolean array and one that yields a non-nullable array of items.
//! Then we could combine these two arrays into a single array where the boolean array is the
//! bitmap.  There are a few subtle differences between the approaches:
//!
//! * Storing things as multiple buffers within the same column is generally more efficient and
//!   easier to schedule.  For example, in-batch coalescing is very easy but can only be done
//!   on data that is in the same page.
//! * When things are stored in multiple columns you have to worry about their pages not being
//!   in sync.  In our previous validity / values example this means we might have to do some
//!   memory copies to get the validity array and values arrays to be the same length as
//!   decode.
//! * When things are stored in a single column, projection is impossible.  For example, if we
//!   tried to store all the struct fields in a single column with lots of buffers then we wouldn't
//!   be able to read back individual fields of the struct.
//!
//! The fixed size list decoding is an interesting example because it is actually both a physical
//! encoding and a logical encoding.  A fixed size list of a physical encoding is, itself, a physical
//! encoding (e.g. a fixed size list of doubles).  However, a fixed size list of a logical encoding
//! is a logical encoding (e.g. a fixed size list of structs).
//!
//! # The scheduling loop
//!
//! Reading a Lance file involves both scheduling and decoding.  Its generally expected that these
//! will run as two separate threads.
//!
//! ```text
//!
//!                                    I/O PARALLELISM
//!                       Issues
//!                       Requests   ┌─────────────────┐
//!                                  │                 │        Wait for
//!                       ┌──────────►   I/O Service   ├─────►  Enough I/O ◄─┐
//!                       │          │                 │        For batch    │
//!                       │          └─────────────────┘             │3      │
//!                       │                                          │       │
//!                       │                                          │       │2
//! ┌─────────────────────┴─┐                              ┌─────────▼───────┴┐
//! │                       │                              │                  │Poll
//! │       Batch Decode    │ Decode tasks sent via channel│   Batch Decode   │1
//! │       Scheduler       ├─────────────────────────────►│   Stream         ◄─────
//! │                       │                              │                  │
//! └─────▲─────────────┬───┘                              └─────────┬────────┘
//!       │             │                                            │4
//!       │             │                                            │
//!       └─────────────┘                                   ┌────────┴────────┐
//!  Caller of schedule_range                Buffer polling │                 │
//!  will be scheduler thread                to achieve CPU │ Decode Batch    ├────►
//!  and schedule one decode                 parallelism    │ Task            │
//!  task (and all needed I/O)               (thread per    │                 │
//!  per logical page                         batch)        └─────────────────┘
//! ```
//!
//! The scheduling thread will work through the file from the
//! start to the end as quickly as possible.  Data is scheduled one page at a time in a row-major
//! fashion.  For example, imagine we have a file with the following page structure:
//!
//! ```text
//! Score (Float32)     | C0P0 |
//! Id (16-byte UUID)   | C1P0 | C1P1 | C1P2 | C1P3 |
//! Vector (4096 bytes) | C2P0 | C2P1 | C2P2 | C2P3 | .. | C2P1024 |
//! ```
//!
//! This would be quite common as each of these pages has the same number of bytes.  Let's pretend
//! each page is 1MiB and so there are 256Ki rows of data.  Each page of `Score` has 256Ki rows.
//! Each page of `Id` has 64Ki rows.  Each page of `Vector` has 256 rows.  The scheduler would then
//! schedule in the following order:
//!
//! C0 P0
//! C1 P0
//! C2 P0
//! C2 P1
//! ... (254 pages omitted)
//! C2 P255
//! C1 P1
//! C2 P256
//! ... (254 pages omitted)
//! C2 P511
//! C1 P2
//! C2 P512
//! ... (254 pages omitted)
//! C2 P767
//! C1 P3
//! C2 P768
//! ... (254 pages omitted)
//! C2 P1024
//!
//! This is the ideal scheduling order because it means we can decode complete rows as quickly as possible.
//! Note that the scheduler thread does not need to wait for I/O to happen at any point.  As soon as it starts
//! it will start scheduling one page of I/O after another until it has scheduled the entire file's worth of
//! I/O.  This is slightly different than other file readers which have "row group parallelism" and will
//! typically only schedule X row groups worth of reads at a time.
//!
//! In the near future there will be a backpressure mechanism and so it may need to stop/pause if the compute
//! falls behind.
//!
//! ## Indirect I/O
//!
//! Regrettably, there are times where we cannot know exactly what data we need until we have partially decoded
//! the file.  This happens when we have variable sized list data.  In that case the scheduling task for that
//! page will only schedule the first part of the read (loading the list offsets).  It will then immediately
//! spawn a new tokio task to wait for that I/O and decode the list offsets.  That follow-up task is not part
//! of the scheduling loop or the decode loop.  It is a free task.  Once the list offsets are decoded we submit
//! a follow-up I/O task.  This task is scheduled at a high priority because the decoder is going to need it soon.
//!
//! # The decode loop
//!
//! As soon as the scheduler starts we can start decoding.  Each time we schedule a page we
//! push a decoder for that page's data into a channel.  The decode loop
//! ([`BatchDecodeStream`]) reads from that channel.  Each time it receives a decoder it
//! waits until the decoder has all of its data.  Then it grabs the next decoder.  Once it has
//! enough loaded decoders to complete a batch worth of rows it will spawn a "decode batch task".
//!
//! These batch decode tasks perform the actual CPU work of decoding the loaded data into Arrow
//! arrays.  This may involve signifciant CPU processing like decompression or arithmetic in order
//! to restore the data to its correct in-memory representation.
//!
//! ## Batch size
//!
//! The `BatchDecodeStream` is configured with a batch size.  This does not need to have any
//! relation to the page size(s) used to write the data.  This keeps our compute work completely
//! independent of our I/O work.  We suggest using small batch sizes:
//!
//!  * Batches should fit in CPU cache (at least L3)
//!  * More batches means more opportunity for parallelism
//!  * The "batch overhead" is very small in Lance compared to other formats because it has no
//!    relation to the way the data is stored.

use std::collections::VecDeque;
use std::sync::{LazyLock, Once};
use std::{ops::Range, sync::Arc};

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{ArrowError, DataType, Field as ArrowField, Fields, Schema as ArrowSchema};
use bytes::Bytes;
use futures::future::{maybe_done, BoxFuture, MaybeDone};
use futures::stream::{self, BoxStream};
use futures::{FutureExt, StreamExt};
use lance_arrow::DataTypeExt;
use lance_core::cache::LanceCache;
use lance_core::datatypes::{Field, Schema, BLOB_DESC_LANCE_FIELD};
use lance_core::utils::futures::FinallyStreamExt;
use log::{debug, trace, warn};
use snafu::location;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{self, unbounded_channel};

use lance_core::{ArrowResult, Error, Result};
use tracing::instrument;

use crate::compression::{DecompressionStrategy, DefaultDecompressionStrategy};
use crate::data::DataBlock;
use crate::encoder::EncodedBatch;
use crate::encodings::logical::list::StructuralListScheduler;
use crate::encodings::logical::primitive::StructuralPrimitiveFieldScheduler;
use crate::encodings::logical::r#struct::{StructuralStructDecoder, StructuralStructScheduler};
use crate::format::pb::{self, column_encoding};
use crate::format::pb21;
use crate::previous::decoder::LogicalPageDecoder;
use crate::previous::encodings::logical::list::OffsetPageInfo;
use crate::previous::encodings::logical::r#struct::{SimpleStructDecoder, SimpleStructScheduler};
use crate::previous::encodings::logical::{
    binary::BinaryFieldScheduler, blob::BlobFieldScheduler, list::ListFieldScheduler,
    primitive::PrimitiveFieldScheduler,
};
use crate::repdef::{CompositeRepDefUnraveler, RepDefUnraveler};
use crate::version::LanceFileVersion;
use crate::{BufferScheduler, EncodingsIo};

// If users are getting batches over 10MiB large then it's time to reduce the batch size
const BATCH_SIZE_BYTES_WARNING: u64 = 10 * 1024 * 1024;

/// Top-level encoding message for a page.  Wraps both the
/// legacy pb::ArrayEncoding and the newer pb::PageLayout
///
/// A file should only use one or the other and never both.
/// 2.0 decoders can always assume this is pb::ArrayEncoding
/// and 2.1+ decoders can always assume this is pb::PageLayout
#[derive(Debug)]
pub enum PageEncoding {
    Legacy(pb::ArrayEncoding),
    Structural(pb21::PageLayout),
}

impl PageEncoding {
    pub fn as_legacy(&self) -> &pb::ArrayEncoding {
        match self {
            Self::Legacy(enc) => enc,
            Self::Structural(_) => panic!("Expected a legacy encoding"),
        }
    }

    pub fn as_structural(&self) -> &pb21::PageLayout {
        match self {
            Self::Structural(enc) => enc,
            Self::Legacy(_) => panic!("Expected a structural encoding"),
        }
    }

    pub fn is_structural(&self) -> bool {
        matches!(self, Self::Structural(_))
    }
}

/// Metadata describing a page in a file
///
/// This is typically created by reading the metadata section of a Lance file
#[derive(Debug)]
pub struct PageInfo {
    /// The number of rows in the page
    pub num_rows: u64,
    /// The priority (top level row number) of the page
    ///
    /// This is only set in 2.1 files and will be 0 for 2.0 files
    pub priority: u64,
    /// The encoding that explains the buffers in the page
    pub encoding: PageEncoding,
    /// The offsets and sizes of the buffers in the file
    pub buffer_offsets_and_sizes: Arc<[(u64, u64)]>,
}

/// Metadata describing a column in a file
///
/// This is typically created by reading the metadata section of a Lance file
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    /// The index of the column in the file
    pub index: u32,
    /// The metadata for each page in the column
    pub page_infos: Arc<[PageInfo]>,
    /// File positions and their sizes of the column-level buffers
    pub buffer_offsets_and_sizes: Arc<[(u64, u64)]>,
    pub encoding: pb::ColumnEncoding,
}

impl ColumnInfo {
    /// Create a new instance
    pub fn new(
        index: u32,
        page_infos: Arc<[PageInfo]>,
        buffer_offsets_and_sizes: Vec<(u64, u64)>,
        encoding: pb::ColumnEncoding,
    ) -> Self {
        Self {
            index,
            page_infos,
            buffer_offsets_and_sizes: buffer_offsets_and_sizes.into_boxed_slice().into(),
            encoding,
        }
    }

    pub fn is_structural(&self) -> bool {
        self.page_infos
            // Can just look at the first since all should be the same
            .first()
            .map(|page| page.encoding.is_structural())
            .unwrap_or(false)
    }
}

enum RootScheduler {
    Structural(Box<dyn StructuralFieldScheduler>),
    Legacy(Arc<dyn crate::previous::decoder::FieldScheduler>),
}

impl RootScheduler {
    fn as_legacy(&self) -> &Arc<dyn crate::previous::decoder::FieldScheduler> {
        match self {
            Self::Structural(_) => panic!("Expected a legacy scheduler"),
            Self::Legacy(s) => s,
        }
    }

    fn as_structural(&self) -> &dyn StructuralFieldScheduler {
        match self {
            Self::Structural(s) => s.as_ref(),
            Self::Legacy(_) => panic!("Expected a structural scheduler"),
        }
    }
}

/// The scheduler for decoding batches
///
/// Lance decoding is done in two steps, scheduling, and decoding.  The
/// scheduling tends to be lightweight and should quickly figure what data
/// is needed from the disk issue the appropriate I/O requests.  A decode task is
/// created to eventually decode the data (once it is loaded) and scheduling
/// moves on to scheduling the next page.
///
/// Meanwhile, it's expected that a decode stream will be setup to run at the
/// same time.  Decode tasks take the data that is loaded and turn it into
/// Arrow arrays.
///
/// This approach allows us to keep our I/O parallelism and CPU parallelism
/// completely separate since those are often two very different values.
///
/// Backpressure should be achieved via the I/O service.  Requests that are
/// issued will pile up if the decode stream is not polling quickly enough.
/// The [`crate::EncodingsIo::submit_request`] function should return a pending
/// future once there are too many I/O requests in flight.
///
/// TODO: Implement backpressure
pub struct DecodeBatchScheduler {
    root_scheduler: RootScheduler,
    pub root_fields: Fields,
    cache: Arc<LanceCache>,
}

pub struct ColumnInfoIter<'a> {
    column_infos: Vec<Arc<ColumnInfo>>,
    column_indices: &'a [u32],
    column_info_pos: usize,
    column_indices_pos: usize,
}

impl<'a> ColumnInfoIter<'a> {
    pub fn new(column_infos: Vec<Arc<ColumnInfo>>, column_indices: &'a [u32]) -> Self {
        let initial_pos = column_indices.first().copied().unwrap_or(0) as usize;
        Self {
            column_infos,
            column_indices,
            column_info_pos: initial_pos,
            column_indices_pos: 0,
        }
    }

    pub fn peek(&self) -> &Arc<ColumnInfo> {
        &self.column_infos[self.column_info_pos]
    }

    pub fn peek_transform(&mut self, transform: impl FnOnce(Arc<ColumnInfo>) -> Arc<ColumnInfo>) {
        let column_info = self.column_infos[self.column_info_pos].clone();
        let transformed = transform(column_info);
        self.column_infos[self.column_info_pos] = transformed;
    }

    pub fn expect_next(&mut self) -> Result<&Arc<ColumnInfo>> {
        self.next().ok_or_else(|| {
            Error::invalid_input(
                "there were more fields in the schema than provided column indices / infos",
                location!(),
            )
        })
    }

    fn next(&mut self) -> Option<&Arc<ColumnInfo>> {
        if self.column_info_pos < self.column_infos.len() {
            let info = &self.column_infos[self.column_info_pos];
            self.column_info_pos += 1;
            Some(info)
        } else {
            None
        }
    }

    pub(crate) fn next_top_level(&mut self) {
        self.column_indices_pos += 1;
        if self.column_indices_pos < self.column_indices.len() {
            self.column_info_pos = self.column_indices[self.column_indices_pos] as usize;
        } else {
            self.column_info_pos = self.column_infos.len();
        }
    }
}

/// These contain the file buffers shared across the entire file
#[derive(Clone, Copy, Debug)]
pub struct FileBuffers<'a> {
    pub positions_and_sizes: &'a [(u64, u64)],
}

/// These contain the file buffers and also buffers specific to a column
#[derive(Clone, Copy, Debug)]
pub struct ColumnBuffers<'a, 'b> {
    pub file_buffers: FileBuffers<'a>,
    pub positions_and_sizes: &'b [(u64, u64)],
}

/// These contain the file & column buffers and also buffers specific to a page
#[derive(Clone, Copy, Debug)]
pub struct PageBuffers<'a, 'b, 'c> {
    pub column_buffers: ColumnBuffers<'a, 'b>,
    pub positions_and_sizes: &'c [(u64, u64)],
}

/// The core decoder strategy handles all the various Arrow types
#[derive(Debug)]
pub struct CoreFieldDecoderStrategy {
    pub validate_data: bool,
    pub decompressor_strategy: Arc<dyn DecompressionStrategy>,
    pub cache_repetition_index: bool,
}

impl Default for CoreFieldDecoderStrategy {
    fn default() -> Self {
        Self {
            validate_data: false,
            decompressor_strategy: Arc::new(DefaultDecompressionStrategy {}),
            cache_repetition_index: false,
        }
    }
}

impl CoreFieldDecoderStrategy {
    /// Create a new strategy with cache_repetition_index enabled
    pub fn with_cache_repetition_index(mut self, cache_repetition_index: bool) -> Self {
        self.cache_repetition_index = cache_repetition_index;
        self
    }

    /// Create a new strategy from decoder config
    pub fn from_decoder_config(config: &DecoderConfig) -> Self {
        Self {
            validate_data: config.validate_on_decode,
            decompressor_strategy: Arc::new(DefaultDecompressionStrategy {}),
            cache_repetition_index: config.cache_repetition_index,
        }
    }

    /// This is just a sanity check to ensure there is no "wrapped encodings"
    /// that haven't been handled.
    fn ensure_values_encoded(column_info: &ColumnInfo, field_name: &str) -> Result<()> {
        let column_encoding = column_info
            .encoding
            .column_encoding
            .as_ref()
            .ok_or_else(|| {
                Error::invalid_input(
                    format!(
                        "the column at index {} was missing a ColumnEncoding",
                        column_info.index
                    ),
                    location!(),
                )
            })?;
        if matches!(
            column_encoding,
            pb::column_encoding::ColumnEncoding::Values(_)
        ) {
            Ok(())
        } else {
            Err(Error::invalid_input(format!("the column at index {} mapping to the input field {} has column encoding {:?} and no decoder is registered to handle it", column_info.index, field_name, column_encoding), location!()))
        }
    }

    fn is_structural_primitive(data_type: &DataType) -> bool {
        if data_type.is_primitive() {
            true
        } else {
            match data_type {
                // DataType::is_primitive doesn't consider these primitive but we do
                DataType::Dictionary(_, value_type) => Self::is_structural_primitive(value_type),
                DataType::Boolean
                | DataType::Null
                | DataType::FixedSizeBinary(_)
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::Utf8
                | DataType::LargeUtf8 => true,
                DataType::FixedSizeList(inner, _) => {
                    Self::is_structural_primitive(inner.data_type())
                }
                _ => false,
            }
        }
    }

    fn is_primitive_legacy(data_type: &DataType) -> bool {
        if data_type.is_primitive() {
            true
        } else {
            match data_type {
                // DataType::is_primitive doesn't consider these primitive but we do
                DataType::Boolean | DataType::Null | DataType::FixedSizeBinary(_) => true,
                DataType::FixedSizeList(inner, _) => Self::is_primitive_legacy(inner.data_type()),
                _ => false,
            }
        }
    }

    fn create_primitive_scheduler(
        &self,
        field: &Field,
        column: &ColumnInfo,
        buffers: FileBuffers,
    ) -> Result<Box<dyn crate::previous::decoder::FieldScheduler>> {
        Self::ensure_values_encoded(column, &field.name)?;
        // Primitive fields map to a single column
        let column_buffers = ColumnBuffers {
            file_buffers: buffers,
            positions_and_sizes: &column.buffer_offsets_and_sizes,
        };
        Ok(Box::new(PrimitiveFieldScheduler::new(
            column.index,
            field.data_type(),
            column.page_infos.clone(),
            column_buffers,
            self.validate_data,
        )))
    }

    /// Helper method to verify the page encoding of a struct header column
    fn check_simple_struct(column_info: &ColumnInfo, field_name: &str) -> Result<()> {
        Self::ensure_values_encoded(column_info, field_name)?;
        if column_info.page_infos.len() != 1 {
            return Err(Error::InvalidInput { source: format!("Due to schema we expected a struct column but we received a column with {} pages and right now we only support struct columns with 1 page", column_info.page_infos.len()).into(), location: location!() });
        }
        let encoding = &column_info.page_infos[0].encoding;
        match encoding.as_legacy().array_encoding.as_ref().unwrap() {
            pb::array_encoding::ArrayEncoding::Struct(_) => Ok(()),
            _ => Err(Error::InvalidInput { source: format!("Expected a struct encoding because we have a struct field in the schema but got the encoding {:?}", encoding).into(), location: location!() }),
        }
    }

    fn check_packed_struct(column_info: &ColumnInfo) -> bool {
        let encoding = &column_info.page_infos[0].encoding;
        matches!(
            encoding.as_legacy().array_encoding.as_ref().unwrap(),
            pb::array_encoding::ArrayEncoding::PackedStruct(_)
        )
    }

    fn create_list_scheduler(
        &self,
        list_field: &Field,
        column_infos: &mut ColumnInfoIter,
        buffers: FileBuffers,
        offsets_column: &ColumnInfo,
    ) -> Result<Box<dyn crate::previous::decoder::FieldScheduler>> {
        Self::ensure_values_encoded(offsets_column, &list_field.name)?;
        let offsets_column_buffers = ColumnBuffers {
            file_buffers: buffers,
            positions_and_sizes: &offsets_column.buffer_offsets_and_sizes,
        };
        let items_scheduler =
            self.create_legacy_field_scheduler(&list_field.children[0], column_infos, buffers)?;

        let (inner_infos, null_offset_adjustments): (Vec<_>, Vec<_>) = offsets_column
            .page_infos
            .iter()
            .filter(|offsets_page| offsets_page.num_rows > 0)
            .map(|offsets_page| {
                if let Some(pb::array_encoding::ArrayEncoding::List(list_encoding)) =
                    &offsets_page.encoding.as_legacy().array_encoding
                {
                    let inner = PageInfo {
                        buffer_offsets_and_sizes: offsets_page.buffer_offsets_and_sizes.clone(),
                        encoding: PageEncoding::Legacy(
                            list_encoding.offsets.as_ref().unwrap().as_ref().clone(),
                        ),
                        num_rows: offsets_page.num_rows,
                        priority: 0,
                    };
                    (
                        inner,
                        OffsetPageInfo {
                            offsets_in_page: offsets_page.num_rows,
                            null_offset_adjustment: list_encoding.null_offset_adjustment,
                            num_items_referenced_by_page: list_encoding.num_items,
                        },
                    )
                } else {
                    // TODO: Should probably return Err here
                    panic!("Expected a list column");
                }
            })
            .unzip();
        let inner = Arc::new(PrimitiveFieldScheduler::new(
            offsets_column.index,
            DataType::UInt64,
            Arc::from(inner_infos.into_boxed_slice()),
            offsets_column_buffers,
            self.validate_data,
        )) as Arc<dyn crate::previous::decoder::FieldScheduler>;
        let items_field = match list_field.data_type() {
            DataType::List(inner) => inner,
            DataType::LargeList(inner) => inner,
            _ => unreachable!(),
        };
        let offset_type = if matches!(list_field.data_type(), DataType::List(_)) {
            DataType::Int32
        } else {
            DataType::Int64
        };
        Ok(Box::new(ListFieldScheduler::new(
            inner,
            items_scheduler.into(),
            items_field,
            offset_type,
            null_offset_adjustments,
        )))
    }

    fn unwrap_blob(column_info: &ColumnInfo) -> Option<ColumnInfo> {
        if let column_encoding::ColumnEncoding::Blob(blob) =
            column_info.encoding.column_encoding.as_ref().unwrap()
        {
            let mut column_info = column_info.clone();
            column_info.encoding = blob.inner.as_ref().unwrap().as_ref().clone();
            Some(column_info)
        } else {
            None
        }
    }

    fn create_structural_field_scheduler(
        &self,
        field: &Field,
        column_infos: &mut ColumnInfoIter,
    ) -> Result<Box<dyn StructuralFieldScheduler>> {
        let data_type = field.data_type();
        if Self::is_structural_primitive(&data_type) {
            let column_info = column_infos.expect_next()?;
            let scheduler = Box::new(StructuralPrimitiveFieldScheduler::try_new(
                column_info.as_ref(),
                self.decompressor_strategy.as_ref(),
                self.cache_repetition_index,
                field,
            )?);

            // advance to the next top level column
            column_infos.next_top_level();

            return Ok(scheduler);
        }
        match &data_type {
            DataType::Struct(fields) => {
                if field.is_packed_struct() {
                    // Packed struct
                    let column_info = column_infos.expect_next()?;
                    let scheduler = Box::new(StructuralPrimitiveFieldScheduler::try_new(
                        column_info.as_ref(),
                        self.decompressor_strategy.as_ref(),
                        self.cache_repetition_index,
                        field,
                    )?);

                    // advance to the next top level column
                    column_infos.next_top_level();

                    return Ok(scheduler);
                }
                // Maybe a blob descriptions struct?
                if field.is_blob() {
                    let column_info = column_infos.peek();
                    if column_info.page_infos.iter().any(|page| {
                        matches!(
                            page.encoding,
                            PageEncoding::Structural(pb21::PageLayout {
                                layout: Some(pb21::page_layout::Layout::BlobLayout(_))
                            })
                        )
                    }) {
                        let column_info = column_infos.expect_next()?;
                        let scheduler = Box::new(StructuralPrimitiveFieldScheduler::try_new(
                            column_info.as_ref(),
                            self.decompressor_strategy.as_ref(),
                            self.cache_repetition_index,
                            field,
                        )?);
                        column_infos.next_top_level();
                        return Ok(scheduler);
                    }
                }

                let mut child_schedulers = Vec::with_capacity(field.children.len());
                for field in field.children.iter() {
                    let field_scheduler =
                        self.create_structural_field_scheduler(field, column_infos)?;
                    child_schedulers.push(field_scheduler);
                }

                let fields = fields.clone();
                Ok(
                    Box::new(StructuralStructScheduler::new(child_schedulers, fields))
                        as Box<dyn StructuralFieldScheduler>,
                )
            }
            DataType::List(_) | DataType::LargeList(_) => {
                let child = field
                    .children
                    .first()
                    .expect("List field must have a child");
                let child_scheduler =
                    self.create_structural_field_scheduler(child, column_infos)?;
                Ok(Box::new(StructuralListScheduler::new(child_scheduler))
                    as Box<dyn StructuralFieldScheduler>)
            }
            _ => todo!("create_structural_field_scheduler for {}", data_type),
        }
    }

    fn create_legacy_field_scheduler(
        &self,
        field: &Field,
        column_infos: &mut ColumnInfoIter,
        buffers: FileBuffers,
    ) -> Result<Box<dyn crate::previous::decoder::FieldScheduler>> {
        let data_type = field.data_type();
        if Self::is_primitive_legacy(&data_type) {
            let column_info = column_infos.expect_next()?;
            let scheduler = self.create_primitive_scheduler(field, column_info, buffers)?;
            return Ok(scheduler);
        } else if data_type.is_binary_like() {
            let column_info = column_infos.expect_next()?.clone();
            // Column is blob and user is asking for binary data
            if let Some(blob_col) = Self::unwrap_blob(column_info.as_ref()) {
                let desc_scheduler =
                    self.create_primitive_scheduler(&BLOB_DESC_LANCE_FIELD, &blob_col, buffers)?;
                let blob_scheduler = Box::new(BlobFieldScheduler::new(desc_scheduler.into()));
                return Ok(blob_scheduler);
            }
            if let Some(page_info) = column_info.page_infos.first() {
                if matches!(
                    page_info.encoding.as_legacy(),
                    pb::ArrayEncoding {
                        array_encoding: Some(pb::array_encoding::ArrayEncoding::List(..))
                    }
                ) {
                    let list_type = if matches!(data_type, DataType::Utf8 | DataType::Binary) {
                        DataType::List(Arc::new(ArrowField::new("item", DataType::UInt8, false)))
                    } else {
                        DataType::LargeList(Arc::new(ArrowField::new(
                            "item",
                            DataType::UInt8,
                            false,
                        )))
                    };
                    let list_field = Field::try_from(ArrowField::new(
                        field.name.clone(),
                        list_type,
                        field.nullable,
                    ))
                    .unwrap();
                    let list_scheduler = self.create_list_scheduler(
                        &list_field,
                        column_infos,
                        buffers,
                        &column_info,
                    )?;
                    let binary_scheduler = Box::new(BinaryFieldScheduler::new(
                        list_scheduler.into(),
                        field.data_type(),
                    ));
                    return Ok(binary_scheduler);
                } else {
                    let scheduler =
                        self.create_primitive_scheduler(field, &column_info, buffers)?;
                    return Ok(scheduler);
                }
            } else {
                return self.create_primitive_scheduler(field, &column_info, buffers);
            }
        }
        match &data_type {
            DataType::FixedSizeList(inner, _dimension) => {
                // A fixed size list column could either be a physical or a logical decoder
                // depending on the child data type.
                if Self::is_primitive_legacy(inner.data_type()) {
                    let primitive_col = column_infos.expect_next()?;
                    let scheduler =
                        self.create_primitive_scheduler(field, primitive_col, buffers)?;
                    Ok(scheduler)
                } else {
                    todo!()
                }
            }
            DataType::Dictionary(_key_type, value_type) => {
                if Self::is_primitive_legacy(value_type) || value_type.is_binary_like() {
                    let primitive_col = column_infos.expect_next()?;
                    let scheduler =
                        self.create_primitive_scheduler(field, primitive_col, buffers)?;
                    Ok(scheduler)
                } else {
                    Err(Error::NotSupported {
                        source: format!(
                            "No way to decode into a dictionary field of type {}",
                            value_type
                        )
                        .into(),
                        location: location!(),
                    })
                }
            }
            DataType::List(_) | DataType::LargeList(_) => {
                let offsets_column = column_infos.expect_next()?.clone();
                column_infos.next_top_level();
                self.create_list_scheduler(field, column_infos, buffers, &offsets_column)
            }
            DataType::Struct(fields) => {
                let column_info = column_infos.expect_next()?;

                // Column is blob and user is asking for descriptions
                if let Some(blob_col) = Self::unwrap_blob(column_info.as_ref()) {
                    // Can use primitive scheduler here since descriptions are always packed struct
                    return self.create_primitive_scheduler(field, &blob_col, buffers);
                }

                if Self::check_packed_struct(column_info) {
                    // use packed struct encoding
                    self.create_primitive_scheduler(field, column_info, buffers)
                } else {
                    // use default struct encoding
                    Self::check_simple_struct(column_info, &field.name).unwrap();
                    let num_rows = column_info
                        .page_infos
                        .iter()
                        .map(|page| page.num_rows)
                        .sum();
                    let mut child_schedulers = Vec::with_capacity(field.children.len());
                    for field in &field.children {
                        column_infos.next_top_level();
                        let field_scheduler =
                            self.create_legacy_field_scheduler(field, column_infos, buffers)?;
                        child_schedulers.push(Arc::from(field_scheduler));
                    }

                    let fields = fields.clone();
                    Ok(Box::new(SimpleStructScheduler::new(
                        child_schedulers,
                        fields,
                        num_rows,
                    )))
                }
            }
            // TODO: Still need support for RLE
            _ => todo!(),
        }
    }
}

/// Create's a dummy ColumnInfo for the root column
fn root_column(num_rows: u64) -> ColumnInfo {
    let num_root_pages = num_rows.div_ceil(u32::MAX as u64);
    let final_page_num_rows = num_rows % (u32::MAX as u64);
    let root_pages = (0..num_root_pages)
        .map(|i| PageInfo {
            num_rows: if i == num_root_pages - 1 {
                final_page_num_rows
            } else {
                u64::MAX
            },
            encoding: PageEncoding::Legacy(pb::ArrayEncoding {
                array_encoding: Some(pb::array_encoding::ArrayEncoding::Struct(
                    pb::SimpleStruct {},
                )),
            }),
            priority: 0, // not used in legacy scheduler
            buffer_offsets_and_sizes: Arc::new([]),
        })
        .collect::<Vec<_>>();
    ColumnInfo {
        buffer_offsets_and_sizes: Arc::new([]),
        encoding: pb::ColumnEncoding {
            column_encoding: Some(pb::column_encoding::ColumnEncoding::Values(())),
        },
        index: u32::MAX,
        page_infos: Arc::from(root_pages),
    }
}

pub enum RootDecoder {
    Structural(StructuralStructDecoder),
    Legacy(SimpleStructDecoder),
}

impl RootDecoder {
    pub fn into_structural(self) -> StructuralStructDecoder {
        match self {
            Self::Structural(decoder) => decoder,
            Self::Legacy(_) => panic!("Expected a structural decoder"),
        }
    }

    pub fn into_legacy(self) -> SimpleStructDecoder {
        match self {
            Self::Legacy(decoder) => decoder,
            Self::Structural(_) => panic!("Expected a legacy decoder"),
        }
    }
}

impl DecodeBatchScheduler {
    /// Creates a new decode scheduler with the expected schema and the column
    /// metadata of the file.
    #[allow(clippy::too_many_arguments)]
    pub async fn try_new<'a>(
        schema: &'a Schema,
        column_indices: &[u32],
        column_infos: &[Arc<ColumnInfo>],
        file_buffer_positions_and_sizes: &'a Vec<(u64, u64)>,
        num_rows: u64,
        _decoder_plugins: Arc<DecoderPlugins>,
        io: Arc<dyn EncodingsIo>,
        cache: Arc<LanceCache>,
        filter: &FilterExpression,
        decoder_config: &DecoderConfig,
    ) -> Result<Self> {
        assert!(num_rows > 0);
        let buffers = FileBuffers {
            positions_and_sizes: file_buffer_positions_and_sizes,
        };
        let arrow_schema = ArrowSchema::from(schema);
        let root_fields = arrow_schema.fields().clone();
        let root_type = DataType::Struct(root_fields.clone());
        let mut root_field = Field::try_from(&ArrowField::new("root", root_type, false))?;
        // root_field.children and schema.fields should be identical at this point but the latter
        // has field ids and the former does not.  This line restores that.
        // TODO:  Is there another way to create the root field without forcing a trip through arrow?
        root_field.children.clone_from(&schema.fields);
        root_field
            .metadata
            .insert("__lance_decoder_root".to_string(), "true".to_string());

        if column_infos.is_empty() || column_infos[0].is_structural() {
            let mut column_iter = ColumnInfoIter::new(column_infos.to_vec(), column_indices);

            let strategy = CoreFieldDecoderStrategy::from_decoder_config(decoder_config);
            let mut root_scheduler =
                strategy.create_structural_field_scheduler(&root_field, &mut column_iter)?;

            let context = SchedulerContext::new(io, cache.clone());
            root_scheduler.initialize(filter, &context).await?;

            Ok(Self {
                root_scheduler: RootScheduler::Structural(root_scheduler),
                root_fields,
                cache,
            })
        } else {
            // The old encoding style expected a header column for structs and so we
            // need a header column for the top-level struct
            let mut columns = Vec::with_capacity(column_infos.len() + 1);
            columns.push(Arc::new(root_column(num_rows)));
            columns.extend(column_infos.iter().cloned());

            let adjusted_column_indices = [0_u32]
                .into_iter()
                .chain(column_indices.iter().map(|i| i.saturating_add(1)))
                .collect::<Vec<_>>();
            let mut column_iter = ColumnInfoIter::new(columns, &adjusted_column_indices);
            let strategy = CoreFieldDecoderStrategy::from_decoder_config(decoder_config);
            let root_scheduler =
                strategy.create_legacy_field_scheduler(&root_field, &mut column_iter, buffers)?;

            let context = SchedulerContext::new(io, cache.clone());
            root_scheduler.initialize(filter, &context).await?;

            Ok(Self {
                root_scheduler: RootScheduler::Legacy(root_scheduler.into()),
                root_fields,
                cache,
            })
        }
    }

    #[deprecated(since = "0.29.1", note = "This is for legacy 2.0 paths")]
    pub fn from_scheduler(
        root_scheduler: Arc<dyn crate::previous::decoder::FieldScheduler>,
        root_fields: Fields,
        cache: Arc<LanceCache>,
    ) -> Self {
        Self {
            root_scheduler: RootScheduler::Legacy(root_scheduler),
            root_fields,
            cache,
        }
    }

    fn do_schedule_ranges_structural(
        &mut self,
        ranges: &[Range<u64>],
        filter: &FilterExpression,
        io: Arc<dyn EncodingsIo>,
        mut schedule_action: impl FnMut(Result<DecoderMessage>) -> bool,
    ) {
        let root_scheduler = self.root_scheduler.as_structural();
        let mut context = SchedulerContext::new(io, self.cache.clone());
        let maybe_root_job = root_scheduler.schedule_ranges(ranges, filter);
        if let Err(schedule_ranges_err) = maybe_root_job {
            schedule_action(Err(schedule_ranges_err));
            return;
        }
        let mut root_job = maybe_root_job.unwrap();
        let mut num_rows_scheduled = 0;
        loop {
            let maybe_next_scan_lines = root_job.schedule_next(&mut context);
            if let Err(err) = maybe_next_scan_lines {
                schedule_action(Err(err));
                return;
            }
            let next_scan_lines = maybe_next_scan_lines.unwrap();
            if next_scan_lines.is_empty() {
                return;
            }
            for next_scan_line in next_scan_lines {
                trace!(
                    "Scheduled scan line of {} rows and {} decoders",
                    next_scan_line.rows_scheduled,
                    next_scan_line.decoders.len()
                );
                num_rows_scheduled += next_scan_line.rows_scheduled;
                if !schedule_action(Ok(DecoderMessage {
                    scheduled_so_far: num_rows_scheduled,
                    decoders: next_scan_line.decoders,
                })) {
                    // Decoder has disconnected
                    return;
                }
            }
        }
    }

    fn do_schedule_ranges_legacy(
        &mut self,
        ranges: &[Range<u64>],
        filter: &FilterExpression,
        io: Arc<dyn EncodingsIo>,
        mut schedule_action: impl FnMut(Result<DecoderMessage>) -> bool,
        // If specified, this will be used as the top_level_row for all scheduling
        // tasks.  This is used by list scheduling to ensure all items scheduling
        // tasks are scheduled at the same top level row.
        priority: Option<Box<dyn PriorityRange>>,
    ) {
        let root_scheduler = self.root_scheduler.as_legacy();
        let rows_requested = ranges.iter().map(|r| r.end - r.start).sum::<u64>();
        trace!(
            "Scheduling {} ranges across {}..{} ({} rows){}",
            ranges.len(),
            ranges.first().unwrap().start,
            ranges.last().unwrap().end,
            rows_requested,
            priority
                .as_ref()
                .map(|p| format!(" (priority={:?})", p))
                .unwrap_or_default()
        );

        let mut context = SchedulerContext::new(io, self.cache.clone());
        let maybe_root_job = root_scheduler.schedule_ranges(ranges, filter);
        if let Err(schedule_ranges_err) = maybe_root_job {
            schedule_action(Err(schedule_ranges_err));
            return;
        }
        let mut root_job = maybe_root_job.unwrap();
        let mut num_rows_scheduled = 0;
        let mut rows_to_schedule = root_job.num_rows();
        let mut priority = priority.unwrap_or(Box::new(SimplePriorityRange::new(0)));
        trace!("Scheduled ranges refined to {} rows", rows_to_schedule);
        while rows_to_schedule > 0 {
            let maybe_next_scan_line = root_job.schedule_next(&mut context, priority.as_ref());
            if let Err(schedule_next_err) = maybe_next_scan_line {
                schedule_action(Err(schedule_next_err));
                return;
            }
            let next_scan_line = maybe_next_scan_line.unwrap();
            priority.advance(next_scan_line.rows_scheduled);
            num_rows_scheduled += next_scan_line.rows_scheduled;
            rows_to_schedule -= next_scan_line.rows_scheduled;
            trace!(
                "Scheduled scan line of {} rows and {} decoders",
                next_scan_line.rows_scheduled,
                next_scan_line.decoders.len()
            );
            if !schedule_action(Ok(DecoderMessage {
                scheduled_so_far: num_rows_scheduled,
                decoders: next_scan_line.decoders,
            })) {
                // Decoder has disconnected
                return;
            }

            trace!("Finished scheduling {} ranges", ranges.len());
        }
    }

    fn do_schedule_ranges(
        &mut self,
        ranges: &[Range<u64>],
        filter: &FilterExpression,
        io: Arc<dyn EncodingsIo>,
        schedule_action: impl FnMut(Result<DecoderMessage>) -> bool,
        // If specified, this will be used as the top_level_row for all scheduling
        // tasks.  This is used by list scheduling to ensure all items scheduling
        // tasks are scheduled at the same top level row.
        priority: Option<Box<dyn PriorityRange>>,
    ) {
        match &self.root_scheduler {
            RootScheduler::Legacy(_) => {
                self.do_schedule_ranges_legacy(ranges, filter, io, schedule_action, priority)
            }
            RootScheduler::Structural(_) => {
                self.do_schedule_ranges_structural(ranges, filter, io, schedule_action)
            }
        }
    }

    // This method is similar to schedule_ranges but instead of
    // sending the decoders to a channel it collects them all into a vector
    pub fn schedule_ranges_to_vec(
        &mut self,
        ranges: &[Range<u64>],
        filter: &FilterExpression,
        io: Arc<dyn EncodingsIo>,
        priority: Option<Box<dyn PriorityRange>>,
    ) -> Result<Vec<DecoderMessage>> {
        let mut decode_messages = Vec::new();
        self.do_schedule_ranges(
            ranges,
            filter,
            io,
            |msg| {
                decode_messages.push(msg);
                true
            },
            priority,
        );
        decode_messages.into_iter().collect::<Result<Vec<_>>>()
    }

    /// Schedules the load of multiple ranges of rows
    ///
    /// Ranges must be non-overlapping and in sorted order
    ///
    /// # Arguments
    ///
    /// * `ranges` - The ranges of rows to load
    /// * `sink` - A channel to send the decode tasks
    /// * `scheduler` An I/O scheduler to issue I/O requests
    #[instrument(skip_all)]
    pub fn schedule_ranges(
        &mut self,
        ranges: &[Range<u64>],
        filter: &FilterExpression,
        sink: mpsc::UnboundedSender<Result<DecoderMessage>>,
        scheduler: Arc<dyn EncodingsIo>,
    ) {
        self.do_schedule_ranges(
            ranges,
            filter,
            scheduler,
            |msg| {
                match sink.send(msg) {
                    Ok(_) => true,
                    Err(SendError { .. }) => {
                        // The receiver has gone away.  We can't do anything about it
                        // so just ignore the error.
                        debug!(
                        "schedule_ranges aborting early since decoder appears to have been dropped"
                    );
                        false
                    }
                }
            },
            None,
        )
    }

    /// Schedules the load of a range of rows
    ///
    /// # Arguments
    ///
    /// * `range` - The range of rows to load
    /// * `sink` - A channel to send the decode tasks
    /// * `scheduler` An I/O scheduler to issue I/O requests
    #[instrument(skip_all)]
    pub fn schedule_range(
        &mut self,
        range: Range<u64>,
        filter: &FilterExpression,
        sink: mpsc::UnboundedSender<Result<DecoderMessage>>,
        scheduler: Arc<dyn EncodingsIo>,
    ) {
        self.schedule_ranges(&[range], filter, sink, scheduler)
    }

    /// Schedules the load of selected rows
    ///
    /// # Arguments
    ///
    /// * `indices` - The row indices to load (these must be in ascending order!)
    /// * `sink` - A channel to send the decode tasks
    /// * `scheduler` An I/O scheduler to issue I/O requests
    pub fn schedule_take(
        &mut self,
        indices: &[u64],
        filter: &FilterExpression,
        sink: mpsc::UnboundedSender<Result<DecoderMessage>>,
        scheduler: Arc<dyn EncodingsIo>,
    ) {
        debug_assert!(indices.windows(2).all(|w| w[0] <= w[1]));
        if indices.is_empty() {
            return;
        }
        trace!("Scheduling take of {} rows", indices.len());
        let ranges = Self::indices_to_ranges(indices);
        self.schedule_ranges(&ranges, filter, sink, scheduler)
    }

    // coalesce continuous indices if possible (the input indices must be sorted and non-empty)
    fn indices_to_ranges(indices: &[u64]) -> Vec<Range<u64>> {
        let mut ranges = Vec::new();
        let mut start = indices[0];

        for window in indices.windows(2) {
            if window[1] != window[0] + 1 {
                ranges.push(start..window[0] + 1);
                start = window[1];
            }
        }

        ranges.push(start..*indices.last().unwrap() + 1);
        ranges
    }
}

pub struct ReadBatchTask {
    pub task: BoxFuture<'static, Result<RecordBatch>>,
    pub num_rows: u32,
}

/// A stream that takes scheduled jobs and generates decode tasks from them.
pub struct BatchDecodeStream {
    context: DecoderContext,
    root_decoder: SimpleStructDecoder,
    rows_remaining: u64,
    rows_per_batch: u32,
    rows_scheduled: u64,
    rows_drained: u64,
    scheduler_exhausted: bool,
    emitted_batch_size_warning: Arc<Once>,
}

impl BatchDecodeStream {
    /// Create a new instance of a batch decode stream
    ///
    /// # Arguments
    ///
    /// * `scheduled` - an incoming stream of decode tasks from a
    ///   [`crate::decode::DecodeBatchScheduler`]
    /// * `schema` - the schema of the data to create
    /// * `rows_per_batch` the number of rows to create before making a batch
    /// * `num_rows` the total number of rows scheduled
    /// * `num_columns` the total number of columns in the file
    pub fn new(
        scheduled: mpsc::UnboundedReceiver<Result<DecoderMessage>>,
        rows_per_batch: u32,
        num_rows: u64,
        root_decoder: SimpleStructDecoder,
    ) -> Self {
        Self {
            context: DecoderContext::new(scheduled),
            root_decoder,
            rows_remaining: num_rows,
            rows_per_batch,
            rows_scheduled: 0,
            rows_drained: 0,
            scheduler_exhausted: false,
            emitted_batch_size_warning: Arc::new(Once::new()),
        }
    }

    fn accept_decoder(&mut self, decoder: crate::previous::decoder::DecoderReady) -> Result<()> {
        if decoder.path.is_empty() {
            // The root decoder we can ignore
            Ok(())
        } else {
            self.root_decoder.accept_child(decoder)
        }
    }

    async fn wait_for_scheduled(&mut self, scheduled_need: u64) -> Result<u64> {
        if self.scheduler_exhausted {
            return Ok(self.rows_scheduled);
        }
        while self.rows_scheduled < scheduled_need {
            let next_message = self.context.source.recv().await;
            match next_message {
                Some(scan_line) => {
                    let scan_line = scan_line?;
                    self.rows_scheduled = scan_line.scheduled_so_far;
                    for message in scan_line.decoders {
                        self.accept_decoder(message.into_legacy())?;
                    }
                }
                None => {
                    // Schedule ended before we got all the data we expected.  This probably
                    // means some kind of pushdown filter was applied and we didn't load as
                    // much data as we thought we would.
                    self.scheduler_exhausted = true;
                    return Ok(self.rows_scheduled);
                }
            }
        }
        Ok(scheduled_need)
    }

    #[instrument(level = "debug", skip_all)]
    async fn next_batch_task(&mut self) -> Result<Option<NextDecodeTask>> {
        trace!(
            "Draining batch task (rows_remaining={} rows_drained={} rows_scheduled={})",
            self.rows_remaining,
            self.rows_drained,
            self.rows_scheduled,
        );
        if self.rows_remaining == 0 {
            return Ok(None);
        }

        let mut to_take = self.rows_remaining.min(self.rows_per_batch as u64);
        self.rows_remaining -= to_take;

        let scheduled_need = (self.rows_drained + to_take).saturating_sub(self.rows_scheduled);
        trace!("scheduled_need = {} because rows_drained = {} and to_take = {} and rows_scheduled = {}", scheduled_need, self.rows_drained, to_take, self.rows_scheduled);
        if scheduled_need > 0 {
            let desired_scheduled = scheduled_need + self.rows_scheduled;
            trace!(
                "Draining from scheduler (desire at least {} scheduled rows)",
                desired_scheduled
            );
            let actually_scheduled = self.wait_for_scheduled(desired_scheduled).await?;
            if actually_scheduled < desired_scheduled {
                let under_scheduled = desired_scheduled - actually_scheduled;
                to_take -= under_scheduled;
            }
        }

        if to_take == 0 {
            return Ok(None);
        }

        // wait_for_loaded waits for *>* loaded_need (not >=) so we do a -1 here
        let loaded_need = self.rows_drained + to_take - 1;
        trace!(
            "Waiting for I/O (desire at least {} fully loaded rows)",
            loaded_need
        );
        self.root_decoder.wait_for_loaded(loaded_need).await?;

        let next_task = self.root_decoder.drain(to_take)?;
        self.rows_drained += to_take;
        Ok(Some(next_task))
    }

    pub fn into_stream(self) -> BoxStream<'static, ReadBatchTask> {
        let stream = futures::stream::unfold(self, |mut slf| async move {
            let next_task = slf.next_batch_task().await;
            let next_task = next_task.transpose().map(|next_task| {
                let num_rows = next_task.as_ref().map(|t| t.num_rows).unwrap_or(0);
                let emitted_batch_size_warning = slf.emitted_batch_size_warning.clone();
                let task = async move {
                    let next_task = next_task?;
                    // Real decode work happens inside into_batch, which can block the current
                    // thread for a long time. By spawning it as a new task, we allow Tokio's
                    // worker threads to keep making progress.
                    tokio::spawn(async move { next_task.into_batch(emitted_batch_size_warning) })
                        .await
                        .map_err(|err| Error::Wrapped {
                            error: err.into(),
                            location: location!(),
                        })?
                };
                (task, num_rows)
            });
            next_task.map(|(task, num_rows)| {
                // This should be true since batch size is u32
                debug_assert!(num_rows <= u32::MAX as u64);
                let next_task = ReadBatchTask {
                    task: task.boxed(),
                    num_rows: num_rows as u32,
                };
                (next_task, slf)
            })
        });
        stream.boxed()
    }
}

// Utility types to smooth out the differences between the 2.0 and 2.1 decoders so that
// we can have a single implementation of the batch decode iterator
enum RootDecoderMessage {
    LoadedPage(LoadedPageShard),
    LegacyPage(crate::previous::decoder::DecoderReady),
}
trait RootDecoderType {
    fn accept_message(&mut self, message: RootDecoderMessage) -> Result<()>;
    fn drain_batch(&mut self, num_rows: u64) -> Result<NextDecodeTask>;
    fn wait(&mut self, loaded_need: u64, runtime: &tokio::runtime::Runtime) -> Result<()>;
}
impl RootDecoderType for StructuralStructDecoder {
    fn accept_message(&mut self, message: RootDecoderMessage) -> Result<()> {
        let RootDecoderMessage::LoadedPage(loaded_page) = message else {
            unreachable!()
        };
        self.accept_page(loaded_page)
    }
    fn drain_batch(&mut self, num_rows: u64) -> Result<NextDecodeTask> {
        self.drain_batch_task(num_rows)
    }
    fn wait(&mut self, _: u64, _: &tokio::runtime::Runtime) -> Result<()> {
        // Waiting happens elsewhere (not as part of the decoder)
        Ok(())
    }
}
impl RootDecoderType for SimpleStructDecoder {
    fn accept_message(&mut self, message: RootDecoderMessage) -> Result<()> {
        let RootDecoderMessage::LegacyPage(legacy_page) = message else {
            unreachable!()
        };
        self.accept_child(legacy_page)
    }
    fn drain_batch(&mut self, num_rows: u64) -> Result<NextDecodeTask> {
        self.drain(num_rows)
    }
    fn wait(&mut self, loaded_need: u64, runtime: &tokio::runtime::Runtime) -> Result<()> {
        runtime.block_on(self.wait_for_loaded(loaded_need))
    }
}

/// A blocking batch decoder that performs synchronous decoding
struct BatchDecodeIterator<T: RootDecoderType> {
    messages: VecDeque<Result<DecoderMessage>>,
    root_decoder: T,
    rows_remaining: u64,
    rows_per_batch: u32,
    rows_scheduled: u64,
    rows_drained: u64,
    emitted_batch_size_warning: Arc<Once>,
    // Note: this is not the runtime on which I/O happens.
    // That's always in the scheduler.  This is just a runtime we use to
    // sleep the current thread if I/O is unready
    wait_for_io_runtime: tokio::runtime::Runtime,
    schema: Arc<ArrowSchema>,
}

impl<T: RootDecoderType> BatchDecodeIterator<T> {
    /// Create a new instance of a batch decode iterator
    pub fn new(
        messages: VecDeque<Result<DecoderMessage>>,
        rows_per_batch: u32,
        num_rows: u64,
        root_decoder: T,
        schema: Arc<ArrowSchema>,
    ) -> Self {
        Self {
            messages,
            root_decoder,
            rows_remaining: num_rows,
            rows_per_batch,
            rows_scheduled: 0,
            rows_drained: 0,
            wait_for_io_runtime: tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap(),
            emitted_batch_size_warning: Arc::new(Once::new()),
            schema,
        }
    }

    /// Wait for a single page of data to finish loading
    ///
    /// If the data is not available this will perform a *blocking* wait (put
    /// the current thread to sleep)
    fn wait_for_page(&self, unloaded_page: UnloadedPageShard) -> Result<LoadedPageShard> {
        match maybe_done(unloaded_page.0) {
            // Fast path, avoid all runtime shenanigans if the data is ready
            MaybeDone::Done(loaded_page) => loaded_page,
            // Slow path, we need to wait on I/O, enter the runtime
            MaybeDone::Future(fut) => self.wait_for_io_runtime.block_on(fut),
            MaybeDone::Gone => unreachable!(),
        }
    }

    /// Waits for I/O until `scheduled_need` rows have been loaded
    ///
    /// Note that `scheduled_need` is cumulative.  E.g. this method
    /// should be called with 5, 10, 15 and not 5, 5, 5
    #[instrument(skip_all)]
    fn wait_for_io(&mut self, scheduled_need: u64, to_take: u64) -> Result<u64> {
        while self.rows_scheduled < scheduled_need && !self.messages.is_empty() {
            let message = self.messages.pop_front().unwrap()?;
            self.rows_scheduled = message.scheduled_so_far;
            for decoder_message in message.decoders {
                match decoder_message {
                    MessageType::UnloadedPage(unloaded_page) => {
                        let loaded_page = self.wait_for_page(unloaded_page)?;
                        self.root_decoder
                            .accept_message(RootDecoderMessage::LoadedPage(loaded_page))?;
                    }
                    MessageType::DecoderReady(decoder_ready) => {
                        // The root decoder we can ignore
                        if !decoder_ready.path.is_empty() {
                            self.root_decoder
                                .accept_message(RootDecoderMessage::LegacyPage(decoder_ready))?;
                        }
                    }
                }
            }
        }

        let loaded_need = self.rows_drained + to_take.min(self.rows_per_batch as u64) - 1;

        self.root_decoder
            .wait(loaded_need, &self.wait_for_io_runtime)?;
        Ok(self.rows_scheduled)
    }

    #[instrument(level = "debug", skip_all)]
    fn next_batch_task(&mut self) -> Result<Option<RecordBatch>> {
        trace!(
            "Draining batch task (rows_remaining={} rows_drained={} rows_scheduled={})",
            self.rows_remaining,
            self.rows_drained,
            self.rows_scheduled,
        );
        if self.rows_remaining == 0 {
            return Ok(None);
        }

        let mut to_take = self.rows_remaining.min(self.rows_per_batch as u64);
        self.rows_remaining -= to_take;

        let scheduled_need = (self.rows_drained + to_take).saturating_sub(self.rows_scheduled);
        trace!("scheduled_need = {} because rows_drained = {} and to_take = {} and rows_scheduled = {}", scheduled_need, self.rows_drained, to_take, self.rows_scheduled);
        if scheduled_need > 0 {
            let desired_scheduled = scheduled_need + self.rows_scheduled;
            trace!(
                "Draining from scheduler (desire at least {} scheduled rows)",
                desired_scheduled
            );
            let actually_scheduled = self.wait_for_io(desired_scheduled, to_take)?;
            if actually_scheduled < desired_scheduled {
                let under_scheduled = desired_scheduled - actually_scheduled;
                to_take -= under_scheduled;
            }
        }

        if to_take == 0 {
            return Ok(None);
        }

        let next_task = self.root_decoder.drain_batch(to_take)?;

        self.rows_drained += to_take;

        let batch = next_task.into_batch(self.emitted_batch_size_warning.clone())?;

        Ok(Some(batch))
    }
}

impl<T: RootDecoderType> Iterator for BatchDecodeIterator<T> {
    type Item = ArrowResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_batch_task()
            .transpose()
            .map(|r| r.map_err(ArrowError::from))
    }
}

impl<T: RootDecoderType> RecordBatchReader for BatchDecodeIterator<T> {
    fn schema(&self) -> Arc<ArrowSchema> {
        self.schema.clone()
    }
}

/// A stream that takes scheduled jobs and generates decode tasks from them.
pub struct StructuralBatchDecodeStream {
    context: DecoderContext,
    root_decoder: StructuralStructDecoder,
    rows_remaining: u64,
    rows_per_batch: u32,
    rows_scheduled: u64,
    rows_drained: u64,
    scheduler_exhausted: bool,
    emitted_batch_size_warning: Arc<Once>,
}

impl StructuralBatchDecodeStream {
    /// Create a new instance of a batch decode stream
    ///
    /// # Arguments
    ///
    /// * `scheduled` - an incoming stream of decode tasks from a
    ///   [`crate::decode::DecodeBatchScheduler`]
    /// * `schema` - the schema of the data to create
    /// * `rows_per_batch` the number of rows to create before making a batch
    /// * `num_rows` the total number of rows scheduled
    /// * `num_columns` the total number of columns in the file
    pub fn new(
        scheduled: mpsc::UnboundedReceiver<Result<DecoderMessage>>,
        rows_per_batch: u32,
        num_rows: u64,
        root_decoder: StructuralStructDecoder,
    ) -> Self {
        Self {
            context: DecoderContext::new(scheduled),
            root_decoder,
            rows_remaining: num_rows,
            rows_per_batch,
            rows_scheduled: 0,
            rows_drained: 0,
            scheduler_exhausted: false,
            emitted_batch_size_warning: Arc::new(Once::new()),
        }
    }

    async fn wait_for_scheduled(&mut self, scheduled_need: u64) -> Result<u64> {
        if self.scheduler_exhausted {
            return Ok(self.rows_scheduled);
        }
        while self.rows_scheduled < scheduled_need {
            let next_message = self.context.source.recv().await;
            match next_message {
                Some(scan_line) => {
                    let scan_line = scan_line?;
                    self.rows_scheduled = scan_line.scheduled_so_far;
                    for message in scan_line.decoders {
                        let unloaded_page = message.into_structural();
                        let loaded_page = unloaded_page.0.await?;
                        self.root_decoder.accept_page(loaded_page)?;
                    }
                }
                None => {
                    // Schedule ended before we got all the data we expected.  This probably
                    // means some kind of pushdown filter was applied and we didn't load as
                    // much data as we thought we would.
                    self.scheduler_exhausted = true;
                    return Ok(self.rows_scheduled);
                }
            }
        }
        Ok(scheduled_need)
    }

    #[instrument(level = "debug", skip_all)]
    async fn next_batch_task(&mut self) -> Result<Option<NextDecodeTask>> {
        trace!(
            "Draining batch task (rows_remaining={} rows_drained={} rows_scheduled={})",
            self.rows_remaining,
            self.rows_drained,
            self.rows_scheduled,
        );
        if self.rows_remaining == 0 {
            return Ok(None);
        }

        let mut to_take = self.rows_remaining.min(self.rows_per_batch as u64);
        self.rows_remaining -= to_take;

        let scheduled_need = (self.rows_drained + to_take).saturating_sub(self.rows_scheduled);
        trace!("scheduled_need = {} because rows_drained = {} and to_take = {} and rows_scheduled = {}", scheduled_need, self.rows_drained, to_take, self.rows_scheduled);
        if scheduled_need > 0 {
            let desired_scheduled = scheduled_need + self.rows_scheduled;
            trace!(
                "Draining from scheduler (desire at least {} scheduled rows)",
                desired_scheduled
            );
            let actually_scheduled = self.wait_for_scheduled(desired_scheduled).await?;
            if actually_scheduled < desired_scheduled {
                let under_scheduled = desired_scheduled - actually_scheduled;
                to_take -= under_scheduled;
            }
        }

        if to_take == 0 {
            return Ok(None);
        }

        let next_task = self.root_decoder.drain_batch_task(to_take)?;
        self.rows_drained += to_take;
        Ok(Some(next_task))
    }

    pub fn into_stream(self) -> BoxStream<'static, ReadBatchTask> {
        let stream = futures::stream::unfold(self, |mut slf| async move {
            let next_task = slf.next_batch_task().await;
            let next_task = next_task.transpose().map(|next_task| {
                let num_rows = next_task.as_ref().map(|t| t.num_rows).unwrap_or(0);
                let emitted_batch_size_warning = slf.emitted_batch_size_warning.clone();
                let task = async move {
                    let next_task = next_task?;
                    // Real decode work happens inside into_batch, which can block the current
                    // thread for a long time. By spawning it as a new task, we allow Tokio's
                    // worker threads to keep making progress.
                    tokio::spawn(async move { next_task.into_batch(emitted_batch_size_warning) })
                        .await
                        .map_err(|err| Error::Wrapped {
                            error: err.into(),
                            location: location!(),
                        })?
                };
                (task, num_rows)
            });
            next_task.map(|(task, num_rows)| {
                // This should be true since batch size is u32
                debug_assert!(num_rows <= u32::MAX as u64);
                let next_task = ReadBatchTask {
                    task: task.boxed(),
                    num_rows: num_rows as u32,
                };
                (next_task, slf)
            })
        });
        stream.boxed()
    }
}

#[derive(Debug)]
pub enum RequestedRows {
    Ranges(Vec<Range<u64>>),
    Indices(Vec<u64>),
}

impl RequestedRows {
    pub fn num_rows(&self) -> u64 {
        match self {
            Self::Ranges(ranges) => ranges.iter().map(|r| r.end - r.start).sum(),
            Self::Indices(indices) => indices.len() as u64,
        }
    }

    pub fn trim_empty_ranges(mut self) -> Self {
        if let Self::Ranges(ranges) = &mut self {
            ranges.retain(|r| !r.is_empty());
        }
        self
    }
}

/// Configuration for decoder behavior
#[derive(Debug, Clone, Default)]
pub struct DecoderConfig {
    /// Whether to cache repetition indices for better performance
    pub cache_repetition_index: bool,
    /// Whether to validate decoded data
    pub validate_on_decode: bool,
}

#[derive(Debug, Clone)]
pub struct SchedulerDecoderConfig {
    pub decoder_plugins: Arc<DecoderPlugins>,
    pub batch_size: u32,
    pub io: Arc<dyn EncodingsIo>,
    pub cache: Arc<LanceCache>,
    /// Decoder configuration
    pub decoder_config: DecoderConfig,
}

fn check_scheduler_on_drop(
    stream: BoxStream<'static, ReadBatchTask>,
    scheduler_handle: tokio::task::JoinHandle<()>,
) -> BoxStream<'static, ReadBatchTask> {
    // This is a bit weird but we create an "empty stream" that unwraps the scheduler handle (which
    // will panic if the scheduler panicked).  This let's us check if the scheduler panicked
    // when the stream finishes.
    let mut scheduler_handle = Some(scheduler_handle);
    let check_scheduler = stream::unfold((), move |_| {
        let handle = scheduler_handle.take();
        async move {
            if let Some(handle) = handle {
                handle.await.unwrap();
            }
            None
        }
    });
    stream.chain(check_scheduler).boxed()
}

pub fn create_decode_stream(
    schema: &Schema,
    num_rows: u64,
    batch_size: u32,
    is_structural: bool,
    should_validate: bool,
    rx: mpsc::UnboundedReceiver<Result<DecoderMessage>>,
) -> BoxStream<'static, ReadBatchTask> {
    if is_structural {
        let arrow_schema = ArrowSchema::from(schema);
        let structural_decoder = StructuralStructDecoder::new(
            arrow_schema.fields,
            should_validate,
            /*is_root=*/ true,
        );
        StructuralBatchDecodeStream::new(rx, batch_size, num_rows, structural_decoder).into_stream()
    } else {
        let arrow_schema = ArrowSchema::from(schema);
        let root_fields = arrow_schema.fields;

        let simple_struct_decoder = SimpleStructDecoder::new(root_fields, num_rows);
        BatchDecodeStream::new(rx, batch_size, num_rows, simple_struct_decoder).into_stream()
    }
}

/// Creates a iterator that decodes a set of messages in a blocking fashion
///
/// See [`schedule_and_decode_blocking`] for more information.
pub fn create_decode_iterator(
    schema: &Schema,
    num_rows: u64,
    batch_size: u32,
    should_validate: bool,
    is_structural: bool,
    messages: VecDeque<Result<DecoderMessage>>,
) -> Box<dyn RecordBatchReader + Send + 'static> {
    let arrow_schema = Arc::new(ArrowSchema::from(schema));
    let root_fields = arrow_schema.fields.clone();
    if is_structural {
        let simple_struct_decoder =
            StructuralStructDecoder::new(root_fields, should_validate, /*is_root=*/ true);
        Box::new(BatchDecodeIterator::new(
            messages,
            batch_size,
            num_rows,
            simple_struct_decoder,
            arrow_schema,
        ))
    } else {
        let root_decoder = SimpleStructDecoder::new(root_fields, num_rows);
        Box::new(BatchDecodeIterator::new(
            messages,
            batch_size,
            num_rows,
            root_decoder,
            arrow_schema,
        ))
    }
}

fn create_scheduler_decoder(
    column_infos: Vec<Arc<ColumnInfo>>,
    requested_rows: RequestedRows,
    filter: FilterExpression,
    column_indices: Vec<u32>,
    target_schema: Arc<Schema>,
    config: SchedulerDecoderConfig,
) -> Result<BoxStream<'static, ReadBatchTask>> {
    let num_rows = requested_rows.num_rows();

    let is_structural = column_infos[0].is_structural();

    let (tx, rx) = mpsc::unbounded_channel();

    let decode_stream = create_decode_stream(
        &target_schema,
        num_rows,
        config.batch_size,
        is_structural,
        config.decoder_config.validate_on_decode,
        rx,
    );

    let scheduler_handle = tokio::task::spawn(async move {
        let mut decode_scheduler = match DecodeBatchScheduler::try_new(
            target_schema.as_ref(),
            &column_indices,
            &column_infos,
            &vec![],
            num_rows,
            config.decoder_plugins,
            config.io.clone(),
            config.cache,
            &filter,
            &config.decoder_config,
        )
        .await
        {
            Ok(scheduler) => scheduler,
            Err(e) => {
                let _ = tx.send(Err(e));
                return;
            }
        };

        match requested_rows {
            RequestedRows::Ranges(ranges) => {
                decode_scheduler.schedule_ranges(&ranges, &filter, tx, config.io)
            }
            RequestedRows::Indices(indices) => {
                decode_scheduler.schedule_take(&indices, &filter, tx, config.io)
            }
        }
    });

    Ok(check_scheduler_on_drop(decode_stream, scheduler_handle))
}

/// Launches a scheduler on a dedicated (spawned) task and creates a decoder to
/// decode the scheduled data and returns the decoder as a stream of record batches.
///
/// This is a convenience function that creates both the scheduler and the decoder
/// which can be a little tricky to get right.
pub fn schedule_and_decode(
    column_infos: Vec<Arc<ColumnInfo>>,
    requested_rows: RequestedRows,
    filter: FilterExpression,
    column_indices: Vec<u32>,
    target_schema: Arc<Schema>,
    config: SchedulerDecoderConfig,
) -> BoxStream<'static, ReadBatchTask> {
    if requested_rows.num_rows() == 0 {
        return stream::empty().boxed();
    }

    // If the user requested any ranges that are empty, ignore them.  They are pointless and
    // trying to read them has caused bugs in the past.
    let requested_rows = requested_rows.trim_empty_ranges();

    let io = config.io.clone();

    // For convenience we really want this method to be a snchronous method where all
    // errors happen on the stream.  There is some async initialization that must happen
    // when creating a scheduler.  We wrap that all up in the very first task.
    match create_scheduler_decoder(
        column_infos,
        requested_rows,
        filter,
        column_indices,
        target_schema,
        config,
    ) {
        // Keep the io alive until the stream is dropped or finishes.  Otherwise the
        // I/O drops as soon as the scheduling is finished and the I/O loop terminates.
        Ok(stream) => stream.finally(move || drop(io)).boxed(),
        // If the initialization failed make it look like a failed task
        Err(e) => stream::once(std::future::ready(ReadBatchTask {
            num_rows: 0,
            task: std::future::ready(Err(e)).boxed(),
        }))
        .boxed(),
    }
}

pub static WAITER_RT: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
});

/// Schedules and decodes the requested data in a blocking fashion
///
/// This function is a blocking version of [`schedule_and_decode`]. It schedules the requested data
/// and decodes it in the current thread.
///
/// This can be useful when the disk is fast (or the data is in memory) and the amount
/// of data is relatively small.  For example, when doing a take against NVMe or in-memory data.
///
/// This should NOT be used for full scans.  Even if the data is in memory this function will
/// not parallelize the decode and will be slower than the async version.  Full scans typically
/// make relatively few IOPs and so the asynchronous overhead is much smaller.
///
/// This method will first completely run the scheduling process.  Then it will run the
/// decode process.
pub fn schedule_and_decode_blocking(
    column_infos: Vec<Arc<ColumnInfo>>,
    requested_rows: RequestedRows,
    filter: FilterExpression,
    column_indices: Vec<u32>,
    target_schema: Arc<Schema>,
    config: SchedulerDecoderConfig,
) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
    if requested_rows.num_rows() == 0 {
        let arrow_schema = Arc::new(ArrowSchema::from(target_schema.as_ref()));
        return Ok(Box::new(RecordBatchIterator::new(vec![], arrow_schema)));
    }

    let num_rows = requested_rows.num_rows();
    let is_structural = column_infos[0].is_structural();

    let (tx, mut rx) = mpsc::unbounded_channel();

    // Initialize the scheduler.  This is still "asynchronous" but we run it with a current-thread
    // runtime.
    let mut decode_scheduler = WAITER_RT.block_on(DecodeBatchScheduler::try_new(
        target_schema.as_ref(),
        &column_indices,
        &column_infos,
        &vec![],
        num_rows,
        config.decoder_plugins,
        config.io.clone(),
        config.cache,
        &filter,
        &config.decoder_config,
    ))?;

    // Schedule the requested rows
    match requested_rows {
        RequestedRows::Ranges(ranges) => {
            decode_scheduler.schedule_ranges(&ranges, &filter, tx, config.io)
        }
        RequestedRows::Indices(indices) => {
            decode_scheduler.schedule_take(&indices, &filter, tx, config.io)
        }
    }

    // Drain the scheduler queue into a vec of decode messages
    let mut messages = Vec::new();
    while rx
        .recv_many(&mut messages, usize::MAX)
        .now_or_never()
        .unwrap()
        != 0
    {}

    // Create a decoder to decode the messages
    let decode_iterator = create_decode_iterator(
        &target_schema,
        num_rows,
        config.batch_size,
        config.decoder_config.validate_on_decode,
        is_structural,
        messages.into(),
    );

    Ok(decode_iterator)
}

/// A decoder for single-column encodings of primitive data (this includes fixed size
/// lists of primitive data)
///
/// Physical decoders are able to decode into existing buffers for zero-copy operation.
///
/// Instances should be stateless and `Send` / `Sync`.  This is because multiple decode
/// tasks could reference the same page.  For example, imagine a page covers rows 0-2000
/// and the decoder stream has a batch size of 1024.  The decoder will be needed by both
/// the decode task for batch 0 and the decode task for batch 1.
///
/// See [`crate::decoder`] for more information
pub trait PrimitivePageDecoder: Send + Sync {
    /// Decode data into buffers
    ///
    /// This may be a simple zero-copy from a disk buffer or could involve complex decoding
    /// such as decompressing from some compressed representation.
    ///
    /// Capacity is stored as a tuple of (num_bytes: u64, is_needed: bool).  The `is_needed`
    /// portion only needs to be updated if the encoding has some concept of an "optional"
    /// buffer.
    ///
    /// Encodings can have any number of input or output buffers.  For example, a dictionary
    /// decoding will convert two buffers (indices + dictionary) into a single buffer
    ///
    /// Binary decodings have two output buffers (one for values, one for offsets)
    ///
    /// Other decodings could even expand the # of output buffers.  For example, we could decode
    /// fixed size strings into variable length strings going from one input buffer to multiple output
    /// buffers.
    ///
    /// Each Arrow data type typically has a fixed structure of buffers and the encoding chain will
    /// generally end at one of these structures.  However, intermediate structures may exist which
    /// do not correspond to any Arrow type at all.  For example, a bitpacking encoding will deal
    /// with buffers that have bits-per-value that is not a multiple of 8.
    ///
    /// The `primitive_array_from_buffers` method has an expected buffer layout for each arrow
    /// type (order matters) and encodings that aim to decode into arrow types should respect
    /// this layout.
    /// # Arguments
    ///
    /// * `rows_to_skip` - how many rows to skip (within the page) before decoding
    /// * `num_rows` - how many rows to decode
    /// * `all_null` - A mutable bool, set to true if a decoder determines all values are null
    fn decode(&self, rows_to_skip: u64, num_rows: u64) -> Result<DataBlock>;
}

/// A scheduler for single-column encodings of primitive data
///
/// The scheduler is responsible for calculating what I/O is needed for the requested rows
///
/// Instances should be stateless and `Send` and `Sync`.  This is because instances can
/// be shared in follow-up I/O tasks.
///
/// See [`crate::decoder`] for more information
pub trait PageScheduler: Send + Sync + std::fmt::Debug {
    /// Schedules a batch of I/O to load the data needed for the requested ranges
    ///
    /// Returns a future that will yield a decoder once the data has been loaded
    ///
    /// # Arguments
    ///
    /// * `range` - the range of row offsets (relative to start of page) requested
    ///   these must be ordered and must not overlap
    /// * `scheduler` - a scheduler to submit the I/O request to
    /// * `top_level_row` - the row offset of the top level field currently being
    ///   scheduled.  This can be used to assign priority to I/O requests
    fn schedule_ranges(
        &self,
        ranges: &[Range<u64>],
        scheduler: &Arc<dyn EncodingsIo>,
        top_level_row: u64,
    ) -> BoxFuture<'static, Result<Box<dyn PrimitivePageDecoder>>>;
}

/// A trait to control the priority of I/O
pub trait PriorityRange: std::fmt::Debug + Send + Sync {
    fn advance(&mut self, num_rows: u64);
    fn current_priority(&self) -> u64;
    fn box_clone(&self) -> Box<dyn PriorityRange>;
}

/// A simple priority scheme for top-level fields with no parent
/// repetition
#[derive(Debug)]
pub struct SimplePriorityRange {
    priority: u64,
}

impl SimplePriorityRange {
    fn new(priority: u64) -> Self {
        Self { priority }
    }
}

impl PriorityRange for SimplePriorityRange {
    fn advance(&mut self, num_rows: u64) {
        self.priority += num_rows;
    }

    fn current_priority(&self) -> u64 {
        self.priority
    }

    fn box_clone(&self) -> Box<dyn PriorityRange> {
        Box::new(Self {
            priority: self.priority,
        })
    }
}

/// Determining the priority of a list request is tricky.  We want
/// the priority to be the top-level row.  So if we have a
/// list<list<int>> and each outer list has 10 rows and each inner
/// list has 5 rows then the priority of the 100th item is 1 because
/// it is the 5th item in the 10th item of the *second* row.
///
/// This structure allows us to keep track of this complicated priority
/// relationship.
///
/// There's a fair amount of bookkeeping involved here.
///
/// A better approach (using repetition levels) is coming in the future.
pub struct ListPriorityRange {
    base: Box<dyn PriorityRange>,
    offsets: Arc<[u64]>,
    cur_index_into_offsets: usize,
    cur_position: u64,
}

impl ListPriorityRange {
    pub(crate) fn new(base: Box<dyn PriorityRange>, offsets: Arc<[u64]>) -> Self {
        Self {
            base,
            offsets,
            cur_index_into_offsets: 0,
            cur_position: 0,
        }
    }
}

impl std::fmt::Debug for ListPriorityRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ListPriorityRange")
            .field("base", &self.base)
            .field("offsets.len()", &self.offsets.len())
            .field("cur_index_into_offsets", &self.cur_index_into_offsets)
            .field("cur_position", &self.cur_position)
            .finish()
    }
}

impl PriorityRange for ListPriorityRange {
    fn advance(&mut self, num_rows: u64) {
        // We've scheduled X items.  Now walk through the offsets to
        // determine how many rows we've scheduled.
        self.cur_position += num_rows;
        let mut idx_into_offsets = self.cur_index_into_offsets;
        while idx_into_offsets + 1 < self.offsets.len()
            && self.offsets[idx_into_offsets + 1] <= self.cur_position
        {
            idx_into_offsets += 1;
        }
        let base_rows_advanced = idx_into_offsets - self.cur_index_into_offsets;
        self.cur_index_into_offsets = idx_into_offsets;
        self.base.advance(base_rows_advanced as u64);
    }

    fn current_priority(&self) -> u64 {
        self.base.current_priority()
    }

    fn box_clone(&self) -> Box<dyn PriorityRange> {
        Box::new(Self {
            base: self.base.box_clone(),
            offsets: self.offsets.clone(),
            cur_index_into_offsets: self.cur_index_into_offsets,
            cur_position: self.cur_position,
        })
    }
}

/// Contains the context for a scheduler
pub struct SchedulerContext {
    recv: Option<mpsc::UnboundedReceiver<DecoderMessage>>,
    io: Arc<dyn EncodingsIo>,
    cache: Arc<LanceCache>,
    name: String,
    path: Vec<u32>,
    path_names: Vec<String>,
}

pub struct ScopedSchedulerContext<'a> {
    pub context: &'a mut SchedulerContext,
}

impl<'a> ScopedSchedulerContext<'a> {
    pub fn pop(self) -> &'a mut SchedulerContext {
        self.context.pop();
        self.context
    }
}

impl SchedulerContext {
    pub fn new(io: Arc<dyn EncodingsIo>, cache: Arc<LanceCache>) -> Self {
        Self {
            io,
            cache,
            recv: None,
            name: "".to_string(),
            path: Vec::new(),
            path_names: Vec::new(),
        }
    }

    pub fn io(&self) -> &Arc<dyn EncodingsIo> {
        &self.io
    }

    pub fn cache(&self) -> &Arc<LanceCache> {
        &self.cache
    }

    pub fn push(&'_ mut self, name: &str, index: u32) -> ScopedSchedulerContext<'_> {
        self.path.push(index);
        self.path_names.push(name.to_string());
        ScopedSchedulerContext { context: self }
    }

    pub fn pop(&mut self) {
        self.path.pop();
        self.path_names.pop();
    }

    pub fn path_name(&self) -> String {
        let path = self.path_names.join("/");
        if self.recv.is_some() {
            format!("TEMP({}){}", self.name, path)
        } else {
            format!("ROOT{}", path)
        }
    }

    pub fn current_path(&self) -> VecDeque<u32> {
        VecDeque::from_iter(self.path.iter().copied())
    }

    #[deprecated(since = "0.29.1", note = "This is for legacy 2.0 paths")]
    pub fn locate_decoder(
        &mut self,
        decoder: Box<dyn crate::previous::decoder::LogicalPageDecoder>,
    ) -> crate::previous::decoder::DecoderReady {
        trace!(
            "Scheduling decoder of type {:?} for {:?}",
            decoder.data_type(),
            self.path,
        );
        crate::previous::decoder::DecoderReady {
            decoder,
            path: self.current_path(),
        }
    }
}

pub struct UnloadedPageShard(pub BoxFuture<'static, Result<LoadedPageShard>>);

impl std::fmt::Debug for UnloadedPageShard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnloadedPage").finish()
    }
}

#[derive(Debug)]
pub struct ScheduledScanLine {
    pub rows_scheduled: u64,
    pub decoders: Vec<MessageType>,
}

pub trait StructuralSchedulingJob: std::fmt::Debug {
    /// Schedule the next batch of data
    ///
    /// Normally this equates to scheduling the next page of data into one task.  Very large pages
    /// might be split into multiple scan lines.  Each scan line has one or more rows.
    ///
    /// If a scheduler ends early it may return an empty vector.
    fn schedule_next(&mut self, context: &mut SchedulerContext) -> Result<Vec<ScheduledScanLine>>;
}

/// A filter expression to apply to the data
///
/// The core decoders do not currently take advantage of filtering in
/// any way.  In order to maintain the abstraction we represent filters
/// as an arbitrary byte sequence.
///
/// We recommend that encodings use Substrait for filters.
pub struct FilterExpression(pub Bytes);

impl FilterExpression {
    /// Create a filter expression that does not filter any data
    ///
    /// This is currently represented by an empty byte array.  Encoders
    /// that are "filter aware" should make sure they handle this case.
    pub fn no_filter() -> Self {
        Self(Bytes::new())
    }

    /// Returns true if the filter is the same as the [`Self::no_filter`] filter
    pub fn is_noop(&self) -> bool {
        self.0.is_empty()
    }
}

pub trait StructuralFieldScheduler: Send + std::fmt::Debug {
    fn initialize<'a>(
        &'a mut self,
        filter: &'a FilterExpression,
        context: &'a SchedulerContext,
    ) -> BoxFuture<'a, Result<()>>;
    fn schedule_ranges<'a>(
        &'a self,
        ranges: &[Range<u64>],
        filter: &FilterExpression,
    ) -> Result<Box<dyn StructuralSchedulingJob + 'a>>;
}

/// A trait for tasks that decode data into an Arrow array
pub trait DecodeArrayTask: Send {
    /// Decodes the data into an Arrow array
    fn decode(self: Box<Self>) -> Result<ArrayRef>;
}

impl DecodeArrayTask for Box<dyn StructuralDecodeArrayTask> {
    fn decode(self: Box<Self>) -> Result<ArrayRef> {
        StructuralDecodeArrayTask::decode(*self).map(|decoded_array| decoded_array.array)
    }
}

/// A task to decode data into an Arrow record batch
///
/// It has a child `task` which decodes a struct array with no nulls.
/// This is then converted into a record batch.
pub struct NextDecodeTask {
    /// The decode task itself
    pub task: Box<dyn DecodeArrayTask>,
    /// The number of rows that will be created
    pub num_rows: u64,
}

impl NextDecodeTask {
    // Run the task and produce a record batch
    //
    // If the batch is very large this function will log a warning message
    // suggesting the user try a smaller batch size.
    #[instrument(name = "task_to_batch", level = "debug", skip_all)]
    fn into_batch(self, emitted_batch_size_warning: Arc<Once>) -> Result<RecordBatch> {
        let struct_arr = self.task.decode();
        match struct_arr {
            Ok(struct_arr) => {
                let batch = RecordBatch::from(struct_arr.as_struct());
                let size_bytes = batch.get_array_memory_size() as u64;
                if size_bytes > BATCH_SIZE_BYTES_WARNING {
                    emitted_batch_size_warning.call_once(|| {
                        let size_mb = size_bytes / 1024 / 1024;
                        debug!("Lance read in a single batch that contained more than {}MiB of data.  You may want to consider reducing the batch size.", size_mb);
                    });
                }
                Ok(batch)
            }
            Err(e) => {
                let e = Error::Internal {
                    message: format!("Error decoding batch: {}", e),
                    location: location!(),
                };
                Err(e)
            }
        }
    }
}

// An envelope to wrap both 2.0 style messages and 2.1 style messages so we can
// share some code paths between the two.  Decoders can safely unwrap into whatever
// style they expect since a file will be either all-2.0 or all-2.1
#[derive(Debug)]
pub enum MessageType {
    // The older v2.0 scheduler/decoder used a scheme where the message was the
    // decoder itself.  The messages were not sent in priority order and the decoder
    // had to wait for I/O, figuring out the correct priority.  This was a lot of
    // complexity.
    DecoderReady(crate::previous::decoder::DecoderReady),
    // Starting in 2.1 we use a simpler scheme where the scheduling happens in priority
    // order and the message is an unloaded decoder.  These can be awaited, in order, and
    // the decoder does not have to worry about waiting for I/O.
    UnloadedPage(UnloadedPageShard),
}

impl MessageType {
    pub fn into_legacy(self) -> crate::previous::decoder::DecoderReady {
        match self {
            Self::DecoderReady(decoder) => decoder,
            Self::UnloadedPage(_) => {
                panic!("Expected DecoderReady but got UnloadedPage")
            }
        }
    }

    pub fn into_structural(self) -> UnloadedPageShard {
        match self {
            Self::UnloadedPage(unloaded) => unloaded,
            Self::DecoderReady(_) => {
                panic!("Expected UnloadedPage but got DecoderReady")
            }
        }
    }
}

pub struct DecoderMessage {
    pub scheduled_so_far: u64,
    pub decoders: Vec<MessageType>,
}

pub struct DecoderContext {
    source: mpsc::UnboundedReceiver<Result<DecoderMessage>>,
}

impl DecoderContext {
    pub fn new(source: mpsc::UnboundedReceiver<Result<DecoderMessage>>) -> Self {
        Self { source }
    }
}

pub struct DecodedPage {
    pub data: DataBlock,
    pub repdef: RepDefUnraveler,
}

pub trait DecodePageTask: Send + std::fmt::Debug {
    /// Decodes the data into an Arrow array
    fn decode(self: Box<Self>) -> Result<DecodedPage>;
}

pub trait StructuralPageDecoder: std::fmt::Debug + Send {
    fn drain(&mut self, num_rows: u64) -> Result<Box<dyn DecodePageTask>>;
    fn num_rows(&self) -> u64;
}

#[derive(Debug)]
pub struct LoadedPageShard {
    // The decoder that is ready to be decoded
    pub decoder: Box<dyn StructuralPageDecoder>,
    // The path to the decoder, the first value is the column index
    // following values, if present, are nested child indices
    //
    // For example, a path of [1, 1, 0] would mean to grab the second
    // column, then the second child, and then the first child.
    //
    // It could represent x in the following schema:
    //
    // score: float64
    // points: struct
    //   color: string
    //   location: struct
    //     x: float64
    //
    // Currently, only struct decoders have "children" although other
    // decoders may at some point as well.  List children are only
    // handled through indirect I/O at the moment and so they don't
    // need to be represented (yet)
    pub path: VecDeque<u32>,
}

pub struct DecodedArray {
    pub array: ArrayRef,
    pub repdef: CompositeRepDefUnraveler,
}

pub trait StructuralDecodeArrayTask: std::fmt::Debug + Send {
    fn decode(self: Box<Self>) -> Result<DecodedArray>;
}

pub trait StructuralFieldDecoder: std::fmt::Debug + Send {
    /// Add a newly scheduled child decoder
    ///
    /// The default implementation does not expect children and returns
    /// an error.
    fn accept_page(&mut self, _child: LoadedPageShard) -> Result<()>;
    /// Creates a task to decode `num_rows` of data into an array
    fn drain(&mut self, num_rows: u64) -> Result<Box<dyn StructuralDecodeArrayTask>>;
    /// The data type of the decoded data
    fn data_type(&self) -> &DataType;
}

#[derive(Debug, Default)]
pub struct DecoderPlugins {}

/// Decodes a batch of data from an in-memory structure created by [`crate::encoder::encode_batch`]
pub async fn decode_batch(
    batch: &EncodedBatch,
    filter: &FilterExpression,
    decoder_plugins: Arc<DecoderPlugins>,
    should_validate: bool,
    version: LanceFileVersion,
    cache: Option<Arc<LanceCache>>,
) -> Result<RecordBatch> {
    // The io is synchronous so it shouldn't be possible for any async stuff to still be in progress
    // Still, if we just use now_or_never we hit misfires because some futures (channels) need to be
    // polled twice.

    let io_scheduler = Arc::new(BufferScheduler::new(batch.data.clone())) as Arc<dyn EncodingsIo>;
    let cache = if let Some(cache) = cache {
        cache
    } else {
        Arc::new(lance_core::cache::LanceCache::with_capacity(
            128 * 1024 * 1024,
        ))
    };
    let mut decode_scheduler = DecodeBatchScheduler::try_new(
        batch.schema.as_ref(),
        &batch.top_level_columns,
        &batch.page_table,
        &vec![],
        batch.num_rows,
        decoder_plugins,
        io_scheduler.clone(),
        cache,
        filter,
        &DecoderConfig::default(),
    )
    .await?;
    let (tx, rx) = unbounded_channel();
    decode_scheduler.schedule_range(0..batch.num_rows, filter, tx, io_scheduler);
    let is_structural = version >= LanceFileVersion::V2_1;
    let mut decode_stream = create_decode_stream(
        &batch.schema,
        batch.num_rows,
        batch.num_rows as u32,
        is_structural,
        should_validate,
        rx,
    );
    decode_stream.next().await.unwrap().task.await
}

#[cfg(test)]
// test coalesce indices to ranges
mod tests {
    use super::*;

    #[test]
    fn test_coalesce_indices_to_ranges_with_single_index() {
        let indices = vec![1];
        let ranges = DecodeBatchScheduler::indices_to_ranges(&indices);
        assert_eq!(ranges, vec![1..2]);
    }

    #[test]
    fn test_coalesce_indices_to_ranges() {
        let indices = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let ranges = DecodeBatchScheduler::indices_to_ranges(&indices);
        assert_eq!(ranges, vec![1..10]);
    }

    #[test]
    fn test_coalesce_indices_to_ranges_with_gaps() {
        let indices = vec![1, 2, 3, 5, 6, 7, 9];
        let ranges = DecodeBatchScheduler::indices_to_ranges(&indices);
        assert_eq!(ranges, vec![1..4, 5..8, 9..10]);
    }
}
