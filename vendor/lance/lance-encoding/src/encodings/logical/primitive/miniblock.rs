// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Routines for encoding and decoding miniblock data
//!
//! Miniblock encoding is one of the two structural encodings in Lance 2.1.
//! In this approach the data is compressed into a series of chunks put into
//! a single buffer.
//!
//! A chunk must be encoded or decoded as a unit.  There is a small amount of
//! chunk metadata such as the number and size of each buffer in the chunk.
//!
//! Any form of compression can be used since we are compressing and decompressing
//! entire chunks.
use crate::{buffer::LanceBuffer, data::DataBlock, format::pb21::CompressiveEncoding};

use lance_core::Result;

pub const MAX_MINIBLOCK_BYTES: u64 = 8 * 1024 - 6;
pub const MAX_MINIBLOCK_VALUES: u64 = 4096;

/// Page data that has been compressed into a series of chunks put into
/// a single buffer.
#[derive(Debug)]
pub struct MiniBlockCompressed {
    /// The buffers of compressed data
    pub data: Vec<LanceBuffer>,
    /// Describes the size of each chunk
    pub chunks: Vec<MiniBlockChunk>,
    /// The number of values in the entire page
    pub num_values: u64,
}

/// Describes the size of a mini-block chunk of data
///
/// Mini-block chunks are designed to be small (just a few disk sectors)
/// and contain a power-of-two number of values (except for the last chunk)
///
/// To enforce this we limit a chunk to 4Ki values and slightly less than
/// 8KiB of compressed data.  This means that even in the extreme case
/// where we have 4 bytes of rep/def then we will have at most 24KiB of
/// data (values, repetition, and definition) per mini-block.
#[derive(Debug)]
pub struct MiniBlockChunk {
    // The size in bytes of each buffer in the chunk.
    //
    // The total size must be less than or equal to 8Ki - 6 (8188)
    pub buffer_sizes: Vec<u16>,
    // The log (base 2) of the number of values in the chunk.  If this is the final chunk
    // then this should be 0 (the number of values will be calculated by subtracting the
    // size of all other chunks from the total size of the page)
    //
    // For example, 1 would mean there are 2 values in the chunk and 12 would mean there
    // are 4Ki values in the chunk.
    //
    // This must be <= 12 (i.e. <= 4096 values)
    pub log_num_values: u8,
}

impl MiniBlockChunk {
    /// Gets the number of values in this block
    ///
    /// This requires `vals_in_prev_blocks` and `total_num_values` because the
    /// last block in a page is a special case which stores 0 for log_num_values
    /// and, in that case, the number of values is determined by subtracting
    /// `vals_in_prev_blocks` from `total_num_values`
    pub fn num_values(&self, vals_in_prev_blocks: u64, total_num_values: u64) -> u64 {
        if self.log_num_values == 0 {
            total_num_values - vals_in_prev_blocks
        } else {
            1 << self.log_num_values
        }
    }
}

/// Trait for compression algorithms that are suitable for use in the miniblock structural encoding
///
/// These compression algorithms should be capable of encoding the data into small chunks
/// where each chunk (except the last) has 2^N values (N can vary between chunks)
pub trait MiniBlockCompressor: std::fmt::Debug + Send + Sync {
    /// Compress a `page` of data into multiple chunks
    ///
    /// See [`MiniBlockCompressed`] for details on how chunks should be sized.
    ///
    /// This method also returns a description of the encoding applied that will be
    /// used at decode time to read the data.
    fn compress(&self, page: DataBlock) -> Result<(MiniBlockCompressed, CompressiveEncoding)>;
}
