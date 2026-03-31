// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! # RLE (Run-Length Encoding) Miniblock Format
//!
//! RLE compression for Lance miniblock format, optimized for data with repeated values.
//!
//! ## Encoding Format
//!
//! RLE uses a dual-buffer format to store compressed data:
//!
//! - **Values Buffer**: Stores unique values in their original data type
//! - **Lengths Buffer**: Stores the repeat count for each value as u8
//!
//! ### Example
//!
//! Input data: `[1, 1, 1, 2, 2, 3, 3, 3, 3]`
//!
//! Encoded as:
//! - Values buffer: `[1, 2, 3]` (3 × 4 bytes for i32)
//! - Lengths buffer: `[3, 2, 4]` (3 × 1 byte for u8)
//!
//! ### Long Run Handling
//!
//! When a run exceeds 255 values, it is split into multiple runs of 255
//! followed by a final run with the remainder. For example, a run of 1000
//! identical values becomes 4 runs: [255, 255, 255, 235].
//!
//! ## Supported Types
//!
//! RLE supports all fixed-width primitive types:
//! - 8-bit: u8, i8
//! - 16-bit: u16, i16
//! - 32-bit: u32, i32, f32
//! - 64-bit: u64, i64, f64
//!
//! ## Compression Strategy
//!
//! RLE is automatically selected when:
//! - The run count (number of value transitions) < 50% of total values
//! - This indicates sufficient repetition for RLE to be effective
//!
//! ## Chunk Handling
//!
//! - Maximum chunk size: 4096 values (miniblock constraint)
//! - All chunks share two global buffers (values and lengths)
//! - Each chunk's buffer_sizes indicate its portion of the global buffers
//! - Non-last chunks always contain power-of-2 values
//! - Byte limits are enforced dynamically during encoding

use arrow_buffer::ArrowNativeType;
use log::trace;
use snafu::location;

use crate::buffer::LanceBuffer;
use crate::compression::MiniBlockDecompressor;
use crate::data::DataBlock;
use crate::data::{BlockInfo, FixedWidthDataBlock};
use crate::encodings::logical::primitive::miniblock::{
    MiniBlockChunk, MiniBlockCompressed, MiniBlockCompressor, MAX_MINIBLOCK_BYTES,
};
use crate::format::pb21::CompressiveEncoding;
use crate::format::ProtobufUtils21;

use lance_core::{Error, Result};

/// RLE encoder for miniblock format
#[derive(Debug, Default)]
pub struct RleMiniBlockEncoder;

impl RleMiniBlockEncoder {
    pub fn new() -> Self {
        Self
    }

    fn encode_data(
        &self,
        data: &LanceBuffer,
        num_values: u64,
        bits_per_value: u64,
    ) -> Result<(Vec<LanceBuffer>, Vec<MiniBlockChunk>)> {
        if num_values == 0 {
            return Ok((Vec::new(), Vec::new()));
        }

        let bytes_per_value = (bits_per_value / 8) as usize;

        // Pre-allocate global buffers with estimated capacity
        // Assume average compression ratio of ~10:1 (10 values per run)
        let estimated_runs = num_values as usize / 10;
        let mut all_values = Vec::with_capacity(estimated_runs * bytes_per_value);
        let mut all_lengths = Vec::with_capacity(estimated_runs);
        let mut chunks = Vec::new();

        let mut offset = 0usize;
        let mut values_remaining = num_values as usize;

        while values_remaining > 0 {
            let values_start = all_values.len();
            let lengths_start = all_lengths.len();

            let (_num_runs, values_processed, is_last_chunk) = match bits_per_value {
                8 => self.encode_chunk_rolling::<u8>(
                    data,
                    offset,
                    values_remaining,
                    &mut all_values,
                    &mut all_lengths,
                ),
                16 => self.encode_chunk_rolling::<u16>(
                    data,
                    offset,
                    values_remaining,
                    &mut all_values,
                    &mut all_lengths,
                ),
                32 => self.encode_chunk_rolling::<u32>(
                    data,
                    offset,
                    values_remaining,
                    &mut all_values,
                    &mut all_lengths,
                ),
                64 => self.encode_chunk_rolling::<u64>(
                    data,
                    offset,
                    values_remaining,
                    &mut all_values,
                    &mut all_lengths,
                ),
                _ => unreachable!("RLE encoding bits_per_value must be 8, 16, 32 or 64"),
            };

            if values_processed == 0 {
                break;
            }

            let log_num_values = if is_last_chunk {
                0
            } else {
                assert!(
                    values_processed.is_power_of_two(),
                    "Non-last chunk must have power-of-2 values"
                );
                values_processed.ilog2() as u8
            };

            let values_size = all_values.len() - values_start;
            let lengths_size = all_lengths.len() - lengths_start;

            let chunk = MiniBlockChunk {
                buffer_sizes: vec![values_size as u16, lengths_size as u16],
                log_num_values,
            };

            chunks.push(chunk);

            offset += values_processed;
            values_remaining -= values_processed;
        }

        // Return exactly two buffers: values and lengths
        Ok((
            vec![
                LanceBuffer::from(all_values),
                LanceBuffer::from(all_lengths),
            ],
            chunks,
        ))
    }

    /// Encodes a chunk of data using RLE compression with dynamic boundary detection.
    ///
    /// This function processes values sequentially, detecting runs (sequences of identical values)
    /// and encoding them as (value, length) pairs. It dynamically determines whether this chunk
    /// should be the last chunk based on how many values were processed.
    ///
    /// # Key Features:
    /// - Tracks byte usage to ensure we don't exceed MAX_MINIBLOCK_BYTES
    /// - Maintains power-of-2 checkpoints for non-last chunks
    /// - Splits long runs (>255) into multiple entries
    /// - Dynamically determines if this is the last chunk
    ///
    /// # Returns:
    /// - num_runs: Number of runs encoded
    /// - values_processed: Number of input values processed
    /// - is_last_chunk: Whether this chunk processed all remaining values
    fn encode_chunk_rolling<T>(
        &self,
        data: &LanceBuffer,
        offset: usize,
        values_remaining: usize,
        all_values: &mut Vec<u8>,
        all_lengths: &mut Vec<u8>,
    ) -> (usize, usize, bool)
    where
        T: bytemuck::Pod + PartialEq + Copy + std::fmt::Debug + ArrowNativeType,
    {
        let type_size = std::mem::size_of::<T>();

        let chunk_start = offset * type_size;
        // FIXME(xuanwo): we don't allow 4096 values as a workaround for https://github.com/lance-format/lance/issues/4429
        // Since while rep/def takes 4B, 4Ki values will lead to the
        // generated chunk buffer too large.MAX_MINIBLOCK_VALUES
        //
        // let max_by_count =  as usize;
        let max_by_count = 2048usize;
        let max_values = values_remaining.min(max_by_count);
        let chunk_end = chunk_start + max_values * type_size;

        if chunk_start >= data.len() {
            return (0, 0, false);
        }

        let chunk_len = chunk_end.min(data.len()) - chunk_start;
        let chunk_buffer = data.slice_with_length(chunk_start, chunk_len);
        let typed_data_ref = chunk_buffer.borrow_to_typed_slice::<T>();
        let typed_data: &[T] = typed_data_ref.as_ref();

        if typed_data.is_empty() {
            return (0, 0, false);
        }

        // Record starting positions for this chunk
        let values_start = all_values.len();

        let mut current_value = typed_data[0];
        let mut current_length = 1u64;
        let mut bytes_used = 0usize;
        let mut total_values_encoded = 0usize; // Track total encoded values

        // Power-of-2 checkpoints for ensuring non-last chunks have valid sizes
        // For smaller data types like u8, we can use larger initial checkpoints
        // since they take less space per value
        let checkpoints = match type_size {
            1 => vec![256, 512, 1024, 2048, 4096], // u8 can start from 256
            2 => vec![128, 256, 512, 1024, 2048, 4096], // u16 can start from 128
            _ => vec![64, 128, 256, 512, 1024, 2048, 4096], // u32/u64: no difference
        };
        let valid_checkpoints: Vec<usize> = checkpoints
            .into_iter()
            .filter(|&p| p <= values_remaining)
            .collect();
        let mut checkpoint_idx = 0;

        // Save state at checkpoints so we can roll back if needed
        let mut last_checkpoint_state = None;

        for &value in typed_data[1..].iter() {
            if value == current_value {
                current_length += 1;
            } else {
                // Calculate space needed (may need multiple u8s if run > 255)
                let run_chunks = current_length.div_ceil(255) as usize;
                let bytes_needed = run_chunks * (type_size + 1);

                // Stop if adding this run would exceed byte limit
                if bytes_used + bytes_needed > MAX_MINIBLOCK_BYTES as usize {
                    if let Some((val_pos, len_pos, _, checkpoint_values)) = last_checkpoint_state {
                        // Roll back to last power-of-2 checkpoint
                        all_values.truncate(val_pos);
                        all_lengths.truncate(len_pos);
                        let num_runs = (val_pos - values_start) / type_size;
                        return (num_runs, checkpoint_values, false);
                    }
                    break;
                }

                bytes_used += self.add_run(&current_value, current_length, all_values, all_lengths);
                total_values_encoded += current_length as usize;
                current_value = value;
                current_length = 1;
            }

            // Check if we reached a power-of-2 checkpoint
            if checkpoint_idx < valid_checkpoints.len()
                && total_values_encoded >= valid_checkpoints[checkpoint_idx]
            {
                last_checkpoint_state = Some((
                    all_values.len(),
                    all_lengths.len(),
                    bytes_used,
                    valid_checkpoints[checkpoint_idx],
                ));
                checkpoint_idx += 1;
            }
        }

        // After the loop, we always have a pending run that needs to be added
        // unless we've exceeded the byte limit
        if current_length > 0 {
            let run_chunks = current_length.div_ceil(255) as usize;
            let bytes_needed = run_chunks * (type_size + 1);

            if bytes_used + bytes_needed <= MAX_MINIBLOCK_BYTES as usize {
                let _ = self.add_run(&current_value, current_length, all_values, all_lengths);
                total_values_encoded += current_length as usize;
            }
        }

        // Determine if we've processed all remaining values
        let is_last_chunk = total_values_encoded == values_remaining;

        // Non-last chunks must have power-of-2 values for miniblock format
        if !is_last_chunk {
            if total_values_encoded.is_power_of_two() {
                // Already at power-of-2 boundary
            } else if let Some((val_pos, len_pos, _, checkpoint_values)) = last_checkpoint_state {
                // Roll back to last valid checkpoint
                all_values.truncate(val_pos);
                all_lengths.truncate(len_pos);
                let num_runs = (val_pos - values_start) / type_size;
                return (num_runs, checkpoint_values, false);
            } else {
                // No valid checkpoint, can't create a valid chunk
                return (0, 0, false);
            }
        }

        let num_runs = (all_values.len() - values_start) / type_size;
        (num_runs, total_values_encoded, is_last_chunk)
    }

    fn add_run<T>(
        &self,
        value: &T,
        length: u64,
        all_values: &mut Vec<u8>,
        all_lengths: &mut Vec<u8>,
    ) -> usize
    where
        T: bytemuck::Pod,
    {
        let value_bytes = bytemuck::bytes_of(value);
        let type_size = std::mem::size_of::<T>();
        let num_full_chunks = (length / 255) as usize;
        let remainder = (length % 255) as u8;

        let total_chunks = num_full_chunks + if remainder > 0 { 1 } else { 0 };
        all_values.reserve(total_chunks * type_size);
        all_lengths.reserve(total_chunks);

        for _ in 0..num_full_chunks {
            all_values.extend_from_slice(value_bytes);
            all_lengths.push(255);
        }

        if remainder > 0 {
            all_values.extend_from_slice(value_bytes);
            all_lengths.push(remainder);
        }

        total_chunks * (type_size + 1)
    }
}

impl MiniBlockCompressor for RleMiniBlockEncoder {
    fn compress(&self, data: DataBlock) -> Result<(MiniBlockCompressed, CompressiveEncoding)> {
        match data {
            DataBlock::FixedWidth(fixed_width) => {
                let num_values = fixed_width.num_values;
                let bits_per_value = fixed_width.bits_per_value;

                let (all_buffers, chunks) =
                    self.encode_data(&fixed_width.data, num_values, bits_per_value)?;

                let compressed = MiniBlockCompressed {
                    data: all_buffers,
                    chunks,
                    num_values,
                };

                let encoding = ProtobufUtils21::rle(
                    ProtobufUtils21::flat(bits_per_value, None),
                    ProtobufUtils21::flat(/*bits_per_value=*/ 8, None),
                );

                Ok((compressed, encoding))
            }
            _ => Err(Error::InvalidInput {
                location: location!(),
                source: "RLE encoding only supports FixedWidth data blocks".into(),
            }),
        }
    }
}

/// RLE decompressor for miniblock format
#[derive(Debug)]
pub struct RleMiniBlockDecompressor {
    bits_per_value: u64,
}

impl RleMiniBlockDecompressor {
    pub fn new(bits_per_value: u64) -> Self {
        Self { bits_per_value }
    }

    fn decode_data(&self, data: Vec<LanceBuffer>, num_values: u64) -> Result<DataBlock> {
        if num_values == 0 {
            return Ok(DataBlock::FixedWidth(FixedWidthDataBlock {
                bits_per_value: self.bits_per_value,
                data: LanceBuffer::from(vec![]),
                num_values: 0,
                block_info: BlockInfo::default(),
            }));
        }

        assert_eq!(
            data.len(),
            2,
            "RLE decompressor expects exactly 2 buffers, got {}",
            data.len()
        );

        let values_buffer = &data[0];
        let lengths_buffer = &data[1];

        let decoded_data = match self.bits_per_value {
            8 => self.decode_generic::<u8>(values_buffer, lengths_buffer, num_values)?,
            16 => self.decode_generic::<u16>(values_buffer, lengths_buffer, num_values)?,
            32 => self.decode_generic::<u32>(values_buffer, lengths_buffer, num_values)?,
            64 => self.decode_generic::<u64>(values_buffer, lengths_buffer, num_values)?,
            _ => unreachable!("RLE decoding bits_per_value must be 8, 16, 32, 64, or 128"),
        };

        Ok(DataBlock::FixedWidth(FixedWidthDataBlock {
            bits_per_value: self.bits_per_value,
            data: LanceBuffer::from(decoded_data),
            num_values,
            block_info: BlockInfo::default(),
        }))
    }

    fn decode_generic<T>(
        &self,
        values_buffer: &LanceBuffer,
        lengths_buffer: &LanceBuffer,
        num_values: u64,
    ) -> Result<Vec<u8>>
    where
        T: bytemuck::Pod + Copy + std::fmt::Debug + ArrowNativeType,
    {
        let type_size = std::mem::size_of::<T>();

        if values_buffer.is_empty() || lengths_buffer.is_empty() {
            if num_values == 0 {
                return Ok(Vec::new());
            } else {
                return Err(Error::InvalidInput {
                    location: location!(),
                    source: format!("Empty buffers but expected {} values", num_values).into(),
                });
            }
        }

        if values_buffer.len() % type_size != 0 || lengths_buffer.is_empty() {
            return Err(Error::InvalidInput {
                location: location!(),
                source: format!(
                    "Invalid buffer sizes for RLE {} decoding: values {} bytes (not divisible by {}), lengths {} bytes",
                    std::any::type_name::<T>(),
                    values_buffer.len(),
                    type_size,
                    lengths_buffer.len()
                )
                .into(),
            });
        }

        let num_runs = values_buffer.len() / type_size;
        let num_length_entries = lengths_buffer.len();
        assert_eq!(
            num_runs, num_length_entries,
            "Inconsistent RLE buffers: {} runs but {} length entries",
            num_runs, num_length_entries
        );

        let values_ref = values_buffer.borrow_to_typed_slice::<T>();
        let values: &[T] = values_ref.as_ref();
        let lengths: &[u8] = lengths_buffer.as_ref();

        let expected_byte_count = num_values as usize * type_size;
        let mut decoded = Vec::with_capacity(expected_byte_count);

        for (value, &length) in values.iter().zip(lengths.iter()) {
            let run_length = length as usize;
            let bytes_to_write = run_length * type_size;
            let bytes_of_value = bytemuck::bytes_of(value);

            if decoded.len() + bytes_to_write > expected_byte_count {
                let remaining_bytes = expected_byte_count - decoded.len();
                let remaining_values = remaining_bytes / type_size;

                for _ in 0..remaining_values {
                    decoded.extend_from_slice(bytes_of_value);
                }
                break;
            }

            for _ in 0..run_length {
                decoded.extend_from_slice(bytes_of_value);
            }
        }

        if decoded.len() != expected_byte_count {
            return Err(Error::InvalidInput {
                location: location!(),
                source: format!(
                    "RLE decoding produced {} bytes, expected {}",
                    decoded.len(),
                    expected_byte_count
                )
                .into(),
            });
        }

        trace!(
            "RLE decoded {} {} values",
            num_values,
            std::any::type_name::<T>()
        );
        Ok(decoded)
    }
}

impl MiniBlockDecompressor for RleMiniBlockDecompressor {
    fn decompress(&self, data: Vec<LanceBuffer>, num_values: u64) -> Result<DataBlock> {
        self.decode_data(data, num_values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::DataBlock;
    use crate::encodings::logical::primitive::miniblock::MAX_MINIBLOCK_VALUES;
    use arrow_array::Int32Array;

    // ========== Core Functionality Tests ==========

    #[test]
    fn test_basic_rle_encoding() {
        let encoder = RleMiniBlockEncoder::new();

        // Test basic RLE pattern: [1, 1, 1, 2, 2, 3, 3, 3, 3]
        let array = Int32Array::from(vec![1, 1, 1, 2, 2, 3, 3, 3, 3]);
        let data_block = DataBlock::from_array(array);

        let (compressed, _) = encoder.compress(data_block).unwrap();

        assert_eq!(compressed.num_values, 9);
        assert_eq!(compressed.chunks.len(), 1);

        // Verify compression happened (3 runs instead of 9 values)
        let values_buffer = &compressed.data[0];
        let lengths_buffer = &compressed.data[1];
        assert_eq!(values_buffer.len(), 12); // 3 i32 values
        assert_eq!(lengths_buffer.len(), 3); // 3 u8 lengths
    }

    #[test]
    fn test_long_run_splitting() {
        let encoder = RleMiniBlockEncoder::new();

        // Create a run longer than 255 to test splitting
        let mut data = vec![42i32; 1000]; // Will be split into 255+255+255+235
        data.extend(&[100i32; 300]); // Will be split into 255+45

        let array = Int32Array::from(data);
        let (compressed, _) = encoder.compress(DataBlock::from_array(array)).unwrap();

        // Should have 6 runs total (4 for first value, 2 for second)
        let lengths_buffer = &compressed.data[1];
        assert_eq!(lengths_buffer.len(), 6);
    }

    // ========== Round-trip Tests for Different Types ==========

    #[test]
    fn test_round_trip_all_types() {
        // Test u8
        test_round_trip_helper(vec![42u8, 42, 42, 100, 100, 255, 255, 255, 255], 8);

        // Test u16
        test_round_trip_helper(vec![1000u16, 1000, 2000, 2000, 2000, 3000], 16);

        // Test i32
        test_round_trip_helper(vec![100i32, 100, 100, -200, -200, 300, 300, 300, 300], 32);

        // Test u64
        test_round_trip_helper(vec![1_000_000_000u64; 5], 64);
    }

    fn test_round_trip_helper<T>(data: Vec<T>, bits_per_value: u64)
    where
        T: bytemuck::Pod + PartialEq + std::fmt::Debug,
    {
        let encoder = RleMiniBlockEncoder::new();
        let bytes: Vec<u8> = data
            .iter()
            .flat_map(|v| bytemuck::bytes_of(v))
            .copied()
            .collect();

        let block = DataBlock::FixedWidth(FixedWidthDataBlock {
            bits_per_value,
            data: LanceBuffer::from(bytes),
            num_values: data.len() as u64,
            block_info: BlockInfo::default(),
        });

        let (compressed, _) = encoder.compress(block).unwrap();
        let decompressor = RleMiniBlockDecompressor::new(bits_per_value);
        let decompressed = decompressor
            .decompress(compressed.data, compressed.num_values)
            .unwrap();

        match decompressed {
            DataBlock::FixedWidth(ref block) => {
                // Verify the decompressed data length matches expected
                assert_eq!(block.data.len(), data.len() * std::mem::size_of::<T>());
            }
            _ => panic!("Expected FixedWidth block"),
        }
    }

    // ========== Chunk Boundary Tests ==========

    #[test]
    fn test_power_of_two_chunking() {
        let encoder = RleMiniBlockEncoder::new();

        // Create data that will require multiple chunks
        let test_sizes = vec![1000, 2500, 5000, 10000];

        for size in test_sizes {
            let data: Vec<i32> = (0..size)
                .map(|i| i / 50) // Create runs of 50
                .collect();

            let array = Int32Array::from(data);
            let (compressed, _) = encoder.compress(DataBlock::from_array(array)).unwrap();

            // Verify all non-last chunks have power-of-2 values
            for (i, chunk) in compressed.chunks.iter().enumerate() {
                if i < compressed.chunks.len() - 1 {
                    assert!(chunk.log_num_values > 0);
                    let chunk_values = 1u64 << chunk.log_num_values;
                    assert!(chunk_values.is_power_of_two());
                    assert!(chunk_values <= MAX_MINIBLOCK_VALUES);
                } else {
                    assert_eq!(chunk.log_num_values, 0);
                }
            }
        }
    }

    // ========== Error Handling Tests ==========

    #[test]
    #[should_panic(expected = "RLE decompressor expects exactly 2 buffers")]
    fn test_invalid_buffer_count() {
        let decompressor = RleMiniBlockDecompressor::new(32);
        let _ = decompressor.decompress(vec![LanceBuffer::from(vec![1, 2, 3, 4])], 10);
    }

    #[test]
    #[should_panic(expected = "Inconsistent RLE buffers")]
    fn test_buffer_consistency() {
        let decompressor = RleMiniBlockDecompressor::new(32);
        let values = LanceBuffer::from(vec![1, 0, 0, 0]); // 1 i32 value
        let lengths = LanceBuffer::from(vec![5, 10]); // 2 lengths - mismatch!
        let _ = decompressor.decompress(vec![values, lengths], 15);
    }

    #[test]
    fn test_empty_data_handling() {
        let encoder = RleMiniBlockEncoder::new();

        // Test empty block
        let empty_block = DataBlock::FixedWidth(FixedWidthDataBlock {
            bits_per_value: 32,
            data: LanceBuffer::from(vec![]),
            num_values: 0,
            block_info: BlockInfo::default(),
        });

        let (compressed, _) = encoder.compress(empty_block).unwrap();
        assert_eq!(compressed.num_values, 0);
        assert!(compressed.data.is_empty());

        // Test decompression of empty data
        let decompressor = RleMiniBlockDecompressor::new(32);
        let decompressed = decompressor.decompress(vec![], 0).unwrap();

        match decompressed {
            DataBlock::FixedWidth(ref block) => {
                assert_eq!(block.num_values, 0);
                assert_eq!(block.data.len(), 0);
            }
            _ => panic!("Expected FixedWidth block"),
        }
    }

    // ========== Integration Test ==========

    #[test]
    fn test_multi_chunk_round_trip() {
        let encoder = RleMiniBlockEncoder::new();

        // Create data that spans multiple chunks with mixed patterns
        let mut data = Vec::new();

        // High compression section
        data.extend(vec![999i32; 2000]);
        // Low compression section
        data.extend(0..1000);
        // Another high compression section
        data.extend(vec![777i32; 2000]);

        let array = Int32Array::from(data.clone());
        let (compressed, _) = encoder.compress(DataBlock::from_array(array)).unwrap();

        // Manually decompress all chunks
        let mut reconstructed = Vec::new();
        let mut values_offset = 0usize;
        let mut lengths_offset = 0usize;
        let mut values_processed = 0u64;

        // We now have exactly 2 global buffers
        assert_eq!(compressed.data.len(), 2);
        let global_values = &compressed.data[0];
        let global_lengths = &compressed.data[1];

        for chunk in &compressed.chunks {
            let chunk_values = if chunk.log_num_values > 0 {
                1u64 << chunk.log_num_values
            } else {
                compressed.num_values - values_processed
            };

            // Extract chunk buffers from global buffers using buffer_sizes
            let values_size = chunk.buffer_sizes[0] as usize;
            let lengths_size = chunk.buffer_sizes[1] as usize;

            let chunk_values_buffer = global_values.slice_with_length(values_offset, values_size);
            let chunk_lengths_buffer =
                global_lengths.slice_with_length(lengths_offset, lengths_size);

            let decompressor = RleMiniBlockDecompressor::new(32);
            let chunk_data = decompressor
                .decompress(
                    vec![chunk_values_buffer, chunk_lengths_buffer],
                    chunk_values,
                )
                .unwrap();

            values_offset += values_size;
            lengths_offset += lengths_size;
            values_processed += chunk_values;

            match chunk_data {
                DataBlock::FixedWidth(ref block) => {
                    let values: &[i32] = bytemuck::cast_slice(block.data.as_ref());
                    reconstructed.extend_from_slice(values);
                }
                _ => panic!("Expected FixedWidth block"),
            }
        }

        assert_eq!(reconstructed, data);
    }

    #[test]
    fn test_1024_boundary_conditions() {
        // Comprehensive test for various boundary conditions at 1024 values
        // This consolidates multiple bug tests that were previously separate
        let encoder = RleMiniBlockEncoder::new();
        let decompressor = RleMiniBlockDecompressor::new(32);

        let test_cases = [
            ("runs_of_2", {
                let mut data = Vec::new();
                for i in 0..512 {
                    data.push(i);
                    data.push(i);
                }
                data
            }),
            ("single_run_1024", vec![42i32; 1024]),
            ("alternating_values", {
                let mut data = Vec::new();
                for i in 0..1024 {
                    data.push(i % 2);
                }
                data
            }),
            ("run_boundary_255s", {
                let mut data = Vec::new();
                data.extend(vec![1i32; 255]);
                data.extend(vec![2i32; 255]);
                data.extend(vec![3i32; 255]);
                data.extend(vec![4i32; 255]);
                data.extend(vec![5i32; 4]);
                data
            }),
            ("unique_values_1024", (0..1024).collect::<Vec<_>>()),
            ("unique_plus_duplicate", {
                // 1023 unique values followed by one duplicate (regression test)
                let mut data = Vec::new();
                for i in 0..1023 {
                    data.push(i);
                }
                data.push(1022i32); // Last value same as second-to-last
                data
            }),
            ("bug_4092_pattern", {
                // Test exact scenario that produces 4092 bytes instead of 4096
                let mut data = Vec::new();
                for i in 0..1022 {
                    data.push(i);
                }
                data.push(999999i32);
                data.push(999999i32);
                data
            }),
        ];

        for (test_name, data) in test_cases.iter() {
            assert_eq!(data.len(), 1024, "Test case {} has wrong length", test_name);

            // Compress the data
            let array = Int32Array::from(data.clone());
            let (compressed, _) = encoder.compress(DataBlock::from_array(array)).unwrap();

            // Decompress and verify
            match decompressor.decompress(compressed.data, compressed.num_values) {
                Ok(decompressed) => match decompressed {
                    DataBlock::FixedWidth(ref block) => {
                        let values: &[i32] = bytemuck::cast_slice(block.data.as_ref());
                        assert_eq!(
                            values.len(),
                            1024,
                            "Test case {} got {} values, expected 1024",
                            test_name,
                            values.len()
                        );
                        assert_eq!(
                            block.data.len(),
                            4096,
                            "Test case {} got {} bytes, expected 4096",
                            test_name,
                            block.data.len()
                        );
                        assert_eq!(values, &data[..], "Test case {} data mismatch", test_name);
                    }
                    _ => panic!("Test case {} expected FixedWidth block", test_name),
                },
                Err(e) => {
                    if e.to_string().contains("4092") {
                        panic!("Test case {} found bug 4092: {}", test_name, e);
                    }
                    panic!("Test case {} failed with error: {}", test_name, e);
                }
            }
        }
    }

    #[test]
    fn test_low_repetition_50pct_bug() {
        // Test case that reproduces the 4092 bytes bug with low repetition (50%)
        // This simulates the 1M benchmark case
        let encoder = RleMiniBlockEncoder::new();

        // Create 1M values with low repetition (50% chance of change)
        let num_values = 1_048_576; // 1M values
        let mut data = Vec::with_capacity(num_values);
        let mut value = 0i32;
        let mut rng = 12345u64; // Simple deterministic RNG

        for _ in 0..num_values {
            data.push(value);
            // Simple LCG for deterministic randomness
            rng = rng.wrapping_mul(1664525).wrapping_add(1013904223);
            // 50% chance to increment value
            if (rng >> 16) & 1 == 1 {
                value += 1;
            }
        }

        let bytes: Vec<u8> = data.iter().flat_map(|v| v.to_le_bytes()).collect();

        let block = DataBlock::FixedWidth(FixedWidthDataBlock {
            bits_per_value: 32,
            data: LanceBuffer::from(bytes),
            num_values: num_values as u64,
            block_info: BlockInfo::default(),
        });

        let (compressed, _) = encoder.compress(block).unwrap();

        // Debug first few chunks
        for (i, chunk) in compressed.chunks.iter().take(5).enumerate() {
            let _chunk_values = if chunk.log_num_values > 0 {
                1 << chunk.log_num_values
            } else {
                // Last chunk - calculate remaining
                let prev_total: usize = compressed.chunks[..i]
                    .iter()
                    .map(|c| 1usize << c.log_num_values)
                    .sum();
                num_values - prev_total
            };
        }

        // Try to decompress
        let decompressor = RleMiniBlockDecompressor::new(32);
        match decompressor.decompress(compressed.data, compressed.num_values) {
            Ok(decompressed) => match decompressed {
                DataBlock::FixedWidth(ref block) => {
                    assert_eq!(
                        block.data.len(),
                        num_values * 4,
                        "Expected {} bytes but got {}",
                        num_values * 4,
                        block.data.len()
                    );
                }
                _ => panic!("Expected FixedWidth block"),
            },
            Err(e) => {
                if e.to_string().contains("4092") {
                    panic!("Bug reproduced! {}", e);
                } else {
                    panic!("Unexpected error: {}", e);
                }
            }
        }
    }

    // ========== Encoding Verification Tests ==========

    #[test_log::test(tokio::test)]
    async fn test_rle_encoding_verification() {
        use crate::testing::{check_round_trip_encoding_of_data, TestCases};
        use crate::version::LanceFileVersion;
        use arrow_array::{Array, Int32Array};
        use lance_datagen::{ArrayGenerator, RowCount};
        use std::collections::HashMap;
        use std::sync::Arc;

        let test_cases = TestCases::default()
            .with_expected_encoding("rle")
            .with_min_file_version(LanceFileVersion::V2_1);

        // Test both explicit metadata and automatic selection
        // 1. Test with explicit RLE threshold metadata (also disable BSS)
        let mut metadata_explicit = HashMap::new();
        metadata_explicit.insert(
            "lance-encoding:rle-threshold".to_string(),
            "0.8".to_string(),
        );
        metadata_explicit.insert("lance-encoding:bss".to_string(), "off".to_string());

        let mut generator = RleDataGenerator::new(vec![1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3]);
        let data_explicit = generator.generate_default(RowCount::from(10000)).unwrap();
        check_round_trip_encoding_of_data(vec![data_explicit], &test_cases, metadata_explicit)
            .await;

        // 2. Test automatic RLE selection based on data characteristics
        // 80% repetition should trigger RLE (> default 50% threshold)
        // Explicitly disable BSS to ensure RLE is tested
        let mut metadata = HashMap::new();
        metadata.insert("lance-encoding:bss".to_string(), "off".to_string());

        let mut values = vec![42i32; 8000]; // 80% repetition
        values.extend([1i32, 2i32, 3i32, 4i32, 5i32].repeat(400)); // 20% variety
        let arr = Arc::new(Int32Array::from(values)) as Arc<dyn Array>;
        check_round_trip_encoding_of_data(vec![arr], &test_cases, metadata).await;
    }

    /// Generator that produces repetitive patterns suitable for RLE
    #[derive(Debug)]
    struct RleDataGenerator {
        pattern: Vec<i32>,
        idx: usize,
    }

    impl RleDataGenerator {
        fn new(pattern: Vec<i32>) -> Self {
            Self { pattern, idx: 0 }
        }
    }

    impl lance_datagen::ArrayGenerator for RleDataGenerator {
        fn generate(
            &mut self,
            _length: lance_datagen::RowCount,
            _rng: &mut rand_xoshiro::Xoshiro256PlusPlus,
        ) -> std::result::Result<std::sync::Arc<dyn arrow_array::Array>, arrow_schema::ArrowError>
        {
            use arrow_array::Int32Array;
            use std::sync::Arc;

            // Generate enough repetitive data to trigger RLE
            let mut values = Vec::new();
            for _ in 0..10000 {
                values.push(self.pattern[self.idx]);
                self.idx = (self.idx + 1) % self.pattern.len();
            }
            Ok(Arc::new(Int32Array::from(values)))
        }

        fn data_type(&self) -> &arrow_schema::DataType {
            &arrow_schema::DataType::Int32
        }

        fn element_size_bytes(&self) -> Option<lance_datagen::ByteCount> {
            Some(lance_datagen::ByteCount::from(4))
        }
    }
}
