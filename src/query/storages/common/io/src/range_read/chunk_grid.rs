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

//! Deterministic chunk grid: maps byte ranges onto absolutely aligned,
//! fixed-size chunks. Alignment makes chunk identities recomputable (no
//! bookkeeping) and cache keys reusable across readers and queries.

use std::ops::Range;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

/// Absolute chunk grid over one immutable file: chunk `i` covers
/// `[i * chunk_size, min((i + 1) * chunk_size, file_len))`. The tail chunk is
/// clamped by `file_len` so every chunk identity is exact.
#[derive(Clone, Copy, Debug)]
pub struct ChunkGrid {
    chunk_size: u64,
    file_len: u64,
}

impl ChunkGrid {
    pub fn new(chunk_size: u64, file_len: u64) -> Result<Self> {
        if chunk_size == 0 {
            return Err(ErrorCode::BadArguments(
                "chunk grid requires a positive chunk size",
            ));
        }
        Ok(Self {
            chunk_size,
            file_len,
        })
    }

    pub fn file_len(&self) -> u64 {
        self.file_len
    }

    /// The full chunks covering `range`, in offset order. Chunks are whole
    /// grid cells: callers slice their sub-range out of them afterwards.
    pub fn chunks_of(&self, range: &Range<u64>) -> Vec<Range<u64>> {
        let begin = range.start.min(self.file_len);
        let end = range.end.min(self.file_len);
        if begin >= end {
            return Vec::new();
        }
        let mut chunks = Vec::new();
        let mut start = (begin / self.chunk_size) * self.chunk_size;
        while start < end {
            let chunk_end = start.saturating_add(self.chunk_size).min(self.file_len);
            chunks.push(start..chunk_end);
            start = chunk_end;
        }
        chunks
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grid_rejects_zero_chunk_size() {
        assert!(ChunkGrid::new(0, 10).is_err());
    }

    #[test]
    fn test_chunks_are_absolutely_aligned() {
        let grid = ChunkGrid::new(4, 16).unwrap();
        // Unaligned range still maps onto whole grid cells.
        assert_eq!(grid.chunks_of(&(5..11)), vec![4..8, 8..12]);
        // Exactly one cell.
        assert_eq!(grid.chunks_of(&(4..8)), vec![4..8]);
        // Sub-cell range maps to its containing cell.
        assert_eq!(grid.chunks_of(&(9..10)), vec![8..12]);
        // Empty range maps to nothing.
        assert!(grid.chunks_of(&(7..7)).is_empty());
    }

    #[test]
    fn test_tail_chunk_is_clamped_by_file_len() {
        let grid = ChunkGrid::new(4, 10).unwrap();
        assert_eq!(grid.chunks_of(&(5..10)), vec![4..8, 8..10]);
        assert_eq!(grid.chunks_of(&(8..10)), vec![8..10]);
        // Ranges beyond the file are clamped; validation is the reader's job.
        assert_eq!(grid.chunks_of(&(8..64)), vec![8..10]);
        assert!(grid.chunks_of(&(10..12)).is_empty());
    }
}
