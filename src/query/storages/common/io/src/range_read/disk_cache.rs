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

//! Chunk-cache link of the ranged-read chain, directly over the shared disk
//! LRU ([`LruDiskCacheHolder`]): chunk entries live in the same LRU and the
//! same storage budget as the column-oriented entries of the column data
//! cache — no dedicated cache slot, no cache abstraction in between.
//!
//! Key spaces cannot collide: chunk keys are `{path}-{offset}-{len}` while
//! column keys are `{path}-{column_id}-{offset}-{len}`, and storage paths
//! never end in `-<digits>`.
//!
//! Split/merge duality of this layer:
//! - Splitting is deterministic ([`ChunkGrid`]) and therefore never recorded:
//!   `read` recomputes the same chunks that `prefetch` produced.
//! - Merging misses into segments is a runtime decision, so the segment
//!   identities are recorded in `dispatched` ("whoever merges keeps the
//!   books"). They cannot be recomputed later: a re-merge across prefetch
//!   batches could produce a segment that was never dispatched downstream.

use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::Range;

use bytes::Bytes;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_storages_common_cache::LruDiskCacheHolder;
use opendal::Buffer;

use crate::range_read::ChunkGrid;
use crate::range_read::RangeReader;

/// Read-through chunk cache over the next chain link.
///
/// Ledgers (each backs one runtime decision, nothing else):
/// - `prefetch_map`: chunks held for a pending read (cache hits and arrived
///   segment pieces); `held_bytes` over it is this layer's pressure term.
/// - `dispatched`: identities of miss segments forwarded downstream. `read`
///   must reuse these exact identities (identity rule R1), chunk membership
///   is recovered arithmetically via the grid.
pub struct DiskCacheRangeReader<R: RangeReader> {
    cache: LruDiskCacheHolder,
    next: R,
    grid: ChunkGrid,
    path: String,
    max_segment_size: u64,
    held_budget: usize,
    prefetch_map: HashMap<Range<u64>, Buffer>,
    dispatched: HashSet<Range<u64>>,
    held_bytes: usize,
}

impl<R: RangeReader> DiskCacheRangeReader<R> {
    /// `file_len` pins the tail chunk of the grid; `max_segment_size` caps how
    /// many adjacent miss chunks merge into one downstream request;
    /// `held_budget` bounds the bytes parked in this layer before it reports
    /// saturation.
    pub fn new(
        cache: LruDiskCacheHolder,
        next: R,
        path: String,
        file_len: u64,
        chunk_size: u64,
        max_segment_size: u64,
        held_budget: usize,
    ) -> Result<Self> {
        Ok(Self {
            cache,
            next,
            grid: ChunkGrid::new(chunk_size, file_len)?,
            path,
            max_segment_size: max_segment_size.max(chunk_size),
            held_budget,
            prefetch_map: HashMap::new(),
            dispatched: HashSet::new(),
            held_bytes: 0,
        })
    }

    fn key(&self, chunk: &Range<u64>) -> String {
        format!("{}-{}-{}", self.path, chunk.start, chunk.end - chunk.start)
    }

    fn covered_by_dispatched(&self, chunk: &Range<u64>) -> Option<Range<u64>> {
        self.dispatched
            .iter()
            .find(|segment| segment.start <= chunk.start && chunk.end <= segment.end)
            .cloned()
    }

    fn hold(&mut self, chunk: Range<u64>, data: Buffer) {
        self.held_bytes += data.len();
        self.prefetch_map.insert(chunk, data);
    }

    fn take_chunk(&mut self, chunk: &Range<u64>) -> Result<Buffer> {
        let data = self.prefetch_map.remove(chunk).ok_or_else(|| {
            ErrorCode::Internal(format!(
                "chunk {chunk:?} of {} disappeared from the cache layer",
                self.path
            ))
        })?;
        self.held_bytes = self.held_bytes.saturating_sub(data.len());
        Ok(data)
    }

    /// Merge adjacent chunks (already offset-ordered) into seamless segments,
    /// capped at `max_segment_size`. Segments never cross gaps, so segment
    /// membership stays recoverable through the grid.
    fn coalesce(&self, chunks: Vec<Range<u64>>) -> Vec<Range<u64>> {
        let mut segments: Vec<Range<u64>> = Vec::new();
        for chunk in chunks {
            if let Some(last) = segments.last_mut()
                && last.end == chunk.start
                && chunk.end - last.start <= self.max_segment_size
            {
                last.end = chunk.end;
                continue;
            }
            segments.push(chunk);
        }
        segments
    }

    /// Split an arrived segment back into grid chunks, asynchronously admit
    /// them into the shared disk LRU and park them for pending reads.
    fn file_segment(&mut self, segment: Range<u64>, data: Buffer) {
        let mut batch = Vec::new();
        for chunk in self.grid.chunks_of(&segment) {
            let start = (chunk.start - segment.start) as usize;
            let end = (chunk.end - segment.start) as usize;
            let piece = data.slice(start..end);
            batch.push((self.key(&chunk), piece.to_bytes()));
            self.hold(chunk, piece);
        }
        // One queue slot per fetched file segment, not per 1 MiB chunk.
        self.cache.populate(batch);
    }

    /// Chunks of `ranges` that are neither parked, nor in flight, nor in the
    /// cache (cache hits are parked as a side effect), offset-ordered and
    /// deduplicated.
    fn collect_misses(&mut self, ranges: &[Range<u64>]) -> Vec<Range<u64>> {
        let mut wanted: Vec<Range<u64>> = Vec::new();
        for range in ranges {
            for chunk in self.grid.chunks_of(range) {
                if self.prefetch_map.contains_key(&chunk)
                    || self.covered_by_dispatched(&chunk).is_some()
                {
                    continue;
                }
                wanted.push(chunk);
            }
        }
        wanted.sort_by_key(|chunk| chunk.start);
        wanted.dedup();

        let keys = wanted
            .iter()
            .map(|chunk| self.key(chunk))
            .collect::<Vec<_>>();
        let mut misses = Vec::new();
        for (chunk, hit) in wanted.into_iter().zip(self.cache.mget(&keys)) {
            match hit {
                Some(bytes) => {
                    let bytes = bytes.as_ref().clone();
                    self.hold(chunk, Buffer::from(bytes));
                }
                None => misses.push(chunk),
            }
        }
        misses
    }
}

impl<R: RangeReader> RangeReader for DiskCacheRangeReader<R> {
    fn prefetch(&mut self, ranges: &[Range<u64>]) -> bool {
        let misses = self.collect_misses(ranges);
        let segments = self.coalesce(misses);
        let downstream = self.next.prefetch(&segments);
        for segment in segments {
            // Recorded even when the hint was dropped downstream: the segment
            // is this layer's identity for those chunks, and `read(segment)`
            // fetches dropped hints on demand.
            self.dispatched.insert(segment);
        }
        self.held_bytes <= self.held_budget && downstream
    }

    fn read(&mut self, range: Range<u64>) -> Result<Buffer> {
        if range.is_empty() {
            return Ok(Buffer::new());
        }

        if range.end > self.grid.file_len() {
            return Err(ErrorCode::BadArguments(format!(
                "range {range:?} exceeds file length {} of {}",
                self.grid.file_len(),
                self.path
            )));
        }

        // Demand misses: never hinted (or this layer dropped them). Merge and
        // consume them on the spot; they do not enter `dispatched`.
        let misses = self.collect_misses(std::slice::from_ref(&range));
        for segment in self.coalesce(misses) {
            let data = self.next.read(segment.clone())?;
            self.file_segment(segment, data);
        }

        let mut parts: Vec<Bytes> = Vec::new();
        for chunk in self.grid.chunks_of(&range) {
            if !self.prefetch_map.contains_key(&chunk) {
                // In flight: pull back the whole dispatched segment it belongs
                // to, using the exact identity recorded at prefetch time.
                let segment = self.covered_by_dispatched(&chunk).ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "chunk {chunk:?} of {} is neither parked nor dispatched",
                        self.path
                    ))
                })?;
                let data = self.next.read(segment.clone())?;
                self.dispatched.remove(&segment);
                self.file_segment(segment, data);
            }
            let data = self.take_chunk(&chunk)?;
            let start = (range.start.max(chunk.start) - chunk.start) as usize;
            let end = (range.end.min(chunk.end) - chunk.start) as usize;
            parts.extend(data.slice(start..end));
        }
        Ok(Buffer::from(parts))
    }
}

#[cfg(test)]
#[allow(clippy::single_range_in_vec_init)]
mod tests {
    use std::time::Duration;
    use std::time::Instant;

    use databend_common_config::DiskCacheKeyReloadPolicy;
    use databend_storages_common_cache::CacheAccessor;
    use databend_storages_common_cache::DiskCacheBuilder;
    use tempfile::TempDir;

    use super::*;
    use crate::init_test_runtime;
    use crate::range_read::OperatorRangeReader;
    use crate::range_read::test_util::*;

    const CONTENT: &[u8] = b"abcdefghijklmnop";

    fn new_cache() -> (TempDir, LruDiskCacheHolder) {
        let dir = TempDir::new().unwrap();
        let accessor = DiskCacheBuilder::try_build_disk_cache(
            "range_reader_test".to_string(),
            &dir.path().to_path_buf(),
            64,
            1 << 20,
            DiskCacheKeyReloadPolicy::Reset,
            false,
        )
        .unwrap();
        let holder = accessor.lru_disk_cache().clone();
        (dir, holder)
    }

    fn wait_admitted(cache: &LruDiskCacheHolder, path: &str, chunks: &[Range<u64>]) {
        let keys = chunks
            .iter()
            .map(|chunk| format!("{path}-{}-{}", chunk.start, chunk.end - chunk.start))
            .collect::<Vec<_>>();
        let deadline = Instant::now() + Duration::from_secs(10);
        while keys.iter().any(|key| !cache.contains_key(key)) {
            assert!(
                Instant::now() < deadline,
                "chunks were not admitted in time"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn seed(cache: &LruDiskCacheHolder, path: &str, chunk: Range<u64>, value: &[u8]) {
        cache.insert(
            format!("{path}-{}-{}", chunk.start, chunk.end - chunk.start),
            Bytes::copy_from_slice(value),
        );
    }

    fn reader_over(
        accessor: std::sync::Arc<RecordingReadAccessor>,
        cache: LruDiskCacheHolder,
        held_budget: usize,
    ) -> DiskCacheRangeReader<OperatorRangeReader> {
        let path = "cached".to_string();
        let next = OperatorRangeReader::new(recording_operator(accessor), path.clone(), 8);
        DiskCacheRangeReader::new(cache, next, path, CONTENT.len() as u64, 4, 16, held_budget)
            .unwrap()
    }

    #[test]
    fn test_cold_read_merges_misses_and_admits_chunks() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(CONTENT, false);
        let (_dir, cache) = new_cache();
        let mut reader = reader_over(accessor.clone(), cache.clone(), usize::MAX);

        assert!(reader.prefetch(&[0..8, 8..12]));
        assert_eq!(
            reader.read(0..12).unwrap().to_bytes().as_ref(),
            &CONTENT[0..12]
        );
        // Adjacent miss chunks merged into one segment.
        assert_eq!(accessor.read_ranges(), vec![0..12]);
        // Every fetched chunk was queued and admitted into the shared LRU.
        wait_admitted(&cache, "cached", &[0..4, 4..8, 8..12]);
        assert_eq!(cache.len(), 3);
    }

    #[test]
    fn test_warm_read_does_no_io() {
        init_test_runtime();
        let (_dir, cache) = new_cache();
        {
            let accessor = RecordingReadAccessor::new(CONTENT, false);
            let mut reader = reader_over(accessor, cache.clone(), usize::MAX);
            reader.read(0..16).unwrap();
        }
        wait_admitted(&cache, "cached", &[0..4, 4..8, 8..12, 12..16]);
        let accessor = RecordingReadAccessor::new(CONTENT, false);
        let mut reader = reader_over(accessor.clone(), cache, usize::MAX);
        assert!(reader.prefetch(&[0..16]));
        assert_eq!(reader.read(0..16).unwrap().to_bytes().as_ref(), CONTENT);
        assert!(accessor.read_ranges().is_empty());
    }

    #[test]
    fn test_cache_hit_splits_miss_segments() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(CONTENT, false);
        let (_dir, cache) = new_cache();
        seed(&cache, "cached", 4..8, &CONTENT[4..8]);
        let mut reader = reader_over(accessor.clone(), cache, usize::MAX);

        assert!(reader.prefetch(&[0..12]));
        assert_eq!(
            reader.read(0..12).unwrap().to_bytes().as_ref(),
            &CONTENT[0..12]
        );
        // The hit chunk in the middle keeps the two misses apart.
        assert_eq!(accessor.read_ranges(), vec![0..4, 8..12]);
    }

    #[test]
    fn test_segment_identity_is_stable_across_batches() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(CONTENT, false);
        let (_dir, cache) = new_cache();
        let mut reader = reader_over(accessor.clone(), cache, usize::MAX);

        // Two prefetch batches produce two adjacent segments; a read spanning
        // both must reuse the recorded identities instead of re-merging them
        // into a never-dispatched [0..12].
        assert!(reader.prefetch(&[0..8]));
        assert!(reader.prefetch(&[8..12]));
        assert_eq!(
            reader.read(0..12).unwrap().to_bytes().as_ref(),
            &CONTENT[0..12]
        );
        assert_eq!(accessor.read_ranges(), vec![0..8, 8..12]);
    }

    #[test]
    fn test_demand_read_without_prefetch_admits_chunks() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(CONTENT, false);
        let (_dir, cache) = new_cache();
        let mut reader = reader_over(accessor.clone(), cache.clone(), usize::MAX);

        assert_eq!(
            reader.read(2..9).unwrap().to_bytes().as_ref(),
            &CONTENT[2..9]
        );
        // One merged demand segment over the three touched chunks.
        assert_eq!(accessor.read_ranges(), vec![0..12]);
        wait_admitted(&cache, "cached", &[0..4, 4..8, 8..12]);
        assert_eq!(cache.len(), 3);
        assert!(reader.dispatched.is_empty());
    }

    #[test]
    fn test_overlapping_ranges_share_chunks_through_the_cache() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(CONTENT, false);
        let (_dir, cache) = new_cache();
        let mut reader = reader_over(accessor.clone(), cache.clone(), usize::MAX);

        assert!(reader.prefetch(&[0..6, 4..10]));
        assert_eq!(
            reader.read(0..6).unwrap().to_bytes().as_ref(),
            &CONTENT[0..6]
        );
        wait_admitted(&cache, "cached", &[0..4, 4..8, 8..12]);
        assert_eq!(
            reader.read(4..10).unwrap().to_bytes().as_ref(),
            &CONTENT[4..10]
        );
        // The shared chunk 4..8 was fetched exactly once; the second read
        // recovered it from the cache after the first read consumed it.
        assert_eq!(accessor.read_ranges(), vec![0..12]);
    }

    #[test]
    fn test_unaligned_tail_read() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(&CONTENT[0..10], false);
        let (_dir, cache) = new_cache();
        let path = "tail".to_string();
        let next = OperatorRangeReader::new(recording_operator(accessor.clone()), path.clone(), 8);
        let mut reader =
            DiskCacheRangeReader::new(cache, next, path, 10, 4, 16, usize::MAX).unwrap();

        assert_eq!(
            reader.read(5..10).unwrap().to_bytes().as_ref(),
            &CONTENT[5..10]
        );
        // Whole grid cells, tail clamped by file_len.
        assert_eq!(accessor.read_ranges(), vec![4..10]);
        assert!(reader.read(8..12).is_err());
    }

    #[test]
    fn test_held_budget_reports_saturation() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(CONTENT, false);
        let (_dir, cache) = new_cache();
        seed(&cache, "cached", 0..4, &CONTENT[0..4]);
        seed(&cache, "cached", 4..8, &CONTENT[4..8]);
        let mut reader = reader_over(accessor, cache, 5);

        // Two hits (8 bytes) exceed the 5-byte budget.
        assert!(!reader.prefetch(&[0..8]));
        // Consuming parked chunks releases the budget.
        assert_eq!(
            reader.read(0..8).unwrap().to_bytes().as_ref(),
            &CONTENT[0..8]
        );
        assert!(reader.prefetch(&[]));
    }

    #[test]
    fn test_max_segment_size_caps_merging() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(CONTENT, false);
        let (_dir, cache) = new_cache();
        let path = "capped".to_string();
        let next = OperatorRangeReader::new(recording_operator(accessor.clone()), path.clone(), 8);
        let mut reader =
            DiskCacheRangeReader::new(cache, next, path, 16, 4, 8, usize::MAX).unwrap();

        assert!(reader.prefetch(&[0..16]));
        reader.read(0..16).unwrap();
        // Four miss chunks, capped at two per segment.
        assert_eq!(accessor.read_ranges(), vec![0..8, 8..16]);
    }

    #[test]
    fn test_error_from_next_layer_propagates() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(CONTENT, true);
        let (_dir, cache) = new_cache();
        let mut reader = reader_over(accessor, cache, usize::MAX);

        assert!(reader.prefetch(&[0..8]));
        assert!(reader.read(0..8).is_err());
        // The tail is poisoned; the segment stays dispatched and keeps failing.
        assert!(reader.read(0..8).is_err());
    }

    #[test]
    fn test_full_chain_facade_cache_operator() {
        use std::io::Read;

        use crate::range_read::ChunkedRangeReader;

        init_test_runtime();
        let (_dir, cache) = new_cache();

        // Cold pass: facade streams through the cache layer down to the tail.
        let cold_accessor = RecordingReadAccessor::new(CONTENT, false);
        let mut cold = ChunkedRangeReader::with_range(
            reader_over(cold_accessor.clone(), cache.clone(), usize::MAX),
            2..15,
            5,
        )
        .unwrap();
        let mut out = Vec::new();
        cold.read_to_end(&mut out).unwrap();
        assert_eq!(out, &CONTENT[2..15]);
        assert!(!cold_accessor.read_ranges().is_empty());
        wait_admitted(&cache, "cached", &[0..4, 4..8, 8..12, 12..16]);

        // Warm pass over a fresh chain sharing the cache: zero remote I/O.
        let warm_accessor = RecordingReadAccessor::new(CONTENT, false);
        let mut warm = ChunkedRangeReader::with_range(
            reader_over(warm_accessor.clone(), cache, usize::MAX),
            2..15,
            5,
        )
        .unwrap();
        let mut out = Vec::new();
        warm.read_to_end(&mut out).unwrap();
        assert_eq!(out, &CONTENT[2..15]);
        assert!(warm_accessor.read_ranges().is_empty());
    }
}
