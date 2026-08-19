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
/// - `parked`: chunk bytes held in memory for reads that are still coming
///   (cache hits and arrived segment pieces); `held_bytes` over it is this
///   layer's pressure term.
/// - `dispatched`: identities of miss segments forwarded downstream. `read`
///   must reuse these exact identities (identity rule R1), chunk membership
///   is recovered arithmetically via the grid.
/// - `pending_reads`: how many times each range was prefetched and not yet
///   read — prefetch twice, read twice. A consumed chunk stays parked while
///   an unfinished read still overlaps it, so ranges sharing a boundary
///   chunk never round-trip through the asynchronous cache admission.
///
/// `populate` gates only the admission side: with it off, fetched chunks are
/// never written to the disk cache, while lookups still serve existing
/// entries — the same semantics as `put_cache` of the column data cache.
pub struct DiskCacheRangeReader<R: RangeReader> {
    cache: LruDiskCacheHolder,
    next: R,
    grid: ChunkGrid,
    path: String,
    max_segment_size: u64,
    held_budget: usize,
    populate: bool,
    parked: HashMap<Range<u64>, Buffer>,
    dispatched: HashSet<Range<u64>>,
    pending_reads: HashMap<Range<u64>, usize>,
    held_bytes: usize,
}

impl<R: RangeReader> DiskCacheRangeReader<R> {
    /// `file_len` pins the tail chunk of the grid; `max_segment_size` caps each
    /// downstream miss envelope, including cache-hit chunks bridged within it;
    /// `held_budget` bounds the bytes parked in this layer before it reports
    /// saturation; `populate` gates admission of fetched chunks into the
    /// disk cache.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cache: LruDiskCacheHolder,
        next: R,
        path: String,
        file_len: u64,
        chunk_size: u64,
        max_segment_size: u64,
        held_budget: usize,
        populate: bool,
    ) -> Result<Self> {
        Ok(Self {
            cache,
            next,
            grid: ChunkGrid::new(chunk_size, file_len)?,
            path,
            max_segment_size: max_segment_size.max(chunk_size),
            held_budget,
            populate,
            parked: HashMap::new(),
            dispatched: HashSet::new(),
            pending_reads: HashMap::new(),
            held_bytes: 0,
        })
    }

    fn cache_key(&self, chunk: &Range<u64>) -> String {
        format!("{}-{}-{}", self.path, chunk.start, chunk.end - chunk.start)
    }

    fn covered_by_dispatched(&self, chunk: &Range<u64>) -> Option<Range<u64>> {
        self.dispatched
            .iter()
            .find(|segment| segment.start <= chunk.start && chunk.end <= segment.end)
            .cloned()
    }

    fn park(&mut self, chunk: Range<u64>, data: Buffer) {
        self.held_bytes += data.len();
        self.parked.insert(chunk, data);
    }

    fn take_parked(&mut self, chunk: &Range<u64>) -> Result<Buffer> {
        let data = self.parked.remove(chunk).ok_or_else(|| {
            ErrorCode::Internal(format!(
                "chunk {chunk:?} of {} disappeared from the cache layer",
                self.path
            ))
        })?;
        self.held_bytes = self.held_bytes.saturating_sub(data.len());
        Ok(data)
    }

    /// Record that one more `read` of this range is coming. Prefetching the
    /// same range again announces one more read served from parked data.
    fn add_pending_read(&mut self, range: &Range<u64>) {
        if range.is_empty() {
            return;
        }
        *self.pending_reads.entry(range.clone()).or_insert(0) += 1;
    }

    /// One announced `read` of this range happened, if any was pending.
    fn finish_pending_read(&mut self, range: &Range<u64>) {
        if let Some(count) = self.pending_reads.get_mut(range) {
            *count -= 1;
            if *count == 0 {
                self.pending_reads.remove(range);
            }
        }
    }

    /// Whether a pending read still needs bytes from this chunk. The scan
    /// is bounded by the callers' lookahead (a handful of pending ranges).
    fn has_pending_read(&self, chunk: &Range<u64>) -> bool {
        self.pending_reads
            .keys()
            .any(|range| range.start < chunk.end && chunk.start < range.end)
    }

    /// Merge misses into downstream segments, capped at `max_segment_size`.
    /// Besides adjacent misses, a segment may span cached chunks when every
    /// intervening chunk belongs to the current requested ranges. This matches
    /// Doris' first-miss-to-last-miss envelope without crossing an unrequested
    /// hole or duplicating a chunk that is already in flight.
    fn coalesce_misses(
        &self,
        misses: Vec<Range<u64>>,
        requested_ranges: &[Range<u64>],
    ) -> Vec<Range<u64>> {
        let requested_chunks = requested_ranges
            .iter()
            .flat_map(|range| self.grid.chunks_of(range))
            .collect::<HashSet<_>>();
        let mut segments: Vec<Range<u64>> = Vec::new();
        for chunk in misses {
            if let Some(last) = segments.last_mut() {
                let bridge_is_cached = last.end <= chunk.start
                    && self
                        .grid
                        .chunks_of(&(last.end..chunk.start))
                        .into_iter()
                        .all(|gap| {
                            requested_chunks.contains(&gap) && self.parked.contains_key(&gap)
                        });
                if bridge_is_cached && chunk.end - last.start <= self.max_segment_size {
                    last.end = chunk.end;
                    continue;
                }
            }
            segments.push(chunk);
        }
        segments
    }

    /// Split an arrived envelope back into grid chunks. Chunks already parked
    /// were cache hits bridged by the envelope: discard their duplicate remote
    /// bytes and admit only the original misses into the shared disk LRU.
    fn park_segment(&mut self, segment: Range<u64>, data: Buffer) {
        let mut batch = Vec::new();
        for chunk in self.grid.chunks_of(&segment) {
            if self.parked.contains_key(&chunk) {
                continue;
            }
            let start = (chunk.start - segment.start) as usize;
            let end = (chunk.end - segment.start) as usize;
            let piece = data.slice(start..end);
            if self.populate {
                batch.push((self.cache_key(&chunk), piece.to_bytes()));
            }
            self.park(chunk, piece);
        }
        // One queue slot per fetched file segment, not per 1 MiB chunk.
        if !batch.is_empty() {
            self.cache.populate(batch);
        }
    }

    /// Chunks of `ranges` that are neither parked nor in flight,
    /// offset-ordered and deduplicated: the candidates for one cache probe.
    fn candidate_chunks(&self, ranges: &[Range<u64>]) -> Vec<Range<u64>> {
        let mut candidates: Vec<Range<u64>> = Vec::new();
        for range in ranges {
            for chunk in self.grid.chunks_of(range) {
                if self.parked.contains_key(&chunk) || self.covered_by_dispatched(&chunk).is_some()
                {
                    continue;
                }
                candidates.push(chunk);
            }
        }
        candidates.sort_by_key(|chunk| chunk.start);
        candidates.dedup();
        candidates
    }

    /// Probe the disk cache for `chunks` in one batch, park the hits and
    /// return the misses.
    fn park_hits(&mut self, chunks: Vec<Range<u64>>) -> Vec<Range<u64>> {
        let keys = chunks
            .iter()
            .map(|chunk| self.cache_key(chunk))
            .collect::<Vec<_>>();
        let mut misses = Vec::new();
        for (chunk, hit) in chunks.into_iter().zip(self.cache.mget(&keys)) {
            match hit {
                Some(bytes) => {
                    let bytes = bytes.as_ref().clone();
                    self.park(chunk, Buffer::from(bytes));
                }
                None => misses.push(chunk),
            }
        }
        misses
    }
}

impl<R: RangeReader> RangeReader for DiskCacheRangeReader<R> {
    fn prefetch(&mut self, ranges: &[Range<u64>]) -> bool {
        for range in ranges {
            self.add_pending_read(range);
        }

        // Budget brake: any candidate could be a hit that parks chunk-sized
        // bytes, so the tail that would overflow the budget is deferred
        // untouched — no cache probe, no dispatch. Deferred chunks are served
        // by a later prefetch or by `read` itself, which is never limited.
        let mut candidates = self.candidate_chunks(ranges);
        let total = candidates.len();
        let mut projected = self.held_bytes;
        let mut kept = 0;
        for chunk in &candidates {
            projected = projected.saturating_add((chunk.end - chunk.start) as usize);
            if projected > self.held_budget {
                break;
            }
            kept += 1;
        }
        candidates.truncate(kept);
        let deferred = kept < total;

        let misses = self.park_hits(candidates);
        let segments = self.coalesce_misses(misses, ranges);
        let downstream = self.next.prefetch(&segments);
        for segment in segments {
            // Recorded even when the hint was dropped downstream: the segment
            // is this layer's identity for those chunks, and `read(segment)`
            // fetches dropped hints on demand.
            self.dispatched.insert(segment);
        }
        !deferred && self.held_bytes <= self.held_budget && downstream
    }

    fn read(&mut self, range: Range<u64>) -> Result<Buffer> {
        if range.is_empty() {
            return Ok(Buffer::new());
        }

        // Count down this range's pending reads; chunks it shares with
        // other pending reads stay parked below.
        self.finish_pending_read(&range);

        if range.end > self.grid.file_len() {
            return Err(ErrorCode::BadArguments(format!(
                "range {range:?} exceeds file length {} of {}",
                self.grid.file_len(),
                self.path
            )));
        }

        // Misses that were never hinted (or deferred): merge and consume
        // them on the spot; they do not enter `dispatched`.
        let candidates = self.candidate_chunks(std::slice::from_ref(&range));
        let misses = self.park_hits(candidates);
        for segment in self.coalesce_misses(misses, std::slice::from_ref(&range)) {
            let data = self.next.read(segment.clone())?;
            self.park_segment(segment, data);
        }

        let mut parts: Vec<Bytes> = Vec::new();
        for chunk in self.grid.chunks_of(&range) {
            if !self.parked.contains_key(&chunk) {
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
                self.park_segment(segment, data);
            }
            let start = (range.start.max(chunk.start) - chunk.start) as usize;
            let end = (range.end.min(chunk.end) - chunk.start) as usize;
            let data = if self.has_pending_read(&chunk) {
                // A pending read still needs this chunk: keep
                // it parked instead of round-tripping through the
                // asynchronous cache admission, which cannot land in time.
                self.parked.get(&chunk).cloned().ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "chunk {chunk:?} of {} disappeared from the cache layer",
                        self.path
                    ))
                })?
            } else {
                self.take_parked(&chunk)?
            };
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
    use databend_storages_common_cache::LruDiskCacheBuilder;
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

    /// A holder with no population worker: nothing is ever admitted, so only
    /// the in-memory pending-read bookkeeping can carry data between reads.
    fn new_cache_without_population() -> (TempDir, LruDiskCacheHolder) {
        let dir = TempDir::new().unwrap();
        let holder = LruDiskCacheBuilder::new_disk_cache(
            &dir.path().to_path_buf(),
            1 << 20,
            DiskCacheKeyReloadPolicy::Reset,
            false,
        )
        .unwrap();
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
        DiskCacheRangeReader::new(
            cache,
            next,
            path,
            CONTENT.len() as u64,
            4,
            16,
            held_budget,
            true,
        )
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
    fn test_cache_hit_is_bridged_by_one_miss_envelope() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(CONTENT, false);
        let (_dir, cache) = new_cache();
        seed(&cache, "cached", 4..8, b"WXYZ");
        let mut reader = reader_over(accessor.clone(), cache.clone(), usize::MAX);

        assert!(reader.prefetch(&[0..12]));
        assert_eq!(
            reader.read(0..12).unwrap().to_bytes().as_ref(),
            b"abcdWXYZijkl"
        );
        // The cache hit in the middle is read through so both misses use one
        // first-miss-to-last-miss request. Its duplicate remote bytes are
        // discarded: the result above still contains the cached sentinel.
        assert_eq!(accessor.read_ranges(), vec![0..12]);
        // The hit was not overwritten; only the two misses were admitted.
        wait_admitted(&cache, "cached", &[0..4, 4..8, 8..12]);
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.get("cached-4-4").unwrap().as_ref().as_ref(), b"WXYZ");
    }

    #[test]
    fn test_direct_read_bridges_cache_hit_with_one_miss_envelope() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(CONTENT, false);
        let (_dir, cache) = new_cache();
        seed(&cache, "cached", 4..8, &CONTENT[4..8]);
        let mut reader = reader_over(accessor.clone(), cache, usize::MAX);

        assert_eq!(
            reader.read(0..12).unwrap().to_bytes().as_ref(),
            &CONTENT[0..12]
        );
        assert_eq!(accessor.read_ranges(), vec![0..12]);
        assert!(reader.dispatched.is_empty());
    }

    #[test]
    fn test_miss_envelope_respects_max_segment_size_across_hits() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(CONTENT, false);
        let (_dir, cache) = new_cache();
        seed(&cache, "capped-envelope", 4..8, &CONTENT[4..8]);
        let path = "capped-envelope".to_string();
        let next = OperatorRangeReader::new(recording_operator(accessor.clone()), path.clone(), 8);
        let mut reader = DiskCacheRangeReader::new(
            cache,
            next,
            path,
            CONTENT.len() as u64,
            4,
            8,
            usize::MAX,
            true,
        )
        .unwrap();

        assert!(reader.prefetch(&[0..16]));
        assert_eq!(reader.read(0..16).unwrap().to_bytes().as_ref(), CONTENT);
        // Bridging the hit would make the first envelope 12 bytes, so the
        // 8-byte cap keeps the leading miss separate.
        assert_eq!(accessor.read_ranges(), vec![0..4, 8..16]);
    }

    #[test]
    fn test_miss_envelope_does_not_cross_unrequested_chunks() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(CONTENT, false);
        let (_dir, cache) = new_cache();
        seed(&cache, "cached", 4..8, &CONTENT[4..8]);
        let mut reader = reader_over(accessor.clone(), cache, usize::MAX);

        assert!(reader.prefetch(&[0..4, 8..12]));
        assert_eq!(
            reader.read(0..4).unwrap().to_bytes().as_ref(),
            &CONTENT[0..4]
        );
        assert_eq!(
            reader.read(8..12).unwrap().to_bytes().as_ref(),
            &CONTENT[8..12]
        );
        // The cached middle chunk was not requested, so it cannot bridge the
        // two downstream requests.
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
    fn test_read_without_prefetch_admits_chunks() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(CONTENT, false);
        let (_dir, cache) = new_cache();
        let mut reader = reader_over(accessor.clone(), cache.clone(), usize::MAX);

        assert_eq!(
            reader.read(2..9).unwrap().to_bytes().as_ref(),
            &CONTENT[2..9]
        );
        // One merged on-the-spot segment over the three touched chunks.
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
            DiskCacheRangeReader::new(cache, next, path, 10, 4, 16, usize::MAX, true).unwrap();

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
    fn test_prefetch_defers_hits_beyond_budget() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(CONTENT, false);
        let (_dir, cache) = new_cache();
        seed(&cache, "cached", 0..4, &CONTENT[0..4]);
        seed(&cache, "cached", 4..8, &CONTENT[4..8]);
        seed(&cache, "cached", 8..12, &CONTENT[8..12]);
        let mut reader = reader_over(accessor.clone(), cache, 5);

        // Only the first hit fits the 5-byte budget; the rest stay in the
        // disk cache — neither parked nor dispatched.
        assert!(!reader.prefetch(&[0..12]));
        assert_eq!(reader.held_bytes, 4);
        assert!(reader.dispatched.is_empty());
        // The read path is never budget-limited and completes from the cache.
        assert_eq!(
            reader.read(0..12).unwrap().to_bytes().as_ref(),
            &CONTENT[0..12]
        );
        assert!(accessor.read_ranges().is_empty());
        assert_eq!(reader.held_bytes, 0);
    }

    #[test]
    fn test_max_segment_size_caps_merging() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(CONTENT, false);
        let (_dir, cache) = new_cache();
        let path = "capped".to_string();
        let next = OperatorRangeReader::new(recording_operator(accessor.clone()), path.clone(), 8);
        let mut reader =
            DiskCacheRangeReader::new(cache, next, path, 16, 4, 8, usize::MAX, true).unwrap();

        assert!(reader.prefetch(&[0..16]));
        reader.read(0..16).unwrap();
        // Four miss chunks, capped at two per segment.
        assert_eq!(accessor.read_ranges(), vec![0..8, 8..16]);
    }

    #[test]
    fn test_shared_boundary_chunk_is_fetched_once() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(CONTENT, false);
        let (_dir, cache) = new_cache_without_population();
        let path = "boundary".to_string();
        let next = OperatorRangeReader::new(recording_operator(accessor.clone()), path.clone(), 8);
        // max_segment_size = 8 keeps the two ranges in separate segments.
        let mut reader = DiskCacheRangeReader::new(
            cache,
            next,
            path,
            CONTENT.len() as u64,
            4,
            8,
            usize::MAX,
            true,
        )
        .unwrap();

        // Both ranges need chunk 4..8; the pending hint keeps it parked when
        // the first read consumes it, without any help from the disk cache.
        assert!(reader.prefetch(&[0..6, 6..10]));
        assert_eq!(
            reader.read(0..6).unwrap().to_bytes().as_ref(),
            &CONTENT[0..6]
        );
        assert_eq!(
            reader.read(6..10).unwrap().to_bytes().as_ref(),
            &CONTENT[6..10]
        );
        assert_eq!(accessor.read_ranges(), vec![0..8, 8..12]);
        // Everything read: nothing stays parked for the rest of the query.
        assert!(reader.pending_reads.is_empty());
        assert!(reader.parked.is_empty());
        assert_eq!(reader.held_bytes, 0);
    }

    #[test]
    fn test_double_hint_serves_double_read() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(CONTENT, false);
        // No population worker: only the pending-read bookkeeping can carry
        // the chunks from the first read to the second.
        let (_dir, cache) = new_cache_without_population();
        let mut reader = reader_over(accessor.clone(), cache, usize::MAX);

        assert!(reader.prefetch(&[0..6]));
        assert!(reader.prefetch(&[0..6]));
        for _ in 0..2 {
            assert_eq!(
                reader.read(0..6).unwrap().to_bytes().as_ref(),
                &CONTENT[0..6]
            );
        }
        // One remote fetch served both reads, then everything drained.
        assert_eq!(accessor.read_ranges(), vec![0..8]);
        assert!(reader.pending_reads.is_empty());
        assert!(reader.parked.is_empty());
        assert_eq!(reader.held_bytes, 0);
    }

    #[test]
    fn test_read_without_prefetch_releases_partially_consumed_chunks() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(CONTENT, false);
        let (_dir, cache) = new_cache();
        let mut reader = reader_over(accessor, cache, usize::MAX);

        // Never hinted: the partially consumed tail chunk 4..8 is not
        // retained, because no pending read needs it.
        assert_eq!(
            reader.read(2..6).unwrap().to_bytes().as_ref(),
            &CONTENT[2..6]
        );
        assert!(reader.pending_reads.is_empty());
        assert!(reader.parked.is_empty());
        assert_eq!(reader.held_bytes, 0);
    }

    #[test]
    fn test_populate_disabled_reads_hits_but_admits_nothing() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(CONTENT, false);
        let (_dir, cache) = new_cache();
        seed(&cache, "cached", 0..4, &CONTENT[0..4]);
        let path = "cached".to_string();
        let next = OperatorRangeReader::new(recording_operator(accessor.clone()), path.clone(), 8);
        let mut reader = DiskCacheRangeReader::new(
            cache.clone(),
            next,
            path,
            CONTENT.len() as u64,
            4,
            16,
            usize::MAX,
            false,
        )
        .unwrap();

        // The seeded chunk still serves as a hit; only the miss is fetched.
        assert_eq!(
            reader.read(0..8).unwrap().to_bytes().as_ref(),
            &CONTENT[0..8]
        );
        assert_eq!(accessor.read_ranges(), vec![4..8]);
        // Nothing was queued for admission: the cache keeps only the seed.
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(cache.len(), 1);
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
            Box::new(reader_over(
                cold_accessor.clone(),
                cache.clone(),
                usize::MAX,
            )),
            2..15,
            5,
            2,
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
            Box::new(reader_over(warm_accessor.clone(), cache, usize::MAX)),
            2..15,
            5,
            2,
        )
        .unwrap();
        let mut out = Vec::new();
        warm.read_to_end(&mut out).unwrap();
        assert_eq!(out, &CONTENT[2..15]);
        assert!(warm_accessor.read_ranges().is_empty());
    }
}
