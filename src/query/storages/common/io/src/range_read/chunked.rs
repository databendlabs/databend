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

//! `io::Read` adapter over a ranged-read chain: splits one contiguous file
//! range into fixed-size windows, prefetches them, and exposes a continuous
//! byte stream.

use std::collections::VecDeque;
use std::io;
use std::ops::Range;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use opendal::Buffer;

use crate::range_read::RangeReader;

/// Adapts one contiguous file range into [`io::Read`] over a [`RangeReader`]
/// chain. Prefetch hints run ahead of consumption; correctness never depends
/// on a hint being accepted.
pub struct ChunkedRangeReader {
    chain: Box<dyn RangeReader>,
    /// Chunks not yet consumed, in consumption order.
    pending: VecDeque<Range<u64>>,
    /// Chunks not yet hinted; always a suffix of `pending`.
    backlog: VecDeque<Range<u64>>,
    /// Remainder of the chunk currently drained through `io::Read`.
    current: Option<Buffer>,
}

impl ChunkedRangeReader {
    /// Split `range` into `chunk_size`d windows and expose them as one
    /// continuous byte stream.
    pub fn with_range(
        chain: Box<dyn RangeReader>,
        range: Range<u64>,
        chunk_size: u64,
    ) -> Result<Self> {
        if chunk_size == 0 {
            return Err(ErrorCode::BadArguments(
                "chunked range reader requires a positive chunk size",
            ));
        }

        let mut chunks = Vec::new();
        let mut start = range.start;
        while start < range.end {
            let end = start.saturating_add(chunk_size).min(range.end);
            chunks.push(start..end);
            start = end;
        }

        let mut reader = Self {
            chain,
            pending: chunks.iter().cloned().collect(),
            backlog: chunks.into_iter().collect(),
            current: None,
        };
        reader.pump();
        Ok(reader)
    }

    /// Feed the current backlog as one batch. Cache layers can therefore do
    /// one `mget` and coalesce adjacent misses across all prefetched ranges.
    ///
    /// When `prefetch` returns false, some or all ranges may already have been
    /// accepted; the backlog is retained and retried after the next read. The
    /// chain deduplicates accepted hints, while dropped hints are fetched on
    /// demand by `read`.
    fn pump(&mut self) {
        if self.backlog.is_empty() {
            return;
        }

        let batch = self.backlog.iter().cloned().collect::<Vec<_>>();
        if self.chain.prefetch(&batch) {
            self.backlog.clear();
        }
    }

    /// Load the next window in file order, or `None` at EOF.
    fn load_next(&mut self) -> Result<Option<Buffer>> {
        let Some(chunk) = self.pending.pop_front() else {
            return Ok(None);
        };
        // Consumption caught up with the hints: take the chunk out of the
        // backlog so a later pump cannot re-dispatch a consumed range; `read`
        // fetches it on the spot instead.
        if self.backlog.front() == Some(&chunk) {
            self.backlog.pop_front();
        }
        let data = self.chain.read(chunk)?;
        self.pump();
        Ok(Some(data))
    }
}

impl io::Read for ChunkedRangeReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        loop {
            if let Some(current) = self.current.as_mut() {
                let read = current.read(buf)?;
                if read != 0 {
                    return Ok(read);
                }
                self.current = None;
            }
            match self.load_next().map_err(io::Error::other)? {
                Some(data) => self.current = Some(data),
                None => return Ok(0),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;
    use crate::init_test_runtime;
    use crate::range_read::OperatorRangeReader;
    use crate::range_read::test_util::*;

    const CONTENT: &[u8] = b"abcdefghijklmnopqrstuvwxyz";

    #[test]
    fn test_streams_whole_range_through_io_read() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(CONTENT, false);
        let chain = OperatorRangeReader::new(recording_operator(accessor.clone()), "s".into(), 2);
        let mut reader = ChunkedRangeReader::with_range(Box::new(chain), 3..23, 8).unwrap();

        let mut out = Vec::new();
        reader.read_to_end(&mut out).unwrap();
        assert_eq!(out, &CONTENT[3..23]);
        assert_eq!(accessor.read_ranges(), vec![3..11, 11..19, 19..23]);
    }

    #[test]
    fn test_no_duplicate_dispatch_when_consumption_catches_up() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(CONTENT, false);
        // Capacity 1: hints lag behind consumption, so most chunks are read on
        // the spot; none may be dispatched twice.
        let chain = OperatorRangeReader::new(recording_operator(accessor.clone()), "d".into(), 1);
        let mut reader = ChunkedRangeReader::with_range(Box::new(chain), 0..25, 5).unwrap();

        let mut out = Vec::new();
        reader.read_to_end(&mut out).unwrap();
        assert_eq!(out, &CONTENT[0..25]);

        let mut seen = accessor.read_ranges();
        assert_eq!(seen.len(), 5, "duplicate dispatch: {seen:?}");
        seen.sort_by_key(|range| range.start);
        assert_eq!(seen, vec![0..5, 5..10, 10..15, 15..20, 20..25]);
    }

    #[test]
    fn test_error_propagates_through_io_read() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(CONTENT, true);
        let chain = OperatorRangeReader::new(recording_operator(accessor), "err".into(), 2);
        let mut reader = ChunkedRangeReader::with_range(Box::new(chain), 0..16, 8).unwrap();

        let mut out = Vec::new();
        assert!(reader.read_to_end(&mut out).is_err());
    }

    #[test]
    fn test_empty_range_yields_eof() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(CONTENT, false);
        let chain = OperatorRangeReader::new(recording_operator(accessor.clone()), "e".into(), 1);
        let mut reader = ChunkedRangeReader::with_range(Box::new(chain), 5..5, 8).unwrap();

        let mut out = Vec::new();
        reader.read_to_end(&mut out).unwrap();
        assert!(out.is_empty());
        assert!(accessor.read_ranges().is_empty());
    }

    #[test]
    fn test_zero_chunk_size_is_rejected() {
        init_test_runtime();
        let chain = OperatorRangeReader::new(memory_operator(), "z".into(), 1);
        assert!(ChunkedRangeReader::with_range(Box::new(chain), 0..8, 0).is_err());
    }
}
