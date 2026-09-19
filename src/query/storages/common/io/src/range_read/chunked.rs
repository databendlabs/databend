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
//! range into fixed-size windows and streams them with a bounded lookahead
//! of prefetch hints running ahead of consumption.

use std::collections::VecDeque;
use std::io;
use std::ops::Range;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use opendal::Buffer;

use crate::range_read::RangeReader;

/// Adapts one contiguous file range into [`io::Read`] over a [`RangeReader`]
/// chain, keeping at most `lookahead` windows hinted ahead of consumption.
///
/// Hints are fire-and-forget: each window is hinted once and a rejected hint
/// is not retried — `read` pays one on-demand I/O for that window instead.
/// Correctness never depends on a hint being accepted, and the memory the
/// chain parks on this reader's behalf is bounded by the lookahead.
pub struct ChunkedRangeReader {
    chain: Box<dyn RangeReader>,
    /// Windows not yet consumed, in consumption order.
    pending: VecDeque<Range<u64>>,
    /// Number of windows kept hinted ahead of consumption.
    lookahead: usize,
    /// Remainder of the window currently drained through `io::Read`.
    current: Option<Buffer>,
}

impl ChunkedRangeReader {
    /// Split `range` into `chunk_size`d windows and expose them as one
    /// continuous byte stream, hinting up to `lookahead` windows ahead.
    pub fn with_range(
        chain: Box<dyn RangeReader>,
        range: Range<u64>,
        chunk_size: u64,
        lookahead: usize,
    ) -> Result<Self> {
        if chunk_size == 0 {
            return Err(ErrorCode::BadArguments(
                "chunked range reader requires a positive chunk size",
            ));
        }

        let mut pending = VecDeque::new();
        let mut start = range.start;
        while start < range.end {
            let end = start.saturating_add(chunk_size).min(range.end);
            pending.push_back(start..end);
            start = end;
        }

        let mut reader = Self {
            chain,
            pending,
            lookahead: lookahead.max(1),
            current: None,
        };

        // One batched hint for the initial window: a cache layer can probe
        // its store once for all of it and coalesce adjacent misses.
        let mut initial = Vec::with_capacity(reader.lookahead);
        for window in reader.pending.iter().take(reader.lookahead) {
            initial.push(window.clone());
        }
        if !initial.is_empty() {
            let _ = reader.chain.prefetch(&initial);
        }
        Ok(reader)
    }

    /// Load the next window in file order, or `None` at EOF.
    fn load_next(&mut self) -> Result<Option<Buffer>> {
        let Some(window) = self.pending.pop_front() else {
            return Ok(None);
        };
        // Replenish the lookahead before blocking on this read: the next
        // window must be announced downstream before this read consumes
        // their shared boundary chunk, and the worker can run
        // ahead while this read blocks. After the pop, the window that
        // extends the maintained lookahead is always at this fixed index.
        if let Some(next) = self.pending.get(self.lookahead - 1) {
            let _ = self.chain.prefetch(std::slice::from_ref(next));
        }
        let data = self.chain.read(window)?;
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
        let mut reader = ChunkedRangeReader::with_range(Box::new(chain), 3..23, 8, 2).unwrap();

        let mut out = Vec::new();
        reader.read_to_end(&mut out).unwrap();
        assert_eq!(out, &CONTENT[3..23]);
        assert_eq!(accessor.read_ranges(), vec![3..11, 11..19, 19..23]);
    }

    #[test]
    fn test_no_duplicate_dispatch_when_hints_are_dropped() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(CONTENT, false);
        // Capacity 1: replenish hints are frequently rejected, so windows
        // alternate between hinted and on-the-spot reads; none may be dispatched
        // twice and none may be retried.
        let chain = OperatorRangeReader::new(recording_operator(accessor.clone()), "d".into(), 1);
        let mut reader = ChunkedRangeReader::with_range(Box::new(chain), 0..25, 5, 1).unwrap();

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
        let mut reader = ChunkedRangeReader::with_range(Box::new(chain), 0..16, 8, 2).unwrap();

        let mut out = Vec::new();
        assert!(reader.read_to_end(&mut out).is_err());
    }

    #[test]
    fn test_empty_range_yields_eof() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(CONTENT, false);
        let chain = OperatorRangeReader::new(recording_operator(accessor.clone()), "e".into(), 1);
        let mut reader = ChunkedRangeReader::with_range(Box::new(chain), 5..5, 8, 1).unwrap();

        let mut out = Vec::new();
        reader.read_to_end(&mut out).unwrap();
        assert!(out.is_empty());
        assert!(accessor.read_ranges().is_empty());
    }

    #[test]
    fn test_zero_chunk_size_is_rejected() {
        init_test_runtime();
        let chain = OperatorRangeReader::new(memory_operator(), "z".into(), 1);
        assert!(ChunkedRangeReader::with_range(Box::new(chain), 0..8, 0, 1).is_err());
    }
}
