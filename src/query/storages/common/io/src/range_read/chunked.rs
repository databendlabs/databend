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

//! Facade of the ranged-read chain: splits a large range into fixed-size
//! chunks, feeds them into the chain with feedback-driven pacing, and streams
//! the results back in order — chunk by chunk or through [`std::io::Read`].

use std::collections::VecDeque;
use std::io;
use std::ops::Range;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use opendal::Buffer;

use crate::range_read::RangeReader;

/// Streams a sequence of chunks through a [`RangeReader`] chain.
///
/// Prefetch hints run ahead of consumption and are re-paced after every read
/// based on the chain's pressure feedback; correctness never depends on a
/// hint being accepted. The facade keeps no data: everything it holds is the
/// planning cursor (`pending`/`backlog`) plus the tail of the chunk currently
/// drained through `io::Read`.
pub struct ChunkedRangeReader<R: RangeReader> {
    chain: R,
    /// Chunks not yet consumed, in consumption order.
    pending: VecDeque<Range<u64>>,
    /// Chunks not yet hinted; always a suffix of `pending`.
    backlog: VecDeque<Range<u64>>,
    /// Remainder of the chunk currently drained through `io::Read`.
    current: Option<Buffer>,
}

impl<R: RangeReader> ChunkedRangeReader<R> {
    /// Stream explicit `chunks` through `chain`, consumed in the given order.
    pub fn new(chain: R, chunks: Vec<Range<u64>>) -> Self {
        let mut reader = Self {
            chain,
            pending: chunks.iter().cloned().collect(),
            backlog: chunks.into_iter().collect(),
            current: None,
        };
        reader.pump();
        reader
    }

    /// Split `range` into `chunk_size`d pieces and stream them through `chain`.
    pub fn with_range(chain: R, range: Range<u64>, chunk_size: u64) -> Result<Self> {
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
        Ok(Self::new(chain, chunks))
    }

    /// Feed pending chunks into the chain until it reports saturation.
    ///
    /// When `prefetch` returns false the front chunk may or may not have been
    /// accepted; it stays in the backlog and is hinted again on the next pump,
    /// which the chain deduplicates (R4: dropped hints are refetched by
    /// `read` on demand).
    fn pump(&mut self) {
        while let Some(chunk) = self.backlog.front() {
            if !self.chain.prefetch(std::slice::from_ref(chunk)) {
                break;
            }
            self.backlog.pop_front();
        }
    }

    /// Next chunk in consumption order, or `None` after the last one.
    pub fn read_next_chunk(&mut self) -> Result<Option<Buffer>> {
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

impl<R: RangeReader> io::Read for ChunkedRangeReader<R> {
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
            match self.read_next_chunk().map_err(io::Error::other)? {
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
        let mut reader = ChunkedRangeReader::with_range(chain, 3..23, 8).unwrap();

        let mut out = Vec::new();
        reader.read_to_end(&mut out).unwrap();
        assert_eq!(out, &CONTENT[3..23]);
        assert_eq!(accessor.read_ranges(), vec![3..11, 11..19, 19..23]);
    }

    #[test]
    fn test_read_next_chunk_in_order_then_none() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(CONTENT, false);
        let chain = OperatorRangeReader::new(recording_operator(accessor.clone()), "c".into(), 4);
        let mut reader = ChunkedRangeReader::with_range(chain, 0..10, 4).unwrap();

        assert_eq!(
            reader
                .read_next_chunk()
                .unwrap()
                .unwrap()
                .to_bytes()
                .as_ref(),
            b"abcd"
        );
        assert_eq!(
            reader
                .read_next_chunk()
                .unwrap()
                .unwrap()
                .to_bytes()
                .as_ref(),
            b"efgh"
        );
        assert_eq!(
            reader
                .read_next_chunk()
                .unwrap()
                .unwrap()
                .to_bytes()
                .as_ref(),
            b"ij"
        );
        assert!(reader.read_next_chunk().unwrap().is_none());
        assert!(reader.read_next_chunk().unwrap().is_none());
    }

    #[test]
    fn test_no_duplicate_dispatch_when_consumption_catches_up() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(CONTENT, false);
        // Capacity 1: hints lag behind consumption, so most chunks are read on
        // the spot; none may be dispatched twice.
        let chain = OperatorRangeReader::new(recording_operator(accessor.clone()), "d".into(), 1);
        let mut reader = ChunkedRangeReader::with_range(chain, 0..25, 5).unwrap();

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
        let mut reader = ChunkedRangeReader::with_range(chain, 0..16, 8).unwrap();

        let mut out = Vec::new();
        assert!(reader.read_to_end(&mut out).is_err());
    }

    #[test]
    fn test_empty_range_yields_eof() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(CONTENT, false);
        let chain = OperatorRangeReader::new(recording_operator(accessor.clone()), "e".into(), 1);
        let mut reader = ChunkedRangeReader::with_range(chain, 5..5, 8).unwrap();

        assert!(reader.read_next_chunk().unwrap().is_none());
        let mut out = Vec::new();
        reader.read_to_end(&mut out).unwrap();
        assert!(out.is_empty());
        assert!(accessor.read_ranges().is_empty());
    }

    #[test]
    fn test_zero_chunk_size_is_rejected() {
        init_test_runtime();
        let chain = OperatorRangeReader::new(memory_operator(), "z".into(), 1);
        assert!(ChunkedRangeReader::with_range(chain, 0..8, 0).is_err());
    }
}
