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

use std::collections::VecDeque;
use std::fmt;
use std::io;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use databend_storages_common_io::OperatorRangeReader;
use databend_storages_common_io::RangeReader;
use opendal::Buffer;
use opendal::Operator;
use tantivy::HasLen;
use tantivy::directory::FileHandle;
use tantivy::directory::OwnedBytes;

/// Bytes fetched per step of the stream; the same size the sequential merge readers used.
pub const STREAMING_CHUNK_SIZE: u64 = 4 * 1024 * 1024;

/// Chunks in flight or arrived but unread, per stream.
const STREAMING_PREFETCH_DEPTH: usize = 2;

/// Fetched chunks kept behind the stream head. The windows in flight at once span a few hundred
/// kilobytes of one source, far less than a chunk, so two chunks serve every merger reading
/// the file; a read behind them is served on the side.
const STREAMING_KEPT_CHUNKS: usize = 2;

/// Read counters of one [`StreamingFileHandle`]; tests pin the access pattern with them.
#[derive(Debug, Default)]
pub struct StreamingReadStats {
    /// Chunks fetched from the stream.
    pub fetches: AtomicU64,
    /// Bytes fetched from the stream.
    pub fetched_bytes: AtomicU64,
    /// Reads behind the kept chunks, served by a one-off range request.
    pub behind_reads: AtomicU64,
    /// Largest number of bytes resident at once.
    pub peak_resident: AtomicU64,
}

impl StreamingReadStats {
    pub fn fetches(&self) -> u64 {
        self.fetches.load(Ordering::Relaxed)
    }

    pub fn fetched_bytes(&self) -> u64 {
        self.fetched_bytes.load(Ordering::Relaxed)
    }

    pub fn behind_reads(&self) -> u64 {
        self.behind_reads.load(Ordering::Relaxed)
    }

    pub fn peak_resident(&self) -> u64 {
        self.peak_resident.load(Ordering::Relaxed)
    }
}

struct Chunk {
    start: u64,
    data: Buffer,
}

impl Chunk {
    fn end(&self) -> u64 {
        self.start + self.data.len() as u64
    }
}

struct Ring {
    reader: Option<OperatorRangeReader>,
    /// Logical offset the stream delivers next.
    stream_at: u64,
    chunks: VecDeque<Chunk>,
    /// Logical range hinted to the reader but not consumed yet.
    hinted: Option<Range<u64>>,
    resident: u64,
}

/// Tantivy file handle over one logical file stored at `base..base + len` of an object, read
/// through one continuous stream and shared by the concurrent window mergers.
///
/// Term windows are processed in roughly increasing order, so their reads move forward through
/// the postings and positions files. The stream only ever advances: a read ahead of the head
/// fetches everything up to it, because the windows between the head and that read are in
/// flight and will ask for those bytes. The last [`STREAMING_KEPT_CHUNKS`] chunks stay resident;
/// a read behind them is served by a one-off range request and leaves the stream alone.
pub struct StreamingFileHandle {
    operator: Operator,
    object: String,
    path: PathBuf,
    base: u64,
    len: u64,
    chunk_size: u64,
    ring: Mutex<Ring>,
    stats: Arc<StreamingReadStats>,
}

impl fmt::Debug for StreamingFileHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("StreamingFileHandle")
            .field("path", &self.path)
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

impl StreamingFileHandle {
    pub fn new(
        operator: Operator,
        object: String,
        path: PathBuf,
        base: u64,
        len: u64,
        chunk_size: u64,
    ) -> Self {
        Self {
            operator,
            object,
            path,
            base,
            len,
            chunk_size: chunk_size.max(1),
            ring: Mutex::new(Ring {
                reader: None,
                stream_at: 0,
                chunks: VecDeque::new(),
                hinted: None,
                resident: 0,
            }),
            stats: Arc::new(StreamingReadStats::default()),
        }
    }

    pub fn stats(&self) -> Arc<StreamingReadStats> {
        self.stats.clone()
    }

    fn physical(&self, logical: &Range<u64>) -> Range<u64> {
        self.base + logical.start..self.base + logical.end
    }

    fn open_stream(&self, ring: &mut Ring, at: u64) -> io::Result<()> {
        if let Some(hinted) = ring.hinted.take()
            && let Some(reader) = ring.reader.as_mut()
        {
            reader.discard(self.physical(&hinted));
        }
        ring.reader = Some(
            OperatorRangeReader::new_streaming(
                self.operator.clone(),
                self.object.clone(),
                self.physical(&(at..self.len)),
                STREAMING_PREFETCH_DEPTH,
            )
            .map_err(|error| io::Error::other(error.to_string()))?,
        );
        ring.stream_at = at;
        Ok(())
    }

    /// Appends the next chunk of the stream and drops the oldest ones the read at `read_start`
    /// no longer needs.
    fn fetch_next(&self, ring: &mut Ring, read_start: u64) -> io::Result<()> {
        let range = ring.stream_at..(ring.stream_at + self.chunk_size).min(self.len);
        if range.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "{} stream exhausted at {}",
                    self.path.display(),
                    range.start
                ),
            ));
        }
        if ring.reader.is_none() {
            self.open_stream(ring, range.start)?;
        }
        let physical = self.physical(&range);
        if let Some(hinted) = ring.hinted.take()
            && hinted != range
        {
            let reader = ring.reader.as_mut().expect("stream opened above");
            reader.discard(self.physical(&hinted));
        }
        // Transport failures are retried below OpenDAL's reader; an error here is final.
        let reader = ring.reader.as_mut().expect("stream opened above");
        let data = RangeReader::read(reader, physical)
            .map_err(|error| io::Error::other(error.to_string()))?;
        if data.len() as u64 != range.end - range.start {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "{} returned {} bytes for {range:?}",
                    self.path.display(),
                    data.len()
                ),
            ));
        }
        self.stats.fetches.fetch_add(1, Ordering::Relaxed);
        self.stats
            .fetched_bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        // Detach from the backend buffer so residency tracks the bytes kept, not the
        // capacity the backend reserved.
        ring.chunks.push_back(Chunk {
            start: range.start,
            data: Buffer::from(data.to_vec()),
        });
        ring.resident += range.end - range.start;
        ring.stream_at = range.end;
        // Keep the chunks the current read spans even when it spans more than the quota.
        while ring.chunks.len() > STREAMING_KEPT_CHUNKS
            && ring
                .chunks
                .front()
                .is_some_and(|front| front.end() <= read_start)
        {
            let dropped = ring.chunks.pop_front().expect("front checked");
            ring.resident -= dropped.data.len() as u64;
        }
        self.stats
            .peak_resident
            .fetch_max(ring.resident, Ordering::Relaxed);

        let next = range.end..(range.end + self.chunk_size).min(self.len);
        if !next.is_empty() {
            let reader = ring.reader.as_mut().expect("stream open");
            if reader.prefetch(&[self.physical(&next)]) {
                ring.hinted = Some(next);
            }
        }
        Ok(())
    }

    /// A read behind the kept chunks: one range request, the stream stays where it is.
    fn read_behind(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        self.stats.behind_reads.fetch_add(1, Ordering::Relaxed);
        let logical = range.start as u64..range.end as u64;
        let mut reader = OperatorRangeReader::new(self.operator.clone(), self.object.clone(), 1);
        let data = RangeReader::read(&mut reader, self.physical(&logical))
            .map_err(|error| io::Error::other(error.to_string()))?;
        if data.len() != range.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "{} returned {} bytes for {range:?}",
                    self.path.display(),
                    data.len()
                ),
            ));
        }
        Ok(OwnedBytes::new(data.to_vec()))
    }
}

impl HasLen for StreamingFileHandle {
    fn len(&self) -> usize {
        self.len as usize
    }
}

#[async_trait::async_trait]
impl FileHandle for StreamingFileHandle {
    fn read_bytes(&self, range: Range<usize>) -> io::Result<OwnedBytes> {
        let start = range.start as u64;
        let end = range.end as u64;
        if end > self.len || start > end {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("read {range:?} outside {} bytes", self.len),
            ));
        }
        if start == end {
            return Ok(OwnedBytes::empty());
        }

        let mut ring = self.ring.lock().unwrap();
        let front_start = ring
            .chunks
            .front()
            .map_or(ring.stream_at, |chunk| chunk.start);
        if start < front_start {
            drop(ring);
            return self.read_behind(range);
        }
        while ring.chunks.back().map_or(ring.stream_at, Chunk::end) < end {
            self.fetch_next(&mut ring, start)?;
        }

        let mut bytes = Vec::with_capacity(range.len());
        for chunk in &ring.chunks {
            if chunk.end() <= start || chunk.start >= end {
                continue;
            }
            let from = start.max(chunk.start) - chunk.start;
            let to = end.min(chunk.end()) - chunk.start;
            bytes.extend_from_slice(&chunk.data.slice(from as usize..to as usize).to_bytes());
        }
        debug_assert_eq!(bytes.len(), range.len());
        Ok(OwnedBytes::new(bytes))
    }
}

#[cfg(test)]
mod tests {
    use databend_common_base::runtime::GlobalIORuntime;
    use opendal::services::Memory;

    use super::*;
    use crate::init_test_runtime;

    fn pattern(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    fn handle(data: &[u8], base: usize, len: usize, chunk: u64) -> StreamingFileHandle {
        init_test_runtime();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let data = data.to_vec();
        let writer = operator.clone();
        GlobalIORuntime::instance()
            .block_on(async move { Ok(writer.write("object", data).await?) })
            .unwrap();
        StreamingFileHandle::new(
            operator,
            "object".to_string(),
            PathBuf::from("f.idx"),
            base as u64,
            len as u64,
            chunk,
        )
    }

    #[test]
    fn test_forward_reads_stream_in_chunks_and_keep_two() {
        let data = pattern(10_000);
        let handle = handle(&data, 100, 9_000, 1_000);
        let file = &data[100..9_100];
        for start in (0..9_000).step_by(700) {
            let end = (start + 700).min(9_000);
            assert_eq!(
                handle.read_bytes(start..end).unwrap().as_slice(),
                &file[start..end]
            );
            assert!(handle.ring.lock().unwrap().resident <= 2_000 + 700);
        }
        let stats = handle.stats();
        assert_eq!(stats.fetches(), 9);
        assert_eq!(stats.fetched_bytes(), 9_000);
        assert_eq!(stats.behind_reads(), 0);
        assert!(stats.peak_resident() <= 3_000);
        assert_eq!(handle.ring.lock().unwrap().chunks.len(), 2);
    }

    #[test]
    fn test_reads_ahead_span_and_behind_stay_correct() {
        let data = pattern(20_000);
        let handle = handle(&data, 0, 20_000, 1_024);
        // Spanning three chunks keeps all three until a later read moves on.
        assert_eq!(
            handle.read_bytes(900..2_100).unwrap().as_slice(),
            &data[900..2_100]
        );
        assert_eq!(handle.stats().fetches(), 3);
        assert_eq!(handle.ring.lock().unwrap().chunks.len(), 3);
        // Ahead of the head: the stream fetches the gap; only the last two chunks stay.
        assert_eq!(
            handle.read_bytes(6_000..6_010).unwrap().as_slice(),
            &data[6_000..6_010]
        );
        assert_eq!(handle.stats().fetches(), 6);
        assert_eq!(handle.ring.lock().unwrap().resident, 2 * 1_024);
        // Behind the kept chunks: a one-off request, the stream is untouched.
        assert_eq!(handle.read_bytes(10..20).unwrap().as_slice(), &data[10..20]);
        assert_eq!(handle.stats().behind_reads(), 1);
        assert_eq!(handle.stats().fetches(), 6);
        assert_eq!(
            handle.read_bytes(5_200..7_000).unwrap().as_slice(),
            &data[5_200..7_000]
        );
        assert_eq!(handle.stats().fetches(), 7);
        assert!(handle.read_bytes(19_990..20_001).is_err());
        assert!(handle.read_bytes(5..5).unwrap().is_empty());
    }
}
