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

use std::fmt;
use std::io;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use databend_storages_common_io::OperatorRangeReader;
use databend_storages_common_io::RangeReader;
use opendal::Buffer;
use opendal::Operator;
use tantivy::HasLen;
use tantivy::directory::FileHandle;
use tantivy::directory::OwnedBytes;

/// Bytes fetched per window when a read moves past the current one.
pub const SEQUENTIAL_WINDOW_SIZE: u64 = 4 * 1024 * 1024;

/// Read counters of one [`SequentialFileHandle`]; tests pin the access pattern with them.
#[derive(Debug, Default)]
pub struct SequentialReadStats {
    /// Range requests sent to the object store.
    pub fetches: AtomicU64,
    /// Bytes fetched from the object store.
    pub fetched_bytes: AtomicU64,
    /// Reads that started before the current window.
    pub backward_reads: AtomicU64,
}

impl SequentialReadStats {
    pub fn fetches(&self) -> u64 {
        self.fetches.load(Ordering::Relaxed)
    }

    pub fn fetched_bytes(&self) -> u64 {
        self.fetched_bytes.load(Ordering::Relaxed)
    }

    pub fn backward_reads(&self) -> u64 {
        self.backward_reads.load(Ordering::Relaxed)
    }
}

struct Window {
    reader: OperatorRangeReader,
    /// Logical offset of `data[0]`.
    start: u64,
    data: Buffer,
    /// Logical range hinted to the reader but not consumed yet.
    hinted: Option<Range<u64>>,
}

/// Tantivy file handle over one logical file stored at `base..base + len` of an object,
/// for readers that move forward through the file: term dictionaries, postings and positions
/// during a merge. It keeps one window of the file in memory and hints the next one, so memory
/// stays at two windows regardless of file size. Reads behind the window still work but are
/// counted; a merge is expected to produce none.
pub struct SequentialFileHandle {
    path: PathBuf,
    base: u64,
    len: u64,
    window_size: u64,
    window: Mutex<Window>,
    stats: Arc<SequentialReadStats>,
}

impl fmt::Debug for SequentialFileHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter
            .debug_struct("SequentialFileHandle")
            .field("path", &self.path)
            .field("len", &self.len)
            .finish_non_exhaustive()
    }
}

impl SequentialFileHandle {
    pub fn new(
        operator: Operator,
        object: String,
        path: PathBuf,
        base: u64,
        len: u64,
        window_size: u64,
    ) -> Self {
        let reader = OperatorRangeReader::new(operator, object, 2);
        Self {
            path,
            base,
            len,
            window_size: window_size.max(1),
            window: Mutex::new(Window {
                reader,
                start: 0,
                data: Buffer::new(),
                hinted: None,
            }),
            stats: Arc::new(SequentialReadStats::default()),
        }
    }

    pub fn stats(&self) -> Arc<SequentialReadStats> {
        self.stats.clone()
    }

    /// Fetches the logical range `start..end` into the window, consuming the hint when it
    /// matches and dropping it otherwise.
    fn fetch(&self, window: &mut Window, range: Range<u64>) -> io::Result<()> {
        if let Some(hinted) = window.hinted.take() {
            if hinted != range {
                window.reader.discard(self.physical(&hinted));
            }
        }
        let data = RangeReader::read(&mut window.reader, self.physical(&range))
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
        window.start = range.start;
        window.data = data;

        let next = range.end..(range.end + self.window_size).min(self.len);
        if !next.is_empty() && window.reader.prefetch(&[self.physical(&next)]) {
            window.hinted = Some(next);
        }
        Ok(())
    }

    fn physical(&self, logical: &Range<u64>) -> Range<u64> {
        self.base + logical.start..self.base + logical.end
    }
}

impl HasLen for SequentialFileHandle {
    fn len(&self) -> usize {
        self.len as usize
    }
}

#[async_trait]
impl FileHandle for SequentialFileHandle {
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

        let mut window = self.window.lock().unwrap();
        let window_end = window.start + window.data.len() as u64;
        if start < window.start {
            self.stats.backward_reads.fetch_add(1, Ordering::Relaxed);
        }
        if start < window.start || end > window_end {
            // The window starts where this read starts so the following reads, which usually
            // continue from here, stay inside it.
            let fetch_end = end.max(start + self.window_size).min(self.len);
            self.fetch(&mut window, start..fetch_end)?;
        }
        let offset = (start - window.start) as usize;
        let bytes = window.data.slice(offset..offset + range.len()).to_bytes();
        Ok(OwnedBytes::new(bytes.to_vec()))
    }
}

#[cfg(test)]
mod tests {
    use databend_common_base::runtime::GlobalIORuntime;
    use opendal::services::Memory;

    use super::*;
    use crate::init_test_runtime;

    fn pattern(len: usize) -> Vec<u8> {
        let mut data = Vec::with_capacity(len);
        for i in 0..len {
            data.push((i % 251) as u8);
        }
        data
    }

    fn handle(data: &[u8], base: usize, len: usize, window: u64) -> SequentialFileHandle {
        init_test_runtime();
        let operator = Operator::new(Memory::default()).unwrap().finish();
        let data = data.to_vec();
        let writer = operator.clone();
        GlobalIORuntime::instance()
            .block_on(async move { Ok(writer.write("object", data).await?) })
            .unwrap();
        SequentialFileHandle::new(
            operator,
            "object".to_string(),
            PathBuf::from("seg.idx"),
            base as u64,
            len as u64,
            window,
        )
    }

    #[test]
    fn test_forward_reads_fetch_one_window_at_a_time() {
        let object = pattern(3000);
        let (base, len) = (500, 2000);
        let file = &object[base..base + len];
        let handle = handle(&object, base, len, 512);

        let mut offset = 0;
        while offset < len {
            let end = (offset + 100).min(len);
            assert_eq!(
                handle.read_bytes(offset..end).unwrap().as_slice(),
                &file[offset..end],
                "{offset}..{end}"
            );
            offset = end;
        }
        let stats = handle.stats();
        assert_eq!(stats.backward_reads(), 0);
        // Windows start at the reads that missed: 0, 500, 1000, 1500; the last is clamped.
        assert_eq!(stats.fetches(), 4);
        assert_eq!(stats.fetched_bytes(), 512 * 3 + 500);
    }

    #[test]
    fn test_read_larger_than_window_is_fetched_whole() {
        let object = pattern(3000);
        let handle = handle(&object, 0, 3000, 256);
        assert_eq!(
            handle.read_bytes(100..2100).unwrap().as_slice(),
            &object[100..2100]
        );
        assert_eq!(handle.stats().fetches(), 1);
        assert_eq!(handle.stats().fetched_bytes(), 2000);
        assert_eq!(
            handle.read_bytes(2000..2100).unwrap().as_slice(),
            &object[2000..2100]
        );
        assert_eq!(handle.stats().fetches(), 1);
    }

    #[test]
    fn test_read_straddling_the_window_end_refetches_from_its_start() {
        let object = pattern(1000);
        let handle = handle(&object, 0, 1000, 256);
        assert_eq!(handle.read_bytes(0..8).unwrap().as_slice(), &object[0..8]);
        assert_eq!(
            handle.read_bytes(250..300).unwrap().as_slice(),
            &object[250..300]
        );
        let stats = handle.stats();
        assert_eq!(stats.fetches(), 2);
        assert_eq!(stats.backward_reads(), 0);
        assert_eq!(stats.fetched_bytes(), 256 + 256);
    }

    #[test]
    fn test_backward_reads_work_but_are_counted() {
        let object = pattern(1000);
        let handle = handle(&object, 0, 1000, 256);
        assert_eq!(
            handle.read_bytes(600..700).unwrap().as_slice(),
            &object[600..700]
        );
        assert_eq!(handle.read_bytes(0..8).unwrap().as_slice(), &object[0..8]);
        assert_eq!(handle.stats().backward_reads(), 1);
        assert_eq!(handle.stats().fetches(), 2);
    }

    #[test]
    fn test_bounds_and_empty_reads() {
        let object = pattern(100);
        let handle = handle(&object, 10, 50, 16);
        assert_eq!(handle.len(), 50);
        assert_eq!(handle.read_bytes(0..0).unwrap().len(), 0);
        assert_eq!(
            handle.read_bytes(40..50).unwrap().as_slice(),
            &object[50..60]
        );
        assert!(handle.read_bytes(45..51).is_err());
        assert_eq!(handle.stats().fetches(), 1);
        assert_eq!(handle.stats().fetched_bytes(), 10);
    }
}
