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

//! A chain-of-responsibility for ranged reads: layers implement [`RangeReader`]
//! and decorate the next layer. `prefetch` is a side-effectful hint flowing down
//! the chain, `read` is the demand path that carries the correctness contract.

mod chunk_grid;
mod chunked;
mod disk_cache;
mod operator;

use std::ops::Range;

use databend_common_exception::Result;
use opendal::Buffer;

pub use self::chunk_grid::ChunkGrid;
pub use self::chunked::ChunkedRangeReader;
pub use self::disk_cache::DiskCacheRangeReader;
pub use self::operator::OperatorRangeReader;

/// One link in the ranged-read chain.
///
/// Identity rule: ranges are exact-match keys between adjacent layers. A layer
/// must `read` with the same ranges it forwarded via `prefetch`; deterministic
/// splitting may be recomputed, only merge decisions need bookkeeping.
pub trait RangeReader: Send {
    /// Side-effectful hint: try to load `ranges` ahead of time.
    ///
    /// A saturated layer may silently drop hints; correctness is preserved
    /// because `read` falls through on demand. Returns `false` when the chain
    /// is saturated and the caller should pause feeding until some `read`s
    /// complete. I/O errors raised while hinting are attached to the affected
    /// range and reported by the corresponding `read`. An empty slice is a
    /// legal capacity probe.
    fn prefetch(&mut self, ranges: &[Range<u64>]) -> bool;

    /// Demand read: the only correctness contract.
    ///
    /// Returns immediately when the range is ready, blocks while it is in
    /// flight, and fetches on the spot when it was never hinted (or the hint
    /// was dropped).
    fn read(&mut self, range: Range<u64>) -> Result<Buffer>;
}

#[cfg(test)]
pub(crate) mod test_util {
    use std::ops::Range;
    use std::sync::Arc;
    use std::sync::Mutex;

    use bytes::Bytes;
    use opendal::Buffer;
    use opendal::Operator;
    use opendal::OperatorBuilder;
    use opendal::raw::Access;
    use opendal::raw::AccessorInfo;
    use opendal::raw::OpRead;
    use opendal::raw::RpRead;
    use opendal::services::Memory;

    use crate::ReadSettings;

    /// Records every ranged read; optionally returns one byte short to
    /// simulate a truncated response.
    #[derive(Debug)]
    pub(crate) struct RecordingReadAccessor {
        content: Bytes,
        read_ranges: Mutex<Vec<Range<u64>>>,
        short_response: bool,
    }

    impl RecordingReadAccessor {
        pub(crate) fn new(content: &'static [u8], short_response: bool) -> Arc<Self> {
            Arc::new(Self {
                content: Bytes::from_static(content),
                read_ranges: Mutex::new(Vec::new()),
                short_response,
            })
        }

        pub(crate) fn read_ranges(&self) -> Vec<Range<u64>> {
            self.read_ranges.lock().unwrap().clone()
        }
    }

    impl Access for RecordingReadAccessor {
        type Reader = Buffer;
        type Writer = ();
        type Lister = ();
        type Deleter = ();

        fn info(&self) -> Arc<AccessorInfo> {
            let info = AccessorInfo::default();
            info.set_native_capability(opendal::Capability {
                read: true,
                ..Default::default()
            });
            info.into()
        }

        async fn read(&self, _path: &str, args: OpRead) -> opendal::Result<(RpRead, Self::Reader)> {
            let range = args.range();
            let start = range.offset();
            let requested = range.size().unwrap_or(self.content.len() as u64 - start);
            let end = start + requested;
            self.read_ranges.lock().unwrap().push(start..end);

            let mut actual_end = end.min(self.content.len() as u64);
            if self.short_response && actual_end > start {
                actual_end -= 1;
            }
            let data = self.content.slice(start as usize..actual_end as usize);
            Ok((RpRead::new(), Buffer::from(data)))
        }
    }

    pub(crate) fn recording_operator(accessor: Arc<RecordingReadAccessor>) -> Operator {
        OperatorBuilder::new(accessor).finish()
    }

    pub(crate) fn memory_operator() -> Operator {
        Operator::new(Memory::default()).unwrap().finish()
    }

    pub(crate) fn settings(max_gap_size: u64, max_range_size: u64) -> ReadSettings {
        ReadSettings {
            max_gap_size,
            max_range_size,
            parquet_fast_read_bytes: 0,
        }
    }
}
