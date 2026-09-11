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

//! The tail of the ranged-read chain: forwards ranges verbatim to a dumb fetch
//! worker on the global I/O runtime and matches responses back by identity.
//! No merging, no splitting: request identity == response identity.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::ops::Range;
use std::time::Duration;
use std::time::Instant;

use databend_common_base::rangemap::RangeMerger;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_metrics::storage::*;
use futures::future::AbortHandle;
use futures::future::AbortRegistration;
use futures::future::Abortable;
use opendal::Buffer;
use opendal::Operator;

use crate::ReadSettings;
use crate::range_read::RangeReader;

struct FetchedData {
    data: Buffer,
    io_time: Duration,
    #[cfg(test)]
    io_thread: std::thread::ThreadId,
}

type FetchRequest = (
    Range<u64>,
    async_channel::Sender<Result<FetchedData>>,
    AbortRegistration,
);

enum SequentialOutput {
    Empty,
    Slice { task: Range<u64>, sub: Range<usize> },
}

struct PrefetchSlot {
    data: Option<Buffer>,
    result_rx: async_channel::Receiver<Result<FetchedData>>,
    abort_handle: AbortHandle,
    pending_reads: usize,
}

/// The chain tail over an OpenDAL [`Operator`].
///
/// State machine per range key in `prefetch_map`: absent = never dispatched,
/// data `None` = in flight, data `Some` = arrived and not yet fully read.
/// Pressure is the number of unconsumed entries; repeated hints of one range
/// add no pressure.
///
/// It also ships a sequential compatibility shell ([`Self::create`] plus the
/// no-argument [`Self::read`]) that preserves the historical batch API: ranges
/// are converged once with [`RangeMerger`] (the shell merges, so the shell
/// keeps the caller-to-task books) and results are returned in caller order.
pub struct OperatorRangeReader {
    req_tx: async_channel::Sender<FetchRequest>,
    prefetch_map: HashMap<Range<u64>, PrefetchSlot>,
    max_unconsumed: usize,
    poisoned: Option<ErrorCode>,

    // Sequential compatibility shell.
    outputs: VecDeque<SequentialOutput>,
    backlog: VecDeque<Range<u64>>,
    task_uses: HashMap<Range<u64>, usize>,
    task_cache: HashMap<Range<u64>, (Buffer, usize)>,
}

impl OperatorRangeReader {
    async fn remote_fetch_worker(
        op: Operator,
        path: String,
        req_rx: async_channel::Receiver<FetchRequest>,
    ) {
        let reader = match op.reader(&path).await {
            Ok(reader) => reader,
            Err(error) => {
                let error: ErrorCode = error.into();
                while let Ok((_, result_tx, _)) = req_rx.recv().await {
                    let _ = result_tx.try_send(Err(error.clone()));
                }
                return;
            }
        };

        while let Ok((range, result_tx, abort_registration)) = req_rx.recv().await {
            let start = Instant::now();
            let result =
                Abortable::new(reader.read(range.start..range.end), abort_registration).await;
            let Ok(result) = result else {
                continue;
            };
            let io_time = start.elapsed();
            let message = match result {
                Ok(data) => {
                    let expected = (range.end - range.start) as usize;
                    if data.len() == expected {
                        Ok(FetchedData {
                            data,
                            io_time,
                            #[cfg(test)]
                            io_thread: std::thread::current().id(),
                        })
                    } else {
                        Err(ErrorCode::StorageOther(format!(
                            "OpenDAL read for {path} returned {} bytes, expected {expected} for range {range:?}",
                            data.len()
                        )))
                    }
                }
                Err(error) => Err(error.into()),
            };
            let _ = result_tx.try_send(message);
        }
    }

    /// Create a bare chain tail. `max_unconsumed` bounds in-flight plus
    /// arrived-but-unread ranges; hints beyond it are dropped.
    pub fn new(op: Operator, path: String, max_unconsumed: usize) -> Self {
        let max_unconsumed = max_unconsumed.max(1);
        let (req_tx, req_rx) = async_channel::unbounded();
        drop(GlobalIORuntime::instance().spawn(Self::remote_fetch_worker(op, path, req_rx)));
        Self {
            req_tx,
            prefetch_map: HashMap::new(),
            max_unconsumed,
            poisoned: None,
            outputs: VecDeque::new(),
            backlog: VecDeque::new(),
            task_uses: HashMap::new(),
            task_cache: HashMap::new(),
        }
    }

    /// Compatibility constructor: converge `ranges` once, prefetch ahead, and
    /// serve them in caller order through the no-argument [`Self::read`].
    /// Empty ranges yield empty buffers without I/O.
    pub fn create(
        settings: &ReadSettings,
        op: Operator,
        path: String,
        ranges: &[Range<u64>],
        max_prefetch: usize,
    ) -> Result<Self> {
        let merger = RangeMerger::from_iter(
            ranges.iter().filter(|range| !range.is_empty()).cloned(),
            settings.max_gap_size,
            settings.max_range_size,
        );
        let tasks = merger.ranges();

        let mut outputs = VecDeque::with_capacity(ranges.len());
        let mut task_uses: HashMap<Range<u64>, usize> = HashMap::new();
        for range in ranges {
            if range.is_empty() {
                outputs.push_back(SequentialOutput::Empty);
                continue;
            }
            let (_, task) = merger.get(range.clone()).ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "range {range:?} not found in merged tasks {tasks:?} for {path}"
                ))
            })?;
            let sub = (range.start - task.start) as usize..(range.end - task.start) as usize;
            *task_uses.entry(task.clone()).or_insert(0) += 1;
            outputs.push_back(SequentialOutput::Slice { task, sub });
        }

        // `+ 1` keeps the pipeline depth comparable with the previous
        // implementation, where the bounded data channel held `max_prefetch`
        // completed reads while the worker was producing the next one.
        let mut reader = Self::new(op, path, max_prefetch.max(1) + 1);
        reader.outputs = outputs;
        reader.task_uses = task_uses;
        reader.backlog = tasks.into();
        reader.pump();
        Ok(reader)
    }

    /// Sequential compatibility read: next range in caller order.
    pub fn read(&mut self) -> Result<Buffer> {
        let output = self
            .outputs
            .pop_front()
            .ok_or_else(|| ErrorCode::Internal("operator range reader has no remaining ranges"))?;
        let (task, sub) = match output {
            SequentialOutput::Empty => return Ok(Buffer::new()),
            SequentialOutput::Slice { task, sub } => (task, sub),
        };

        let buffer = match self.task_cache.get_mut(&task) {
            Some((buffer, uses)) => {
                let buffer = buffer.clone();
                *uses -= 1;
                if *uses == 0 {
                    self.task_cache.remove(&task);
                }
                buffer
            }
            None => {
                // The task may still sit in the backlog when its hint was
                // deferred or dropped; remove it so pump never re-dispatches
                // a consumed task.
                if !self.prefetch_map.contains_key(&task)
                    && let Some(position) = self.backlog.iter().position(|t| *t == task)
                {
                    self.backlog.remove(position);
                }
                let buffer = RangeReader::read(self, task.clone())?;
                let remaining = self.task_uses.get(&task).copied().unwrap_or(1) - 1;
                if remaining > 0 {
                    self.task_cache
                        .insert(task.clone(), (buffer.clone(), remaining));
                }
                buffer
            }
        };

        self.pump();
        Ok(buffer.slice(sub))
    }

    /// Feed pending tasks into the pipeline until it reports saturation.
    fn pump(&mut self) {
        if self.poisoned.is_some() {
            return;
        }
        while let Some(task) = self.backlog.front().cloned() {
            if self.prefetch_map.contains_key(&task) {
                self.backlog.pop_front();
                continue;
            }
            if self.prefetch_map.len() >= self.max_unconsumed || !self.dispatch(&task) {
                break;
            }
            self.backlog.pop_front();
        }
    }

    /// Send one range to the fetch worker and account for it.
    /// Returns false when the worker is gone.
    fn dispatch(&mut self, range: &Range<u64>) -> bool {
        let (result_tx, result_rx) = async_channel::bounded(1);
        let (abort_handle, abort_registration) = AbortHandle::new_pair();
        let request = (range.clone(), result_tx, abort_registration);
        if self.req_tx.try_send(request).is_err() {
            return false;
        }

        metrics_inc_remote_io_seeks_after_merged(1);
        metrics_inc_remote_io_read_bytes_after_merged(range.end - range.start);
        self.prefetch_map.insert(range.clone(), PrefetchSlot {
            data: None,
            result_rx,
            abort_handle,
            pending_reads: 1,
        });
        true
    }

    #[cfg(test)]
    fn unconsumed(&self) -> usize {
        self.prefetch_map.len()
    }
}

impl RangeReader for OperatorRangeReader {
    fn prefetch(&mut self, ranges: &[Range<u64>]) -> bool {
        if self.poisoned.is_some() {
            return false;
        }
        for range in ranges {
            if range.is_empty() {
                continue;
            }
            if let Some(slot) = self.prefetch_map.get_mut(range) {
                slot.pending_reads += 1;
                continue;
            }
            if self.prefetch_map.len() >= self.max_unconsumed || !self.dispatch(range) {
                return false;
            }
        }
        self.prefetch_map.len() < self.max_unconsumed
    }

    fn read(&mut self, range: Range<u64>) -> Result<Buffer> {
        if let Some(error) = &self.poisoned {
            return Err(error.clone());
        }
        if range.is_empty() {
            return Ok(Buffer::new());
        }
        if !self.prefetch_map.contains_key(&range) {
            // Never hinted, or the hint was dropped: fetch on the spot.
            if !self.dispatch(&range) {
                let error =
                    ErrorCode::Internal("operator range reader fetch worker exited unexpectedly");
                self.poisoned = Some(error.clone());
                return Err(error);
            }
        }

        if self
            .prefetch_map
            .get(&range)
            .is_some_and(|slot| slot.data.is_none())
        {
            let result_rx = self
                .prefetch_map
                .get(&range)
                .expect("operator range was dispatched above")
                .result_rx
                .clone();
            match result_rx.recv_blocking() {
                Ok(Ok(fetched)) => {
                    metrics_inc_remote_io_read_milliseconds(fetched.io_time.as_millis() as u64);
                    self.prefetch_map
                        .get_mut(&range)
                        .expect("operator range cannot retire during read")
                        .data = Some(fetched.data);
                }
                Ok(Err(error)) => {
                    self.prefetch_map.remove(&range);
                    self.poisoned = Some(error.clone());
                    return Err(error);
                }
                Err(_) => {
                    self.prefetch_map.remove(&range);
                    let error =
                        ErrorCode::Internal("operator range reader fetch task exited unexpectedly");
                    self.poisoned = Some(error.clone());
                    return Err(error);
                }
            }
        }

        let slot = self
            .prefetch_map
            .get_mut(&range)
            .expect("operator range was dispatched above");
        slot.pending_reads -= 1;
        if slot.pending_reads > 0 {
            return Ok(slot
                .data
                .as_ref()
                .expect("completed operator range has data")
                .clone());
        }

        let slot = self
            .prefetch_map
            .remove(&range)
            .expect("last operator range use owns its slot");
        Ok(slot.data.expect("completed operator range has data"))
    }

    fn discard(&mut self, range: Range<u64>) {
        let Some(slot) = self.prefetch_map.get_mut(&range) else {
            return;
        };
        slot.pending_reads -= 1;
        if slot.pending_reads != 0 {
            return;
        }

        slot.abort_handle.abort();
        self.prefetch_map.remove(&range);
    }
}

impl Drop for OperatorRangeReader {
    fn drop(&mut self) {
        for slot in self.prefetch_map.values() {
            slot.abort_handle.abort();
        }
        self.req_tx.close();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::thread;
    use std::time::Duration;

    use bytes::Bytes;
    use databend_common_base::runtime::Thread;
    use opendal::OperatorBuilder;
    use opendal::raw::Access;
    use opendal::raw::AccessorInfo;
    use opendal::raw::OpRead;
    use opendal::raw::RpRead;

    use super::*;
    use crate::init_test_runtime;
    use crate::range_read::test_util::*;

    #[derive(Debug)]
    struct BlockingFirstReadAccessor {
        content: Bytes,
        calls: AtomicUsize,
    }

    impl BlockingFirstReadAccessor {
        fn new(content: &'static [u8]) -> Self {
            Self {
                content: Bytes::from_static(content),
                calls: AtomicUsize::new(0),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::Acquire)
        }
    }

    impl Access for BlockingFirstReadAccessor {
        type Reader = Buffer;
        type Writer = ();
        type Lister = ();
        type Deleter = ();

        fn info(&self) -> std::sync::Arc<AccessorInfo> {
            let info = AccessorInfo::default();
            info.set_native_capability(opendal::Capability {
                read: true,
                ..Default::default()
            });
            info.into()
        }

        async fn read(&self, _path: &str, args: OpRead) -> opendal::Result<(RpRead, Self::Reader)> {
            if self.calls.fetch_add(1, Ordering::AcqRel) == 0 {
                futures::future::pending::<()>().await;
            }
            let range = args.range();
            let start = range.offset() as usize;
            let end = start + range.size().unwrap() as usize;
            Ok((RpRead::new(), Buffer::from(self.content.slice(start..end))))
        }
    }

    #[test]
    fn test_discard_does_not_block_same_range_redispatch() {
        init_test_runtime();
        let accessor = std::sync::Arc::new(BlockingFirstReadAccessor::new(b"data"));
        let op = OperatorBuilder::new(accessor.clone()).finish();
        let mut reader = OperatorRangeReader::new(op, "cancel".to_string(), 1);

        let range = 0..4;
        assert!(!reader.prefetch(std::slice::from_ref(&range)));
        let deadline = Instant::now() + Duration::from_secs(5);
        while accessor.calls() == 0 {
            assert!(Instant::now() < deadline, "prefetch did not start");
            thread::yield_now();
        }

        let (discarded_tx, discarded_rx) = std::sync::mpsc::sync_channel(1);
        let discarded_range = range.clone();
        let discard_thread = Thread::spawn(move || {
            reader.discard(discarded_range);
            discarded_tx.send(reader).unwrap();
        });
        let mut reader = discarded_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("discard waited for storage I/O");
        discard_thread.join().unwrap();
        assert_eq!(reader.unconsumed(), 0);

        assert!(!reader.prefetch(std::slice::from_ref(&range)));
        assert_eq!(
            RangeReader::read(&mut reader, range)
                .unwrap()
                .to_bytes()
                .as_ref(),
            b"data"
        );
        assert_eq!(accessor.calls(), 2);
    }

    #[test]
    fn test_discard_retires_one_duplicate_hint() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(b"data", false);
        let mut reader = OperatorRangeReader::new(
            recording_operator(accessor.clone()),
            "duplicate-discard".to_string(),
            1,
        );

        let range = 0..4;
        assert!(!reader.prefetch(std::slice::from_ref(&range)));
        assert!(!reader.prefetch(std::slice::from_ref(&range)));
        reader.discard(range.clone());
        assert_eq!(reader.unconsumed(), 1);
        assert_eq!(
            RangeReader::read(&mut reader, range)
                .unwrap()
                .to_bytes()
                .as_ref(),
            b"data"
        );
        assert_eq!(accessor.read_ranges(), vec![0..4]);
        assert_eq!(reader.unconsumed(), 0);
    }

    #[test]
    fn test_sequential_roundtrip_with_unordered_and_empty_ranges() {
        init_test_runtime();
        let op = memory_operator();
        let path = "roundtrip".to_string();
        GlobalIORuntime::instance()
            .block_on(async {
                op.write(&path, b"0123456789abcdef".to_vec())
                    .await
                    .map_err(ErrorCode::from)
            })
            .unwrap();

        let consumer_thread = thread::current().id();
        let byte_ranges = [12..16, 4..4, 0..4, 6..10];
        let mut reader =
            OperatorRangeReader::create(&settings(1, 1024), op, path, &byte_ranges, 1).unwrap();
        let result = byte_ranges
            .iter()
            .map(|_| reader.read().unwrap().to_bytes())
            .collect::<Vec<_>>();
        assert!(reader.read().is_err());
        assert_eq!(thread::current().id(), consumer_thread);
        assert_eq!(result[0].as_ref(), b"cdef");
        assert!(result[1].is_empty());
        assert_eq!(result[2].as_ref(), b"0123");
        assert_eq!(result[3].as_ref(), b"6789");
    }

    #[test]
    fn test_one_task_serves_multiple_caller_ranges() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(b"0123456789abcdef", false);
        let op = recording_operator(accessor.clone());
        let mut reader = OperatorRangeReader::create(
            &settings(16, 1024),
            op,
            "merged".to_string(),
            &[0..4, 6..10, 12..16],
            1,
        )
        .unwrap();

        assert_eq!(reader.read().unwrap().to_bytes().as_ref(), b"0123");
        assert_eq!(reader.read().unwrap().to_bytes().as_ref(), b"6789");
        assert_eq!(reader.read().unwrap().to_bytes().as_ref(), b"cdef");
        // One merged task, one request.
        assert_eq!(accessor.read_ranges(), vec![0..16]);
    }

    #[test]
    fn test_adjacent_tasks_fetch_separately() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(b"0123456789", false);
        let op = recording_operator(accessor.clone());
        let mut reader = OperatorRangeReader::create(
            &settings(0, 4),
            op,
            "adjacent".to_string(),
            &[0..4, 4..8, 8..10],
            3,
        )
        .unwrap();

        assert_eq!(reader.read().unwrap().to_bytes().as_ref(), b"0123");
        assert_eq!(reader.read().unwrap().to_bytes().as_ref(), b"4567");
        assert_eq!(reader.read().unwrap().to_bytes().as_ref(), b"89");
        assert_eq!(accessor.read_ranges(), vec![0..4, 4..8, 8..10]);
    }

    #[test]
    fn test_gap_separated_tasks_fetch_separately() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(b"0123456789", false);
        let op = recording_operator(accessor.clone());
        let mut reader =
            OperatorRangeReader::create(&settings(0, 4), op, "gaps".to_string(), &[0..4, 6..10], 2)
                .unwrap();

        assert_eq!(reader.read().unwrap().to_bytes().as_ref(), b"0123");
        assert_eq!(reader.read().unwrap().to_bytes().as_ref(), b"6789");
        assert_eq!(accessor.read_ranges(), vec![0..4, 6..10]);
    }

    #[test]
    fn test_short_read_poisons_reader() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(b"01234567", true);
        let op = recording_operator(accessor.clone());
        let mut reader =
            OperatorRangeReader::create(&settings(0, 4), op, "short".to_string(), &[0..4, 4..8], 2)
                .unwrap();

        let error = reader.read().unwrap_err();
        assert!(
            error.message().contains("too little data") || error.message().contains("expected"),
            "unexpected short-read error: {error:?}"
        );
        // Poisoned: every subsequent read fails as well.
        assert!(reader.read().is_err());
    }

    #[test]
    #[allow(clippy::single_range_in_vec_init)]
    fn test_missing_path_error_propagates() {
        init_test_runtime();
        let mut reader = OperatorRangeReader::create(
            &settings(16, 1024),
            memory_operator(),
            "missing".to_string(),
            &[0..1],
            1,
        )
        .unwrap();
        assert!(reader.read().is_err());
    }

    #[test]
    fn test_trait_read_without_prefetch() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(b"0123456789", false);
        let op = recording_operator(accessor.clone());
        let mut reader = OperatorRangeReader::new(op, "spot".to_string(), 4);

        // An unhinted read penetrates on the spot.
        assert_eq!(
            RangeReader::read(&mut reader, 2..6)
                .unwrap()
                .to_bytes()
                .as_ref(),
            b"2345"
        );
        // Out-of-order reads against earlier hints.
        assert!(reader.prefetch(&[6..8, 0..2]));
        assert_eq!(
            RangeReader::read(&mut reader, 0..2)
                .unwrap()
                .to_bytes()
                .as_ref(),
            b"01"
        );
        assert_eq!(
            RangeReader::read(&mut reader, 6..8)
                .unwrap()
                .to_bytes()
                .as_ref(),
            b"67"
        );
        assert_eq!(accessor.read_ranges(), vec![2..6, 6..8, 0..2]);
    }

    #[test]
    #[allow(clippy::single_range_in_vec_init)]
    fn test_double_hint_serves_double_read() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(b"0123456789", false);
        let op = recording_operator(accessor.clone());
        let mut reader = OperatorRangeReader::new(op, "twice".to_string(), 4);

        assert!(reader.prefetch(&[0..4]));
        assert!(reader.prefetch(&[0..4]));
        for _ in 0..2 {
            assert_eq!(
                RangeReader::read(&mut reader, 0..4)
                    .unwrap()
                    .to_bytes()
                    .as_ref(),
                b"0123"
            );
        }
        // One fetch served both hinted reads; the slot is gone afterwards.
        assert_eq!(accessor.read_ranges(), vec![0..4]);
        assert_eq!(reader.unconsumed(), 0);
    }

    #[test]
    fn test_saturation_drops_hints_and_read_penetrates() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(b"0123456789", false);
        let op = recording_operator(accessor.clone());
        let mut reader = OperatorRangeReader::new(op, "saturated".to_string(), 2);

        // Third hint exceeds max_unconsumed and is dropped.
        assert!(!reader.prefetch(&[0..2, 2..4, 4..6]));
        assert_eq!(reader.unconsumed(), 2);
        // Dropped hint is still readable on demand.
        assert_eq!(
            RangeReader::read(&mut reader, 4..6)
                .unwrap()
                .to_bytes()
                .as_ref(),
            b"45"
        );
        assert_eq!(
            RangeReader::read(&mut reader, 0..2)
                .unwrap()
                .to_bytes()
                .as_ref(),
            b"01"
        );
        assert_eq!(
            RangeReader::read(&mut reader, 2..4)
                .unwrap()
                .to_bytes()
                .as_ref(),
            b"23"
        );
    }

    #[test]
    fn test_empty_prefetch_is_capacity_probe() {
        init_test_runtime();
        let op = memory_operator();
        let mut reader = OperatorRangeReader::new(op, "probe".to_string(), 1);
        assert!(reader.prefetch(&[]));
    }

    #[test]
    fn test_slots_stay_bounded_and_drop_cancels() {
        init_test_runtime();
        let op = memory_operator();
        let path = "bounded".to_string();
        GlobalIORuntime::instance()
            .block_on(async {
                op.write(&path, vec![0_u8; 64])
                    .await
                    .map_err(ErrorCode::from)
            })
            .unwrap();
        let ranges = (0..8).map(|i| i * 8..(i + 1) * 8).collect::<Vec<_>>();
        let reader = OperatorRangeReader::create(&settings(0, 8), op, path, &ranges, 1).unwrap();
        thread::sleep(Duration::from_millis(20));
        assert!(reader.prefetch_map.len() <= reader.max_unconsumed);
        assert!(
            reader
                .prefetch_map
                .values()
                .all(|slot| slot.result_rx.len() <= 1)
        );
        drop(reader);
    }

    #[test]
    fn test_io_and_consumer_run_on_different_threads() {
        init_test_runtime();
        let op = memory_operator();
        let path = "threads".to_string();
        GlobalIORuntime::instance()
            .block_on(async {
                op.write(&path, b"data".to_vec())
                    .await
                    .map_err(ErrorCode::from)
            })
            .unwrap();
        let consumer = thread::current().id();
        let mut reader = OperatorRangeReader::new(op, path, 1);
        // The hint is accepted and exactly fills the capacity: saturated now.
        let range = 0..4;
        assert!(!reader.prefetch(std::slice::from_ref(&range)));
        let fetched = reader
            .prefetch_map
            .get(&range)
            .unwrap()
            .result_rx
            .recv_blocking()
            .unwrap()
            .unwrap();
        assert_ne!(fetched.io_thread, consumer);
    }

    #[test]
    fn test_all_empty_ranges_do_no_io() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(b"0123456789", false);
        let op = recording_operator(accessor.clone());
        let mut reader = OperatorRangeReader::create(
            &settings(16, 1024),
            op,
            "empty".to_string(),
            &[3..3, 7..7],
            1,
        )
        .unwrap();
        assert!(reader.read().unwrap().is_empty());
        assert!(reader.read().unwrap().is_empty());
        assert!(reader.read().is_err());
        assert!(accessor.read_ranges().is_empty());
    }
}
