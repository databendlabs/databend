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
use opendal::Buffer;
use opendal::Operator;

use crate::ReadSettings;
use crate::range_read::RangeReader;

struct FetchedData {
    range: Range<u64>,
    data: Buffer,
    io_time: Duration,
    #[cfg(test)]
    io_thread: std::thread::ThreadId,
}

/// Dumb executor: receive a range, issue one ranged read, echo it back with
/// its identity. Zero range math; stops on the first error or when either
/// channel closes.
async fn remote_fetch_worker(
    op: Operator,
    path: String,
    req_rx: async_channel::Receiver<Range<u64>>,
    data_tx: async_channel::Sender<Result<FetchedData>>,
) {
    let reader = match op.reader(&path).await {
        Ok(reader) => reader,
        Err(error) => {
            let _ = data_tx.send(Err(error.into())).await;
            return;
        }
    };
    while let Ok(range) = req_rx.recv().await {
        let start = Instant::now();
        let result = reader.read(range.start..range.end).await;
        let io_time = start.elapsed();
        let message = match result {
            Ok(data) => {
                let expected = (range.end - range.start) as usize;
                if data.len() == expected {
                    Ok(FetchedData {
                        range,
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
        let failed = message.is_err();
        if data_tx.send(message).await.is_err() || failed {
            return;
        }
    }
}

enum SequentialOutput {
    Empty,
    Slice { task: Range<u64>, sub: Range<usize> },
}

/// The chain tail over an OpenDAL [`Operator`].
///
/// State machine per range key in `prefetch_map`: absent = never dispatched,
/// `None` = in flight, `Some` = arrived and not yet consumed. Pressure is the
/// number of unconsumed entries.
///
/// It also ships a sequential compatibility shell ([`Self::create`] plus the
/// no-argument [`Self::read`]) that preserves the historical batch API: ranges
/// are converged once with [`RangeMerger`] (the shell merges, so the shell
/// keeps the caller-to-task books) and results are returned in caller order.
pub struct OperatorRangeReader {
    req_tx: async_channel::Sender<Range<u64>>,
    data_rx: async_channel::Receiver<Result<FetchedData>>,
    prefetch_map: HashMap<Range<u64>, Option<Buffer>>,
    max_unconsumed: usize,
    poisoned: Option<ErrorCode>,

    // Sequential compatibility shell.
    outputs: VecDeque<SequentialOutput>,
    backlog: VecDeque<Range<u64>>,
    task_uses: HashMap<Range<u64>, usize>,
    task_cache: HashMap<Range<u64>, (Buffer, usize)>,
}

impl OperatorRangeReader {
    /// Create a bare chain tail. `max_unconsumed` bounds in-flight plus
    /// arrived-but-unread ranges; hints beyond it are dropped.
    pub fn new(op: Operator, path: String, max_unconsumed: usize) -> Self {
        let max_unconsumed = max_unconsumed.max(1);
        let (req_tx, req_rx) = async_channel::unbounded();
        let (data_tx, data_rx) = async_channel::bounded(max_unconsumed);
        GlobalIORuntime::instance().spawn(remote_fetch_worker(op, path, req_rx, data_tx));
        Self {
            req_tx,
            data_rx,
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
        metrics_inc_remote_io_seeks_after_merged(1);
        metrics_inc_remote_io_read_bytes_after_merged(range.end - range.start);
        if self.req_tx.try_send(range.clone()).is_err() {
            return false;
        }
        self.prefetch_map.insert(range.clone(), None);
        true
    }

    /// Block until the next fetched range arrives and file it into
    /// `prefetch_map`. Poisons the reader on the first error.
    fn drain_one(&mut self) -> Result<()> {
        match self.data_rx.recv_blocking() {
            Ok(Ok(fetched)) => {
                metrics_inc_remote_io_read_milliseconds(fetched.io_time.as_millis() as u64);
                if let Some(slot) = self.prefetch_map.get_mut(&fetched.range) {
                    *slot = Some(fetched.data);
                }
                Ok(())
            }
            Ok(Err(error)) => {
                self.poisoned = Some(error.clone());
                Err(error)
            }
            Err(_) => {
                let error =
                    ErrorCode::Internal("operator range reader fetch worker exited unexpectedly");
                self.poisoned = Some(error.clone());
                Err(error)
            }
        }
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
            if range.is_empty() || self.prefetch_map.contains_key(range) {
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
        loop {
            match self.prefetch_map.get(&range) {
                Some(Some(_)) => {
                    let Some(Some(data)) = self.prefetch_map.remove(&range) else {
                        unreachable!("checked above")
                    };
                    return Ok(data);
                }
                Some(None) => self.drain_one()?,
                None => unreachable!("dispatched above"),
            }
        }
    }
}

impl Drop for OperatorRangeReader {
    fn drop(&mut self) {
        self.req_tx.close();
        self.data_rx.close();
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    use super::*;
    use crate::init_test_runtime;
    use crate::range_read::test_util::*;

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
        let mut reader = OperatorRangeReader::new(op, "demand".to_string(), 4);

        // Demand read penetrates without any hint.
        assert_eq!(
            RangeReader::read(&mut reader, 2..6)
                .unwrap()
                .to_bytes()
                .as_ref(),
            b"2345"
        );
        // Out-of-order demand reads against earlier hints.
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
    fn test_duplicate_hints_fetch_once() {
        init_test_runtime();
        let accessor = RecordingReadAccessor::new(b"0123456789", false);
        let op = recording_operator(accessor.clone());
        let mut reader = OperatorRangeReader::new(op, "dedup".to_string(), 4);

        assert!(reader.prefetch(&[0..4]));
        assert!(reader.prefetch(&[0..4]));
        assert_eq!(
            RangeReader::read(&mut reader, 0..4)
                .unwrap()
                .to_bytes()
                .as_ref(),
            b"0123"
        );
        assert_eq!(accessor.read_ranges(), vec![0..4]);
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
    fn test_data_channel_stays_bounded_and_drop_cancels() {
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
        assert!(reader.data_rx.len() <= reader.max_unconsumed);
        drop(reader);
    }

    #[test]
    #[allow(clippy::single_range_in_vec_init)]
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
        assert!(!reader.prefetch(&[0..4]));
        let fetched = reader.data_rx.recv_blocking().unwrap().unwrap();
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
