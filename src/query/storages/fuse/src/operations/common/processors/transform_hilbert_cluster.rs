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

use std::any::Any;
use std::array::from_fn;
use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::sampler::FixedSizeSampler;
use databend_common_expression::types::UInt64Type;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_pipeline_transforms::traits::DataBlockSpill;
use databend_common_pipeline_transforms::traits::Location;
use databend_storages_common_table_meta::table::HILBERT_CLUSTER_DIMENSIONS;
use rand::SeedableRng;
use rand::rngs::SmallRng;

use super::hilbert_range_exchange::HilbertRangeExchange;
use super::hilbert_range_exchange::HilbertSample;
use super::hilbert_range_exchange::mix64;

enum HilbertWorkerPhase {
    Sampling(FixedSizeSampler<HilbertSample, SmallRng>),
    WaitSketches,
    Resample {
        cursor: usize,
        sampler: FixedSizeSampler<HilbertSample, SmallRng>,
    },
    WaitPlan,
    Replay {
        next_salt_row: Option<u64>,
        output_data: Option<DataBlock>,
    },
}

impl HilbertWorkerPhase {
    fn take_sampler(&mut self, next: Self) -> Result<FixedSizeSampler<HilbertSample, SmallRng>> {
        let previous = std::mem::replace(self, next);
        match previous {
            Self::Sampling(sampler) | Self::Resample { sampler, .. } => Ok(sampler),
            previous => {
                *self = previous;
                Err(ErrorCode::Internal(
                    "Hilbert processor tried to take a sampler outside a sampling phase",
                ))
            }
        }
    }
}

/// One buffered input chunk, either resident in memory or spilled to storage.
enum PendingChunk {
    Memory(DataBlock),
    Spilled(Location),
}

/// Maintains the original block order across in-memory and spilled chunks.
///
/// Chunks are spilled oldest-first when the standard query/global/workload memory policy requests
/// spilling and enough resident input has accumulated to form one spill unit. Each batch merges
/// consecutive leading memory chunks until their uncompressed size reaches that unit; a single
/// input block may exceed the target.
struct SequentialSpillBuffer<S: DataBlockSpill> {
    spiller: S,
    chunks: VecDeque<PendingChunk>,
    memory_bytes: usize,
    memory_settings: MemorySettings,
}

impl<S: DataBlockSpill> SequentialSpillBuffer<S> {
    fn new(spiller: S, memory_settings: MemorySettings) -> Self {
        Self {
            spiller,
            chunks: VecDeque::new(),
            memory_bytes: 0,
            memory_settings,
        }
    }

    fn push(&mut self, block: DataBlock) {
        self.memory_bytes += block.memory_size();
        self.chunks.push_back(PendingChunk::Memory(block));
    }

    /// Gather consecutive resident chunks into one spill batch. Chunks spill oldest-first, and
    /// every spilled chunk is recorded at the position its memory chunks occupied, so the chunk
    /// sequence always reflects the original input order.
    fn take_spill_batch(&mut self) -> Option<(usize, Vec<DataBlock>)> {
        let spill_unit_size = self.memory_settings.spill_unit_size.max(1);
        if self.memory_bytes < spill_unit_size || !self.memory_settings.check_spill() {
            return None;
        }
        let start = self
            .chunks
            .iter()
            .position(|chunk| matches!(chunk, PendingChunk::Memory(_)))?;
        let mut batch = Vec::new();
        let mut bytes = 0usize;
        while self
            .chunks
            .get(start)
            .is_some_and(|chunk| matches!(chunk, PendingChunk::Memory(_)))
        {
            let Some(PendingChunk::Memory(block)) = self.chunks.remove(start) else {
                break;
            };
            bytes += block.memory_size();
            batch.push(block);
            if bytes >= spill_unit_size {
                break;
            }
        }
        Some((start, batch))
    }

    /// Remove the next chunk for replay. Resident bytes stop counting as buffered input once
    /// ownership moves to the downstream output slot.
    fn pop_replay_chunk(&mut self) -> Option<PendingChunk> {
        let chunk = self.chunks.pop_front()?;
        if let PendingChunk::Memory(block) = &chunk {
            self.memory_bytes = self.memory_bytes.saturating_sub(block.memory_size());
        }
        Some(chunk)
    }
}

/// A single in-flight async spill/restore operation.
enum PendingIo {
    Spill(usize, Vec<DataBlock>),
    Restore(Location),
}

/// Samples one input stream, waits for all streams, then replays its buffered blocks.
/// Buffered blocks follow the standard memory-pressure spill policy; sampling is value-based so
/// spilling never changes the resulting sketch, and replay always follows the original input
/// order so routing salts stay deterministic.
pub struct TransformHilbertCluster<S: DataBlockSpill> {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    exchange: Arc<HilbertRangeExchange>,
    worker_id: usize,
    buffer: SequentialSpillBuffer<S>,
    io: Option<PendingIo>,
    phase: HilbertWorkerPhase,
}

impl<S: DataBlockSpill> TransformHilbertCluster<S> {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        exchange: Arc<HilbertRangeExchange>,
        worker_id: usize,
        spiller: S,
        memory_settings: MemorySettings,
    ) -> Box<dyn Processor> {
        Box::new(Self {
            input,
            output,
            buffer: SequentialSpillBuffer::new(spiller, memory_settings),
            io: None,
            phase: HilbertWorkerPhase::Sampling(FixedSizeSampler::new(
                exchange.local_sample_size(),
                SmallRng::seed_from_u64(mix64(worker_id as u64)),
            )),
            worker_id,
            exchange,
        })
    }
}

/// Attach deterministic routing salt when the plan contains hot keys.
fn prepare_replay_block(
    mut block: DataBlock,
    worker_id: usize,
    next_salt_row: &mut Option<u64>,
) -> DataBlock {
    if let Some(next_row) = next_salt_row {
        let worker = (worker_id as u64) << 48;
        block.add_column(UInt64Type::from_data(
            (0..block.num_rows())
                .map(|row| mix64(worker ^ (*next_row + row as u64)))
                .collect(),
        ));
        *next_row += block.num_rows() as u64;
    }
    block
}

fn sample_block(
    sampler: &mut FixedSizeSampler<HilbertSample, SmallRng>,
    dimension_offsets: [usize; HILBERT_CLUSTER_DIMENSIONS],
    block: &DataBlock,
) -> Result<()> {
    let dimensions = dimension_offsets.map(|offset| {
        block.columns().get(offset).ok_or_else(|| {
            ErrorCode::Internal(format!(
                "Hilbert dimension offset {offset} is outside the input block"
            ))
        })
    });
    let [x, y] = dimensions;
    let dimensions = [x?, y?];
    if dimensions
        .iter()
        .any(|dimension| dimension.len() != block.num_rows())
    {
        return Err(ErrorCode::Internal(
            "Hilbert dimension column length does not match the input block",
        ));
    }
    if sampler.rows_seen().checked_add(block.num_rows()).is_none() {
        return Err(ErrorCode::Internal("Hilbert sampled row count overflowed"));
    }

    let mut missing_row = false;
    sampler.add_block(block.num_rows(), |row| {
        from_fn(|dimension| match dimensions[dimension].index(row) {
            Some(value) => value.to_owned(),
            None => {
                missing_row = true;
                Scalar::Null
            }
        })
    });
    if missing_row {
        return Err(ErrorCode::Internal(
            "Hilbert sample row exceeded the input column length",
        ));
    }
    Ok(())
}

#[async_trait::async_trait]
impl<S: DataBlockSpill> Processor for TransformHilbertCluster<S> {
    fn name(&self) -> String {
        "TransformHilbertCluster".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.exchange.cancel_before_plan();
            self.input.finish();
            return Ok(Event::Finished);
        }
        // A peer can fail between any two local events, including after this worker completed I/O
        // or prepared replay output. Observe the shared terminal state before exposing more data.
        self.exchange.check_error()?;
        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }
        // An async spill/restore is in flight; drain it before advancing the phase.
        if self.io.is_some() {
            self.input.set_not_need_data();
            return Ok(Event::Async);
        }
        match &mut self.phase {
            HilbertWorkerPhase::Sampling(sampler) => {
                // Finish reducing an oversized resident buffer before accepting more input.
                if let Some((index, blocks)) = self.buffer.take_spill_batch() {
                    self.io = Some(PendingIo::Spill(index, blocks));
                    return Ok(Event::Async);
                }
                if self.input.has_data() {
                    let Some(block) = self.input.pull_data() else {
                        let error = ErrorCode::Internal(
                            "Hilbert input reported data but no block was available",
                        );
                        self.exchange.fail(error);
                        return self.exchange.check_error().map(|_| Event::Async);
                    };
                    let block = block.inspect_err(|error| self.exchange.fail(error.clone()))?;
                    if !block.is_empty() {
                        if let Err(error) =
                            sample_block(sampler, self.exchange.dimension_offsets(), &block)
                        {
                            self.exchange.fail(error);
                            return self.exchange.check_error().map(|_| Event::Async);
                        }
                        self.buffer.push(block);
                    }
                    // `pull_data` clears the port's NEED_DATA flag. Re-arm it before returning
                    // `NeedData`, otherwise both this processor and its upstream become idle.
                    self.input.set_need_data();
                    return Ok(Event::NeedData);
                }
                if self.input.is_finished() {
                    let sampler = match self.phase.take_sampler(HilbertWorkerPhase::WaitSketches) {
                        Ok(sampler) => sampler,
                        Err(error) => {
                            self.exchange.fail(error);
                            return self.exchange.check_error().map(|_| Event::Async);
                        }
                    };
                    let rows = sampler.rows_seen();
                    self.exchange
                        .submit_initial(self.worker_id, rows, sampler.into_samples());
                    return Ok(Event::Async);
                }
                self.input.set_need_data();
                Ok(Event::NeedData)
            }
            HilbertWorkerPhase::WaitSketches => Ok(Event::Async),
            HilbertWorkerPhase::WaitPlan => {
                if self.exchange.should_build_plan() {
                    Ok(Event::Sync)
                } else {
                    Ok(Event::Async)
                }
            }
            HilbertWorkerPhase::Resample { cursor, sampler } => {
                // Walk chunks in order; spilled chunks are restored one by one.
                loop {
                    let next = match self.buffer.chunks.get(*cursor) {
                        Some(PendingChunk::Memory(block)) => {
                            if let Err(error) =
                                sample_block(sampler, self.exchange.dimension_offsets(), block)
                            {
                                self.exchange.fail(error);
                                return self.exchange.check_error().map(|_| Event::Async);
                            }
                            *cursor += 1;
                            continue;
                        }
                        Some(PendingChunk::Spilled(location)) => Some(location.clone()),
                        None => None,
                    };
                    if let Some(location) = next {
                        self.io = Some(PendingIo::Restore(location));
                        return Ok(Event::Async);
                    }
                    break;
                }
                let sampler = match self.phase.take_sampler(HilbertWorkerPhase::WaitPlan) {
                    Ok(sampler) => sampler,
                    Err(error) => {
                        self.exchange.fail(error);
                        return self.exchange.check_error().map(|_| Event::Async);
                    }
                };
                let rows = sampler.rows_seen();
                if let Err(error) =
                    self.exchange
                        .complete_resample(self.worker_id, rows, sampler.into_samples())
                {
                    self.exchange.fail(error);
                    return self.exchange.check_error().map(|_| Event::Async);
                }
                if self.exchange.should_build_plan() {
                    Ok(Event::Sync)
                } else {
                    Ok(Event::Async)
                }
            }
            HilbertWorkerPhase::Replay {
                next_salt_row,
                output_data,
            } => {
                if let Some(block) = output_data.take() {
                    self.output.push_data(Ok(block));
                    return Ok(Event::NeedConsume);
                }
                if let Some(next) = self.buffer.pop_replay_chunk() {
                    match next {
                        PendingChunk::Memory(block) => {
                            let block = prepare_replay_block(block, self.worker_id, next_salt_row);
                            self.output.push_data(Ok(block));
                            return Ok(Event::NeedConsume);
                        }
                        PendingChunk::Spilled(location) => {
                            self.io = Some(PendingIo::Restore(location));
                            return Ok(Event::Async);
                        }
                    }
                }
                self.output.finish();
                Ok(Event::Finished)
            }
        }
    }

    fn process(&mut self) -> Result<()> {
        if matches!(&self.phase, HilbertWorkerPhase::WaitPlan) {
            self.exchange.publish_plan();
        }
        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        // A task-wide failure (e.g. another worker's spill error) wakes every waiter.
        self.exchange.check_error()?;
        if let Some(io) = self.io.take() {
            match io {
                PendingIo::Spill(index, blocks) => {
                    let bytes = blocks.iter().map(DataBlock::memory_size).sum();
                    let location = match self.buffer.spiller.merge_and_spill(blocks).await {
                        Ok(location) => location,
                        Err(error) => {
                            let error = ErrorCode::from_string(format!(
                                "Hilbert recluster failed to spill buffered input: {error}"
                            ));
                            self.exchange.fail(error);
                            // `fail` preserves the first task-wide error, which may have been
                            // published by a peer while this spill was in flight.
                            return self.exchange.check_error();
                        }
                    };
                    // Another worker may have failed while this I/O was in flight. Do not advance
                    // local state after the task has entered its terminal failure state.
                    self.exchange.check_error()?;
                    // Resident memory drops once the batch is durable. The merged spill occupies
                    // the original batch position, preserving input order for resample and replay.
                    self.buffer.memory_bytes = self.buffer.memory_bytes.saturating_sub(bytes);
                    self.buffer
                        .chunks
                        .insert(index, PendingChunk::Spilled(location));
                }
                PendingIo::Restore(location) => {
                    let block = match self.buffer.spiller.restore(&location).await {
                        Ok(block) => block,
                        Err(error) => {
                            self.exchange.fail(error);
                            // Return the shared first error if a peer failed during restore.
                            return self.exchange.check_error();
                        }
                    };
                    // A peer may fail while restore is in flight. In particular, replay must not
                    // expose a restored block after the shared task has already failed.
                    self.exchange.check_error()?;
                    match &mut self.phase {
                        HilbertWorkerPhase::Resample { cursor, sampler } => {
                            if let Err(error) =
                                sample_block(sampler, self.exchange.dimension_offsets(), &block)
                            {
                                self.exchange.fail(error);
                                return self.exchange.check_error();
                            }
                            *cursor += 1;
                        }
                        HilbertWorkerPhase::Replay {
                            next_salt_row,
                            output_data,
                        } => {
                            *output_data =
                                Some(prepare_replay_block(block, self.worker_id, next_salt_row));
                        }
                        _ => {
                            let error = ErrorCode::Internal(
                                "Hilbert restore completed outside restore phase",
                            );
                            self.exchange.fail(error);
                            return self.exchange.check_error();
                        }
                    }
                }
            }
            return Ok(());
        }
        match &self.phase {
            HilbertWorkerPhase::WaitSketches => {
                self.exchange.wait_sketches().await?;
                let resample_request = match self.exchange.resample_request(self.worker_id) {
                    Ok(request) => request,
                    Err(error) => {
                        self.exchange.fail(error);
                        return self.exchange.check_error();
                    }
                };
                self.phase = if let Some(sample_size) = resample_request {
                    HilbertWorkerPhase::Resample {
                        cursor: 0,
                        sampler: FixedSizeSampler::new(
                            sample_size,
                            SmallRng::seed_from_u64(mix64(self.worker_id as u64 ^ 0x9e37_79b9)),
                        ),
                    }
                } else {
                    HilbertWorkerPhase::WaitPlan
                };
            }
            HilbertWorkerPhase::WaitPlan => {
                self.phase = HilbertWorkerPhase::Replay {
                    next_salt_row: self.exchange.wait_plan().await?.then_some(0),
                    output_data: None,
                };
            }
            HilbertWorkerPhase::Sampling(_)
            | HilbertWorkerPhase::Resample { .. }
            | HilbertWorkerPhase::Replay { .. } => {
                let error = ErrorCode::Internal(
                    "Hilbert processor received an async event outside a barrier phase",
                );
                self.exchange.fail(error);
                return self.exchange.check_error();
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use databend_common_expression::Scalar;
    use databend_common_expression::types::Int32Type;
    use databend_common_pipeline::basic::Exchange;
    use databend_common_pipeline::core::port::connect;
    use databend_common_pipeline_transforms::traits::Location as SpillLocation;

    use super::*;

    #[derive(Clone, Default)]
    struct MockSpiller {
        files: Arc<Mutex<HashMap<String, DataBlock>>>,
    }

    #[derive(Clone)]
    struct FailingSpiller;

    #[derive(Clone)]
    struct PeerFailingSpiller {
        exchange: Arc<HilbertRangeExchange>,
        restored: DataBlock,
    }

    #[derive(Clone)]
    struct DoubleFailingSpiller {
        exchange: Arc<HilbertRangeExchange>,
    }

    #[async_trait::async_trait]
    impl DataBlockSpill for DoubleFailingSpiller {
        async fn merge_and_spill(&self, _blocks: Vec<DataBlock>) -> Result<SpillLocation> {
            self.exchange.fail(ErrorCode::Internal("peer failed first"));
            Err(ErrorCode::Internal("local spill failed later"))
        }

        async fn restore(&self, _location: &SpillLocation) -> Result<DataBlock> {
            self.exchange.fail(ErrorCode::Internal("peer failed first"));
            Err(ErrorCode::Internal("local restore failed later"))
        }
    }

    #[async_trait::async_trait]
    impl DataBlockSpill for PeerFailingSpiller {
        async fn merge_and_spill(&self, _blocks: Vec<DataBlock>) -> Result<SpillLocation> {
            self.exchange
                .fail(ErrorCode::Internal("peer failed during I/O"));
            Ok(SpillLocation::Remote("completed-spill".to_string()))
        }

        async fn restore(&self, _location: &SpillLocation) -> Result<DataBlock> {
            self.exchange
                .fail(ErrorCode::Internal("peer failed during I/O"));
            Ok(self.restored.clone())
        }
    }

    #[async_trait::async_trait]
    impl DataBlockSpill for FailingSpiller {
        async fn merge_and_spill(&self, _blocks: Vec<DataBlock>) -> Result<SpillLocation> {
            Err(ErrorCode::Internal("forced spill failure"))
        }

        async fn restore(&self, _location: &SpillLocation) -> Result<DataBlock> {
            Err(ErrorCode::Internal("forced restore failure"))
        }
    }

    impl MockSpiller {
        fn spilled_count(&self) -> usize {
            self.files.lock().unwrap().len()
        }
    }

    #[async_trait::async_trait]
    impl DataBlockSpill for MockSpiller {
        async fn merge_and_spill(&self, blocks: Vec<DataBlock>) -> Result<SpillLocation> {
            let path = format!("mock/{}", self.spilled_count());
            self.files
                .lock()
                .unwrap()
                .insert(path.clone(), DataBlock::concat(&blocks)?);
            Ok(SpillLocation::Remote(path))
        }

        async fn restore(&self, location: &SpillLocation) -> Result<DataBlock> {
            let SpillLocation::Remote(path) = location else {
                return Err(ErrorCode::Internal("unexpected local mock spill"));
            };
            self.files
                .lock()
                .unwrap()
                .get(path)
                .cloned()
                .ok_or_else(|| ErrorCode::Internal("missing mock spill"))
        }
    }

    struct TestPorts {
        input: Arc<InputPort>,
        upstream: Arc<OutputPort>,
        output: Arc<OutputPort>,
        downstream: Arc<InputPort>,
    }

    impl TestPorts {
        fn new() -> Self {
            let ports = Self {
                input: InputPort::create(),
                upstream: OutputPort::create(),
                output: OutputPort::create(),
                downstream: InputPort::create(),
            };
            // SAFETY: each pair is connected exactly once before either port is used.
            unsafe {
                connect(&ports.input, &ports.upstream);
                connect(&ports.downstream, &ports.output);
            }
            ports.downstream.set_need_data();
            ports
        }
    }

    fn no_spill_settings() -> MemorySettings {
        MemorySettings::builder().with_workload_group(false).build()
    }

    fn block(x: Vec<i32>, y: Vec<i32>) -> DataBlock {
        DataBlock::new_from_columns(vec![Int32Type::from_data(x), Int32Type::from_data(y)])
    }

    fn test_processor<S: DataBlockSpill + 'static>(
        exchange: Arc<HilbertRangeExchange>,
        worker_id: usize,
        spiller: S,
    ) -> (TestPorts, Box<dyn Processor>) {
        let ports = TestPorts::new();
        let processor = TransformHilbertCluster::create(
            ports.input.clone(),
            ports.output.clone(),
            exchange,
            worker_id,
            spiller,
            no_spill_settings(),
        );
        (ports, processor)
    }

    fn waiting_processor(
        exchange: Arc<HilbertRangeExchange>,
        worker_id: usize,
    ) -> (TestPorts, Box<dyn Processor>) {
        let (ports, processor) = test_processor(exchange, worker_id, MockSpiller::default());
        ports.input.finish();
        (ports, processor)
    }

    fn hilbert_processor<S: DataBlockSpill + 'static>(
        processor: &mut Box<dyn Processor>,
    ) -> &mut TransformHilbertCluster<S> {
        processor.as_any().downcast_mut().unwrap()
    }

    fn processor_with_io<S: DataBlockSpill + 'static>(
        exchange: Arc<HilbertRangeExchange>,
        spiller: S,
        io: PendingIo,
    ) -> (TestPorts, Box<dyn Processor>) {
        let (ports, mut processor) = test_processor(exchange, 0, spiller);
        hilbert_processor::<S>(&mut processor).io = Some(io);
        (ports, processor)
    }

    #[test]
    fn test_spill_batch_preserves_interleaved_chunk_order() -> Result<()> {
        let first = block(vec![1], vec![10]);
        let second = block(vec![2], vec![20]);
        let trailing = block(vec![3], vec![30]);
        let batch_bytes = first.memory_size() + second.memory_size();
        let trailing_bytes = trailing.memory_size();
        let mut buffer = SequentialSpillBuffer::new(
            MockSpiller::default(),
            MemorySettings::builder()
                .with_max_memory_usage(0)
                .with_spill_unit_size(batch_bytes)
                .build(),
        );
        buffer
            .chunks
            .push_back(PendingChunk::Spilled(SpillLocation::Remote(
                "before".to_string(),
            )));
        buffer.push(first);
        buffer.push(second);
        buffer
            .chunks
            .push_back(PendingChunk::Spilled(SpillLocation::Remote(
                "middle".to_string(),
            )));
        buffer.push(trailing);

        let (index, batch) = buffer.take_spill_batch().unwrap();
        assert_eq!(index, 1);
        assert_eq!(
            DataBlock::concat(&batch)?.get_by_offset(0).index(0),
            Some(Scalar::from(1).as_ref())
        );
        assert_eq!(
            DataBlock::concat(&batch)?.get_by_offset(0).index(1),
            Some(Scalar::from(2).as_ref())
        );
        assert_eq!(buffer.memory_bytes, batch_bytes + trailing_bytes);

        buffer.memory_bytes -= batch_bytes;
        buffer.chunks.insert(
            index,
            PendingChunk::Spilled(SpillLocation::Remote("replacement".to_string())),
        );
        let locations = buffer
            .chunks
            .iter()
            .map(|chunk| match chunk {
                PendingChunk::Spilled(SpillLocation::Remote(path)) => path.as_str(),
                PendingChunk::Spilled(SpillLocation::Local(_)) => "local",
                PendingChunk::Memory(_) => "memory",
            })
            .collect::<Vec<_>>();
        assert_eq!(locations, ["before", "replacement", "middle", "memory"]);
        assert_eq!(buffer.memory_bytes, trailing_bytes);
        Ok(())
    }

    #[test]
    fn test_replay_salt_state_across_blocks() {
        let block = || block(vec![1, 2], vec![3, 4]);

        let mut no_salt = None;
        let unsalted = prepare_replay_block(block(), 7, &mut no_salt);
        assert_eq!(unsalted.num_columns(), 2);
        assert_eq!(no_salt, None);

        let mut next_salt_row = Some(0);
        let first = prepare_replay_block(block(), 7, &mut next_salt_row);
        let second = prepare_replay_block(block(), 7, &mut next_salt_row);
        assert_eq!(next_salt_row, Some(4));

        let worker = 7_u64 << 48;
        for (block, start) in [(&first, 0), (&second, 2)] {
            assert_eq!(block.num_columns(), 3);
            for row in 0..2 {
                assert_eq!(
                    block.get_by_offset(2).index(row),
                    Some(Scalar::from(mix64(worker ^ (start + row) as u64)).as_ref())
                );
            }
        }
    }

    #[tokio::test]
    async fn test_spill_replay_preserves_order_and_rows() -> Result<()> {
        let ports = TestPorts::new();
        let spiller = MockSpiller::default();
        let exchange = HilbertRangeExchange::create([0, 1], 12, 1, 1);
        let mut processor = TransformHilbertCluster::create(
            ports.input,
            ports.output,
            exchange,
            0,
            spiller.clone(),
            MemorySettings::builder()
                .with_max_memory_usage(0)
                .with_spill_unit_size(1)
                .build(),
        );
        let blocks = [
            [(0, 100), (10, 90)],
            [(20, 80), (80, 20)],
            [(30, 70), (70, 30)],
            [(40, 60), (60, 40)],
            [(5, 95), (95, 5)],
            [(15, 85), (85, 15)],
        ];

        for values in &blocks {
            ports.upstream.push_data(Ok(block(
                values.iter().map(|value| value.0).collect(),
                values.iter().map(|value| value.1).collect(),
            )));
            loop {
                match processor.event()? {
                    Event::NeedData => break,
                    Event::Async => processor.async_process().await?,
                    Event::Sync => processor.process()?,
                    event => panic!("unexpected sampling event {event:?}"),
                }
            }
        }

        ports.upstream.finish();
        let mut replayed = Vec::new();
        loop {
            match processor.event()? {
                Event::Finished => break,
                Event::Async => processor.async_process().await?,
                Event::Sync => processor.process()?,
                Event::NeedConsume => {
                    replayed.push(
                        ports
                            .downstream
                            .pull_data()
                            .expect("NeedConsume must expose one replayed block")?,
                    );
                    ports.downstream.set_need_data();
                }
                Event::NeedData => panic!("finished input unexpectedly requested more data"),
            }
        }

        assert!(spiller.spilled_count() > 0);
        let replayed = DataBlock::concat(&replayed)?;
        assert_eq!(replayed.num_rows(), 12);
        for (row, expected) in blocks.iter().flatten().enumerate() {
            assert_eq!(
                replayed.get_by_offset(0).index(row),
                Some(Scalar::from(expected.0).as_ref()),
                "row {row} x"
            );
            assert_eq!(
                replayed.get_by_offset(1).index(row),
                Some(Scalar::from(expected.1).as_ref()),
                "row {row} y"
            );
        }
        Ok(())
    }

    #[test]
    fn test_sampling_rearms_input() -> Result<()> {
        let ports = TestPorts::new();
        let exchange = HilbertRangeExchange::create([0, 1], 4, 1, 1);
        let mut processor = TransformHilbertCluster::create(
            ports.input,
            ports.output,
            exchange,
            0,
            MockSpiller::default(),
            no_spill_settings(),
        );

        assert!(matches!(processor.event()?, Event::NeedData));
        for block in [block(vec![1, 2], vec![3, 4]), DataBlock::empty()] {
            ports.upstream.push_data(Ok(block));
            assert!(matches!(processor.event()?, Event::NeedData));
            assert!(ports.upstream.can_push());
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_downstream_cancellation_wakes_preplan_barrier() -> Result<()> {
        let exchange = HilbertRangeExchange::create([0, 1], 1, 2, 1);
        let (_, mut waiting) = waiting_processor(exchange.clone(), 0);
        let (cancelled_ports, mut cancelled) = test_processor(exchange, 1, MockSpiller::default());

        assert!(matches!(waiting.event()?, Event::Async));
        cancelled_ports.downstream.finish();
        assert!(matches!(cancelled.event()?, Event::Finished));
        assert!(cancelled_ports.input.is_finished());
        let error = waiting.async_process().await.unwrap_err();
        assert_eq!(error.name(), "AbortedQuery");
        assert_eq!(
            error.message(),
            "Hilbert recluster cancelled before its range plan was ready"
        );
        Ok(())
    }

    #[test]
    fn test_downstream_cancellation_after_plan_is_not_an_error() -> Result<()> {
        let ports = TestPorts::new();
        let exchange = HilbertRangeExchange::create([0, 1], 1, 1, 1);
        exchange.submit_initial(0, 1, vec![[Scalar::from(1), Scalar::from(1)]]);
        exchange.publish_plan();
        let mut processor = TransformHilbertCluster::create(
            ports.input.clone(),
            ports.output,
            exchange.clone(),
            0,
            MockSpiller::default(),
            no_spill_settings(),
        );
        ports.downstream.finish();

        assert!(matches!(processor.event()?, Event::Finished));
        assert!(ports.input.is_finished());
        exchange.check_error()?;
        Ok(())
    }

    #[test]
    fn test_sample_row_count_overflow_is_an_error() {
        let mut sampler = FixedSizeSampler::new(1, SmallRng::seed_from_u64(1));
        sampler.add_block(usize::MAX, |_| [Scalar::Null, Scalar::Null]);

        assert_eq!(
            sample_block(&mut sampler, [0, 1], &block(vec![1], vec![1]))
                .unwrap_err()
                .message(),
            "Hilbert sampled row count overflowed"
        );
    }

    #[tokio::test]
    async fn test_malformed_sampling_block_wakes_barrier() -> Result<()> {
        let ports = TestPorts::new();
        let exchange = HilbertRangeExchange::create([0, 2], 1, 1, 1);
        let mut processor = TransformHilbertCluster::create(
            ports.input,
            ports.output,
            exchange.clone(),
            0,
            MockSpiller::default(),
            no_spill_settings(),
        );
        ports.upstream.push_data(Ok(block(vec![1], vec![1])));

        assert_eq!(
            processor.event().unwrap_err().message(),
            "Hilbert dimension offset 2 is outside the input block"
        );
        assert_eq!(
            exchange.wait_sketches().await.unwrap_err().message(),
            "Hilbert dimension offset 2 is outside the input block"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_input_error_wakes_barrier() -> Result<()> {
        let exchange = HilbertRangeExchange::create([0, 1], 1, 2, 1);
        let (_, mut waiting) = waiting_processor(exchange.clone(), 0);
        let (failing_ports, mut failing) = test_processor(exchange, 1, MockSpiller::default());

        assert!(matches!(waiting.event()?, Event::Async));
        failing_ports
            .upstream
            .push_data(Err(ErrorCode::Internal("upstream failure")));
        assert_eq!(failing.event().unwrap_err().message(), "upstream failure");
        assert_eq!(
            waiting.async_process().await.unwrap_err().message(),
            "upstream failure"
        );
        Ok(())
    }

    #[test]
    fn test_routing_failure_blocks_peer_replay_output() {
        let exchange = HilbertRangeExchange::create([0, 1], 2, 2, 2);
        exchange.submit_initial(0, 1, vec![[Scalar::from(1), Scalar::from(1)]]);
        exchange.submit_initial(1, 1, vec![[Scalar::from(1), Scalar::from(1)]]);
        exchange.publish_plan();
        let (ports, mut processor) = test_processor(exchange.clone(), 0, MockSpiller::default());
        hilbert_processor::<MockSpiller>(&mut processor).phase = HilbertWorkerPhase::Replay {
            next_salt_row: None,
            output_data: Some(block(vec![1], vec![1])),
        };

        assert_eq!(
            exchange
                .partition(block(vec![1], vec![1]), 2)
                .unwrap_err()
                .message(),
            "Hilbert routing salt must be UInt64"
        );
        assert_eq!(
            processor.event().unwrap_err().message(),
            "Hilbert routing salt must be UInt64"
        );
        assert!(!ports.downstream.has_data());
    }

    #[tokio::test]
    async fn test_peer_failure_during_io_stops_local_progress() {
        let spill_block = block(vec![1], vec![1]);
        let spill_bytes = spill_block.memory_size();
        let exchange = HilbertRangeExchange::create([0, 1], 1, 2, 1);
        let (_, mut processor) = processor_with_io(
            exchange.clone(),
            PeerFailingSpiller {
                exchange,
                restored: DataBlock::empty(),
            },
            PendingIo::Spill(0, vec![spill_block]),
        );
        let processor = hilbert_processor::<PeerFailingSpiller>(&mut processor);
        processor.buffer.memory_bytes = spill_bytes;

        assert_eq!(
            processor.async_process().await.unwrap_err().message(),
            "peer failed during I/O"
        );
        assert_eq!(processor.buffer.memory_bytes, spill_bytes);
        assert!(processor.buffer.chunks.is_empty());

        let exchange = HilbertRangeExchange::create([0, 1], 1, 2, 1);
        let (_, mut processor) = processor_with_io(
            exchange.clone(),
            PeerFailingSpiller {
                exchange,
                restored: block(vec![1], vec![1]),
            },
            PendingIo::Restore(SpillLocation::Remote("completed-restore".to_string())),
        );
        let processor = hilbert_processor::<PeerFailingSpiller>(&mut processor);
        processor.phase = HilbertWorkerPhase::Replay {
            next_salt_row: Some(0),
            output_data: None,
        };

        assert_eq!(
            processor.async_process().await.unwrap_err().message(),
            "peer failed during I/O"
        );
        let HilbertWorkerPhase::Replay {
            next_salt_row,
            output_data,
        } = &processor.phase
        else {
            panic!("restore must preserve replay state after peer failure");
        };
        assert_eq!(*next_salt_row, Some(0));
        assert!(output_data.is_none());
    }

    #[tokio::test]
    async fn test_peer_error_wins_over_later_io_error() {
        for io in [
            PendingIo::Spill(0, vec![block(vec![1], vec![1])]),
            PendingIo::Restore(SpillLocation::Remote("failing-restore".to_string())),
        ] {
            let exchange = HilbertRangeExchange::create([0, 1], 1, 2, 1);
            let (_, mut processor) = processor_with_io(
                exchange.clone(),
                DoubleFailingSpiller {
                    exchange: exchange.clone(),
                },
                io,
            );
            let processor = hilbert_processor::<DoubleFailingSpiller>(&mut processor);
            if matches!(&processor.io, Some(PendingIo::Restore(_))) {
                processor.phase = HilbertWorkerPhase::Replay {
                    next_salt_row: Some(0),
                    output_data: None,
                };
            }

            assert_eq!(
                processor.async_process().await.unwrap_err().message(),
                "peer failed first"
            );
            assert_eq!(
                exchange.wait_sketches().await.unwrap_err().message(),
                "peer failed first"
            );
        }
    }

    #[tokio::test]
    async fn test_invariant_errors_wake_barrier() -> Result<()> {
        let exchange = HilbertRangeExchange::create([0, 1], 1, 2, 1);
        let spiller = MockSpiller::default();
        let location = spiller
            .merge_and_spill(vec![block(vec![1], vec![1])])
            .await?;
        let (_, mut processor) =
            processor_with_io(exchange.clone(), spiller, PendingIo::Restore(location));
        let processor_error = processor.async_process().await.unwrap_err();
        assert_eq!(
            processor_error.message(),
            "Hilbert restore completed outside restore phase"
        );
        assert_eq!(
            exchange.wait_sketches().await.unwrap_err().message(),
            processor_error.message()
        );

        let exchange = HilbertRangeExchange::create([0, 1], 1, 2, 1);
        let (_, mut processor) = test_processor(exchange.clone(), 0, MockSpiller::default());
        let processor_error = processor.async_process().await.unwrap_err();
        assert_eq!(
            processor_error.message(),
            "Hilbert processor received an async event outside a barrier phase"
        );
        assert_eq!(
            exchange.wait_sketches().await.unwrap_err().message(),
            processor_error.message()
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_spill_and_restore_errors_wake_barrier() {
        for (io, expected_cause) in [
            (
                PendingIo::Spill(0, vec![block(vec![1], vec![1])]),
                "forced spill failure",
            ),
            (
                PendingIo::Restore(SpillLocation::Remote("missing".to_string())),
                "forced restore failure",
            ),
        ] {
            let exchange = HilbertRangeExchange::create([0, 1], 1, 2, 1);
            let (_, mut processor) = processor_with_io(exchange.clone(), FailingSpiller, io);
            let processor = hilbert_processor::<FailingSpiller>(&mut processor);
            if matches!(&processor.io, Some(PendingIo::Restore(_))) {
                processor.phase = HilbertWorkerPhase::Resample {
                    cursor: 0,
                    sampler: FixedSizeSampler::new(1, SmallRng::seed_from_u64(1)),
                };
            }

            let processor_error = processor.async_process().await.unwrap_err();
            assert!(processor_error.message().contains(expected_cause));
            assert_eq!(
                exchange.wait_sketches().await.unwrap_err().message(),
                processor_error.message()
            );
        }
    }

    #[tokio::test]
    async fn test_spilled_chunk_restores_into_resample() -> Result<()> {
        let ports = TestPorts::new();
        let spiller = MockSpiller::default();
        let block = block(vec![1, 2], vec![3, 4]);
        let location = spiller.merge_and_spill(vec![block]).await?;
        let exchange = HilbertRangeExchange::create([0, 1], 2, 1, 1);
        exchange.submit_initial(0, 2, vec![[Scalar::from(1), Scalar::from(3)]]);
        let mut processor = TransformHilbertCluster::create(
            ports.input,
            ports.output,
            exchange,
            0,
            spiller,
            no_spill_settings(),
        );
        let processor = hilbert_processor::<MockSpiller>(&mut processor);
        processor
            .buffer
            .chunks
            .push_back(PendingChunk::Spilled(location));
        processor.phase = HilbertWorkerPhase::Resample {
            cursor: 0,
            sampler: FixedSizeSampler::new(2, SmallRng::seed_from_u64(1)),
        };

        assert!(matches!(processor.event()?, Event::Async));
        processor.async_process().await?;
        let HilbertWorkerPhase::Resample { cursor, sampler } = &processor.phase else {
            panic!("restore must preserve the resample phase");
        };
        assert_eq!(*cursor, 1);
        assert_eq!(sampler.rows_seen(), 2);
        assert!(matches!(processor.event()?, Event::Sync));
        Ok(())
    }

    #[test]
    fn test_last_resample_publishes_plan() -> Result<()> {
        let ports = TestPorts::new();
        let exchange = HilbertRangeExchange::create([0, 1], 2, 1, 1);
        exchange.submit_initial(0, 2, vec![[Scalar::from(1), Scalar::from(1)]]);
        let mut processor = TransformHilbertCluster::create(
            ports.input,
            ports.output,
            exchange.clone(),
            0,
            MockSpiller::default(),
            no_spill_settings(),
        );
        let processor = hilbert_processor::<MockSpiller>(&mut processor);
        let mut sampler = FixedSizeSampler::new(2, SmallRng::seed_from_u64(1));
        sample_block(&mut sampler, [0, 1], &block(vec![1, 2], vec![1, 2]))?;
        processor.phase = HilbertWorkerPhase::Resample { cursor: 0, sampler };

        assert!(matches!(processor.event()?, Event::Sync));
        processor.process()?;
        let _ = exchange.partition(block(vec![1], vec![1]), 1)?;
        Ok(())
    }
}
