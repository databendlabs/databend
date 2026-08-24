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

#[derive(Clone, Copy)]
enum HilbertWorkerPhase {
    Sampling,
    WaitSketches,
    Resample,
    WaitPlan,
    Replay { add_routing_salt: bool },
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
            let PendingChunk::Memory(block) = self.chunks.remove(start).unwrap() else {
                unreachable!()
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
    Spill {
        index: usize,
        blocks: Vec<DataBlock>,
        bytes: usize,
    },
    RestoreResample {
        location: Location,
    },
    RestoreReplay {
        location: Location,
        add_salt: bool,
    },
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
    sampler: Option<FixedSizeSampler<HilbertSample, SmallRng>>,
    buffer: SequentialSpillBuffer<S>,
    io: Option<PendingIo>,
    // Cursor over buffer chunks while resampling after quota re-balancing.
    resample_cursor: usize,
    resample_sampler: Option<FixedSizeSampler<HilbertSample, SmallRng>>,
    output_data: Option<DataBlock>,
    next_replay_row: u64,
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
            sampler: Some(FixedSizeSampler::new(
                exchange.local_sample_size(),
                SmallRng::seed_from_u64(mix64(worker_id as u64)),
            )),
            buffer: SequentialSpillBuffer::new(spiller, memory_settings),
            io: None,
            resample_cursor: 0,
            resample_sampler: None,
            output_data: None,
            next_replay_row: 0,
            phase: HilbertWorkerPhase::Sampling,
            worker_id,
            exchange,
        })
    }

    /// Attach deterministic routing salt when the plan contains hot keys.
    fn prepare_replay_block(&mut self, mut block: DataBlock, add_salt: bool) -> DataBlock {
        if add_salt {
            let start = self.next_replay_row;
            let worker = (self.worker_id as u64) << 48;
            block.add_column(UInt64Type::from_data(
                (0..block.num_rows())
                    .map(|row| mix64(worker ^ (start + row as u64)))
                    .collect(),
            ));
        }
        self.next_replay_row += block.num_rows() as u64;
        block
    }
}

fn sample_block(
    sampler: &mut FixedSizeSampler<HilbertSample, SmallRng>,
    dimension_offsets: [usize; HILBERT_CLUSTER_DIMENSIONS],
    block: &DataBlock,
) {
    sampler.add_block(block.num_rows(), |row| {
        from_fn(|dimension| {
            block
                .get_by_offset(dimension_offsets[dimension])
                .index(row)
                .expect("sample row is within the input block")
                .to_owned()
        })
    });
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
            self.input.finish();
            return Ok(Event::Finished);
        }
        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }
        if let Some(block) = self.output_data.take() {
            self.output.push_data(Ok(block));
            return Ok(Event::NeedConsume);
        }
        // An async spill/restore is in flight; drain it before advancing the phase.
        if self.io.is_some() {
            self.input.set_not_need_data();
            return Ok(Event::Async);
        }
        match self.phase {
            HilbertWorkerPhase::Sampling => {
                // Finish reducing an oversized resident buffer before accepting more input.
                if let Some((index, batch)) = self.buffer.take_spill_batch() {
                    let bytes = batch.iter().map(DataBlock::memory_size).sum();
                    self.io = Some(PendingIo::Spill {
                        index,
                        blocks: batch,
                        bytes,
                    });
                    return Ok(Event::Async);
                }
                if self.input.has_data() {
                    let block = self
                        .input
                        .pull_data()
                        .unwrap()
                        .inspect_err(|error| self.exchange.fail(error.clone()))?;
                    if !block.is_empty() {
                        sample_block(
                            self.sampler
                                .as_mut()
                                .expect("sampling phase requires a sampler"),
                            self.exchange.dimension_offsets(),
                            &block,
                        );
                        self.buffer.push(block);
                    }
                    // `pull_data` clears the port's NEED_DATA flag. Re-arm it before returning
                    // `NeedData`, otherwise both this processor and its upstream become idle.
                    self.input.set_need_data();
                    return Ok(Event::NeedData);
                }
                if self.input.is_finished() {
                    let sampler = self
                        .sampler
                        .take()
                        .expect("Hilbert sampler is submitted exactly once");
                    let rows = sampler.rows_seen();
                    self.exchange
                        .submit_initial(self.worker_id, rows, sampler.into_samples());
                    self.phase = HilbertWorkerPhase::WaitSketches;
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
            HilbertWorkerPhase::Resample => {
                // Walk chunks in order; spilled chunks are restored one by one.
                loop {
                    let next = match self.buffer.chunks.get(self.resample_cursor) {
                        Some(PendingChunk::Memory(block)) => {
                            sample_block(
                                self.resample_sampler
                                    .as_mut()
                                    .expect("resample phase requires a prepared sampler"),
                                self.exchange.dimension_offsets(),
                                block,
                            );
                            self.resample_cursor += 1;
                            continue;
                        }
                        Some(PendingChunk::Spilled(location)) => Some(location.clone()),
                        None => None,
                    };
                    if let Some(location) = next {
                        self.io = Some(PendingIo::RestoreResample { location });
                        return Ok(Event::Async);
                    }
                    break;
                }
                let sampler = self
                    .resample_sampler
                    .take()
                    .expect("resample phase requires a prepared sampler");
                let rows = sampler.rows_seen();
                self.exchange
                    .complete_resample(self.worker_id, rows, sampler.into_samples());
                self.phase = HilbertWorkerPhase::WaitPlan;
                if self.exchange.should_build_plan() {
                    Ok(Event::Sync)
                } else {
                    Ok(Event::Async)
                }
            }
            HilbertWorkerPhase::Replay { add_routing_salt } => {
                if let Some(next) = self.buffer.pop_replay_chunk() {
                    match next {
                        PendingChunk::Memory(block) => {
                            let block = self.prepare_replay_block(block, add_routing_salt);
                            self.output.push_data(Ok(block));
                            return Ok(Event::NeedConsume);
                        }
                        PendingChunk::Spilled(location) => {
                            self.io = Some(PendingIo::RestoreReplay {
                                location,
                                add_salt: add_routing_salt,
                            });
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
        match self.phase {
            HilbertWorkerPhase::WaitPlan => {
                if self.exchange.should_build_plan() {
                    self.exchange.publish_plan();
                }
            }
            HilbertWorkerPhase::Sampling
            | HilbertWorkerPhase::WaitSketches
            | HilbertWorkerPhase::Resample
            | HilbertWorkerPhase::Replay { .. } => {}
        }
        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        // A task-wide failure (e.g. another worker's spill error) wakes every waiter.
        self.exchange.check_error()?;
        if let Some(io) = self.io.take() {
            match io {
                PendingIo::Spill {
                    index,
                    blocks,
                    bytes,
                } => {
                    let location = match self.buffer.spiller.merge_and_spill(blocks).await {
                        Ok(location) => location,
                        Err(error) => {
                            let error = ErrorCode::from_string(format!(
                                "Hilbert recluster failed to spill buffered input: {error}"
                            ));
                            self.exchange.fail(error.clone());
                            return Err(error);
                        }
                    };
                    // Resident memory drops once the batch is durable. The merged spill occupies
                    // the original batch position, preserving input order for resample and replay.
                    self.buffer.memory_bytes = self.buffer.memory_bytes.saturating_sub(bytes);
                    self.buffer
                        .chunks
                        .insert(index, PendingChunk::Spilled(location));
                }
                PendingIo::RestoreResample { location } => {
                    let block = self
                        .buffer
                        .spiller
                        .restore(&location)
                        .await
                        .inspect_err(|error| self.exchange.fail(error.clone()))?;
                    sample_block(
                        self.resample_sampler
                            .as_mut()
                            .expect("resample phase requires a prepared sampler"),
                        self.exchange.dimension_offsets(),
                        &block,
                    );
                    self.resample_cursor += 1;
                }
                PendingIo::RestoreReplay { location, add_salt } => {
                    let block = self
                        .buffer
                        .spiller
                        .restore(&location)
                        .await
                        .inspect_err(|error| self.exchange.fail(error.clone()))?;
                    self.output_data = Some(self.prepare_replay_block(block, add_salt));
                }
            }
            return Ok(());
        }
        match self.phase {
            HilbertWorkerPhase::WaitSketches => {
                self.exchange.wait_sketches().await?;
                self.phase =
                    if let Some(sample_size) = self.exchange.resample_request(self.worker_id) {
                        self.resample_sampler = Some(FixedSizeSampler::new(
                            sample_size,
                            SmallRng::seed_from_u64(mix64(self.worker_id as u64 ^ 0x9e37_79b9)),
                        ));
                        self.resample_cursor = 0;
                        HilbertWorkerPhase::Resample
                    } else {
                        HilbertWorkerPhase::WaitPlan
                    };
            }
            HilbertWorkerPhase::WaitPlan => {
                let add_routing_salt = self.exchange.wait_plan().await?;
                self.phase = HilbertWorkerPhase::Replay { add_routing_salt };
            }
            HilbertWorkerPhase::Sampling
            | HilbertWorkerPhase::Resample
            | HilbertWorkerPhase::Replay { .. } => {
                return Err(ErrorCode::Internal(
                    "Hilbert processor received an async event outside a barrier phase",
                ));
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
    use databend_common_pipeline::core::port::connect;
    use databend_common_pipeline_transforms::traits::Location as SpillLocation;

    use super::*;

    #[derive(Clone, Default)]
    struct MockSpiller {
        files: Arc<Mutex<HashMap<String, DataBlock>>>,
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

    fn force_spill_settings(spill_unit_size: usize) -> MemorySettings {
        MemorySettings::builder()
            .with_max_memory_usage(0)
            .with_spill_unit_size(spill_unit_size)
            .build()
    }

    fn no_spill_settings() -> MemorySettings {
        MemorySettings::builder().with_workload_group(false).build()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_spill_replay_preserves_order_and_rows() -> Result<()> {
        let ports = TestPorts::new();
        let spiller = MockSpiller::default();
        let exchange = HilbertRangeExchange::create([0, 1], 12, 1, 1);
        let mut processor = TransformHilbertCluster::create(
            ports.input.clone(),
            ports.output.clone(),
            exchange,
            0,
            spiller.clone(),
            force_spill_settings(1),
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
            ports
                .upstream
                .push_data(Ok(DataBlock::new_from_columns(vec![
                    Int32Type::from_data(values.iter().map(|value| value.0).collect()),
                    Int32Type::from_data(values.iter().map(|value| value.1).collect()),
                ])));
            loop {
                match processor.event()? {
                    Event::NeedData => break,
                    Event::Async => processor.async_process().await?,
                    Event::Sync => processor.process()?,
                    event => panic!("unexpected sampling event {event:?}"),
                }
            }
            assert!(ports.upstream.can_push());
        }

        ports.input.finish();
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
        let spiller = MockSpiller::default();
        let exchange = HilbertRangeExchange::create([0, 1], 4, 1, 1);
        let mut processor = TransformHilbertCluster::create(
            ports.input,
            ports.output,
            exchange,
            0,
            spiller.clone(),
            no_spill_settings(),
        );

        assert!(matches!(processor.event()?, Event::NeedData));
        for block in [
            DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![1, 2]),
                Int32Type::from_data(vec![3, 4]),
            ]),
            DataBlock::empty(),
        ] {
            ports.upstream.push_data(Ok(block));
            assert!(matches!(processor.event()?, Event::NeedData));
            assert!(ports.upstream.can_push());
        }
        assert_eq!(spiller.spilled_count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_input_error_wakes_barrier() -> Result<()> {
        let exchange = HilbertRangeExchange::create([0, 1], 1, 2, 1);
        let waiting_ports = TestPorts::new();
        let failing_ports = TestPorts::new();
        let mut waiting = TransformHilbertCluster::create(
            waiting_ports.input.clone(),
            waiting_ports.output,
            exchange.clone(),
            0,
            MockSpiller::default(),
            no_spill_settings(),
        );
        let mut failing = TransformHilbertCluster::create(
            failing_ports.input,
            failing_ports.output,
            exchange,
            1,
            MockSpiller::default(),
            no_spill_settings(),
        );

        waiting_ports.input.finish();
        assert!(matches!(waiting.event()?, Event::Async));
        failing_ports
            .upstream
            .push_data(Err(ErrorCode::Internal("upstream failure")));
        assert!(
            failing
                .event()
                .unwrap_err()
                .message()
                .contains("upstream failure")
        );
        assert!(
            waiting
                .async_process()
                .await
                .unwrap_err()
                .message()
                .contains("upstream failure")
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_last_resample_publishes_plan() -> Result<()> {
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
        let processor = processor
            .as_any()
            .downcast_mut::<TransformHilbertCluster<MockSpiller>>()
            .unwrap();
        processor.phase = HilbertWorkerPhase::Resample;
        processor.resample_sampler = Some(FixedSizeSampler::new(2, SmallRng::seed_from_u64(1)));
        sample_block(
            processor.resample_sampler.as_mut().unwrap(),
            [0, 1],
            &DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![1, 2]),
                Int32Type::from_data(vec![1, 2]),
            ]),
        );

        assert!(matches!(processor.event()?, Event::Sync));
        processor.process()?;
        assert!(!exchange.wait_plan().await?);
        Ok(())
    }
}
