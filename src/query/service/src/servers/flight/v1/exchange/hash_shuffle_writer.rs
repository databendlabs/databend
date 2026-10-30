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

//! Sender side of the thread-level hash shuffle (GlobalShuffle).
//!
//! Rows are addressed to one of `P = sum(threads of every node)` partitions and
//! copied into per-partition builders as they arrive, so input blocks are released
//! right away. Partitions are not sent one by one when they fill up: the sender
//! holds at most `budget` bytes in total and then sends every partition at once.
//! Memory is bounded by the budget whatever the cluster size, while each send
//! still carries `budget / P` bytes per thread. Every remote node receives the
//! blocks of all its threads in one call, which the send buffer packs into shared
//! frames.
//!
//! While one round is sending, the sender keeps buffering its input into the next
//! round, so its upstream (e.g. a storage scan) keeps running. It waits only when
//! that round reaches the budget too, so at most two rounds are held at once.
//!
//! A block that carries a meta and no rows, such as the states of a partial
//! aggregate, is routed whole by [`FlightScatter::route_meta_block`] instead, and
//! its parts wait for the next send under the same budget.

use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;
use std::task::Poll;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockPartitionStream;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::ExecutorWaker;
use databend_common_pipeline::core::InputPort;
use petgraph::graph::NodeIndex;

use crate::servers::flight::v1::network::SyncTaskHandle;
use crate::servers::flight::v1::network::SyncTaskSet;
use crate::servers::flight::v1::network::outbound::HashShuffleDispatcher;
use crate::servers::flight::v1::network::outbound::ShuffleSendAddress;
use crate::servers::flight::v1::network::outbound::ShuffleSendFuture;
use crate::servers::flight::v1::network::outbound::block_owned_bytes;
use crate::servers::flight::v1::scatter::FlightScatter;

/// Per-thread send buffer: the share of `node_budget` of each local sender.
pub fn hash_shuffle_send_budget(local_threads: usize, node_budget: usize) -> usize {
    (node_budget / local_threads.max(1)).max(1)
}

/// The threads of one node, which own partitions `partitions`.
#[derive(Clone)]
pub struct ShuffleNode {
    pub partitions: Range<usize>,
    pub dispatcher: HashShuffleDispatcher,
}

type ShuffleSendResults = Vec<(ShuffleSendAddress, Result<()>)>;

pub struct HashShuffleWriter {
    id: NodeIndex,
    tasks: SyncTaskSet,
    handle: Option<SyncTaskHandle<'static, ShuffleSendResults>>,

    scatter: Arc<Box<dyn FlightScatter>>,
    stream: BlockPartitionStream,
    num_partitions: usize,
    /// Rows decide whether there is anything to send: a block may have no columns
    /// (e.g. under `count(*)`) or only NULL columns, and so a zero memory size.
    buffered_rows: usize,
    buffered_bytes: usize,
    /// Blocks routed whole to each partition, sent after its rows.
    routed: Vec<Vec<DataBlock>>,
    routed_blocks: usize,
    budget: usize,
    nodes: Vec<ShuffleNode>,
}

impl HashShuffleWriter {
    pub fn create(
        worker_id: usize,
        waker: Arc<ExecutorWaker>,
        scatter: Arc<Box<dyn FlightScatter>>,
        nodes: Vec<ShuffleNode>,
        budget: usize,
    ) -> HashShuffleWriter {
        let num_partitions = nodes.last().map(|x| x.partitions.end).unwrap_or_default();

        HashShuffleWriter {
            id: NodeIndex::default(),
            tasks: SyncTaskSet::new(worker_id, waker),
            handle: None,
            scatter,
            // No per-partition thresholds: partitions are only sent together.
            stream: BlockPartitionStream::create(0, 0, num_partitions),
            num_partitions,
            buffered_rows: 0,
            buffered_bytes: 0,
            routed: vec![vec![]; num_partitions],
            routed_blocks: 0,
            budget,
            nodes,
        }
    }

    pub fn set_id(&mut self, id: NodeIndex) {
        self.id = id;
    }

    /// Poll the in-flight sends. Returns true while they are still pending.
    pub fn poll_sending(&mut self, reset: bool) -> Result<bool> {
        let Some(mut handle) = self.handle.take() else {
            return Ok(false);
        };

        match handle.poll(reset) {
            Poll::Ready(results) => {
                self.handle_results(results)?;
                Ok(false)
            }
            Poll::Pending => {
                self.handle = Some(handle);
                Ok(true)
            }
        }
    }

    /// True when the buffered rows reached the budget and should be flushed.
    pub fn budget_reached(&self) -> bool {
        self.buffered_bytes >= self.budget
    }

    /// The event of a sender whose previous round is still sending: it buffers
    /// `input` into the next round until that round reaches the budget too. The
    /// sends wake the sender when they complete.
    pub fn buffer_while_sending(&mut self, input: &InputPort) -> Result<Event> {
        if !self.budget_reached() && input.has_data() {
            let data_block = input.pull_data().unwrap()?;
            self.add_block(data_block)?;
        }

        if self.budget_reached() || input.is_finished() {
            return Ok(Event::NeedConsume);
        }

        input.set_need_data();
        Ok(Event::NeedData)
    }

    /// Copy the rows of `block` into their partitions. Returns true when the
    /// buffered rows reached the budget and should be flushed.
    ///
    /// Blocks may be added while the previous round is still sending.
    pub fn add_block(&mut self, block: DataBlock) -> Result<bool> {
        if block.is_empty() {
            return match block.get_meta() {
                Some(_) => self.add_routed_block(block),
                None => Ok(false),
            };
        }

        let Some(partition_ids) = self.scatter.scatter_indices(&block)? else {
            return Err(ErrorCode::Internal(format!(
                "Logical error, {} scatter cannot be used for hash shuffle",
                self.scatter.name()
            )));
        };

        self.buffered_rows += block.num_rows();
        self.stream.partition(partition_ids, block, false);
        // Constants stay constant in the partitions they fill alone, but are counted
        // as if expanded, so a block of constants still reaches the budget.
        self.buffered_bytes = self.stream.memory_size();
        Ok(self.buffered_bytes >= self.budget)
    }

    /// Route a block with a meta and no rows whole to its partitions.
    fn add_routed_block(&mut self, block: DataBlock) -> Result<bool> {
        let Some(parts) = self.scatter.route_meta_block(block)? else {
            return Ok(false);
        };
        if parts.len() != self.num_partitions {
            return Err(ErrorCode::Internal(format!(
                "Logical error, {} scatter routed a block to {} of {} partitions",
                self.scatter.name(),
                parts.len(),
                self.num_partitions
            )));
        }

        for (partition, part) in parts.into_iter().enumerate() {
            if part.is_empty() && part.get_meta().is_none() {
                continue;
            }
            self.buffered_bytes += block_owned_bytes(&part);
            self.routed[partition].push(part);
            self.routed_blocks += 1;
        }
        Ok(self.buffered_bytes >= self.budget)
    }

    /// Send every buffered partition. Must not be called while the previous round
    /// is still sending, which keeps the rounds of each destination in order.
    ///
    /// The rows of partition `own` are returned instead of sent, so the caller can
    /// push them to its own output. Returns whether sends are still pending.
    pub fn flush(&mut self, own: Option<usize>) -> Result<(Option<DataBlock>, bool)> {
        debug_assert!(self.handle.is_none());

        if self.buffered_rows == 0 && self.routed_blocks == 0 {
            return Ok((None, false));
        }
        self.buffered_rows = 0;
        self.buffered_bytes = 0;
        self.routed_blocks = 0;

        let mut rows: Vec<Option<DataBlock>> = vec![None; self.num_partitions];
        for (partition, block) in self.stream.take_partitions(&HashSet::new()) {
            if !block.is_empty() {
                rows[partition] = Some(block);
            }
        }

        // Keep the rows of the direct-output partition out of both local and remote delivery.
        let own_block = own.and_then(|partition| rows[partition].take());
        let mut partitions = rows
            .into_iter()
            .zip(self.routed.iter_mut())
            .map(|(rows, routed)| {
                let mut blocks = Vec::with_capacity(rows.is_some() as usize + routed.len());
                blocks.extend(rows);
                blocks.append(routed);
                blocks
            })
            .collect::<Vec<_>>();
        let mut futures: Vec<ShuffleSendFuture> = vec![];

        for (node_idx, node) in self.nodes.iter().enumerate() {
            let blocks = &mut partitions[node.partitions.clone()];
            node.dispatcher
                .prepare_send(node_idx, blocks, &mut futures)?;
        }

        if futures.is_empty() {
            return Ok((own_block, false));
        }

        let mut handle = self
            .tasks
            .spawn(self.id, Box::pin(futures::future::join_all(futures)));

        match handle.poll(true) {
            Poll::Ready(results) => {
                self.handle_results(results)?;
                Ok((own_block, false))
            }
            Poll::Pending => {
                self.handle = Some(handle);
                Ok((own_block, true))
            }
        }
    }

    fn handle_results(&mut self, results: ShuffleSendResults) -> Result<()> {
        for (address, result) in results {
            match result {
                Ok(()) => {}
                Err(cause) if cause.code() == ErrorCode::ABORTED_QUERY => {
                    self.nodes[address.node]
                        .dispatcher
                        .stop_sending(address.thread);
                }
                Err(cause) => return Err(cause),
            }
        }

        Ok(())
    }

    /// Stop this writer's sends to `partition`, which belongs to the local node.
    pub fn stop_sending_partition(&mut self, partition: usize) {
        for node in &mut self.nodes {
            if node.partitions.contains(&partition) {
                node.dispatcher
                    .stop_sending(Some(partition - node.partitions.start));
                return;
            }
        }
    }

    pub fn stop_sending_all(&mut self) {
        for node in &mut self.nodes {
            node.dispatcher.stop_sending(None);
        }
    }

    /// True when no destination can accept data any more.
    pub fn all_closed(&self) -> bool {
        self.nodes.iter().all(|node| node.dispatcher.is_closed())
    }

    pub fn details_status(&self) -> String {
        let closed = self
            .nodes
            .iter()
            .map(|node| node.dispatcher.closed_partitions(node.partitions.len()))
            .sum::<usize>();

        format!(
            "handle_pending={}, closed_partitions={}/{}, buffered_rows={}, buffered_bytes={}, budget={}",
            self.handle.is_some(),
            closed,
            self.num_partitions,
            self.buffered_rows,
            self.buffered_bytes,
            self.budget,
        )
    }
}

#[cfg(test)]
mod tests {
    use arrow_flight::FlightData;
    use arrow_schema::Schema as ArrowSchema;
    use databend_common_base::runtime::Runtime;
    use databend_common_expression::BlockEntry;
    use databend_common_expression::BlockMetaInfo;
    use databend_common_expression::BlockMetaInfoDowncast;
    use databend_common_expression::BlockProfileStatistics;
    use databend_common_expression::FromData;
    use databend_common_expression::Scalar;
    use databend_common_expression::types::AccessType;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::StringType;
    use databend_common_expression::types::UInt64Type;
    use databend_common_settings::FlightCompression;
    use tonic::Status;

    use super::*;
    use crate::servers::flight::v1::network::PingPongTransport;
    use crate::servers::flight::v1::network::inbound::ExchangeReceiveChannels;
    use crate::servers::flight::v1::network::inbound::exchange_queue::QueueItem;
    use crate::servers::flight::v1::network::outbound::ExchangeSendBuffer;
    use crate::servers::flight::v1::network::outbound::ExchangeSendBufferConfig;
    use crate::servers::flight::v1::network::outbound::create_local_block_senders;
    use crate::servers::flight::v1::network::wire_format::deserialize_flight_data;
    use crate::servers::flight::v1::network::wire_format::extract_tid;
    use crate::servers::flight::v1::network::wire_format::split_batch_flight_data;
    use crate::servers::flight::v1::network::wire_format::strip_tid;

    /// Routes each row to partition `value % num_partitions` of its first column.
    struct ModuloScatter {
        num_partitions: u64,
    }

    impl FlightScatter for ModuloScatter {
        fn name(&self) -> &'static str {
            "Modulo"
        }

        fn execute(&self, _data_block: DataBlock) -> Result<Vec<DataBlock>> {
            unreachable!()
        }

        fn scatter_indices(&self, data_block: &DataBlock) -> Result<Option<Vec<u64>>> {
            Ok(Some(
                values(data_block)
                    .into_iter()
                    .map(|x| x % self.num_partitions)
                    .collect(),
            ))
        }
    }

    /// Routes row `i` to partition `i % num_partitions`, for blocks without keys.
    struct RowScatter {
        num_partitions: u64,
    }

    impl FlightScatter for RowScatter {
        fn name(&self) -> &'static str {
            "Row"
        }

        fn execute(&self, _data_block: DataBlock) -> Result<Vec<DataBlock>> {
            unreachable!()
        }

        fn scatter_indices(&self, data_block: &DataBlock) -> Result<Option<Vec<u64>>> {
            Ok(Some(
                (0..data_block.num_rows() as u64)
                    .map(|x| x % self.num_partitions)
                    .collect(),
            ))
        }
    }

    fn values(block: &DataBlock) -> Vec<u64> {
        let column = block.get_by_offset(0).to_column();
        UInt64Type::try_downcast_column(&column)
            .unwrap()
            .iter()
            .copied()
            .collect()
    }

    fn strings(block: &DataBlock) -> Vec<String> {
        let column = block.get_by_offset(1).to_column();
        StringType::try_downcast_column(&column)
            .unwrap()
            .iter()
            .map(|x| x.to_string())
            .collect()
    }

    /// Rows `range`, with a long string per row so that strings live in data buffers.
    fn make_block(range: std::ops::Range<u64>) -> DataBlock {
        let ids = range.collect::<Vec<_>>();
        let names = ids
            .iter()
            .map(|x| format!("a fairly long string payload {x:08}"))
            .collect::<Vec<_>>();
        DataBlock::new_from_columns(vec![
            UInt64Type::from_data(ids),
            StringType::from_data(names),
        ])
    }

    fn check_rows(block: &DataBlock, partition: u64, num_partitions: u64) {
        assert!(
            values(block)
                .iter()
                .all(|x| x % num_partitions == partition)
        );
        assert!(
            strings(block)
                .iter()
                .zip(values(block))
                .all(|(s, v)| s.ends_with(&format!("{v:08}")))
        );
    }

    fn local_nodes(
        num_nodes: usize,
        threads: usize,
    ) -> (Vec<ShuffleNode>, Vec<async_channel::Receiver<QueueItem>>) {
        let mut nodes = vec![];
        let mut receivers = vec![];
        for node in 0..num_nodes {
            let receive_channels = ExchangeReceiveChannels::new(threads);
            let channels = create_local_block_senders(&receive_channels);
            receivers.extend(receive_channels.receivers.iter().cloned());
            nodes.push(ShuffleNode {
                partitions: node * threads..(node + 1) * threads,
                dispatcher: HashShuffleDispatcher::local(channels),
            });
        }
        (nodes, receivers)
    }

    fn drain_blocks(
        receiver: &async_channel::Receiver<QueueItem>,
    ) -> impl Iterator<Item = DataBlock> + '_ {
        std::iter::from_fn(move || receiver.try_recv().ok()).map(|item| match item {
            QueueItem::LocalData(item) => item.into_data(),
            QueueItem::RemoteData(_) => panic!("expected a local block"),
        })
    }

    fn writer(nodes: Vec<ShuffleNode>, budget: usize) -> HashShuffleWriter {
        let num_partitions = nodes.last().unwrap().partitions.end as u64;
        let scatter: Arc<Box<dyn FlightScatter>> =
            Arc::new(Box::new(ModuloScatter { num_partitions }));
        HashShuffleWriter::create(0, ExecutorWaker::create(), scatter, nodes, budget)
    }

    /// Blocks without columns (e.g. under `count(*)`) or with only NULL columns
    /// have a zero memory size, but their rows must still be sent.
    #[test]
    fn test_writer_sends_rows_of_zero_size_blocks() -> Result<()> {
        let no_columns = || DataBlock::new(vec![], 10);
        let only_null = || {
            DataBlock::new(
                vec![BlockEntry::new_const_column(
                    DataType::Null,
                    Scalar::Null,
                    10,
                )],
                10,
            )
        };

        for make_block in [&no_columns as &dyn Fn() -> DataBlock, &only_null] {
            let (nodes, receivers) = local_nodes(1, 4);
            let scatter: Arc<Box<dyn FlightScatter>> =
                Arc::new(Box::new(RowScatter { num_partitions: 4 }));
            let mut writer =
                HashShuffleWriter::create(0, ExecutorWaker::create(), scatter, nodes, usize::MAX);

            assert_eq!(make_block().memory_size(), 0);
            writer.add_block(make_block())?;
            writer.add_block(make_block())?;
            writer.flush(None)?;

            let rows = receivers
                .iter()
                .flat_map(drain_blocks)
                .map(|block| block.num_rows())
                .sum::<usize>();
            assert_eq!(rows, 20);
        }
        Ok(())
    }

    /// Constant columns stay constant through the partitions, but count as if
    /// expanded, so they still reach the budget.
    #[test]
    fn test_writer_keeps_constants_counted_expanded() -> Result<()> {
        let block = || {
            DataBlock::new(
                vec![BlockEntry::new_const_column(
                    DataType::String,
                    Scalar::String("x".repeat(64)),
                    1000,
                )],
                1000,
            )
        };
        let expanded = block().convert_to_full().memory_size();
        assert!(block().memory_size() * 100 < expanded);

        let (nodes, receivers) = local_nodes(1, 4);
        let scatter: Arc<Box<dyn FlightScatter>> =
            Arc::new(Box::new(RowScatter { num_partitions: 4 }));
        let mut writer =
            HashShuffleWriter::create(0, ExecutorWaker::create(), scatter, nodes, expanded / 2);

        assert!(writer.add_block(block())?);
        assert!(writer.buffered_bytes >= expanded / 2);
        writer.flush(None)?;

        let blocks = receivers.iter().flat_map(drain_blocks).collect::<Vec<_>>();
        assert_eq!(blocks.iter().map(|x| x.num_rows()).sum::<usize>(), 1000);
        for block in &blocks {
            assert!(matches!(block.get_by_offset(0), BlockEntry::Const(..)));
        }
        Ok(())
    }

    /// Stands for data a block carries in its meta, like the states of a partial aggregate.
    #[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
    struct TestStates {
        partition: usize,
        bytes: usize,
    }

    #[typetag::serde(name = "test_hash_shuffle_states")]
    impl BlockMetaInfo for TestStates {
        fn output_stats(&self) -> Option<BlockProfileStatistics> {
            Some(BlockProfileStatistics {
                rows: 1,
                bytes: self.bytes,
            })
        }
    }

    fn states_block(partition: usize, bytes: usize) -> DataBlock {
        DataBlock::empty_with_meta(Box::new(TestStates { partition, bytes }))
    }

    /// Routes rows like `ModuloScatter`, and states whole to their partition.
    struct StatesScatter {
        num_partitions: usize,
    }

    impl FlightScatter for StatesScatter {
        fn name(&self) -> &'static str {
            "States"
        }

        fn execute(&self, _data_block: DataBlock) -> Result<Vec<DataBlock>> {
            unreachable!()
        }

        fn scatter_indices(&self, data_block: &DataBlock) -> Result<Option<Vec<u64>>> {
            ModuloScatter {
                num_partitions: self.num_partitions as u64,
            }
            .scatter_indices(data_block)
        }

        fn route_meta_block(&self, data_block: DataBlock) -> Result<Option<Vec<DataBlock>>> {
            let partition = TestStates::downcast_ref_from(data_block.get_meta().unwrap())
                .unwrap()
                .partition;
            let mut blocks = vec![DataBlock::empty(); self.num_partitions];
            blocks[partition] = data_block;
            Ok(Some(blocks))
        }
    }

    /// Blocks routed whole count the bytes of their meta against the budget, and are delivered
    /// through the channels with the next flush, also for the own partition.
    #[test]
    fn test_writer_routes_meta_blocks_under_budget() -> Result<()> {
        let (nodes, receivers) = local_nodes(1, 4);
        let scatter: Arc<Box<dyn FlightScatter>> =
            Arc::new(Box::new(StatesScatter { num_partitions: 4 }));
        let mut writer = HashShuffleWriter::create(0, ExecutorWaker::create(), scatter, nodes, 100);

        assert!(!writer.add_block(states_block(2, 60))?);
        assert_eq!(writer.buffered_bytes, 60);
        assert!(writer.add_block(states_block(1, 50))?);

        let (own, pending) = writer.flush(Some(1))?;
        assert!(!pending);
        assert!(own.is_none());
        assert_eq!(writer.buffered_bytes, 0);
        for (partition, receiver) in receivers.iter().enumerate() {
            let states = drain_blocks(receiver)
                .map(|block| {
                    TestStates::downcast_ref_from(block.get_meta().unwrap())
                        .unwrap()
                        .partition
                })
                .collect::<Vec<_>>();
            match partition {
                1 | 2 => assert_eq!(states, vec![partition]),
                _ => assert!(states.is_empty()),
            }
        }

        // Rows of the own partition still bypass the channels next to routed blocks.
        writer.add_block(make_block(0..8))?;
        writer.add_block(states_block(1, 1))?;
        let (own, _) = writer.flush(Some(1))?;
        check_rows(&own.unwrap(), 1, 4);
        let routed = drain_blocks(&receivers[1]).collect::<Vec<_>>();
        assert_eq!(routed.len(), 1);
        assert!(routed[0].get_meta().is_some());
        Ok(())
    }

    /// A scatter that only partitions rows drops blocks without rows, as before.
    #[test]
    fn test_writer_drops_meta_blocks_without_routing() -> Result<()> {
        let (nodes, receivers) = local_nodes(1, 4);
        let mut writer = writer(nodes, usize::MAX);

        assert!(!writer.add_block(states_block(0, 10))?);
        assert_eq!(writer.flush(None)?.0.map(|b| b.num_rows()), None);
        assert!(receivers.iter().all(|receiver| receiver.is_empty()));
        Ok(())
    }

    #[test]
    fn test_writer_returns_own_partition_and_skips_closed() -> Result<()> {
        let (nodes, receivers) = local_nodes(1, 4);
        let mut writer = writer(nodes, usize::MAX);

        receivers[3].close();
        writer.add_block(make_block(0..100))?;

        let (own, pending) = writer.flush(Some(1))?;
        assert!(!pending);
        check_rows(&own.unwrap(), 1, 4);

        assert!(receivers[1].is_empty());
        assert!(receivers[3].is_empty());
        for partition in [0, 2] {
            let rows = drain_blocks(&receivers[partition])
                .map(|block| block.num_rows())
                .sum::<usize>();
            assert_eq!(rows, 25);
        }

        assert!(!writer.all_closed());
        writer.stop_sending_all();
        assert!(writer.all_closed());
        Ok(())
    }

    #[test]
    fn test_writer_returns_own_partition_with_node_offset() -> Result<()> {
        let (nodes, receivers) = local_nodes(2, 4);
        let mut writer = writer(nodes, usize::MAX);
        writer.add_block(make_block(0..200))?;

        let (own, pending) = writer.flush(Some(5))?;
        assert!(!pending);
        let own = own.unwrap();
        assert_eq!(own.num_rows(), 25);
        check_rows(&own, 5, 8);
        for (partition, receiver) in receivers.iter().enumerate() {
            let blocks = drain_blocks(receiver).collect::<Vec<_>>();
            if partition == 5 {
                assert!(blocks.is_empty());
            } else {
                assert_eq!(blocks.iter().map(|x| x.num_rows()).sum::<usize>(), 25);
                for block in blocks.iter() {
                    check_rows(block, partition as u64, 8);
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_writer_closes_local_partitions_independently() -> Result<()> {
        let (nodes, receivers) = local_nodes(1, 4);
        let mut writer = writer(nodes, usize::MAX);

        writer.stop_sending_partition(1);
        assert!(!writer.all_closed());
        for (partition, receiver) in receivers.iter().enumerate() {
            assert_eq!(receiver.is_closed(), partition == 1);
        }

        writer.add_block(make_block(0..100))?;
        assert!(!writer.flush(None)?.1);
        for (partition, receiver) in receivers.iter().enumerate() {
            let rows = drain_blocks(receiver)
                .map(|block| block.num_rows())
                .sum::<usize>();
            assert_eq!(rows, if partition == 1 { 0 } else { 25 });
        }

        // Closing a partition again must not affect the other threads.
        writer.stop_sending_partition(1);
        writer.stop_sending_all();
        assert!(writer.all_closed());
        assert!(receivers.iter().all(|x| x.is_closed()));
        Ok(())
    }

    #[test]
    fn test_stopping_one_writer_keeps_other_writers_sending() -> Result<()> {
        let (nodes, receivers) = local_nodes(1, 2);
        let mut first = writer(nodes.clone(), usize::MAX);
        let mut second = writer(nodes, usize::MAX);

        first.stop_sending_all();
        assert!(first.all_closed());
        assert!(!second.all_closed());
        assert!(receivers.iter().all(|receiver| !receiver.is_closed()));

        second.add_block(make_block(0..100))?;
        assert!(!second.flush(None)?.1);
        for (partition, receiver) in receivers.iter().enumerate() {
            let rows = drain_blocks(receiver)
                .map(|block| {
                    check_rows(&block, partition as u64, 2);
                    block.num_rows()
                })
                .sum::<usize>();
            assert_eq!(rows, 50);
        }

        second.stop_sending_all();
        assert!(receivers.iter().all(|receiver| receiver.is_closed()));
        Ok(())
    }

    fn create_mock_exchange(
        num_threads: usize,
    ) -> (
        PingPongTransport,
        async_channel::Receiver<FlightData>,
        async_channel::Sender<std::result::Result<FlightData, Status>>,
    ) {
        let (send_tx, send_rx) = async_channel::bounded(1);
        let (pong_tx, pong_rx) = async_channel::unbounded();
        let exchange = PingPongTransport::from_stream(
            num_threads,
            send_tx,
            pong_rx,
            "query-node-0",
            "query-node-1",
        );
        (exchange, send_rx, pong_tx)
    }

    /// Blocks without columns or with only NULL columns keep their row count over
    /// the wire, with and without LZ4 body compression.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_remote_zero_size_blocks_keep_rows() -> Result<()> {
        let runtime = Runtime::with_worker_threads(2, None)?;
        let blocks = [
            DataBlock::new(vec![], 10),
            DataBlock::new(
                vec![BlockEntry::new_const_column(
                    DataType::Null,
                    Scalar::Null,
                    10,
                )],
                10,
            ),
        ];

        for block in blocks {
            for compression in [
                None,
                Some(FlightCompression::Lz4),
                Some(FlightCompression::Zstd),
            ] {
                let (exchange, send_rx, _pong_tx) = create_mock_exchange(1);
                let buffer = Arc::new(ExchangeSendBuffer::create(
                    vec![exchange],
                    ExchangeSendBufferConfig::default(),
                    &runtime,
                )?);

                let schema = Arc::new(block.infer_schema());
                let arrow_schema = Arc::new(ArrowSchema::from(schema.as_ref()));
                let dispatcher =
                    HashShuffleDispatcher::remote(0, buffer, compression, arrow_schema.clone())?;
                let mut sends = Vec::new();
                dispatcher.prepare_send(0, &mut [vec![block.clone()]], &mut sends)?;
                for (_, result) in futures::future::join_all(sends).await {
                    result?;
                }

                let item = send_rx.recv().await.unwrap();
                let decoded = deserialize_flight_data(strip_tid(item), &schema, &arrow_schema)?;
                assert_eq!(decoded.num_rows(), 10);
                assert_eq!(decoded.num_columns(), block.num_columns());
            }
        }
        Ok(())
    }

    /// The rows of all threads of a remote node travel in one frame, each item
    /// tagged with its destination thread, uncompressed or with a compressed body.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_writer_sends_one_frame_per_remote_node() -> Result<()> {
        for compression in [
            None,
            Some(FlightCompression::Lz4),
            Some(FlightCompression::Zstd),
        ] {
            let runtime = Runtime::with_worker_threads(2, None)?;
            let (exchange, send_rx, _pong_tx) = create_mock_exchange(4);
            let buffer = Arc::new(ExchangeSendBuffer::create(
                vec![exchange],
                ExchangeSendBufferConfig::default(),
                &runtime,
            )?);

            let schema = Arc::new(make_block(0..1).infer_schema());
            let arrow_schema = Arc::new(ArrowSchema::from(schema.as_ref()));

            let weak_buffer = Arc::downgrade(&buffer);
            let (mut nodes, _receivers) = local_nodes(1, 4);
            nodes.push(ShuffleNode {
                partitions: 4..8,
                dispatcher: HashShuffleDispatcher::remote(
                    0,
                    buffer,
                    compression,
                    arrow_schema.clone(),
                )?,
            });

            let mut writer = writer(nodes, usize::MAX);
            writer.add_block(make_block(0..400))?;
            writer.add_block(make_block(400..800))?;
            assert!(!writer.flush(None)?.1);

            let frame = send_rx.recv().await.unwrap();
            assert!(send_rx.try_recv().is_err(), "expected a single frame");

            let items = split_batch_flight_data(frame);
            assert_eq!(items.len(), 4);

            let mut total = 0;
            for item in items {
                let tid = extract_tid(&item) as u64;
                let block = deserialize_flight_data(strip_tid(item), &schema, &arrow_schema)?;
                check_rows(&block, 4 + tid, 8);
                total += block.num_rows();
            }
            assert_eq!(total, 400);

            assert!(!writer.all_closed());
            writer.stop_sending_all();
            assert!(writer.all_closed());
            assert!(
                weak_buffer.upgrade().is_none(),
                "stopped dispatcher must release its send buffer"
            );

            writer.add_block(make_block(800..1200))?;
            assert!(!writer.flush(None)?.1);
            assert!(
                send_rx.try_recv().is_err(),
                "closed node must not receive frames"
            );
        }
        Ok(())
    }
}
