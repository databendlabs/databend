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
//! Blocks whose rows cannot be addressed (e.g. the states of a partial
//! aggregate) are routed whole by the scatter, one block per partition, and wait
//! in a per-partition send queue. The queue resizes them to about `budget / P`
//! bytes: a large block is split into pieces sent over successive flushes, and
//! small meta-only blocks are merged, through the hooks of their `BlockMetaInfo`.
//! Each flush sends at most one piece per partition.

use std::collections::HashSet;
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;
use std::task::Poll;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockPartitionStream;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::ExecutorWaker;
use petgraph::graph::NodeIndex;

use crate::servers::flight::v1::network::SyncTaskHandle;
use crate::servers::flight::v1::network::SyncTaskSet;
use crate::servers::flight::v1::network::outbound::HashShuffleDispatcher;
use crate::servers::flight::v1::network::outbound::ShuffleSendAddress;
use crate::servers::flight::v1::network::outbound::ShuffleSendFuture;
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
    budget: usize,
    /// The target size of what one flush sends to a partition.
    piece_bytes: usize,
    /// The pieces waiting to be sent to each partition, with their sizes.
    queues: Vec<VecDeque<(usize, DataBlock)>>,
    queued_bytes: usize,
    queued_pieces: usize,
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
            budget,
            piece_bytes: (budget / num_partitions.max(1)).max(1),
            queues: (0..num_partitions).map(|_| VecDeque::new()).collect(),
            queued_bytes: 0,
            queued_pieces: 0,
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

    /// Buffer the rows of `block` in their partitions, or route it whole and
    /// queue its blocks when the scatter cannot index its rows. Returns
    /// `needs_flush()`.
    pub fn add_block(&mut self, block: DataBlock) -> Result<bool> {
        if block.is_empty() && block.get_meta().is_none() {
            return Ok(false);
        }

        // The partition builders store constants row by row, so count them expanded:
        // a constant's memory size is a single value.
        let block = block.consume_convert_to_full();

        match self.scatter.scatter_indices(&block)? {
            Some(partition_ids) => {
                self.buffered_rows += block.num_rows();
                self.buffered_bytes += block.memory_size();
                self.stream.partition(partition_ids, block, false);
            }
            None => {
                let blocks = self.scatter.execute(block)?;
                if blocks.len() != self.num_partitions {
                    return Err(ErrorCode::Internal(format!(
                        "{} scatter returned {} blocks for {} partitions",
                        self.scatter.name(),
                        blocks.len(),
                        self.num_partitions
                    )));
                }
                for (partition, block) in blocks.into_iter().enumerate() {
                    self.enqueue(partition, block)?;
                }
            }
        }
        Ok(self.needs_flush())
    }

    /// True when the writer should flush before taking more input: the buffered
    /// and queued data reached the budget, or a partition has more than one piece
    /// waiting.
    pub fn needs_flush(&self) -> bool {
        self.buffered_bytes.saturating_add(self.queued_bytes) >= self.budget
            || self.queues.iter().any(|queue| queue.len() > 1)
    }

    /// True while there is data left to flush.
    pub fn has_queued(&self) -> bool {
        self.queued_pieces != 0 || self.buffered_rows != 0
    }

    /// Queue `block` for `partition` in pieces of about `piece_bytes`.
    fn enqueue(&mut self, partition: usize, block: DataBlock) -> Result<()> {
        // A block holding its data in the meta reports its size through it.
        let stats = block.profile_statistics();
        if stats.rows == 0 {
            return Ok(());
        }

        // Leave blocks up to twice the piece size whole, so that a block only
        // slightly over it is not cut into a full piece and a sliver.
        if stats.bytes <= self.piece_bytes.saturating_mul(2) {
            return self.push_piece(partition, stats.bytes, block);
        }

        for piece in self.split(block, stats.rows, stats.bytes)? {
            let bytes = piece.profile_statistics().bytes;
            self.push_piece(partition, bytes, piece)?;
        }
        Ok(())
    }

    fn split(&self, mut block: DataBlock, rows: usize, bytes: usize) -> Result<Vec<DataBlock>> {
        if is_meta_only(&block) {
            let mut meta = block.take_meta().unwrap();
            let rest = meta.split(self.piece_bytes)?;
            return Ok(std::iter::once(meta)
                .chain(rest)
                .map(DataBlock::empty_with_meta)
                .collect());
        }

        // The meta of a block with columns describes the block as a whole.
        if block.get_meta().is_some() {
            return Ok(vec![block]);
        }

        let pieces = bytes.div_ceil(self.piece_bytes).min(rows);
        Ok(block.split_by_rows_no_tail(rows.div_ceil(pieces)))
    }

    /// Append `block` to the queue of `partition`, merged into the last piece
    /// when both are meta-only and fit in a piece together.
    fn push_piece(&mut self, partition: usize, bytes: usize, mut block: DataBlock) -> Result<()> {
        self.queued_bytes += bytes;

        let piece_bytes = self.piece_bytes;
        let queue = &mut self.queues[partition];
        if let Some((back_bytes, back)) = queue.back_mut() {
            if is_meta_only(back) && is_meta_only(&block) && *back_bytes + bytes <= piece_bytes {
                let meta = block.take_meta().unwrap();
                match back.mut_meta().unwrap().merge(meta)? {
                    None => {
                        *back_bytes += bytes;
                        return Ok(());
                    }
                    Some(meta) => block = DataBlock::empty_with_meta(meta),
                }
            }
        }

        queue.push_back((bytes, block));
        self.queued_pieces += 1;
        Ok(())
    }

    /// Send one piece of every partition, after queueing the buffered rows.
    /// Callers flush until `has_queued()` is false to send everything.
    ///
    /// The rows of partition `own` are returned instead of sent, so the caller can
    /// push them to its own output. Returns whether sends are still pending.
    pub fn flush(&mut self, own: Option<usize>) -> Result<(Option<DataBlock>, bool)> {
        debug_assert!(self.handle.is_none());

        if self.buffered_rows != 0 {
            self.buffered_rows = 0;
            self.buffered_bytes = 0;
            for (partition, block) in self.stream.take_partitions(&HashSet::new()) {
                if !block.is_empty() {
                    self.enqueue(partition, block)?;
                }
            }
        }

        if self.queued_pieces == 0 {
            return Ok((None, false));
        }

        let mut partitions = Vec::with_capacity(self.num_partitions);
        for queue in &mut self.queues {
            let Some((bytes, block)) = queue.pop_front() else {
                partitions.push(None);
                continue;
            };
            self.queued_bytes -= bytes;
            self.queued_pieces -= 1;
            partitions.push(Some(block));
        }

        // Keep the direct-output partition out of both local and remote delivery.
        let own_block = own.and_then(|partition| partitions[partition].take());
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
            "handle_pending={}, closed_partitions={}/{}, buffered_rows={}, buffered_bytes={}, queued_pieces={}, queued_bytes={}, budget={}",
            self.handle.is_some(),
            closed,
            self.num_partitions,
            self.buffered_rows,
            self.buffered_bytes,
            self.queued_pieces,
            self.queued_bytes,
            self.budget,
        )
    }
}

/// A block carrying its data only in the meta, which knows how to split and
/// merge it.
fn is_meta_only(block: &DataBlock) -> bool {
    block.num_columns() == 0 && block.num_rows() == 0 && block.get_meta().is_some()
}

#[cfg(test)]
mod tests {
    use arrow_flight::FlightData;
    use arrow_schema::Schema as ArrowSchema;
    use databend_common_base::runtime::Runtime;
    use databend_common_expression::BlockEntry;
    use databend_common_expression::BlockMetaInfo;
    use databend_common_expression::BlockMetaInfoDowncast;
    use databend_common_expression::BlockMetaInfoPtr;
    use databend_common_expression::BlockProfileStatistics;
    use databend_common_expression::FromData;
    use databend_common_expression::Scalar;
    use databend_common_expression::local_block_meta_serde;
    use databend_common_expression::types::AccessType;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::StringType;
    use databend_common_expression::types::UInt64Type;
    use databend_common_settings::FlightCompression;
    use tonic::Status;

    use super::*;
    use crate::servers::flight::v1::network::HashShuffleCodec;
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

    /// Values carried only in meta, standing in for a producer's own block form.
    #[derive(Debug)]
    struct ValuesMeta(Vec<u64>);

    local_block_meta_serde!(ValuesMeta);

    impl BlockMetaInfo for ValuesMeta {
        fn typetag_deserialize(&self) {
            unimplemented!("ValuesMeta is local to the test")
        }

        fn typetag_name(&self) -> &'static str {
            unimplemented!("ValuesMeta is local to the test")
        }

        fn output_stats(&self) -> Option<BlockProfileStatistics> {
            Some(BlockProfileStatistics {
                rows: self.0.len(),
                bytes: self.0.len() * size_of::<u64>(),
            })
        }

        fn split(&mut self, max_bytes: usize) -> Result<Vec<BlockMetaInfoPtr>> {
            let per_part = (max_bytes / size_of::<u64>()).max(1);
            if self.0.len() <= per_part {
                return Ok(vec![]);
            }
            let rest = self.0.split_off(per_part);
            Ok(rest
                .chunks(per_part)
                .map(|values| Box::new(ValuesMeta(values.to_vec())) as BlockMetaInfoPtr)
                .collect())
        }

        fn merge(&mut self, other: BlockMetaInfoPtr) -> Result<Option<BlockMetaInfoPtr>> {
            match ValuesMeta::downcast_from_err(other) {
                Ok(other) => {
                    self.0.extend(other.0);
                    Ok(None)
                }
                Err(other) => Ok(Some(other)),
            }
        }
    }

    /// Routes value `v` to partition `v % num_partitions`, keeping the values of
    /// a partition in the meta of its block.
    struct ValuesScatter {
        num_partitions: usize,
    }

    impl FlightScatter for ValuesScatter {
        fn name(&self) -> &'static str {
            "Values"
        }

        fn execute(&self, mut block: DataBlock) -> Result<Vec<DataBlock>> {
            let values = block
                .take_meta()
                .and_then(ValuesMeta::downcast_from)
                .unwrap()
                .0;
            let mut partitions = vec![vec![]; self.num_partitions];
            for value in values {
                partitions[value as usize % self.num_partitions].push(value);
            }
            Ok(partitions
                .into_iter()
                .map(|values| DataBlock::empty_with_meta(Box::new(ValuesMeta(values))))
                .collect())
        }
    }

    /// Encodes the values of a partition as a `UInt64` column.
    struct ValuesCodec;

    impl HashShuffleCodec for ValuesCodec {
        fn encode(&self, mut block: DataBlock) -> Result<DataBlock> {
            let values = block
                .take_meta()
                .and_then(ValuesMeta::downcast_from)
                .unwrap()
                .0;
            Ok(DataBlock::new_from_columns(vec![UInt64Type::from_data(
                values,
            )]))
        }

        fn decode(&self, block: DataBlock) -> Result<DataBlock> {
            Ok(DataBlock::empty_with_meta(Box::new(ValuesMeta(values(
                &block,
            )))))
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

    fn meta_values(block: &DataBlock) -> &[u64] {
        &block
            .get_meta()
            .and_then(ValuesMeta::downcast_ref_from)
            .expect("local partitions must receive the producer's form")
            .0
    }

    fn values_writer(nodes: Vec<ShuffleNode>, budget: usize) -> HashShuffleWriter {
        let num_partitions = nodes.last().unwrap().partitions.end;
        let scatter: Arc<Box<dyn FlightScatter>> =
            Arc::new(Box::new(ValuesScatter { num_partitions }));
        HashShuffleWriter::create(0, ExecutorWaker::create(), scatter, nodes, budget)
    }

    fn values_block(range: std::ops::Range<u64>) -> DataBlock {
        DataBlock::empty_with_meta(Box::new(ValuesMeta(range.collect())))
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

    /// Constant columns count at the size the partition builders receive, not the
    /// size of their single value, so they still reach the budget.
    #[test]
    fn test_writer_counts_constants_expanded() -> Result<()> {
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
            HashShuffleWriter::create(0, ExecutorWaker::create(), scatter, nodes, expanded);

        assert!(writer.add_block(block())?);
        assert_eq!(writer.buffered_bytes, expanded);
        // The partition builders copy each string, so the send queue may split
        // the partitions into pieces sent over several flushes.
        while writer.has_queued() {
            writer.flush(None)?;
        }

        let rows = receivers
            .iter()
            .flat_map(drain_blocks)
            .map(|block| block.num_rows())
            .sum::<usize>();
        assert_eq!(rows, 1000);
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
                let dispatcher = HashShuffleDispatcher::remote(
                    0,
                    buffer,
                    compression,
                    arrow_schema.clone(),
                    None,
                )?;
                let mut sends = Vec::new();
                dispatcher.prepare_send(0, &mut [Some(block.clone())], &mut sends)?;
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

    /// Blocks routed whole are resized to `budget / P` per partition: small ones
    /// are merged until the budget is reached, and a large one is split into
    /// pieces sent one per partition per flush, all in the producer's form.
    #[test]
    fn test_writer_merges_and_splits_blocks_routed_whole() -> Result<()> {
        // 4 partitions of 80 bytes, i.e. 10 values, each.
        let (nodes, receivers) = local_nodes(1, 4);
        let mut writer = values_writer(nodes, 320);

        assert!(!writer.add_block(values_block(0..20))?);
        assert!(writer.add_block(values_block(20..40))?);
        let (own, pending) = writer.flush(Some(0))?;
        assert!(!pending);
        assert!(!writer.has_queued());

        let own = own.expect("the own partition must be returned");
        assert_eq!(meta_values(&own), (0..40).step_by(4).collect::<Vec<_>>());
        for (partition, receiver) in receivers.iter().enumerate().skip(1) {
            let blocks = drain_blocks(receiver).collect::<Vec<_>>();
            assert_eq!(blocks.len(), 1, "small blocks must be merged");
            assert_eq!(
                meta_values(&blocks[0]),
                (partition as u64..40).step_by(4).collect::<Vec<_>>()
            );
        }

        assert!(writer.add_block(values_block(0..160))?);
        let mut received = vec![vec![]; 4];
        for _ in 0..4 {
            assert!(writer.needs_flush());
            assert!(!writer.flush(None)?.1);
            for (partition, receiver) in receivers.iter().enumerate() {
                let blocks = drain_blocks(receiver).collect::<Vec<_>>();
                assert_eq!(blocks.len(), 1, "one piece per partition per flush");
                assert_eq!(meta_values(&blocks[0]).len(), 10);
                received[partition].extend_from_slice(meta_values(&blocks[0]));
            }
        }
        assert!(!writer.has_queued());
        for (partition, values) in received.iter().enumerate() {
            assert_eq!(
                values,
                &(partition as u64..160).step_by(4).collect::<Vec<_>>()
            );
        }
        Ok(())
    }

    /// The sender's own partition is returned, the other local ones are delivered
    /// in the producer's form, and remote ones are encoded by the codec.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_writer_encodes_blocks_routed_whole_for_remote_nodes() -> Result<()> {
        let runtime = Runtime::with_worker_threads(2, None)?;
        let (exchange, send_rx, pong_tx) = create_mock_exchange(2);
        let buffer = Arc::new(ExchangeSendBuffer::create(
            vec![exchange],
            ExchangeSendBufferConfig::default(),
            &runtime,
        )?);

        let schema = Arc::new(
            DataBlock::new_from_columns(vec![UInt64Type::from_data(vec![0_u64])]).infer_schema(),
        );
        let arrow_schema = Arc::new(ArrowSchema::from(schema.as_ref()));
        let codec: Arc<dyn HashShuffleCodec> = Arc::new(ValuesCodec);

        let (mut nodes, receivers) = local_nodes(1, 2);
        nodes.push(ShuffleNode {
            partitions: 2..4,
            dispatcher: HashShuffleDispatcher::remote(
                0,
                buffer,
                None,
                arrow_schema.clone(),
                Some(codec.clone()),
            )?,
        });

        // 4 partitions of 5 values each fill the budget.
        let mut writer = values_writer(nodes, 160);
        assert!(writer.add_block(values_block(0..20))?);
        let (own, pending) = writer.flush(Some(0))?;
        assert!(!pending);

        let own = own.expect("the own partition must be returned");
        assert_eq!(meta_values(&own), (0..20).step_by(4).collect::<Vec<_>>());
        assert_eq!(drain_blocks(&receivers[0]).count(), 0);

        let local = drain_blocks(&receivers[1]).collect::<Vec<_>>();
        assert_eq!(local.len(), 1);
        assert_eq!(
            meta_values(&local[0]),
            (1..20).step_by(4).collect::<Vec<_>>()
        );

        let items = split_batch_flight_data(send_rx.recv().await.unwrap());
        assert_eq!(items.len(), 2);
        for item in items {
            let tid = extract_tid(&item) as u64;
            let block = deserialize_flight_data(strip_tid(item), &schema, &arrow_schema)?;
            assert_eq!(values(&block), (2 + tid..20).step_by(4).collect::<Vec<_>>());
        }
        pong_tx.send(Ok(FlightData::default())).await.unwrap();

        writer.stop_sending_all();
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
                    None,
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
