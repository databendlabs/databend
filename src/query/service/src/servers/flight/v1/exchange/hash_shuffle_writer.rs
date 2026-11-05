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
//! appended to per-partition builders as they arrive. String views retain shared
//! input buffers while buffering. Before publication, direct, local and remote
//! outputs compact sparse string backing buffers; near-dense outputs may keep sharing.
//! This bounds the physical String leaves visited by compaction at publication,
//! not arbitrary later slices, opaque metadata, constants or peak memory usage.
//! Partitions are not sent one by one when they fill up: the sender
//! buffers until it reaches `budget` bytes and then sends every partition at once.
//! Shared buffers count in every retaining partition, conservatively reaching the
//! budget sooner than their unique allocation size. An input block may overshoot
//! the budget. Every remote node receives the
//! blocks of all its threads in one call, which the send buffer packs into shared
//! frames.
//!
//! A block that carries a meta and no rows, such as the states of a partial
//! aggregate, is routed whole by [`FlightScatter::route_meta_block`] instead, and
//! its parts wait for the next send under the same budget.
//!
//! While one round is sending, the sender keeps buffering its input into the next
//! round, so its upstream (e.g. a storage scan) keeps running. It waits only when
//! that round reaches the budget too, so at most two rounds are held at once.

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
    /// Bytes of the buffered rows and of the routed blocks.
    buffered_bytes: usize,
    /// Blocks routed whole to each partition, sent after its rows.
    routed: Vec<Vec<DataBlock>>,
    routed_blocks: usize,
    routed_bytes: usize,
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
            stream: BlockPartitionStream::create_with_string_views(0, 0, num_partitions),
            num_partitions,
            buffered_rows: 0,
            buffered_bytes: 0,
            routed: vec![vec![]; num_partitions],
            routed_blocks: 0,
            routed_bytes: 0,
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

    /// Append the rows of `block` to their partitions, sharing string buffers. Returns true when the
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
        self.buffered_bytes = self.stream.memory_size() + self.routed_bytes;
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
            let bytes = block_owned_bytes(&part);
            self.routed_bytes += bytes;
            self.buffered_bytes += bytes;
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
        self.routed_bytes = 0;

        let mut rows: Vec<Option<DataBlock>> = vec![None; self.num_partitions];
        for (partition, block) in self.stream.take_partitions(&HashSet::new()) {
            if !block.is_empty() {
                rows[partition] = Some(block);
            }
        }

        // Keep the rows of the direct-output partition out of both local and remote delivery.
        let own_block = own
            .and_then(|partition| rows[partition].take())
            .map(DataBlock::compact_string_buffers);
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
    use arrow_ipc::writer::IpcWriteOptions;
    use arrow_schema::Schema as ArrowSchema;
    use bumpalo::Bump;
    use databend_common_base::runtime::Runtime;
    use databend_common_expression::AggregateHashTable;
    use databend_common_expression::BlockEntry;
    use databend_common_expression::BlockMetaInfo;
    use databend_common_expression::BlockMetaInfoDowncast;
    use databend_common_expression::BlockProfileStatistics;
    use databend_common_expression::Column;
    use databend_common_expression::DataField;
    use databend_common_expression::DataSchema;
    use databend_common_expression::FromData;
    use databend_common_expression::HashTableConfig;
    use databend_common_expression::PayloadFlushState;
    use databend_common_expression::ProbeState;
    use databend_common_expression::ProjectedBlock;
    use databend_common_expression::Scalar;
    use databend_common_expression::ScalarRef;
    use databend_common_expression::aggregate_function::RawAggregateCall;
    use databend_common_expression::types::AccessType;
    use databend_common_expression::types::ArgType;
    use databend_common_expression::types::ArrayColumn;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::StringType;
    use databend_common_expression::types::UInt64Type;
    use databend_common_expression::types::encoded::DictColumn;
    use databend_common_expression::types::encoded::EncodedColumn;
    use databend_common_expression::types::encoded::NarrowBuffer;
    use databend_common_expression::types::nullable::NullableColumn;
    use databend_common_expression::types::string::StringColumn;
    use databend_common_functions::aggregates::AGGR_REGISTRY;
    use databend_common_settings::FlightCompression;
    use tonic::Status;

    use super::*;
    use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
    use crate::pipelines::processors::transforms::aggregator::AggregatePayload;
    use crate::pipelines::processors::transforms::aggregator::AggregateScatter;
    use crate::pipelines::processors::transforms::aggregator::AggregateStatesMeta;
    use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
    use crate::pipelines::processors::transforms::aggregator::PartitionedData;
    use crate::servers::flight::v1::exchange::block_coalescer::BlockCoalescer;
    use crate::servers::flight::v1::exchange::block_coalescer::CoalesceThreshold;
    use crate::servers::flight::v1::exchange::serde;
    use crate::servers::flight::v1::network::PingPongTransport;
    use crate::servers::flight::v1::network::inbound::ExchangeReceiveChannels;
    use crate::servers::flight::v1::network::inbound::exchange_queue::QueueItem;
    use crate::servers::flight::v1::network::outbound::ExchangeSendBuffer;
    use crate::servers::flight::v1::network::outbound::ExchangeSendBufferConfig;
    use crate::servers::flight::v1::network::outbound::create_local_block_senders;
    use crate::servers::flight::v1::network::wire_format::deserialize_flight_data;
    use crate::servers::flight::v1::network::wire_format::encode_block_for_thread;
    use crate::servers::flight::v1::network::wire_format::extract_tid;
    use crate::servers::flight::v1::network::wire_format::make_ipc_options;
    use crate::servers::flight::v1::network::wire_format::serialize_block_for_thread;
    use crate::servers::flight::v1::network::wire_format::split_batch_flight_data;
    use crate::servers::flight::v1::network::wire_format::strip_tid;
    use crate::servers::flight::v1::packets::DataPacket;

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
        let ids = values(block);
        assert!(ids.iter().all(|x| x % num_partitions == partition));
        assert_eq!(
            strings(block),
            ids.iter()
                .map(|id| format!("a fairly long string payload {id:08}"))
                .collect::<Vec<_>>()
        );
    }

    fn assert_block_values(actual: &DataBlock, expected: &DataBlock) {
        assert_eq!(actual.num_rows(), expected.num_rows());
        assert_eq!(actual.num_columns(), expected.num_columns());
        assert_eq!(actual.infer_schema(), expected.infer_schema());
        for (actual, expected) in actual.columns().iter().zip(expected.columns()) {
            assert_eq!(actual.to_column(), expected.to_column());
        }
    }

    fn string_column(block: &DataBlock, offset: usize) -> StringColumn {
        StringType::try_downcast_column(&block.get_by_offset(offset).to_column()).unwrap()
    }

    fn generic_roundtrips(
        block: &DataBlock,
        options: &IpcWriteOptions,
    ) -> Result<[(usize, DataBlock); 2]> {
        let schema = Arc::new(block.infer_schema());
        let arrow_schema = Arc::new(ArrowSchema::from(schema.as_ref()));
        let mut ordinary = serialize_block_for_thread(block.clone(), 0, options)?;
        assert_eq!(ordinary.len(), 1);
        let ordinary = ordinary.pop().unwrap();
        let ordinary_bytes = ordinary.data_body.len();
        let ordinary = deserialize_flight_data(strip_tid(ordinary), &schema, &arrow_schema)?;
        let mut legacy = serde::serialize_block(0, block.clone(), options)?;
        let mut legacy =
            serde::ExchangeSerializeMeta::downcast_from(legacy.take_meta().unwrap()).unwrap();
        let Some(DataPacket::FragmentData(fragment)) = legacy.packet.pop() else {
            unreachable!()
        };
        let legacy_bytes = fragment.data.data_body.len();
        let legacy = serde::deserialize_block(legacy.packet, fragment, &schema, arrow_schema)?;
        Ok([(ordinary_bytes, ordinary), (legacy_bytes, legacy)])
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

    #[test]
    fn test_writer_shares_near_dense_buffers_for_direct_and_local_delivery() -> Result<()> {
        let source = make_block(0..6);
        let column = source.get_by_offset(1).to_column();
        let strings = StringType::try_downcast_column(&column).unwrap();
        let buffer = strings.data_buffers()[0].as_ptr();
        let (nodes, receivers) = local_nodes(1, 2);
        let mut writer = writer(nodes, usize::MAX);
        writer.add_block(source)?;
        let (own, pending) = writer.flush(Some(0))?;
        assert!(!pending);
        drop(writer);
        let local = drain_blocks(&receivers[1]).next().unwrap();
        for (block, ids) in [(own.unwrap(), vec![0, 2, 4]), (local, vec![1, 3, 5])] {
            let expected = DataBlock::new_from_columns(vec![
                UInt64Type::from_data(ids.clone()),
                StringType::from_data(
                    ids.iter()
                        .map(|id| format!("a fairly long string payload {id:08}"))
                        .collect::<Vec<_>>(),
                ),
            ]);
            assert_block_values(&block, &expected);
            assert_eq!(string_column(&block, 1).data_buffers()[0].as_ptr(), buffer);
        }
        Ok(())
    }

    #[test]
    fn test_local_hash_then_generic_remote_compacts_at_producer() -> Result<()> {
        // Without producer-boundary compaction, three small local partitions retain the unused
        // 64 KiB source and copy it at each generic edge. Compacting hash outputs cuts that chain.
        // This is a producer-snapshot guarantee, not a bound after arbitrary later selections.
        let backing_bytes = 65536;
        let source = DataBlock::new_from_columns(vec![
            UInt64Type::from_data(vec![1, 0, 2, 3, 5, 4, 6, 7]),
            StringType::from_data(vec![
                "z".repeat(backing_bytes),
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "d".to_string(),
                "e".to_string(),
                "f".to_string(),
                "g".to_string(),
            ]),
        ]);
        let column = source.get_by_offset(1).to_column();
        let source_strings = StringType::try_downcast_column(&column).unwrap();
        assert_eq!(source_strings.data_buffers().len(), 1);
        assert_eq!(source_strings.data_buffers()[0].len(), backing_bytes);
        // The same input bypassing hash still preserves its representation at generic edges.
        let plain = source.slice(1..2);
        for (body_bytes, decoded) in generic_roundtrips(&plain, &make_ipc_options(None)?)? {
            assert_block_values(&decoded, &plain);
            assert!(body_bytes >= backing_bytes);
            assert_eq!(decoded.memory_size(), backing_bytes + 24);
        }
        let (nodes, receivers) = local_nodes(1, 4);
        let mut writer = writer(nodes, usize::MAX);
        writer.add_block(source)?;
        let (own, pending) = writer.flush(Some(0))?;
        assert!(!pending);
        drop(writer);
        assert!(drain_blocks(&receivers[0]).next().is_none());
        let forwarded = [
            (own.unwrap(), vec![0, 4], vec!["a", "e"]),
            (
                drain_blocks(&receivers[2]).next().unwrap(),
                vec![2, 6],
                vec!["b", "f"],
            ),
            (
                drain_blocks(&receivers[3]).next().unwrap(),
                vec![3, 7],
                vec!["c", "g"],
            ),
        ];
        for (block, ids, names) in forwarded {
            let expected = DataBlock::new_from_columns(vec![
                UInt64Type::from_data(ids),
                StringType::from_data(names),
            ]);
            // Independently constructed inline-only rows: 2 * (8-byte id + 16-byte view).
            assert_eq!(expected.memory_size(), 48);
            assert_block_values(&block, &expected);
            assert_eq!(block.memory_size(), 48);
            assert!(string_column(&block, 1).data_buffers().is_empty());
            let mut coalescer =
                BlockCoalescer::create(Some(CoalesceThreshold::create(8192, 4 * 1024 * 1024)));
            coalescer.push(block)?;
            assert!(coalescer.pop().is_none());
            assert!(coalescer.flush()?);
            let block = coalescer.pop().unwrap();
            assert_eq!(block.memory_size(), 48);
            for compression in [
                None,
                Some(FlightCompression::Lz4),
                Some(FlightCompression::Zstd),
            ] {
                for (body_bytes, decoded) in
                    generic_roundtrips(&block, &make_ipc_options(compression)?)?
                {
                    assert_block_values(&decoded, &expected);
                    assert!(body_bytes < 512);
                    assert_eq!(decoded.memory_size(), 48);
                    assert!(string_column(&decoded, 1).data_buffers().is_empty());
                }
            }
            let schema = Arc::new(block.infer_schema());
            let arrow_schema = Arc::new(ArrowSchema::from(schema.as_ref()));
            let hash =
                encode_block_for_thread(0, block, &arrow_schema, &make_ipc_options(None)?, None)?;
            assert!(hash.data_body.len() < 512);
            let compact = deserialize_flight_data(strip_tid(hash), &schema, &arrow_schema)?;
            assert_block_values(&compact, &expected);
            assert_eq!(compact.memory_size(), 48);
        }
        assert_eq!(source_strings.total_buffer_len(), backing_bytes);
        Ok(())
    }

    #[test]
    fn test_writer_compacts_own_without_other_sends() -> Result<()> {
        let source = StringType::from_data(vec!["z".repeat(1024), "a".to_string()]);
        let input =
            DataBlock::new_from_columns(vec![UInt64Type::from_data(vec![0]), source.slice(1..2)]);
        let expected = DataBlock::new_from_columns(vec![
            UInt64Type::from_data(vec![0]),
            StringType::from_data(vec!["a"]),
        ]);
        let (nodes, receivers) = local_nodes(1, 1);
        let mut writer = writer(nodes, 1);
        assert!(writer.add_block(input.clone())?);
        assert!(writer.buffered_bytes >= 1024);
        let (own, pending) = writer.flush(Some(0))?;
        assert!(!pending);
        let own = own.unwrap();
        assert_block_values(&own, &expected);
        assert_eq!(own.memory_size(), 24);
        assert!(receivers[0].is_empty());
        assert_eq!(writer.buffered_bytes, 0);
        assert_eq!(string_column(&input, 1).total_buffer_len(), 1024);
        Ok(())
    }

    #[test]
    fn test_writer_local_density_boundary_and_tail() -> Result<()> {
        for junk_bytes in [95, 96, 97] {
            for own in [None, Some(0)] {
                let source = DataBlock::new_from_columns(vec![
                    UInt64Type::from_data(vec![1, 0]),
                    StringType::from_data(vec!["z".repeat(junk_bytes), "r".repeat(32)]),
                ]);
                let original = string_column(&source, 1);
                let (nodes, receivers) = local_nodes(1, 2);
                let mut writer = writer(nodes, 1);
                assert!(writer.add_block(source.clone())?);
                // The source is charged in both retaining partitions before any publication GC.
                assert!(writer.buffered_bytes >= 2 * (junk_bytes + 32));
                let (direct, pending) = writer.flush(own)?;
                assert!(!pending);
                let selected =
                    direct.unwrap_or_else(|| drain_blocks(&receivers[0]).next().unwrap());
                let expected = DataBlock::new_from_columns(vec![
                    UInt64Type::from_data(vec![0]),
                    StringType::from_data(vec!["r".repeat(32)]),
                ]);
                assert_block_values(&selected, &expected);
                let selected = string_column(&selected, 1);
                if junk_bytes < 96 {
                    assert_eq!(selected.total_buffer_len(), 127);
                    assert_eq!(
                        selected.data_buffers()[0].as_ptr(),
                        original.data_buffers()[0].as_ptr()
                    );
                } else {
                    assert_eq!(selected.total_buffer_len(), 32);
                    assert_ne!(
                        selected.data_buffers()[0].as_ptr(),
                        original.data_buffers()[0].as_ptr()
                    );
                }
                let other = drain_blocks(&receivers[1]).next().unwrap();
                assert_block_values(
                    &other,
                    &DataBlock::new_from_columns(vec![
                        UInt64Type::from_data(vec![1]),
                        StringType::from_data(vec!["z".repeat(junk_bytes)]),
                    ]),
                );
                assert_eq!(original.total_buffer_len(), junk_bytes + 32);
                assert_eq!(writer.buffered_bytes, 0);
            }
        }
        Ok(())
    }

    #[test]
    fn test_writer_compacts_repeated_buffer_lists() -> Result<()> {
        let source = DataBlock::new_from_columns(vec![
            UInt64Type::from_data(vec![1, 0, 0]),
            StringType::from_data(vec!["z".repeat(1024), "r".repeat(32), "a".to_string()]),
        ]);
        let original = string_column(&source, 1);
        let other = DataBlock::new_from_columns(vec![
            UInt64Type::from_data(vec![1, 0, 0]),
            StringType::from_data(vec!["y".repeat(2048), "r".repeat(32), "a".to_string()]),
        ]);
        let retained = 2 * 1056 + 2080;
        let mut probe = BlockPartitionStream::create_with_string_views(0, 0, 2);
        for input in [source.clone(), source.clone(), other.clone()] {
            assert!(probe.partition(vec![1, 0, 0], input, false).is_empty());
        }
        let repeated = probe.finalize_partition(0).unwrap();
        let repeated = string_column(&repeated, 1);
        assert_eq!(repeated.data_buffers().len(), 3);
        assert_eq!(repeated.total_buffer_len(), retained);
        assert_eq!(
            repeated.data_buffers()[0].as_ptr(),
            repeated.data_buffers()[1].as_ptr()
        );
        assert_ne!(
            repeated.data_buffers()[0].as_ptr(),
            repeated.data_buffers()[2].as_ptr()
        );
        for own in [None, Some(0)] {
            let (nodes, receivers) = local_nodes(1, 2);
            let mut writer = writer(nodes, usize::MAX);
            writer.add_block(source.clone())?;
            writer.add_block(source.clone())?;
            writer.add_block(other.clone())?;
            assert!(writer.buffered_bytes >= 2 * retained);
            let (direct, pending) = writer.flush(own)?;
            assert!(!pending);
            let selected = direct.unwrap_or_else(|| drain_blocks(&receivers[0]).next().unwrap());
            let expected = DataBlock::new_from_columns(vec![
                UInt64Type::from_data(vec![0, 0, 0, 0, 0, 0]),
                StringType::from_data(vec![
                    "r".repeat(32),
                    "a".to_string(),
                    "r".repeat(32),
                    "a".to_string(),
                    "r".repeat(32),
                    "a".to_string(),
                ]),
            ]);
            assert_block_values(&selected, &expected);
            assert_eq!(string_column(&selected, 1).total_buffer_len(), 96);
            let dense = drain_blocks(&receivers[1]).next().unwrap();
            assert_block_values(
                &dense,
                &DataBlock::new_from_columns(vec![
                    UInt64Type::from_data(vec![1, 1, 1]),
                    StringType::from_data(vec![
                        "z".repeat(1024),
                        "z".repeat(1024),
                        "y".repeat(2048),
                    ]),
                ]),
            );
            assert_eq!(string_column(&dense, 1).total_buffer_len(), retained);
            assert_eq!(original.total_buffer_len(), 1056);
        }
        Ok(())
    }

    #[test]
    fn test_writer_pending_own_compaction_keeps_next_round_charged() -> Result<()> {
        use crate::servers::flight::v1::network::outbound::LOCAL_CHANNEL_MAX_BYTES;

        let (nodes, receivers) = local_nodes(1, 2);
        let mut writer = writer(nodes, 2048);
        let fill = DataBlock::new_from_columns(vec![
            UInt64Type::from_data(vec![1]),
            StringType::from_data(vec!["q".repeat(LOCAL_CHANNEL_MAX_BYTES)]),
        ]);
        assert!(writer.add_block(fill.clone())?);
        assert!(!writer.flush(None)?.1);
        let input = DataBlock::new_from_columns(vec![
            UInt64Type::from_data(vec![1, 0]),
            StringType::from_data(vec!["z".repeat(1024), "a".to_string()]),
        ]);
        assert!(writer.add_block(input.clone())?);
        let (own, pending) = writer.flush(Some(0))?;
        assert!(pending);
        let expected_own = DataBlock::new_from_columns(vec![
            UInt64Type::from_data(vec![0]),
            StringType::from_data(vec!["a"]),
        ]);
        let own = own.unwrap();
        assert_block_values(&own, &expected_own);
        assert_eq!(own.memory_size(), 24);
        assert!(string_column(&own, 1).data_buffers().is_empty());
        assert!(writer.poll_sending(true)?);
        // Another round is still charged for both source-buffer references, not its future GC size.
        assert!(writer.add_block(input)?);
        assert!(writer.buffered_bytes >= 2 * 1024);
        assert!(writer.budget_reached());
        let first = drain_blocks(&receivers[1]).next().unwrap();
        assert_block_values(&first, &fill);
        assert!(!writer.poll_sending(true)?);
        let second = drain_blocks(&receivers[1]).next().unwrap();
        let expected_other = DataBlock::new_from_columns(vec![
            UInt64Type::from_data(vec![1]),
            StringType::from_data(vec!["z".repeat(1024)]),
        ]);
        assert_block_values(&second, &expected_other);
        assert!(!writer.flush(None)?.1);
        let tail = drain_blocks(&receivers[0]).next().unwrap();
        assert_block_values(&tail, &expected_own);
        assert_eq!(tail.memory_size(), 24);
        assert_block_values(
            &drain_blocks(&receivers[1]).next().unwrap(),
            &expected_other,
        );
        assert_eq!(writer.buffered_bytes, 0);
        Ok(())
    }

    #[test]
    fn test_later_slice_is_outside_producer_density_guarantee() -> Result<()> {
        let source = DataBlock::new_from_columns(vec![
            UInt64Type::from_data(vec![0, 0]),
            StringType::from_data(vec!["a".to_string(), "z".repeat(65536)]),
        ]);
        let (nodes, _) = local_nodes(1, 1);
        let mut writer = writer(nodes, usize::MAX);
        writer.add_block(source.clone())?;
        let published = writer.flush(Some(0))?.0.unwrap();
        assert_block_values(&published, &source);
        assert_eq!(string_column(&published, 1).total_buffer_len(), 65536);
        // Generic forwarding deliberately does not re-enforce density after a later selection.
        let sliced = published.slice(0..1);
        let expected = DataBlock::new_from_columns(vec![
            UInt64Type::from_data(vec![0]),
            StringType::from_data(vec!["a"]),
        ]);
        for (body_bytes, decoded) in generic_roundtrips(&sliced, &make_ipc_options(None)?)? {
            assert_block_values(&decoded, &expected);
            assert!(body_bytes >= 65536);
            assert_eq!(string_column(&decoded, 1).total_buffer_len(), 65536);
        }
        Ok(())
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

    /// A test route that materializes ordinary columns from a meta-only input, like remote
    /// aggregate state routing. No production metadata/protocol type is added.
    struct MaterializedScatter {
        blocks: Vec<DataBlock>,
    }

    impl FlightScatter for MaterializedScatter {
        fn name(&self) -> &'static str {
            "Materialized"
        }

        fn execute(&self, _: DataBlock) -> Result<Vec<DataBlock>> {
            unreachable!()
        }

        fn scatter_indices(&self, block: &DataBlock) -> Result<Option<Vec<u64>>> {
            ModuloScatter {
                num_partitions: self.blocks.len() as u64,
            }
            .scatter_indices(block)
        }

        fn route_meta_block(&self, _: DataBlock) -> Result<Option<Vec<DataBlock>>> {
            Ok(Some(self.blocks.clone()))
        }
    }

    #[test]
    fn test_local_routed_columns_and_own_routed_are_compacted() -> Result<()> {
        let source =
            StringType::from_data(vec!["z".repeat(1024), "a".to_string(), "b".to_string()]);
        let routed = DataBlock::new_with_meta(
            vec![source.slice(1..3).into()],
            2,
            Some(AggregateStatesMeta::create(
                Arc::new(DataSchema::new(vec![DataField::new(
                    "state",
                    DataType::String,
                )])),
                vec![7],
                vec![2],
            )),
        );
        let retained = routed.memory_size();
        let scatter: Arc<Box<dyn FlightScatter>> = Arc::new(Box::new(MaterializedScatter {
            blocks: vec![routed.clone(), routed.clone()],
        }));
        for own in [None, Some(0)] {
            let (nodes, receivers) = local_nodes(1, 2);
            let mut writer =
                HashShuffleWriter::create(0, ExecutorWaker::create(), scatter.clone(), nodes, 1);
            assert!(writer.add_block(states_block(0, retained))?);
            assert_eq!(writer.routed_bytes, 2 * retained);
            assert_eq!(writer.buffered_bytes, 2 * retained);
            let raw = DataBlock::new_from_columns(vec![
                UInt64Type::from_data(vec![0, 1]),
                StringType::from_data(vec!["c", "d"]),
            ]);
            writer.add_block(raw.clone())?;
            assert_eq!(writer.buffered_bytes, 2 * retained + raw.memory_size());
            let (direct, pending) = writer.flush(own)?;
            if let Some(direct) = direct {
                assert_block_values(&direct, &raw.slice(0..1));
            } else {
                assert!(own.is_none());
            }
            assert!(!pending);
            for (partition, receiver) in receivers.iter().enumerate() {
                let mut outputs = drain_blocks(receiver);
                if own != Some(partition) {
                    assert_block_values(
                        &outputs.next().unwrap(),
                        &raw.slice(partition..partition + 1),
                    );
                }
                let output = outputs.next().unwrap();
                assert_block_values(&output, &routed);
                assert_eq!(output.get_meta(), routed.get_meta());
                assert_eq!(string_column(&output, 0).total_buffer_len(), 0);
                assert!(outputs.next().is_none());
            }
            assert_eq!(writer.buffered_bytes, 0);
            assert_eq!(writer.routed_bytes, 0);
        }
        assert_eq!(routed.memory_size(), retained);
        Ok(())
    }

    #[test]
    fn test_routed_nested_scope_and_closed_destination() -> Result<()> {
        let values = StringType::from_data(vec!["z".repeat(1024), "r".repeat(32), "a".to_string()])
            .slice(1..3);
        let keys = NarrowBuffer::U8(vec![1, 1].into());
        let nested = Column::Tuple(vec![
            NullableColumn::new_column(values.clone(), [false, true].into_iter().collect()),
            Column::Array(Box::new(ArrayColumn::new(
                values.clone(),
                vec![0, 1, 2].into(),
            ))),
            Column::Map(Box::new(ArrayColumn::new(
                Column::Tuple(vec![values.clone(), UInt64Type::from_data(vec![7, 8])]),
                vec![0, 1, 2].into(),
            ))),
            Column::Encoded(Box::new(EncodedColumn::Dict(DictColumn::new(
                keys.clone(),
                values.clone(),
            )))),
        ]);
        let block = DataBlock::new_with_meta(
            vec![
                nested.into(),
                BlockEntry::new_const_column(
                    DataType::Array(Box::new(DataType::String)),
                    Scalar::Array(values.clone()),
                    2,
                ),
            ],
            2,
            Some(Box::new(())),
        );
        let scatter: Arc<Box<dyn FlightScatter>> = Arc::new(Box::new(MaterializedScatter {
            blocks: vec![block.clone(), block.clone()],
        }));
        let (nodes, receivers) = local_nodes(1, 2);
        receivers[1].close();
        let mut writer = HashShuffleWriter::create(0, ExecutorWaker::create(), scatter, nodes, 1);
        assert!(writer.add_block(states_block(0, block.memory_size()))?);
        let (own, pending) = writer.flush(Some(0))?;
        assert!(own.is_none()); // own routed blocks must use the local dispatcher, not direct output.
        assert!(!pending);
        let output = drain_blocks(&receivers[0]).next().unwrap();
        assert_block_values(&output, &block);
        assert_eq!(output.get_meta(), block.get_meta());
        assert!(receivers[1].is_empty());
        let Column::Tuple(fields) = output.get_by_offset(0).to_column() else {
            unreachable!()
        };
        let Column::Nullable(nullable) = &fields[0] else {
            unreachable!()
        };
        assert_eq!(nullable.validity.iter().collect::<Vec<_>>(), vec![
            false, true
        ]);
        // The invalid position's physical noninline value is included in the producer bound.
        let child = StringType::try_downcast_column(&nullable.column).unwrap();
        assert_eq!(child.iter().collect::<Vec<_>>(), vec![
            "r".repeat(32),
            "a".to_string()
        ]);
        assert_eq!(child.total_buffer_len(), 32);
        let Column::Array(array) = &fields[1] else {
            unreachable!()
        };
        assert_eq!(
            StringType::try_downcast_column(&array.underlying_column())
                .unwrap()
                .total_buffer_len(),
            32
        );
        let Column::Map(map) = &fields[2] else {
            unreachable!()
        };
        let Column::Tuple(entries) = map.underlying_column() else {
            unreachable!()
        };
        assert_eq!(
            StringType::try_downcast_column(&entries[0])
                .unwrap()
                .total_buffer_len(),
            32
        );
        let Column::Encoded(encoded) = &fields[3] else {
            unreachable!()
        };
        let EncodedColumn::Dict(dict) = encoded.as_ref() else {
            unreachable!()
        };
        assert!(dict.keys.shares_storage(&keys));
        // Keys use only "a", but compaction deliberately keeps the physical dictionary values.
        assert_eq!(
            StringType::try_downcast_column(&dict.values)
                .unwrap()
                .total_buffer_len(),
            32
        );
        let Some(Scalar::Array(constant)) = output.get_by_offset(1).as_scalar() else {
            unreachable!()
        };
        assert_eq!(
            StringType::try_downcast_column(constant)
                .unwrap()
                .total_buffer_len(),
            1056
        );
        assert_eq!(
            StringType::try_downcast_column(&values)
                .unwrap()
                .total_buffer_len(),
            1056
        );
        Ok(())
    }

    #[test]
    fn test_local_publication_preserves_real_aggregate_states() -> Result<()> {
        let group_type = UInt64Type::data_type();
        let params = AggregatorParams::try_create(
            Arc::new(DataSchema::new(vec![DataField::new(
                "key",
                group_type.clone(),
            )])),
            vec![group_type.clone()],
            &[0],
            &[],
            &[],
            true,
            1024,
            1024 * 1024,
        )?;
        let keys = DataBlock::new_from_columns(vec![UInt64Type::from_data(
            (0..256).collect::<Vec<u64>>(),
        )]);
        let mut table = AggregateHashTable::new(
            vec![group_type],
            vec![],
            HashTableConfig::partial_aggregate(),
            Arc::new(Bump::new()),
        );
        table.add_groups(
            &mut ProbeState::default(),
            ProjectedBlock::project(&[0], &keys),
            &[],
            (&[]).into(),
            keys.num_rows(),
        )?;
        let payloads = table
            .payload
            .into_non_empty_bucket_payloads()
            .map(|(bucket, payload)| AggregatePayload {
                bucket: bucket as isize,
                payload,
                max_partition_count: 0,
            })
            .collect();
        let block = DataBlock::empty_with_meta(AggregateMeta::create_partitioned(
            None,
            PartitionedData::AggregatePayload(payloads),
        ));
        let scatter: Arc<Box<dyn FlightScatter>> =
            Arc::new(Box::new(AggregateScatter::create(4, 0..4, params)));
        let (nodes, receivers) = local_nodes(1, 4);
        let mut writer = HashShuffleWriter::create(0, ExecutorWaker::create(), scatter, nodes, 1);
        assert!(writer.add_block(block)?);
        let (own, pending) = writer.flush(Some(0))?;
        assert!(own.is_none());
        assert!(!pending);
        let mut delivered = vec![];
        for receiver in &receivers {
            for mut block in drain_blocks(receiver) {
                assert_eq!(block.num_columns(), 0);
                assert_eq!(
                    block_owned_bytes(&block),
                    block.get_meta().unwrap().output_stats().unwrap().bytes
                );
                let AggregateMeta::Partitioned {
                    data: PartitionedData::AggregatePayload(payloads),
                    ..
                } = AggregateMeta::downcast_from(block.take_meta().unwrap()).unwrap()
                else {
                    unreachable!()
                };
                for payload in payloads {
                    let flushed = payload.payload.aggregate_flush_all()?;
                    delivered.extend(
                        UInt64Type::try_downcast_column(&flushed.get_by_offset(0).to_column())
                            .unwrap()
                            .iter()
                            .copied(),
                    );
                }
            }
        }
        delivered.sort_unstable();
        assert_eq!(delivered, (0..256).collect::<Vec<u64>>());
        Ok(())
    }

    #[test]
    fn test_local_aggregate_string_states_outlive_producers() -> Result<()> {
        const GROUPS: usize = 16;
        const ITEMS: usize = 3;
        let key = |group| format!("{}-{group:02}", "group".repeat(8));
        let value = |group, item| format!("{}-{group:02}-{item}", "value".repeat(8));
        let (receivers, own_partition) = {
            let function = AGGR_REGISTRY.resolve(RawAggregateCall {
                name: "array_agg",
                params: &[],
                args_type: &[DataType::String],
                distinct: false,
                order_by: &[],
            })?;
            assert!(function.state().need_manual_drop());
            assert_eq!(
                function.signature().return_type,
                DataType::Array(Box::new(DataType::String))
            );
            let params = AggregatorParams::try_create(
                Arc::new(DataSchema::new(vec![
                    DataField::new("key", DataType::String),
                    DataField::new("value", DataType::String),
                ])),
                vec![DataType::String],
                &[0],
                std::slice::from_ref(&function),
                &[vec![1]],
                true,
                1024,
                1024 * 1024,
            )?;
            let input = DataBlock::new_from_columns(vec![
                StringType::from_data(
                    (0..GROUPS * ITEMS)
                        .map(|row| key(row % GROUPS))
                        .collect::<Vec<_>>(),
                ),
                StringType::from_data(
                    (0..GROUPS * ITEMS)
                        .map(|row| value(row % GROUPS, row / GROUPS))
                        .collect::<Vec<_>>(),
                ),
            ]);
            let arena = Arc::new(Bump::new());
            let mut table = AggregateHashTable::new(
                vec![DataType::String],
                params.aggregate_functions.clone(),
                HashTableConfig::partial_aggregate(),
                arena.clone(),
            );
            table.add_groups(
                &mut ProbeState::default(),
                ProjectedBlock::project(&[0], &input),
                &[ProjectedBlock::project(&[1], &input)],
                (&[]).into(),
                input.num_rows(),
            )?;
            assert_eq!(table.len(), GROUPS);
            let payloads = table
                .take_partial_hot()
                .into_non_empty_bucket_payloads()
                .map(|(bucket, payload)| AggregatePayload {
                    bucket: bucket as isize,
                    payload,
                    max_partition_count: 0,
                })
                .collect();
            // The moved payload, not the original table or input, must keep keys and states alive.
            drop(table);
            drop(input);
            drop(arena);
            let block = DataBlock::empty_with_meta(AggregateMeta::create_partitioned(
                None,
                PartitionedData::AggregatePayload(payloads),
            ));
            let scatter: Arc<Box<dyn FlightScatter>> =
                Arc::new(Box::new(AggregateScatter::create(4, 0..4, params.clone())));
            let own_partition = scatter
                .scatter_indices(&DataBlock::new_from_columns(vec![StringType::from_data(
                    vec![key(0)],
                )]))?
                .unwrap()[0] as usize;
            let (nodes, receivers) = local_nodes(1, 4);
            let mut writer =
                HashShuffleWriter::create(0, ExecutorWaker::create(), scatter.clone(), nodes, 1);
            assert!(writer.add_block(block)?);
            let (own, pending) = writer.flush(Some(own_partition))?;
            assert!(own.is_none());
            assert!(!pending);
            // Queued payloads must survive all producer owners, including the own routed path.
            drop(writer);
            drop(scatter);
            drop(params);
            drop(function);
            (receivers, own_partition)
        };
        let mut delivered = vec![];
        for (partition, receiver) in receivers.iter().enumerate() {
            assert!(receiver.is_closed());
            let mut received_groups = 0;
            for mut block in drain_blocks(receiver) {
                assert_eq!(block.num_columns(), 0);
                assert_eq!(
                    block_owned_bytes(&block),
                    block.get_meta().unwrap().output_stats().unwrap().bytes
                );
                let AggregateMeta::Partitioned {
                    data: PartitionedData::AggregatePayload(payloads),
                    ..
                } = AggregateMeta::downcast_from(block.take_meta().unwrap()).unwrap()
                else {
                    unreachable!()
                };
                for payload in payloads {
                    let mut state = PayloadFlushState::default();
                    while let Some(result) = payload.payload.merge_result(&mut state)? {
                        received_groups += result.num_rows();
                        assert_eq!(result.num_columns(), 2);
                        assert_eq!(
                            result.get_by_offset(0).data_type(),
                            DataType::Array(Box::new(DataType::String))
                        );
                        assert_eq!(result.get_by_offset(1).data_type(), DataType::String);
                        for row in 0..result.num_rows() {
                            let Some(ScalarRef::String(group)) = result.value_at(1, row) else {
                                unreachable!()
                            };
                            let Some(ScalarRef::Array(items)) = result.value_at(0, row) else {
                                unreachable!()
                            };
                            let mut values = items
                                .iter()
                                .map(|item| {
                                    let ScalarRef::String(value) = item else {
                                        unreachable!()
                                    };
                                    value.to_string()
                                })
                                .collect::<Vec<_>>();
                            assert_eq!(values.len(), ITEMS);
                            // No ORDER BY was requested; compare the full multiset, not state order.
                            values.sort_unstable();
                            delivered.push((group.to_string(), values));
                        }
                    }
                }
            }
            assert!(receiver.is_empty());
            if partition == own_partition {
                assert!(received_groups > 0);
            }
        }
        delivered.sort_unstable();
        let mut expected = (0..GROUPS)
            .map(|group| {
                let mut values = (0..ITEMS)
                    .map(|item| value(group, item))
                    .collect::<Vec<_>>();
                values.sort_unstable();
                (key(group), values)
            })
            .collect::<Vec<_>>();
        expected.sort_unstable();
        assert_eq!(delivered, expected);
        Ok(())
    }

    /// Rows buffered after routed blocks still count the bytes of the routed blocks, as when a
    /// partial aggregate emits its states and then forwards raw rows.
    #[test]
    fn test_writer_counts_routed_bytes_with_rows() -> Result<()> {
        let (nodes, _receivers) = local_nodes(1, 4);
        let scatter: Arc<Box<dyn FlightScatter>> =
            Arc::new(Box::new(StatesScatter { num_partitions: 4 }));
        let states_bytes = 1 << 20;
        let rows = make_block(0..100);
        assert!(rows.memory_size() < states_bytes);
        let mut writer =
            HashShuffleWriter::create(0, ExecutorWaker::create(), scatter, nodes, states_bytes + 1);

        assert!(!writer.add_block(states_block(2, states_bytes))?);
        // The rows alone are far below the budget, the rows and the states reach it.
        assert!(writer.add_block(rows)?);
        assert!(writer.buffered_bytes > states_bytes);

        writer.flush(None)?;
        assert_eq!(writer.buffered_bytes, 0);
        assert_eq!(writer.routed_bytes, 0);
        Ok(())
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
