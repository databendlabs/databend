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

use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;
use std::task::Poll;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockPartitionStream;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::ExecutorWaker;
use futures::future::BoxFuture;
use petgraph::graph::NodeIndex;

use crate::servers::flight::v1::network::OutboundChannel;
use crate::servers::flight::v1::network::RemoteNodeChannel;
use crate::servers::flight::v1::network::SyncTaskHandle;
use crate::servers::flight::v1::network::SyncTaskSet;
use crate::servers::flight::v1::scatter::FlightScatter;

/// Per-thread send buffer: the share of `node_budget` of each local sender.
pub fn hash_shuffle_send_budget(local_threads: usize, node_budget: usize) -> usize {
    (node_budget / local_threads.max(1)).max(1)
}

#[derive(Clone)]
pub enum ShuffleTarget {
    /// Threads of the local node, one channel per thread. `None` once closed.
    Local(Vec<Option<Arc<dyn OutboundChannel>>>),
    /// All threads of a remote node, reached through one channel. `None` once closed.
    Remote(Option<Arc<RemoteNodeChannel>>),
}

/// The threads of one node, which own partitions `partitions`.
#[derive(Clone)]
pub struct ShuffleNode {
    pub partitions: Range<usize>,
    pub target: ShuffleTarget,
}

#[derive(Clone, Copy, Debug)]
struct SendTarget {
    node: usize,
    /// Thread of a local node; remote nodes are sent to as a whole.
    thread: Option<usize>,
}

type SendResults = Vec<(SendTarget, Result<()>)>;

pub struct HashShuffleWriter {
    id: NodeIndex,
    tasks: SyncTaskSet,
    handle: Option<SyncTaskHandle<'static, SendResults>>,

    scatter: Arc<Box<dyn FlightScatter>>,
    stream: BlockPartitionStream,
    num_partitions: usize,
    buffered_bytes: usize,
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
            buffered_bytes: 0,
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

    /// Copy the rows of `block` into their partitions. Returns true when the
    /// buffered rows reached the budget and should be flushed.
    pub fn add_block(&mut self, block: DataBlock) -> Result<bool> {
        if block.is_empty() {
            return Ok(false);
        }

        let Some(partition_ids) = self.scatter.scatter_indices(&block)? else {
            return Err(ErrorCode::Internal(format!(
                "Logical error, {} scatter cannot be used for hash shuffle",
                self.scatter.name()
            )));
        };

        self.buffered_bytes += block.memory_size();
        self.stream.partition(partition_ids, block, false);
        Ok(self.buffered_bytes >= self.budget)
    }

    /// Send every buffered partition.
    ///
    /// The rows of partition `own` are returned instead of sent, so the caller can
    /// push them to its own output. Returns whether sends are still pending.
    pub fn flush(&mut self, own: Option<usize>) -> Result<(Option<DataBlock>, bool)> {
        debug_assert!(self.handle.is_none());

        if self.buffered_bytes == 0 {
            return Ok((None, false));
        }
        self.buffered_bytes = 0;

        let mut partitions: Vec<Option<DataBlock>> = vec![None; self.num_partitions];
        for (partition, block) in self.stream.take_partitions(&HashSet::new()) {
            if !block.is_empty() {
                partitions[partition] = Some(block);
            }
        }

        let mut own_block = None;
        let mut futures: Vec<BoxFuture<'static, (SendTarget, Result<()>)>> = vec![];

        for (node_idx, node) in self.nodes.iter().enumerate() {
            let blocks = &mut partitions[node.partitions.clone()];

            match &node.target {
                ShuffleTarget::Remote(None) => {}
                ShuffleTarget::Remote(Some(channel)) => {
                    if channel.is_closed() {
                        continue;
                    }

                    // Encode now, so that only compressed bytes wait for the network.
                    let mut items = Vec::with_capacity(blocks.len());
                    for (thread, block) in blocks.iter_mut().enumerate() {
                        if let Some(block) = block.take() {
                            items.push(channel.encode(thread, block)?);
                        }
                    }

                    if items.is_empty() {
                        continue;
                    }

                    let channel = channel.clone();
                    let target = SendTarget {
                        node: node_idx,
                        thread: None,
                    };
                    futures.push(Box::pin(
                        async move { (target, channel.add_items(items).await) },
                    ));
                }
                ShuffleTarget::Local(channels) => {
                    for (thread, block) in blocks.iter_mut().enumerate() {
                        let Some(block) = block.take() else {
                            continue;
                        };

                        if own == Some(node.partitions.start + thread) {
                            own_block = Some(block);
                            continue;
                        }

                        let Some(channel) = &channels[thread] else {
                            continue;
                        };

                        if channel.is_closed() {
                            continue;
                        }

                        let channel = channel.clone();
                        let target = SendTarget {
                            node: node_idx,
                            thread: Some(thread),
                        };
                        futures.push(Box::pin(
                            async move { (target, channel.add_block(block).await) },
                        ));
                    }
                }
            }
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

    fn handle_results(&mut self, results: SendResults) -> Result<()> {
        for (target, result) in results {
            match result {
                Ok(()) => {}
                Err(cause) if cause.code() == ErrorCode::ABORTED_QUERY => self.close(target),
                Err(cause) => return Err(cause),
            }
        }

        Ok(())
    }

    fn close(&mut self, target: SendTarget) {
        // Closed destinations still own the shared send buffer. Release their
        // references so other destinations can receive EOF before the graph drops.
        match (&mut self.nodes[target.node].target, target.thread) {
            (ShuffleTarget::Local(channels), Some(thread)) => {
                if let Some(channel) = channels[thread].take() {
                    channel.close();
                }
            }
            (ShuffleTarget::Local(channels), None) => {
                for channel in channels.iter_mut().filter_map(Option::take) {
                    channel.close();
                }
            }
            (ShuffleTarget::Remote(channel), _) => {
                channel.take();
            }
        }
    }

    /// Close the channel of `partition`, which must belong to the local node.
    pub fn close_partition(&mut self, partition: usize) {
        for (node_idx, node) in self.nodes.iter().enumerate() {
            if node.partitions.contains(&partition) {
                return self.close(SendTarget {
                    node: node_idx,
                    thread: Some(partition - node.partitions.start),
                });
            }
        }
    }

    pub fn close_all(&mut self) {
        for node in 0..self.nodes.len() {
            self.close(SendTarget { node, thread: None });
        }
    }

    /// True when no destination can accept data any more.
    pub fn all_closed(&self) -> bool {
        self.nodes.iter().all(|node| match &node.target {
            ShuffleTarget::Local(channels) => channels
                .iter()
                .all(|x| x.as_ref().is_none_or(|x| x.is_closed())),
            ShuffleTarget::Remote(channel) => channel.as_ref().is_none_or(|x| x.is_closed()),
        })
    }

    pub fn details_status(&self) -> String {
        let closed = self
            .nodes
            .iter()
            .map(|node| match &node.target {
                ShuffleTarget::Local(channels) => channels
                    .iter()
                    .filter(|x| x.as_ref().is_none_or(|x| x.is_closed()))
                    .count(),
                ShuffleTarget::Remote(channel) => match channel.as_ref() {
                    Some(channel) if !channel.is_closed() => 0,
                    _ => node.partitions.len(),
                },
            })
            .sum::<usize>();

        format!(
            "handle_pending={}, closed_partitions={}/{}, buffered_bytes={}, budget={}",
            self.handle.is_some(),
            closed,
            self.num_partitions,
            self.buffered_bytes,
            self.budget,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;

    use arrow_flight::FlightData;
    use arrow_schema::Schema as ArrowSchema;
    use databend_common_base::runtime::Runtime;
    use databend_common_expression::FromData;
    use databend_common_expression::types::AccessType;
    use databend_common_expression::types::StringType;
    use databend_common_expression::types::UInt64Type;
    use databend_common_settings::FlightCompression;
    use parking_lot::Mutex;
    use tonic::Status;

    use super::*;
    use crate::servers::flight::v1::network::ExchangeBufferConfig;
    use crate::servers::flight::v1::network::ExchangeSinkBuffer;
    use crate::servers::flight::v1::network::PingPongExchange;
    use crate::servers::flight::v1::network::inbound_channel::deserialize_flight_data;
    use crate::servers::flight::v1::network::inbound_channel::extract_tid;
    use crate::servers::flight::v1::network::inbound_channel::split_batch_flight_data;
    use crate::servers::flight::v1::network::inbound_channel::strip_tid;

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

    #[derive(Default)]
    struct CollectChannel {
        closed: AtomicBool,
        blocks: Mutex<Vec<DataBlock>>,
    }

    #[async_trait::async_trait]
    impl OutboundChannel for CollectChannel {
        fn close(&self) {}

        fn is_closed(&self) -> bool {
            self.closed.load(Ordering::SeqCst)
        }

        async fn add_block(&self, block: DataBlock) -> Result<()> {
            self.blocks.lock().push(block);
            Ok(())
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
    ) -> (Vec<ShuffleNode>, Vec<Arc<CollectChannel>>) {
        let mut nodes = vec![];
        let mut collectors = vec![];
        for node in 0..num_nodes {
            let channels = (0..threads)
                .map(|_| {
                    let channel = Arc::new(CollectChannel::default());
                    collectors.push(channel.clone());
                    Some(channel as Arc<dyn OutboundChannel>)
                })
                .collect();

            nodes.push(ShuffleNode {
                partitions: node * threads..(node + 1) * threads,
                target: ShuffleTarget::Local(channels),
            });
        }
        (nodes, collectors)
    }

    fn writer(nodes: Vec<ShuffleNode>, budget: usize) -> HashShuffleWriter {
        let num_partitions = nodes.last().unwrap().partitions.end as u64;
        let scatter: Arc<Box<dyn FlightScatter>> =
            Arc::new(Box::new(ModuloScatter { num_partitions }));
        HashShuffleWriter::create(0, ExecutorWaker::create(), scatter, nodes, budget)
    }

    #[test]
    fn test_send_budget() {
        assert_eq!(hash_shuffle_send_budget(16, 256 << 20), 16 << 20);
        assert_eq!(hash_shuffle_send_budget(16, 1), 1);
        assert_eq!(hash_shuffle_send_budget(0, 1024), 1024);
    }

    /// 30 nodes x 16 threads: every row reaches its partition exactly once, and the
    /// writer flushes whenever it buffers the budget.
    #[test]
    fn test_writer_routes_rows_within_budget() -> Result<()> {
        let (num_nodes, threads) = (30, 16);
        let num_partitions = (num_nodes * threads) as u64;
        let (nodes, collectors) = local_nodes(num_nodes, threads);

        let budget = 256 * 1024;
        let mut writer = writer(nodes, budget);

        let mut flushes = 0;
        for idx in 0..100 {
            let block = make_block(idx * 1000..(idx + 1) * 1000);
            let block_bytes = block.memory_size();

            if writer.add_block(block)? {
                assert!(writer.buffered_bytes < budget + block_bytes);
                assert!(!writer.flush(None)?.1);
                assert_eq!(writer.buffered_bytes, 0);
                flushes += 1;
            }
        }
        writer.flush(None)?;
        assert!(flushes > 1, "budget should trigger flushes before the end");

        let mut total = 0;
        for (partition, collector) in collectors.iter().enumerate() {
            for block in collector.blocks.lock().iter() {
                check_rows(block, partition as u64, num_partitions);
                total += block.num_rows();
            }
        }
        assert_eq!(total, 100_000);
        Ok(())
    }

    #[test]
    fn test_writer_returns_own_partition_and_skips_closed() -> Result<()> {
        let (nodes, collectors) = local_nodes(1, 4);
        let mut writer = writer(nodes, usize::MAX);

        collectors[3].closed.store(true, Ordering::SeqCst);
        writer.add_block(make_block(0..100))?;

        let (own, pending) = writer.flush(Some(1))?;
        assert!(!pending);
        check_rows(&own.unwrap(), 1, 4);

        assert!(collectors[1].blocks.lock().is_empty());
        assert!(collectors[3].blocks.lock().is_empty());
        for partition in [0, 2] {
            let blocks = collectors[partition].blocks.lock();
            assert_eq!(blocks.iter().map(|x| x.num_rows()).sum::<usize>(), 25);
        }

        assert!(!writer.all_closed());
        writer.close_all();
        assert!(writer.all_closed());
        Ok(())
    }

    fn create_mock_exchange(
        num_threads: usize,
    ) -> (
        PingPongExchange,
        async_channel::Receiver<FlightData>,
        async_channel::Sender<std::result::Result<FlightData, Status>>,
    ) {
        let (send_tx, send_rx) = async_channel::bounded(1);
        let (pong_tx, pong_rx) = async_channel::unbounded();
        let exchange = PingPongExchange::from_stream(
            num_threads,
            send_tx,
            pong_rx,
            "query-node-0",
            "query-node-1",
        );
        (exchange, send_rx, pong_tx)
    }

    /// The rows of all threads of a remote node travel in one frame, each item
    /// tagged with its destination thread, with and without LZ4 body compression.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_writer_sends_one_frame_per_remote_node() -> Result<()> {
        for compression in [None, Some(FlightCompression::Lz4)] {
            let runtime = Runtime::with_worker_threads(2, None)?;
            let (exchange, send_rx, _pong_tx) = create_mock_exchange(4);
            let buffer = Arc::new(ExchangeSinkBuffer::create(
                vec![exchange],
                ExchangeBufferConfig::default(),
                &runtime,
            )?);

            let schema = Arc::new(make_block(0..1).infer_schema());
            let arrow_schema = Arc::new(ArrowSchema::from(schema.as_ref()));

            let (mut nodes, _collectors) = local_nodes(1, 4);
            nodes.push(ShuffleNode {
                partitions: 4..8,
                target: ShuffleTarget::Remote(Some(RemoteNodeChannel::create(
                    0,
                    buffer,
                    compression,
                    arrow_schema.clone(),
                )?)),
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
        }
        Ok(())
    }
}
