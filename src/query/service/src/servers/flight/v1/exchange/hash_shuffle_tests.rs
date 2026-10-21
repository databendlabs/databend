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

//! End-to-end tests of the hash shuffle over a simulated network.
//!
//! The senders of node 0 shuffle to its own threads and to `nodes - 1` remote
//! nodes through the real writer, send buffer, local channels and receivers. Each
//! remote connection is a ping-pong exchange served by a "network" thread, which
//! delays a frame by half the round trip plus its transfer time, hands it to the
//! remote node's inbound channels, and answers after the other half.
//!
//! The tests check results and memory only. `hash_shuffle_bench` measures speed
//! locally and is not run in CI:
//! `cargo test --release -p databend-query --lib -- --ignored --nocapture hash_shuffle_bench`

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use arrow_flight::FlightData;
use arrow_schema::Schema as ArrowSchema;
use databend_common_base::runtime::GlobalStatBuffer;
use databend_common_base::runtime::MemStat;
use databend_common_base::runtime::MemStatBuffer;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::ThreadTracker;
use databend_common_column::bitmap::Bitmap;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::Decimal128Type;
use databend_common_expression::types::Float64Type;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::decimal::DecimalSize;
use databend_common_pipeline::core::ExecutorWaker;
use databend_common_settings::FlightCompression;
use parking_lot::Mutex;
use proptest::prelude::*;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use tonic::Status;

use super::broadcast_recv_transform::BlockCoalescer;
use super::broadcast_recv_transform::CoalesceThreshold;
use super::hash_shuffle_writer::HashShuffleWriter;
use super::hash_shuffle_writer::ShuffleNode;
use super::hash_shuffle_writer::ShuffleTarget;
use super::hash_shuffle_writer::hash_shuffle_send_budget;
use crate::servers::flight::v1::network::ExchangeBufferConfig;
use crate::servers::flight::v1::network::ExchangeSinkBuffer;
use crate::servers::flight::v1::network::InboundChannel;
use crate::servers::flight::v1::network::NetworkInboundChannelSet;
use crate::servers::flight::v1::network::NetworkInboundSender;
use crate::servers::flight::v1::network::PingPongExchange;
use crate::servers::flight::v1::network::RemoteNodeChannel;
use crate::servers::flight::v1::network::create_local_channels;
use crate::servers::flight::v1::network::inbound_channel::flight_data_size;
use crate::servers::flight::v1::network::local_channel::LOCAL_CHANNEL_MAX_BYTES;
use crate::servers::flight::v1::scatter::FlightScatter;

const MIB: usize = 1024 * 1024;

/// One direction of the link between node 0 and a remote node.
#[derive(Clone, Copy)]
struct Link {
    rtt: Duration,
    /// Unlimited when `None`.
    bytes_per_sec: Option<f64>,
}

struct ShuffleSetup {
    nodes: usize,
    threads: usize,
    node_budget: usize,
    buffer: ExchangeBufferConfig,
    /// Receive quota of each connection.
    recv_quota: usize,
    compression: Option<FlightCompression>,
    coalesce: Option<CoalesceThreshold>,
    link: Link,
}

#[derive(Default)]
struct ShuffleRun {
    elapsed: Duration,
    wire_bytes: AtomicUsize,
    frames: AtomicUsize,
}

/// Publish this thread's buffered allocation stats, so the measured peak is not
/// skewed by threads each holding up to 4 MiB of unpublished deltas.
fn flush_mem_stat() {
    let _ = MemStatBuffer::current().flush::<false>(0);
    let _ = GlobalStatBuffer::current().flush::<false>(0);
}

/// Wakes the sender thread when its sends make progress, like the executor would.
struct ThreadWake(std::thread::Thread);

impl databend_common_pipeline::core::WakeCallback for ThreadWake {
    fn wake(&self, _id: petgraph::stable_graph::NodeIndex, _worker_id: usize) -> Result<()> {
        self.0.unpark();
        Ok(())
    }
}

fn thread_waker() -> Arc<ExecutorWaker> {
    let waker = ExecutorWaker::create();
    waker.bind(Arc::new(ThreadWake(std::thread::current())));
    waker
}

fn network_thread(
    link: Link,
    send_rx: async_channel::Receiver<FlightData>,
    pong_tx: async_channel::Sender<std::result::Result<FlightData, Status>>,
    inbound: NetworkInboundSender,
    run: &ShuffleRun,
) {
    while let Ok(data) = send_rx.recv_blocking() {
        let bytes = flight_data_size(&data);
        run.wire_bytes.fetch_add(bytes, Ordering::Relaxed);
        run.frames.fetch_add(1, Ordering::Relaxed);

        let transfer = link
            .bytes_per_sec
            .map(|bps| Duration::from_secs_f64(bytes as f64 / bps))
            .unwrap_or_default();
        std::thread::sleep(link.rtt / 2 + transfer);

        if futures::executor::block_on(inbound.add_data(data)).is_err() {
            break;
        }
        flush_mem_stat();

        std::thread::sleep(link.rtt / 2);
        if pong_tx.send_blocking(Ok(FlightData::default())).is_err() {
            break;
        }
    }
}

fn receive<O>(
    partition: usize,
    receiver: Arc<dyn InboundChannel>,
    coalesce: Option<CoalesceThreshold>,
    output: &O,
) -> Result<()>
where
    O: Fn(usize, DataBlock),
{
    let mut coalescer = BlockCoalescer::create(coalesce);
    while let Some(block) = futures::executor::block_on(receiver.recv())? {
        coalescer.push(block)?;
        while let Some(block) = coalescer.pop() {
            output(partition, block);
        }
        flush_mem_stat();
    }

    coalescer.flush()?;
    while let Some(block) = coalescer.pop() {
        output(partition, block);
    }
    Ok(())
}

fn flush_writer(writer: &mut HashShuffleWriter) -> Result<()> {
    let (own, mut pending) = writer.flush(None)?;
    assert!(own.is_none());
    while pending {
        std::thread::park_timeout(Duration::from_millis(5));
        pending = writer.poll_sending(true)?;
    }
    Ok(())
}

fn send(
    thread: usize,
    scatter: Arc<Box<dyn FlightScatter>>,
    nodes: Vec<ShuffleNode>,
    budget: usize,
    blocks: impl Iterator<Item = DataBlock>,
) -> Result<()> {
    let mut writer = HashShuffleWriter::create(thread, thread_waker(), scatter, nodes, budget);
    for block in blocks {
        if writer.add_block(block)? {
            flush_writer(&mut writer)?;
        }
        flush_mem_stat();
    }

    flush_writer(&mut writer)?;
    writer.close_all();
    flush_mem_stat();
    Ok(())
}

/// Shuffle the blocks `input(t)` returns for each sender thread `t` of node 0.
/// `output(partition, block)` sees every block the receiver of `partition` outputs;
/// partition `p` is thread `p % threads` of node `p / threads`.
fn run_shuffle<I, It, O>(
    setup: &ShuffleSetup,
    schema: &DataSchemaRef,
    scatter: Box<dyn FlightScatter>,
    input: I,
    output: O,
) -> Result<ShuffleRun>
where
    I: Fn(usize) -> It + Sync,
    It: Iterator<Item = DataBlock>,
    O: Fn(usize, DataBlock) + Sync,
{
    let threads = setup.threads;
    let arrow_schema = Arc::new(ArrowSchema::from(schema.as_ref()));
    let runtime = Runtime::with_worker_threads(2, None)?;

    let local_set = NetworkInboundChannelSet::new(threads);
    let mut receivers = (0..threads)
        .map(|t| (t, local_set.create_receiver(t, schema)))
        .collect::<Vec<_>>();
    let local_channels = create_local_channels(&local_set);
    drop(local_set);

    let mut exchanges = vec![];
    let mut links = vec![];
    for node in 1..setup.nodes {
        let (send_tx, send_rx) = async_channel::bounded(1);
        let (pong_tx, pong_rx) = async_channel::unbounded();
        exchanges.push(PingPongExchange::from_stream(
            threads,
            send_tx,
            pong_rx,
            "node-0",
            format!("node-{node}"),
        ));

        let set = NetworkInboundChannelSet::new(threads);
        receivers
            .extend((0..threads).map(|t| (node * threads + t, set.create_receiver(t, schema))));
        links.push((
            send_rx,
            pong_tx,
            NetworkInboundSender::new(&set, setup.recv_quota),
        ));
    }

    let buffer = Arc::new(ExchangeSinkBuffer::create(
        exchanges,
        setup.buffer.clone(),
        &runtime,
    )?);

    let mut nodes = vec![ShuffleNode {
        partitions: 0..threads,
        target: ShuffleTarget::Local(local_channels.into_iter().map(Some).collect()),
    }];
    for node in 1..setup.nodes {
        nodes.push(ShuffleNode {
            partitions: node * threads..(node + 1) * threads,
            target: ShuffleTarget::Remote(Some(RemoteNodeChannel::create(
                node - 1,
                buffer.clone(),
                setup.compression,
                arrow_schema.clone(),
            )?)),
        });
    }
    drop(buffer);

    let scatter = Arc::new(scatter);
    let budget = hash_shuffle_send_budget(threads, setup.node_budget);
    let mut run = ShuffleRun::default();
    let started = Instant::now();

    std::thread::scope(|scope| -> Result<()> {
        for (send_rx, pong_tx, inbound) in links {
            let (link, run) = (setup.link, &run);
            scope.spawn(ThreadTracker::tracking_function(move || {
                network_thread(link, send_rx, pong_tx, inbound, run)
            }));
        }

        let receivers = receivers
            .into_iter()
            .map(|(partition, receiver)| {
                let (coalesce, output) = (setup.coalesce, &output);
                scope.spawn(ThreadTracker::tracking_function(move || {
                    receive(partition, receiver, coalesce, output)
                }))
            })
            .collect::<Vec<_>>();

        let senders = (0..threads)
            .map(|t| {
                let (scatter, nodes, input) = (scatter.clone(), nodes.clone(), &input);
                scope.spawn(ThreadTracker::tracking_function(move || {
                    send(t, scatter, nodes, budget, input(t))
                }))
            })
            .collect::<Vec<_>>();
        drop(nodes);

        for sender in senders {
            sender.join().unwrap()?;
        }
        for receiver in receivers {
            receiver.join().unwrap()?;
        }
        Ok(())
    })?;

    run.elapsed = started.elapsed();
    drop(runtime);
    flush_mem_stat();
    Ok(run)
}

/// Routes each row to partition `key % num_partitions`, the key being column 0.
struct KeyScatter {
    num_partitions: u64,
}

impl FlightScatter for KeyScatter {
    fn name(&self) -> &'static str {
        "Key"
    }

    fn execute(&self, _data_block: DataBlock) -> Result<Vec<DataBlock>> {
        unreachable!()
    }

    fn scatter_indices(&self, data_block: &DataBlock) -> Result<Option<Vec<u64>>> {
        let column = data_block.get_by_offset(0).to_column();
        let keys = UInt64Type::try_downcast_column(&column).unwrap();
        Ok(Some(keys.iter().map(|x| x % self.num_partitions).collect()))
    }
}

/// Spreads the rows of blocks without columns over the partitions.
struct SpreadScatter {
    num_partitions: u64,
    next: AtomicU64,
}

impl FlightScatter for SpreadScatter {
    fn name(&self) -> &'static str {
        "Spread"
    }

    fn execute(&self, _data_block: DataBlock) -> Result<Vec<DataBlock>> {
        unreachable!()
    }

    fn scatter_indices(&self, data_block: &DataBlock) -> Result<Option<Vec<u64>>> {
        let rows = data_block.num_rows() as u64;
        let start = self.next.fetch_add(rows, Ordering::Relaxed);
        Ok(Some(
            (start..start + rows)
                .map(|x| {
                    x.wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(29) % self.num_partitions
                })
                .collect(),
        ))
    }
}

fn random_string(rng: &mut StdRng, max_len: usize) -> String {
    let len = rng.gen_range(0..=max_len);
    let mut value = String::with_capacity(len);
    while value.len() < len {
        value.push_str(&format!("{:016x}", rng.r#gen::<u64>()));
    }
    value.truncate(len);
    value
}

/// A full column of `kind`, NULL with a random density when `nullable`.
fn random_column(kind: u8, nullable: bool, rows: usize, rng: &mut StdRng) -> Column {
    let column = match kind {
        0 => UInt64Type::from_data((0..rows).map(|_| rng.r#gen::<u64>()).collect::<Vec<_>>()),
        1 => Int32Type::from_data((0..rows).map(|_| rng.r#gen::<i32>()).collect::<Vec<_>>()),
        2 => Float64Type::from_data((0..rows).map(|_| rng.r#gen::<f64>()).collect::<Vec<_>>()),
        3 => StringType::from_data(
            (0..rows)
                .map(|_| random_string(rng, 12))
                .collect::<Vec<_>>(),
        ),
        4 => StringType::from_data(
            (0..rows)
                .map(|_| random_string(rng, 2048))
                .collect::<Vec<_>>(),
        ),
        5 => BooleanType::from_data((0..rows).map(|_| rng.r#gen::<bool>()).collect::<Vec<_>>()),
        6 => Decimal128Type::from_data_with_size(
            (0..rows)
                .map(|_| rng.r#gen::<i64>() as i128 * 10_000)
                .collect::<Vec<_>>(),
            Some(DecimalSize::new_unchecked(38, 4)),
        ),
        _ => DateType::from_data(
            (0..rows)
                .map(|_| rng.gen_range(-100_000..100_000))
                .collect::<Vec<_>>(),
        ),
    };

    if !nullable {
        return column;
    }
    let null_density = [0.0, 0.1, 0.5, 1.0][rng.gen_range(0..4)];
    let validity = (0..rows)
        .map(|_| !rng.gen_bool(null_density))
        .collect::<Bitmap>();
    column.wrap_nullable(Some(validity))
}

/// A full or constant column of `kind`; a nullable constant may be NULL.
fn random_entry(
    kind: u8,
    nullable: bool,
    const_ratio: f64,
    rows: usize,
    rng: &mut StdRng,
) -> BlockEntry {
    if rng.gen_bool(const_ratio) {
        let column = random_column(kind, nullable, 1, rng);
        let scalar = column.index(0).unwrap().to_owned();
        BlockEntry::new_const_column(column.data_type(), scalar, rows)
    } else {
        random_column(kind, nullable, rows, rng).into()
    }
}

fn row_values(block: &DataBlock, row: usize) -> Vec<Scalar> {
    block
        .columns()
        .iter()
        .map(|entry| entry.index(row).unwrap().to_owned())
        .collect()
}

fn u64_at(block: &DataBlock, column: usize, row: usize) -> u64 {
    let value = block.get_by_offset(column).index(row).unwrap();
    UInt64Type::try_downcast_scalar(&value).unwrap()
}

/// A random shuffle: cluster shape, buffer sizes, codec and data all vary.
#[derive(Debug)]
struct FuzzCase {
    nodes: usize,
    threads: usize,
    node_budget: usize,
    max_queued_bytes: usize,
    max_batch_bytes: usize,
    recv_quota: usize,
    compression: u8,
    coalesce: Option<(usize, usize)>,
    rtt_us: u64,
    /// `(kind, nullable)` of the payload columns, after the key and row id columns.
    columns: Vec<(u8, bool)>,
    /// Blocks without columns, as under `count(*)`.
    no_columns: bool,
    blocks_per_thread: usize,
    max_rows: usize,
    seed: u64,
}

/// A size in `[2^min_log, 2^(max_log + 1))`, spread evenly over the exponents.
fn log_scale(min_log: u32, max_log: u32) -> impl Strategy<Value = usize> {
    (min_log..=max_log, any::<u64>()).prop_map(|(log, x)| (1 << log) + (x % (1 << log)) as usize)
}

fn fuzz_case() -> impl Strategy<Value = FuzzCase> {
    (
        (2usize..=30, 1usize..=8).prop_filter("too many receiver threads", |(n, t)| n * t <= 96),
        log_scale(0, 24),
        log_scale(0, 26),
        log_scale(0, 22),
        log_scale(0, 24),
        0u8..3,
        proptest::option::of((1usize..=8192, log_scale(0, 22))),
        0u64..=200,
        proptest::collection::vec((0u8..8, any::<bool>()), 0..=6),
        proptest::bool::weighted(0.1),
        (1usize..=8, 0usize..=600),
        any::<u64>(),
    )
        .prop_map(
            |(
                (nodes, threads),
                node_budget,
                max_queued_bytes,
                max_batch_bytes,
                recv_quota,
                compression,
                coalesce,
                rtt_us,
                columns,
                no_columns,
                (blocks_per_thread, max_rows),
                seed,
            )| FuzzCase {
                nodes,
                threads,
                node_budget,
                max_queued_bytes,
                max_batch_bytes,
                recv_quota,
                compression,
                coalesce,
                rtt_us,
                columns,
                no_columns,
                blocks_per_thread,
                max_rows,
                seed,
            },
        )
}

impl FuzzCase {
    fn setup(&self) -> ShuffleSetup {
        ShuffleSetup {
            nodes: self.nodes,
            threads: self.threads,
            node_budget: self.node_budget,
            buffer: ExchangeBufferConfig {
                max_queued_bytes: self.max_queued_bytes,
                max_batch_bytes: self.max_batch_bytes,
            },
            recv_quota: self.recv_quota,
            compression: match self.compression {
                0 => None,
                1 => Some(FlightCompression::Lz4),
                _ => Some(FlightCompression::Zstd),
            },
            coalesce: self
                .coalesce
                .map(|(rows, bytes)| CoalesceThreshold::create(rows, bytes)),
            link: Link {
                rtt: Duration::from_micros(self.rtt_us),
                bytes_per_sec: None,
            },
        }
    }

    fn schema(&self) -> DataSchemaRef {
        if self.no_columns {
            return DataSchemaRefExt::create(vec![]);
        }

        let mut rng = StdRng::seed_from_u64(0);
        let mut fields = vec![
            DataField::new("key", DataType::Number(NumberDataType::UInt64)),
            DataField::new("id", DataType::Number(NumberDataType::UInt64)),
        ];
        for (i, (kind, nullable)) in self.columns.iter().enumerate() {
            let data_type = random_column(*kind, *nullable, 0, &mut rng).data_type();
            fields.push(DataField::new(&format!("c{i}"), data_type));
        }
        DataSchemaRefExt::create(fields)
    }

    /// The blocks of each sender thread. Columns are full or constant, constants
    /// may be NULL, and some blocks have no rows.
    fn inputs(&self) -> Vec<Vec<DataBlock>> {
        let mut rng = StdRng::seed_from_u64(self.seed);
        let mut next_id = 0u64;

        (0..self.threads)
            .map(|_| {
                (0..self.blocks_per_thread)
                    .map(|_| {
                        let rows = match rng.gen_bool(0.1) {
                            true => 0,
                            false => rng.gen_range(0..=self.max_rows),
                        };
                        if self.no_columns {
                            return DataBlock::empty_with_rows(rows);
                        }

                        let ids = (next_id..next_id + rows as u64).collect::<Vec<_>>();
                        next_id += rows as u64;

                        let mut entries = vec![
                            random_entry(0, false, 0.1, rows, &mut rng),
                            UInt64Type::from_data(ids).into(),
                        ];
                        for (kind, nullable) in &self.columns {
                            entries.push(random_entry(*kind, *nullable, 0.3, rows, &mut rng));
                        }
                        DataBlock::new(entries, rows)
                    })
                    .collect()
            })
            .collect()
    }
}

fn check_fuzz_case(case: &FuzzCase) {
    let setup = case.setup();
    let schema = case.schema();
    let inputs = case.inputs();
    let num_partitions = (case.nodes * case.threads) as u64;

    let scatter: Box<dyn FlightScatter> = match case.no_columns {
        true => Box::new(SpreadScatter {
            num_partitions,
            next: AtomicU64::new(0),
        }),
        false => Box::new(KeyScatter { num_partitions }),
    };

    let received = Mutex::new(vec![]);
    run_shuffle(
        &setup,
        &schema,
        scatter,
        |t| inputs[t].clone().into_iter(),
        |partition, block| received.lock().push((partition, block)),
    )
    .unwrap();
    let received = received.into_inner();

    if case.no_columns {
        let sent = inputs.iter().flatten().map(|x| x.num_rows()).sum::<usize>();
        let arrived = received.iter().map(|(_, x)| x.num_rows()).sum::<usize>();
        assert_eq!(arrived, sent, "every row must arrive exactly once");
        assert!(received.iter().all(|(_, x)| x.num_columns() == 0));
        return;
    }

    let mut expected = HashMap::new();
    for block in inputs.iter().flatten() {
        for row in 0..block.num_rows() {
            expected.insert(u64_at(block, 1, row), row_values(block, row));
        }
    }

    let mut actual = HashMap::with_capacity(expected.len());
    for (partition, block) in &received {
        assert_eq!(block.num_columns(), schema.fields().len());
        for row in 0..block.num_rows() {
            let key = u64_at(block, 0, row);
            assert_eq!(
                key % num_partitions,
                *partition as u64,
                "row with key {key} arrived at the wrong partition"
            );

            let id = u64_at(block, 1, row);
            let previous = actual.insert(id, row_values(block, row));
            assert!(previous.is_none(), "row {id} arrived twice");
        }
    }

    assert_eq!(actual.len(), expected.len(), "every row must arrive");
    for (id, values) in &expected {
        assert_eq!(actual.get(id), Some(values), "values of row {id} changed");
    }
}

fn fuzz_cases() -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|x| x.parse().ok())
        .unwrap_or(32)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(fuzz_cases()))]

    /// Every row arrives exactly once, at the partition of its key, with its
    /// values unchanged, whatever the cluster shape, sizes, codec and data.
    #[test]
    fn test_hash_shuffle_delivers_every_row(case in fuzz_case()) {
        check_fuzz_case(&case);
    }
}

/// A block of `rows` random keys, each with a 120-byte string.
fn wide_block(rng: &mut StdRng, rows: usize) -> DataBlock {
    let keys = (0..rows).map(|_| rng.r#gen::<u64>()).collect::<Vec<_>>();
    let strings = keys
        .iter()
        .map(|k| format!("{k:016x}").repeat(8)[..120].to_string())
        .collect::<Vec<_>>();
    DataBlock::new_from_columns(vec![
        UInt64Type::from_data(keys),
        StringType::from_data(strings),
    ])
}

/// Shuffle 256 MiB over links slower than the senders, so the buffers fill up,
/// and check that peak memory stays within the configured buffer sizes.
fn check_memory_bound(nodes: usize) {
    const INPUT_BYTES: usize = 256 * MIB;
    const ROWS: usize = 8192;
    let threads = 4;
    let recv_bytes = 8 * MIB;

    let setup = ShuffleSetup {
        nodes,
        threads,
        node_budget: 16 * MIB,
        buffer: ExchangeBufferConfig {
            max_queued_bytes: 8 * MIB,
            max_batch_bytes: 4 * MIB,
        },
        recv_quota: recv_bytes / (nodes - 1),
        compression: None,
        // Receivers drop blocks as they arrive: in this process every node's
        // receivers are counted, a real node only has its own.
        coalesce: None,
        // The links carry 64 MiB/s together, whatever the number of nodes.
        link: Link {
            rtt: Duration::from_millis(1),
            bytes_per_sec: Some(64.0 * MIB as f64 / (nodes - 1) as f64),
        },
    };

    let block_bytes = wide_block(&mut StdRng::seed_from_u64(0), ROWS).memory_size();
    let blocks_per_thread = INPUT_BYTES / block_bytes / threads;
    let schema = Arc::new(wide_block(&mut StdRng::seed_from_u64(0), 1).infer_schema());

    let mem_stat = MemStat::create(format!("hash_shuffle_memory_{nodes}_nodes"));
    let mut payload = ThreadTracker::new_tracking_payload();
    payload.mem_stat = Some(mem_stat.clone());
    let guard = ThreadTracker::tracking(payload);

    let received = AtomicUsize::new(0);
    run_shuffle(
        &setup,
        &schema,
        Box::new(KeyScatter {
            num_partitions: (nodes * threads) as u64,
        }),
        |t| {
            // Generated on the fly, so the input itself is never held.
            let mut rng = StdRng::seed_from_u64(t as u64);
            (0..blocks_per_thread).map(move |_| wide_block(&mut rng, ROWS))
        },
        |_, block| {
            received.fetch_add(block.num_rows(), Ordering::Relaxed);
        },
    )
    .unwrap();
    drop(guard);

    assert_eq!(
        received.load(Ordering::Relaxed),
        threads * blocks_per_thread * ROWS
    );

    // Senders hold their buffered rows plus the encoded copy waiting for the send
    // queue; the rest is the send queue, the local channels and the receive quota.
    // The slack covers unpublished per-thread stats and blocks being generated.
    let bound = 3 * setup.node_budget
        + setup.buffer.max_queued_bytes
        + LOCAL_CHANNEL_MAX_BYTES
        + recv_bytes
        + 16 * MIB;
    let peak = mem_stat.get_peak_memory_usage().max(0) as usize;
    assert!(
        peak <= bound,
        "peak memory {} MiB with {nodes} nodes exceeds the bound of {} MiB",
        peak / MIB,
        bound / MIB
    );
}

#[test]
fn test_hash_shuffle_memory_bound_3_nodes() {
    check_memory_bound(3);
}

#[test]
fn test_hash_shuffle_memory_bound_30_nodes() {
    check_memory_bound(30);
}

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|x| x.parse().ok())
        .unwrap_or(default)
}

/// Speed of the hash shuffle with production defaults; prints, does not assert.
///
/// Configure with HASH_SHUFFLE_BENCH_{NODES,THREADS,BLOCKS,ROWS,RTT_US,MBPS,CODEC}:
/// MBPS limits each link (0 = unlimited), CODEC is 0 none, 1 LZ4, 2 ZSTD.
#[test]
#[ignore]
fn hash_shuffle_bench() {
    let nodes = env("HASH_SHUFFLE_BENCH_NODES", 3) as usize;
    let threads = env("HASH_SHUFFLE_BENCH_THREADS", 16) as usize;
    let blocks = env("HASH_SHUFFLE_BENCH_BLOCKS", 64) as usize;
    let rows = env("HASH_SHUFFLE_BENCH_ROWS", 8192) as usize;
    let mbps = env("HASH_SHUFFLE_BENCH_MBPS", 0);

    let setup = ShuffleSetup {
        nodes,
        threads,
        node_budget: 256 * MIB,
        buffer: ExchangeBufferConfig::default(),
        recv_quota: (64 * MIB / (nodes - 1)).max(MIB),
        compression: match env("HASH_SHUFFLE_BENCH_CODEC", 1) {
            0 => None,
            1 => Some(FlightCompression::Lz4),
            _ => Some(FlightCompression::Zstd),
        },
        coalesce: Some(CoalesceThreshold::create(8192, 4 * MIB)),
        link: Link {
            rtt: Duration::from_micros(env("HASH_SHUFFLE_BENCH_RTT_US", 200)),
            bytes_per_sec: (mbps != 0).then_some(mbps as f64 * MIB as f64),
        },
    };

    // Generated before measuring, outside the tracked memory.
    let inputs = (0..threads)
        .map(|t| {
            let mut rng = StdRng::seed_from_u64(t as u64);
            (0..blocks)
                .map(|_| wide_block(&mut rng, rows))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let input_bytes = inputs
        .iter()
        .flatten()
        .map(|x| x.memory_size())
        .sum::<usize>();
    let schema = Arc::new(inputs[0][0].infer_schema());

    let mem_stat = MemStat::create("hash_shuffle_bench".to_string());
    let mut payload = ThreadTracker::new_tracking_payload();
    payload.mem_stat = Some(mem_stat.clone());
    let guard = ThreadTracker::tracking(payload);

    let received_rows = AtomicUsize::new(0);
    let received_blocks = AtomicUsize::new(0);
    let run = run_shuffle(
        &setup,
        &schema,
        Box::new(KeyScatter {
            num_partitions: (nodes * threads) as u64,
        }),
        |t| inputs[t].iter().cloned(),
        |_, block| {
            received_rows.fetch_add(block.num_rows(), Ordering::Relaxed);
            received_blocks.fetch_add(1, Ordering::Relaxed);
        },
    )
    .unwrap();
    drop(guard);

    assert_eq!(
        received_rows.load(Ordering::Relaxed),
        threads * blocks * rows
    );
    println!(
        "hash_shuffle_bench nodes={nodes} threads={threads} input_mib={:.0} wall_ms={} peak_mib={:.1} wire_mib={:.1} frames={} recv_blocks={}",
        input_bytes as f64 / MIB as f64,
        run.elapsed.as_millis(),
        mem_stat.get_peak_memory_usage() as f64 / MIB as f64,
        run.wire_bytes.load(Ordering::Relaxed) as f64 / MIB as f64,
        run.frames.load(Ordering::Relaxed),
        received_blocks.load(Ordering::Relaxed),
    );
}
