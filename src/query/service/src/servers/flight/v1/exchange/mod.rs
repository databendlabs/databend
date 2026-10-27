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

//! Exchange planning, pipeline processors, and destination routing.
//!
//! This module decides which node and thread receives each block. The
//! [`network`](super::network) module provides block senders, byte-limited queues,
//! wire encoding, and Flight transport.
//!
//! # Broadcast and global hash shuffle
//!
//! These paths share the network machinery but have different delivery policies:
//!
//! | Component | Responsibility |
//! | --- | --- |
//! | [`HashSendSink`] / [`HashSendTransform`] | Drive hash shuffle from pipeline events; the sink sends all partitions through channels, while the transform can output its own partition directly. |
//! | `HashShuffleWriter` | Partition rows, account for buffered data, flush all partitions at the budget, and poll the resulting send tasks. |
//! | [`HashShuffleDispatcher`](super::network::outbound::HashShuffleDispatcher) | Deliver one node's partitions: local blocks go to their assigned threads; remote blocks are encoded with their thread IDs and enqueued together. |
//! | [`BroadcastSendTransform`] | Deliver one copy of each block to every node, using a round-robin block sender to choose a thread within each node. |
//! | [`ExchangeRecvTransform`] | Combine the processor's own output path with queued incoming data, optionally coalescing small blocks for downstream operators. |
//!
//! Hash shuffle fixes the destination thread when it partitions the input. It
//! therefore bypasses the broadcast round-robin policy. Its remote dispatcher
//! calls wire encoding and the shared send buffer directly; there is no
//! intermediate remote sender object for each thread.
//!
//! The writer takes its own output block by global partition index before passing
//! the remaining partitions to the outbound dispatcher. That block is returned
//! to the processor for its output port instead of entering a local queue.
//! Broadcast also uses its output port directly when possible, falling back to
//! the local node's round-robin sender under backpressure.
//!
//! A producer may also keep its own block form, with the data in the block meta,
//! through the hash shuffle: its [`FlightScatter`](super::scatter::FlightScatter)
//! routes such blocks whole, one per partition. The writer's send queue resizes
//! them to its per-partition send size through the `split` and `merge` hooks
//! of `BlockMetaInfo`, as it slices plain blocks. The local node receives them in
//! the producer's form, and only blocks for remote nodes are converted by its
//! [`HashShuffleCodec`](super::network::HashShuffleCodec) before the wire format
//! and after arrival.
//!
//! # Preparation, progress, and ownership
//!
//! `HashShuffleDispatcher::prepare_send` encodes remote blocks synchronously,
//! before returning futures that wait for send-buffer capacity. This keeps
//! unencoded partition blocks out of the network wait. The writer owns polling
//! and executor wakeups; the dispatcher owns delivery policy, not scheduling.
//! See [`network`](super::network) for the queue and acknowledgement boundaries.
//!
//! Dispatcher copies share their block senders or send buffers across processor instances.
//! `stop_sending` releases the current writer's references; broadcast similarly
//! removes its sender references when a destination ends or its input finishes.
//! Neither operation forcibly closes a connection used by another processor.
//! Prepared send futures retain their own references until they complete or are
//! dropped. Releasing producer references promptly lets receivers observe EOF
//! without waiting for the entire pipeline graph to be destroyed.

mod block_coalescer;
mod broadcast_recv_transform;
mod broadcast_send_transform;
mod data_exchange;
mod exchange_injector;
mod exchange_manager;
mod exchange_params;
mod exchange_sink;
mod exchange_sink_writer;
mod exchange_sorting;
mod exchange_source;
mod exchange_source_reader;
mod exchange_transform;
mod exchange_transform_scatter;
mod exchange_transform_shuffle;
mod fuse_part_exchange;
mod hash_send_sink;
mod hash_send_source;
mod hash_send_transform;
#[cfg(test)]
mod hash_shuffle_tests;
mod hash_shuffle_writer;
mod statistics_receiver;
mod statistics_sender;

pub mod serde;

pub use broadcast_recv_transform::ExchangeRecvTransform;
pub type BroadcastRecvTransform = ExchangeRecvTransform;
pub type HashRecvTransform = ExchangeRecvTransform;
pub use broadcast_send_transform::BroadcastSendTransform;
pub use data_exchange::BroadcastExchange;
pub use data_exchange::DataExchange;
pub use data_exchange::MergeExchange;
pub use data_exchange::NodeToNodeExchange;
pub use exchange_injector::DefaultExchangeInjector;
pub use exchange_injector::ExchangeInjector;
pub use exchange_manager::DataExchangeManager;
pub use exchange_params::MergeExchangeParams;
pub use exchange_params::ShuffleExchangeParams;
pub use exchange_sorting::ExchangeSorting;
pub use exchange_transform_scatter::ScatterTransform;
pub use exchange_transform_shuffle::ExchangeShuffleMeta;
pub use exchange_transform_shuffle::ExchangeShuffleTransform;
pub use fuse_part_exchange::FusePartExchangeInjector;
pub use hash_send_sink::HashSendSink;
pub use hash_send_source::HashSendSource;
pub use hash_send_transform::HashSendTransform;
