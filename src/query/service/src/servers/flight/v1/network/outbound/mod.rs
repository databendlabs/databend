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

//! Deliver blocks locally or through a buffered remote connection.
//!
//! [`BlockSender`] defines delivery. [`LocalBlockSender`] moves blocks into a
//! receive queue; [`RemoteBlockSender`] encodes blocks for a fixed remote thread
//! and enqueues them in [`ExchangeSendBuffer`]. [`RoundRobinBlockSender`] composes
//! other senders to select one thread for each block.
//!
//! [`HashShuffleDispatcher`] delivers an already-partitioned group of blocks to
//! one node's threads. It uses local senders or synchronously encodes remote
//! blocks before creating futures that wait for send-buffer quota. Partitioning,
//! direct output of the writer's own partition, and polling remain in exchange.
//!
//! [`ExchangeSendBuffer`] applies shared byte quota and batches queued items into
//! frames. It uses the shared [`PingPongTransport`](super::PingPongTransport) to
//! send frames and receive acknowledgements, allowing one request in flight per
//! connection. Incoming data is handled by [`inbound`](super::inbound).
//!
//! Sending completes at queue admission, not downstream consumption. Send-buffer
//! quota remains attached to queued and in-flight items until acknowledgement.
//! Dropping one shared sender leaves other holders usable; releasing the final
//! send-buffer owner initiates draining and transport shutdown. Response callbacks
//! retain the queues while they drain; a response EOF or send/response error marks
//! the destination closed and clears both queued and in-flight permits. Enqueue
//! checks that terminal state again after acquiring quota, before inserting data.

pub mod block_sender;
pub mod exchange_send_buffer;
pub mod hash_shuffle_dispatcher;

pub use block_sender::BlockSender;
pub use block_sender::LOCAL_CHANNEL_MAX_BYTES;
pub use block_sender::LocalBlockSender;
pub use block_sender::RemoteBlockSender;
pub use block_sender::RoundRobinBlockSender;
pub use block_sender::block_owned_bytes;
pub use block_sender::create_local_block_senders;
pub use exchange_send_buffer::ExchangeSendBuffer;
pub use exchange_send_buffer::ExchangeSendBufferConfig;
pub use hash_shuffle_dispatcher::HashShuffleDispatcher;
pub use hash_shuffle_dispatcher::ShuffleSendAddress;
pub use hash_shuffle_dispatcher::ShuffleSendFuture;
