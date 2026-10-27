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

//! Block delivery, send buffering, and Flight transport for broadcast and global
//! hash shuffle. Routing and pipeline scheduling live in [`exchange`](super::exchange).
//!
//! # Layers and data flow
//!
//! | Layer | Components | Unit of work |
//! | --- | --- | --- |
//! | Block delivery | [`BlockSender`](outbound::BlockSender), implemented by [`LocalBlockSender`](outbound::LocalBlockSender), [`RemoteBlockSender`](outbound::RemoteBlockSender), and [`RoundRobinBlockSender`](outbound::RoundRobinBlockSender) | One `DataBlock`; the round-robin implementation selects one thread sender. |
//! | Hash shuffle delivery | [`HashShuffleDispatcher`](outbound::HashShuffleDispatcher) | Deliver one node's already-partitioned blocks locally or encode and enqueue them remotely. |
//! | Producer codec | [`HashShuffleCodec`] | Optionally convert a producer's own block form to and from plain blocks, only for remote nodes. |
//! | Send buffering | [`ExchangeSendBuffer`](outbound::ExchangeSendBuffer) | Encoded items for a remote node; shared byte quota, per-node FIFO queues, and frame batching. |
//! | Transport | [`PingPongTransport`] | One Flight frame per request, with at most one request in flight per connection. |
//! | Frame routing | [`ExchangeFrameRouter`](inbound::ExchangeFrameRouter) | Split incoming frames, acquire connection receive quota, and route items by thread ID. |
//! | Receive buffering | Per-thread channels assembled by [`ExchangeReceiveChannels`](inbound::ExchangeReceiveChannels) | Queue local blocks and remote encoded items for the same consumer. |
//! | Block consumption | [`ExchangeQueueReceiver`](inbound::ExchangeQueueReceiver) | Return local blocks directly and decode remote items when consumed. |
//!
//! ```text
//! HashShuffleWriter -> HashShuffleDispatcher
//!   local  -> LocalBlockSender -> local receive queue
//!   remote -> wire_format::encode_block_for_thread -> ExchangeSendBuffer
//!
//! BroadcastSendTransform -> RoundRobinBlockSender (one per destination node)
//!   local  -> LocalBlockSender -> local receive queue
//!   remote -> RemoteBlockSender -> wire_format -> ExchangeSendBuffer
//!
//! ExchangeSendBuffer -> PingPongTransport -> Flight do_exchange
//!   -> ExchangeFrameRouter -> per-thread channel
//!   -> ExchangeQueueReceiver -> DataBlock
//! ```
//!
//! Local delivery moves blocks without wire serialization. Remote block senders
//! encode for a fixed thread; hash shuffle supplies each thread ID while preparing
//! a node's batch. Both remote paths use the same buffer and transport.
//! [`wire_format`] owns item encoding, thread-ID metadata, compression, and
//! frame packing/splitting. The send buffer chooses which items form a frame;
//! it does not choose their destination threads or decode their contents.
//!
//! [`outbound`] owns block delivery and send buffering; [`inbound`] owns frame
//! routing and block consumption. [`ping_pong_transport`] provides the underlying
//! frame transport and acknowledgement lifecycle used by the send buffer.
//! [`inbound::exchange_queue`] assembles receive endpoints and defines payloads
//! carrying receive quota; the send buffer owns its pending items and send quota.
//! [`wire_format`] defines the encoding used by both directions.
//!
//! # Completion and backpressure
//!
//! `send_block` completes when a block has been handed to the local receive queue
//! or remote send buffer, or discarded for a closed receiver. `enqueue_items`
//! waits for send quota, not for downstream consumption. Its quota follows each
//! item through the queue and in-flight frame until a response releases it.
//! Writer-owned partition builders and prepared sends waiting for quota are
//! outside that buffer's accounting.
//!
//! On the receiver, `do_exchange` replies only after `ExchangeFrameRouter` has
//! routed the frame's items through the connection's receive quota. A pong permits
//! the next frame; it does not mean a downstream operator has consumed the rows.
//! Local and remote receive permits are released when items are taken from their
//! queues for delivery/decoding, rather than being held by downstream operators.
//!
//! # Lifetime
//!
//! [`ExchangeReceiveChannels`](inbound::ExchangeReceiveChannels) retains the initial channel endpoints during construction.
//! Remote routers bind during `INIT_QUERY_ENV`; local producers and consumers bind
//! while building pipelines. Before execution, the coordinator releases its initial
//! endpoints and stops accepting registrations. Runtime routers and local block
//! senders own only sending endpoints; block receivers own only receiving endpoints.
//! No consumer or coordinator keeps a producer alive. The last sender's drop closes
//! its channel, and the receiver drains queued items before observing EOF.
//!
//! Senders are shared ownership handles. Dropping one handle leaves other holders
//! usable. Receive queues reach EOF after all their producers finish, while
//! releasing the final send-buffer owner initiates draining and shutdown of the
//! remote transports. Receiver closure and transport errors can end delivery
//! earlier. The exchange processors must release completed destinations promptly;
//! keeping their handles until the pipeline graph is dropped can delay EOF.

mod hash_shuffle_codec;
pub mod inbound;
pub mod outbound;
pub mod ping_pong_transport;
pub mod wire_format;

pub use databend_common_pipeline::core::SyncTaskHandle;
pub use databend_common_pipeline::core::SyncTaskSet;
pub use hash_shuffle_codec::HashShuffleCodec;
pub use ping_pong_transport::PingPongCallback;
pub use ping_pong_transport::PingPongResponse;
pub use ping_pong_transport::PingPongTransport;
pub use ping_pong_transport::PingPongTransportInner;
