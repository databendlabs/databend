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

//! Exchange block delivery and Flight transport.
//!
//! [`outbound`] owns block senders and per-destination send buffering;
//! [`inbound`] owns receive channels, frame routing, and block consumption.
//! [`wire_format`] handles serialization and single-thread batch framing.
//! [`ping_pong_transport`] provides the request/response transport shared by them.
//!
//! Local delivery passes a DataBlock directly to its receive channel. Remote
//! delivery serializes it, waits for send-queue capacity, and sends a Flight
//! frame. The frame router routes its items to receive channels before replying.
//! Pipeline receivers deserialize remote items on the thread polling `recv`.
//!
//! Receive channels keep both endpoints. Local senders and connection routers
//! share an explicit producer count and close the channel when the last producer
//! drops. Receivers drain queued items before EOF, or close and drain on cancel.
//! The coordinator retains the channel collection throughout execution.

pub mod inbound;
pub mod outbound;
pub mod ping_pong_transport;
pub mod wire_format;

pub use databend_common_pipeline::core::SyncTaskHandle;
pub use databend_common_pipeline::core::SyncTaskSet;
pub use ping_pong_transport::PingPongCallback;
pub use ping_pong_transport::PingPongResponse;
pub use ping_pong_transport::PingPongTransport;
pub use ping_pong_transport::PingPongTransportInner;
