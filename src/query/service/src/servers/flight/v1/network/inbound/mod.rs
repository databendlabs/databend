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

//! Receive exchange data and deliver blocks to pipeline consumers.
//!
//! [`ExchangeFrameRouter`] splits incoming Flight frames and routes encoded items
//! to per-thread queues after acquiring the connection's receive quota. Local
//! senders write blocks directly to those same queues. [`exchange_queue`] owns
//! queue construction, payloads, and consumption: [`ExchangeReceiveChannels`]
//! assembles the initial endpoints, and [`ExchangeQueueReceiver`] returns local
//! blocks directly and decodes remote items on the thread polling `recv_block`.
//!
//! `do_exchange` acknowledges a frame after routing completes. Receive permits
//! are released when items leave the queues, before decoding or downstream work.
//! Runtime routers own only senders and queue receivers own only receiving
//! endpoints. After construction releases the initial endpoints, the final
//! producer's drop lets consumers drain their queues and observe EOF.

pub mod exchange_frame_router;
pub mod exchange_queue;

pub use exchange_frame_router::ExchangeFrameRouter;
pub use exchange_queue::ExchangeQueueReceiver;
pub use exchange_queue::ExchangeReceiveChannels;
