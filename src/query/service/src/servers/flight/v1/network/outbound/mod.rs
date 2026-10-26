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

//! Deliver blocks through local receive channels or buffered remote connections.
//!
//! [`BlockSender`] is implemented by [`LocalBlockSender`], [`RemoteBlockSender`],
//! [`RoundRobinBlockSender`], and [`DummyBlockSender`]. The round-robin sender
//! selects a thread sender; the dummy represents a closed destination.
//! [`ExchangeSendBuffer`] retains per-thread pending queues for each remote node.

pub mod block_sender;
pub mod exchange_send_buffer;

pub use block_sender::BlockSender;
pub use block_sender::DummyBlockSender;
pub use block_sender::LocalBlockSender;
pub use block_sender::RemoteBlockSender;
pub use block_sender::RoundRobinBlockSender;
pub use block_sender::create_local_block_senders;
pub use exchange_send_buffer::ExchangeSendBuffer;
pub use exchange_send_buffer::ExchangeSendBufferConfig;
