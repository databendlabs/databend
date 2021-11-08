// Copyright 2020 Datafuse Labs.
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

pub use message::AdminRequest;
pub use message::AdminRequestInner;
pub use message::JoinRequest;
pub use meta_service_impl::MetaServiceImpl;
pub use network::Network;
pub use raftmeta::MetaNode;
pub use raftmeta::MetaRaftStore;

pub use crate::errors::ForwardToLeader;
pub use crate::errors::InvalidMembership;
pub use crate::errors::MetaError;
pub use crate::errors::RetryableError;
pub use crate::errors::ShutdownError;

mod message;
mod meta_leader;
pub mod meta_service_impl;
pub mod network;
pub mod raftmeta;

#[cfg(test)]
mod meta_service_impl_test;
#[cfg(test)]
mod meta_store_test;
#[cfg(test)]
pub mod raftmeta_test;
