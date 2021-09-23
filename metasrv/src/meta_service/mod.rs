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

pub use cmd::Cmd;
pub use errors::RetryableError;
pub use errors::ShutdownError;
pub use log_entry::LogEntry;
pub use meta_service_impl::MetaServiceImpl;
pub use network::Network;
pub use raft_state_kv::RaftStateKey;
pub use raft_state_kv::RaftStateValue;
pub use raft_txid::RaftTxId;
pub use raft_types::LogIndex;
pub use raft_types::NodeId;
pub use raft_types::Term;
pub use raftmeta::MetaNode;
pub use raftmeta::MetaRaftStore;

pub use crate::protobuf::meta_service_client::MetaServiceClient;
pub use crate::protobuf::meta_service_server::MetaService;
pub use crate::protobuf::meta_service_server::MetaServiceServer;
pub use crate::protobuf::GetReply;
pub use crate::protobuf::GetReq;
pub use crate::protobuf::RaftMes;
pub use crate::raft::state_machine::applied_state::AppliedState;
pub use crate::raft::state_machine::placement::Placement;

pub mod cmd;
pub mod errors;
pub mod log_entry;
pub mod meta_service_impl;
pub mod network;
pub mod raft_state;
pub mod raft_state_kv;
pub mod raft_txid;
pub mod raft_types;
pub mod raftmeta;

#[cfg(test)]
mod meta_service_impl_test;
#[cfg(test)]
mod meta_store_test;
#[cfg(test)]
mod raft_state_test;
#[cfg(test)]
mod raft_types_test;
#[cfg(test)]
pub mod raftmeta_test;
#[cfg(test)]
pub mod testing;
