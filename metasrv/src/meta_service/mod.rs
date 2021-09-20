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

pub mod applied_state;
pub mod cmd;
pub mod errors;
pub mod log_entry;
pub mod meta_service_impl;
pub mod network;
pub mod placement;
pub mod raft_db;
pub mod raft_log;
pub mod raft_state;
pub mod raft_state_kv;
pub mod raft_txid;
pub mod raft_types;
pub mod raftmeta;
pub mod seq_num;
pub mod sled_key_space;
pub mod sled_serde;
pub mod sled_tree;
pub mod snapshot;
pub mod state_machine;
pub mod state_machine_meta;

pub use applied_state::AppliedState;
pub use cmd::Cmd;
pub use errors::RetryableError;
pub use errors::ShutdownError;
pub use log_entry::LogEntry;
pub use meta_service_impl::MetaServiceImpl;
pub use network::Network;
pub use placement::Placement;
pub use raft_state_kv::RaftStateKey;
pub use raft_state_kv::RaftStateValue;
pub use raft_txid::RaftTxId;
pub use raft_types::LogIndex;
pub use raft_types::NodeId;
pub use raft_types::Term;
pub use raftmeta::MetaNode;
pub use raftmeta::MetaRaftStore;
pub use seq_num::SeqNum;
pub use sled_serde::SledOrderedSerde;
pub use sled_serde::SledSerde;
pub use sled_tree::AsKeySpace;
pub use sled_tree::SledTree;
pub use sled_tree::SledValueToKey;
pub use snapshot::Snapshot;
pub use state_machine::Node;
pub use state_machine::Slot;
pub use state_machine::StateMachine;
pub use state_machine_meta::StateMachineMetaKey;
pub use state_machine_meta::StateMachineMetaValue;

pub use crate::protobuf::meta_service_client::MetaServiceClient;
pub use crate::protobuf::meta_service_server::MetaService;
pub use crate::protobuf::meta_service_server::MetaServiceServer;
pub use crate::protobuf::GetReply;
pub use crate::protobuf::GetReq;
pub use crate::protobuf::RaftMes;

#[cfg(test)]
mod meta_service_impl_test;
#[cfg(test)]
mod meta_store_test;
#[cfg(test)]
mod placement_test;
#[cfg(test)]
mod raft_log_test;
#[cfg(test)]
mod raft_state_test;
#[cfg(test)]
mod raft_types_test;
#[cfg(test)]
mod raftmeta_test;
#[cfg(test)]
mod sled_serde_test;
#[cfg(test)]
mod sled_tree_test;
#[cfg(test)]
mod state_machine_test;
#[cfg(test)]
mod testing;
