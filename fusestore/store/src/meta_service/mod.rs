// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

pub mod applied_state;
pub mod cmd;
pub mod errors;
pub mod log_entry;
pub mod meta_service_impl;
pub mod network;
pub mod placement;
pub mod raft_txid;
pub mod raftmeta;
pub mod snapshot;
pub mod state_machine;

pub use applied_state::AppliedState;
pub use async_raft::NodeId;
pub use cmd::Cmd;
pub use errors::RetryableError;
pub use errors::ShutdownError;
pub use log_entry::LogEntry;
pub use meta_service_impl::MetaServiceImpl;
pub use network::Network;
pub use placement::Placement;
pub use raft_txid::RaftTxId;
pub use raftmeta::MetaNode;
pub use raftmeta::MetaStore;
pub use snapshot::Snapshot;
pub use state_machine::Node;
pub use state_machine::Slot;
pub use state_machine::StateMachine;

pub use crate::protobuf::meta_service_client::MetaServiceClient;
pub use crate::protobuf::meta_service_server::MetaService;
pub use crate::protobuf::meta_service_server::MetaServiceServer;
pub use crate::protobuf::GetReply;
pub use crate::protobuf::GetReq;
pub use crate::protobuf::RaftMes;

#[cfg(test)]
mod meta_service_impl_test;
#[cfg(test)]
mod placement_test;
mod raft_state;
#[cfg(test)]
mod raft_state_test;
#[cfg(test)]
mod raftmeta_test;
#[cfg(test)]
mod state_machine_test;
