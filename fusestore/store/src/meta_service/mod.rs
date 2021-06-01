// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

mod meta;
mod meta_service_impl;
pub mod placement;
mod raftmeta;

pub use async_raft::NodeId;
pub use meta::Meta;
pub use meta::Node;
pub use meta::Slot;
pub use meta_service_impl::MetaServiceImpl;
pub use placement::IPlacement;
pub use raftmeta::ClientRequest;
pub use raftmeta::ClientResponse;
pub use raftmeta::Cmd;
pub use raftmeta::MemStore;
pub use raftmeta::MemStoreStateMachine;
pub use raftmeta::MetaNode;
pub use raftmeta::Network;
pub use raftmeta::RaftTxId;
pub use raftmeta::ShutdownError;

pub use crate::protobuf::meta_service_client::MetaServiceClient;
pub use crate::protobuf::meta_service_server::MetaService;
pub use crate::protobuf::meta_service_server::MetaServiceServer;
pub use crate::protobuf::GetReply;
pub use crate::protobuf::GetReq;
pub use crate::protobuf::RaftMes;

#[cfg(test)]
mod meta_service_impl_test;
#[cfg(test)]
mod meta_test;
#[cfg(test)]
mod placement_test;
#[cfg(test)]
mod raftmeta_test;
