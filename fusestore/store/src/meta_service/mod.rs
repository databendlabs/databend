// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

mod meta;
mod meta_service_impl;

pub use meta::IMeta;
pub use meta::Meta;
pub use meta::Node;
pub use meta::NodeId;
pub use meta::Slot;
pub use meta_service_impl::MetaServiceImpl;

pub use crate::protobuf::meta_service_client::MetaServiceClient;
pub use crate::protobuf::meta_service_server::MetaService;
pub use crate::protobuf::meta_service_server::MetaServiceServer;
pub use crate::protobuf::GetReply;
pub use crate::protobuf::GetReq;
pub use crate::protobuf::SetReply;
pub use crate::protobuf::SetReq;

#[cfg(test)]
mod meta_service_impl_test;
