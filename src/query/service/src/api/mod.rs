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

// The api module only used for internal communication, such as GRPC between cluster and the managed HTTP REST API.

pub use http_service::HttpService;
pub use rpc::serialize_block;
pub use rpc::BroadcastExchange;
pub use rpc::BroadcastFlightScatter;
pub use rpc::ConnectionInfo;
pub use rpc::DataExchange;
pub use rpc::DataExchangeManager;
pub use rpc::DataPacket;
pub use rpc::DatabendQueryFlightService;
pub use rpc::DefaultExchangeInjector;
pub use rpc::ExchangeDeserializeMeta;
pub use rpc::ExchangeInjector;
pub use rpc::ExchangeSerializeMeta;
pub use rpc::ExchangeShuffleMeta;
pub use rpc::ExchangeSorting;
pub use rpc::ExecutePartialQueryPacket;
pub use rpc::FlightAction;
pub use rpc::FlightClient;
pub use rpc::FlightScatter;
pub use rpc::FragmentData;
pub use rpc::FragmentPlanPacket;
pub use rpc::HashFlightScatter;
pub use rpc::InitNodesChannelPacket;
pub use rpc::KillQueryPacket;
pub use rpc::MergeExchange;
pub use rpc::MergeExchangeParams;
pub use rpc::Packet;
pub use rpc::QueryFragmentsPlanPacket;
pub use rpc::ShuffleDataExchange;
pub use rpc::ShuffleExchangeParams;
pub use rpc::TransformExchangeDeserializer;
pub use rpc::TruncateTablePacket;
pub use rpc_service::RpcService;

pub mod http;
mod http_service;
mod rpc;
mod rpc_service;
