// Copyright 2021 Datafuse Labs.
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
pub use rpc::BroadcastAction;
pub use rpc::CancelAction;
pub use rpc::DatabendQueryFlightDispatcher;
pub use rpc::DatabendQueryFlightService;
pub use rpc::FlightAction;
pub use rpc::FlightClient;
pub use rpc::FlightTicket;
pub use rpc::ShuffleAction;
pub use rpc::StreamTicket;
pub use rpc_service::RpcService;

pub mod http;
mod http_service;
mod rpc;
mod rpc_service;
