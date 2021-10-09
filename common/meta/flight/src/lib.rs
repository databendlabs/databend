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

mod flight_client;
#[macro_use]
mod flight_action;
mod flight_api;
mod flight_client_conf;

pub mod impls;

pub use flight_action::MetaFlightAction;
pub use flight_action::RequestFor;
pub use flight_api::MetaApi;
pub use flight_client::MetaFlightClient;
pub use flight_client_conf::MetaFlightClientConf;

// ProtoBuf generated files.
#[allow(clippy::all)]
pub mod protobuf {
    tonic::include_proto!("metaflight");
}
