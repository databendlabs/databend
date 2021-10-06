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

pub use impl_flights::kv_api_impl;
pub use impl_flights::meta_api_impl;
pub use store_client::StoreClient;
pub use store_client_conf::StoreClientConf;
pub use store_do_action::RequestFor;
pub use store_do_action::StoreDoAction;

mod impl_flights;
mod store_client;
#[macro_use]
mod store_do_action;
pub mod meta_api;
mod store_client_conf;
pub mod vo;

// ProtoBuf generated files.
#[allow(clippy::all)]
pub mod protobuf {
    tonic::include_proto!("metaflight");
}
