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

mod clickhouse_federated;
mod clickhouse_handler;
pub mod error;
mod http_services;
pub mod middleware;
pub mod v1;

pub use clickhouse_federated::ClickHouseFederated;
pub use http_services::HttpHandler;
pub use http_services::HttpHandlerKind;

pub const CLICKHOUSE_VERSION: &str = "8.12.14";
