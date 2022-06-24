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

mod writers;

mod clickhouse_federated;
mod clickhouse_handler;
mod clickhouse_metrics;
mod clickhouse_session;
mod interactive_worker;
mod interactive_worker_base;
mod reject_connection;

pub use clickhouse_federated::CLickHouseFederated;
pub use clickhouse_handler::ClickHouseHandler;
pub use writers::from_clickhouse_block;

// from clickhouse-sqlalchemy:
//  supports_update = version >= (18, 12, 14)
//  supports_engine_reflection = version >= (18, 16, 0)
const CLICKHOUSE_VERSION: &str = "8.12.14";
