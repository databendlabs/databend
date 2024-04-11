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

// The servers module used for external communication with user, such as MySQL wired protocol, etc.

pub use server::Server;
pub use server::ShutdownHandle;

pub use self::flight_sql::FlightSQLServer;
pub use self::http::HttpHandler;
pub use self::http::HttpHandlerKind;
pub use self::mysql::MySQLConnection;
pub use self::mysql::MySQLFederated;
pub use self::mysql::MySQLHandler;
pub use self::mysql::MySQLTlsConfig;

pub mod admin;
pub(crate) mod federated_helper;
pub mod flight;
pub mod flight_sql;
pub mod http;
pub mod metrics;
mod mysql;
pub(crate) mod server;
