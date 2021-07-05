// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

// The servers module used for external communication with user, such as MySQL wired protocol, etc.

pub use clickhouse::ClickHouseHandler;
pub use server::Server;
pub use server::ShutdownHandle;

pub use self::mysql::MySQLConnection;
pub use self::mysql::MySQLHandler;

mod server;
mod clickhouse;
mod mysql;
