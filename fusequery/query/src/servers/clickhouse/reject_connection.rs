// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::{Result, ErrorCode};
use common_runtime::tokio::net::TcpStream;
use std::time::Instant;
use clickhouse_srv::connection::Connection;
use clickhouse_srv::{ClickHouseSession, CHContext, QueryState};
use std::sync::Arc;
use clickhouse_srv::protocols::Packet;
use clickhouse_srv::errors::{Error, ServerError};
use clickhouse_srv::error_codes::NO_FREE_CONNECTION;
use clickhouse_srv::errors::Result as CHResult;

pub struct RejectCHConnection;

impl RejectCHConnection {
    pub async fn reject(stream: TcpStream, error: ErrorCode) -> Result<()> {
        let instant = Instant::now();
        let mut ctx = CHContext::new(QueryState::default());

        let dummy_session = DummyCHSession::create();
        match Connection::new(stream, dummy_session, String::from("UTC")) {
            Err(_) => Err(ErrorCode::LogicalError("Cannot create connection")),
            Ok(mut connection) => {
                match connection.read_packet(&mut ctx).await {
                    Ok(Some(Packet::Hello(packet))) => {
                        println!("receive packet: {:?}, {:?}", packet, instant.elapsed());
                        let server_error = Error::Server(ServerError {
                            code: NO_FREE_CONNECTION,
                            name: String::from("NO_FREE_CONNECTION"),
                            message: error.message(),
                            stack_trace: String::from(""),
                        });
                        let _ = connection.write_error(&server_error).await;
                    }
                    _ => { /* do nothing */ }
                };

                Ok(())
            }
        }
    }
}

struct DummyCHSession;

impl DummyCHSession {
    pub fn create() -> Arc<dyn ClickHouseSession> {
        Arc::new(DummyCHSession {})
    }
}

#[async_trait::async_trait]
impl ClickHouseSession for DummyCHSession {
    async fn execute_query(&self, _: &mut CHContext, _: &mut Connection) -> CHResult<()> {
        unimplemented!()
    }
}

