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

use std::sync::Arc;

use common_base::tokio::net::TcpStream;
use common_clickhouse_srv::connection::Connection;
use common_clickhouse_srv::error_codes::NO_FREE_CONNECTION;
use common_clickhouse_srv::errors::Error;
use common_clickhouse_srv::errors::Result as CHResult;
use common_clickhouse_srv::errors::ServerError;
use common_clickhouse_srv::protocols::Packet;
use common_clickhouse_srv::CHContext;
use common_clickhouse_srv::ClickHouseSession;
use common_clickhouse_srv::QueryState;
use common_exception::ErrorCode;
use common_exception::Result;

pub struct RejectCHConnection;

impl RejectCHConnection {
    pub async fn reject(stream: TcpStream, error: ErrorCode) -> Result<()> {
        let mut ctx = CHContext::new(QueryState::default());

        let dummy_session = DummyCHSession::create();
        match Connection::new(stream, dummy_session, String::from("UTC")) {
            Err(_) => Err(ErrorCode::LogicalError("Cannot create connection")),
            Ok(mut connection) => {
                if let Ok(Some(Packet::Hello(_))) = connection.read_packet(&mut ctx).await {
                    let server_error = Error::Server(ServerError {
                        code: NO_FREE_CONNECTION,
                        name: String::from("NO_FREE_CONNECTION"),
                        message: error.message(),
                        stack_trace: String::from(""),
                    });
                    let _ = connection.write_error(&server_error).await;
                }

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
