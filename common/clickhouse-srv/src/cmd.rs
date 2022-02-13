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

use crate::binary::Encoder;
use crate::connection::Connection;
use crate::error_codes::WRONG_PASSWORD;
use crate::errors::Error;
use crate::errors::Result;
use crate::errors::ServerError;
use crate::protocols::HelloResponse;
use crate::protocols::Packet;
use crate::protocols::Stage;
use crate::protocols::SERVER_PONG;
use crate::CHContext;

pub struct Cmd {
    packet: Packet,
}

impl Cmd {
    pub fn create(packet: Packet) -> Self {
        Self { packet }
    }

    pub async fn apply(self, connection: &mut Connection, ctx: &mut CHContext) -> Result<()> {
        let mut encoder = Encoder::new();
        match self.packet {
            Packet::Ping => {
                encoder.uvarint(SERVER_PONG);
            }
            // todo cancel
            Packet::Cancel => {}
            Packet::Hello(hello) => {
                if !connection
                    .session
                    .authenticate(&hello.user, &hello.password, &connection.client_addr)
                    .await
                {
                    let err = Error::Server(ServerError {
                        code: WRONG_PASSWORD,
                        name: "AuthenticateException".to_owned(),
                        message: "Unknown user or wrong password".to_owned(),
                        stack_trace: "".to_owned(),
                    });
                    connection.write_error(&err).await?;
                    return Err(err);
                }

                let response = HelloResponse {
                    dbms_name: connection.session.dbms_name().to_string(),
                    dbms_version_major: connection.session.dbms_version_major(),
                    dbms_version_minor: connection.session.dbms_version_minor(),
                    dbms_tcp_protocol_version: connection.session.dbms_tcp_protocol_version(),
                    timezone: connection.session.timezone().to_string(),
                    server_display_name: connection.session.server_display_name().to_string(),
                    dbms_version_patch: connection.session.dbms_version_patch(),
                };

                ctx.client_revision = connection
                    .session
                    .dbms_tcp_protocol_version()
                    .min(hello.client_revision);
                ctx.hello = Some(hello);

                response.encode(&mut encoder, ctx.client_revision)?;
            }
            Packet::Query(query) => {
                ctx.state.query = query.query.clone();
                ctx.state.compression = query.compression;

                let session = connection.session.clone();
                session.execute_query(ctx, connection).await?;

                if ctx.state.out.is_some() {
                    ctx.state.stage = Stage::InsertPrepare;
                } else {
                    connection.write_end_of_stream().await?;
                }
            }
            Packet::Data(block) => {
                if block.is_empty() {
                    match ctx.state.stage {
                        Stage::InsertPrepare => {
                            ctx.state.stage = Stage::InsertStarted;
                        }
                        Stage::InsertStarted => {
                            // reset will reset the out, so the outer stream will break
                            ctx.state.reset();

                            ctx.state.sent_all_data.notified().await;
                            // wait stream finished
                            connection.write_end_of_stream().await?;
                            ctx.state.stage = Stage::Default;
                        }
                        _ => {}
                    }
                } else if let Some(out) = &ctx.state.out {
                    // out.block_stream.
                    out.send(block).await.unwrap();
                }
            }
        };

        let bytes = encoder.get_buffer();
        if !bytes.is_empty() {
            connection.write_bytes(bytes).await?;
        }
        Ok(())
    }
}
