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

// https://github.com/rust-lang/rust-clippy/issues/8334
#![allow(clippy::ptr_arg)]

use std::sync::Arc;

use common_tracing::tracing;
use errors::Result;
use protocols::Stage;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;
use tokio::sync::Notify;

use crate::cmd::Cmd;
use crate::connection::Connection;
use crate::protocols::HelloRequest;
use crate::types::Block;
use crate::types::Progress;

pub mod binary;
pub mod cmd;
pub mod connection;
pub mod error_codes;
pub mod errors;
pub mod protocols;
pub mod types;

#[async_trait::async_trait]
pub trait ClickHouseSession: Send + Sync {
    async fn execute_query(&self, ctx: &mut CHContext, connection: &mut Connection) -> Result<()>;

    fn with_stack_trace(&self) -> bool {
        false
    }

    fn dbms_name(&self) -> &str {
        "clickhouse-server"
    }

    // None is by default, which will use same version as client send
    fn dbms_version_major(&self) -> u64 {
        19
    }

    fn dbms_version_minor(&self) -> u64 {
        17
    }

    fn dbms_tcp_protocol_version(&self) -> u64 {
        54428
    }

    fn timezone(&self) -> &str {
        "UTC"
    }

    fn server_display_name(&self) -> &str {
        "clickhouse-server"
    }

    fn dbms_version_patch(&self) -> u64 {
        1
    }

    fn get_progress(&self) -> Progress {
        Progress::default()
    }

    async fn authenticate(&self, _username: &str, _password: &[u8], _client_addr: &str) -> bool {
        true
    }
}

#[derive(Default)]
pub struct QueryState {
    pub query_id: String,
    pub stage: Stage,
    pub compression: u64,
    pub query: String,
    pub is_cancelled: bool,
    pub is_connection_closed: bool,
    /// empty or not
    pub is_empty: bool,

    /// Data was sent.
    pub sent_all_data: Arc<Notify>,
    pub out: Option<Sender<Block>>,
}

impl QueryState {
    fn reset(&mut self) {
        self.stage = Stage::Default;
        self.is_cancelled = false;
        self.is_connection_closed = false;
        self.is_empty = false;
        self.out = None;
    }
}

pub struct CHContext {
    pub state: QueryState,

    pub client_revision: u64,
    pub hello: Option<HelloRequest>,
}

impl CHContext {
    pub fn new(state: QueryState) -> Self {
        Self {
            state,
            client_revision: 0,
            hello: None,
        }
    }
}

/// A server that speaks the ClickHouseprotocol, and can delegate client commands to a backend
/// that implements [`ClickHouseSession`]
pub struct ClickHouseServer {}

impl ClickHouseServer {
    pub async fn run_on_stream(
        session: Arc<dyn ClickHouseSession>,
        stream: TcpStream,
    ) -> Result<()> {
        ClickHouseServer::run_on(session, stream).await
    }
}

impl ClickHouseServer {
    async fn run_on(session: Arc<dyn ClickHouseSession>, stream: TcpStream) -> Result<()> {
        let mut srv = ClickHouseServer {};
        srv.run(session, stream).await?;
        Ok(())
    }

    async fn run(&mut self, session: Arc<dyn ClickHouseSession>, stream: TcpStream) -> Result<()> {
        tracing::debug!("Handle New session");
        let tz = session.timezone().to_string();
        let mut ctx = CHContext::new(QueryState::default());
        let mut connection = Connection::new(stream, session, tz)?;

        loop {
            // signal.
            let maybe_packet = tokio::select! {
               res = connection.read_packet(&mut ctx) => res,
            };

            let packet = match maybe_packet {
                Ok(Some(packet)) => packet,
                Err(e) => {
                    ctx.state.reset();
                    connection.write_error(&e).await?;
                    return Err(e);
                }
                Ok(None) => {
                    tracing::debug!("{:?}", "none data reset");
                    ctx.state.reset();
                    return Ok(());
                }
            };
            let cmd = Cmd::create(packet);
            cmd.apply(&mut connection, &mut ctx).await?;
        }
    }
}

#[macro_export]
macro_rules! row {
    () => { $crate::types::RNil };
    ( $i:ident, $($tail:tt)* ) => {
        row!( $($tail)* ).put(stringify!($i).into(), $i.into())
    };
    ( $i:ident ) => { row!($i: $i) };

    ( $k:ident: $v:expr ) => {
        $crate::types::RNil.put(stringify!($k).into(), $v.into())
    };

    ( $k:ident: $v:expr, $($tail:tt)* ) => {
        row!( $($tail)* ).put(stringify!($k).into(), $v.into())
    };

    ( $k:expr => $v:expr ) => {
        $crate::types::RNil.put($k.into(), $v.into())
    };

    ( $k:expr => $v:expr, $($tail:tt)* ) => {
        row!( $($tail)* ).put($k.into(), $v.into())
    };
}
