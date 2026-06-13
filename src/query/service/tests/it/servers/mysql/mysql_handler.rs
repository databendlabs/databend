// Copyright 2022 Datafuse Labs.
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

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use databend_common_base::runtime::Runtime;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::ToErrorCode;
use databend_query::servers::MySQLHandler;
use databend_query::servers::MySQLTlsConfig;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestFixture;
use mysql_async::FromRowError;
use mysql_async::Row;
use mysql_async::SslOpts;
use mysql_async::consts::ColumnFlags;
use mysql_async::consts::ColumnType;
use mysql_async::prelude::FromRow;
use mysql_async::prelude::Queryable;
use tokio::sync::Barrier;

use crate::tests::tls_constants::*;

#[tokio::test(flavor = "current_thread")]
async fn test_generic_code_with_on_query() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;

    let tcp_keepalive_timeout_secs = 120;
    let mut handler = MySQLHandler::create(tcp_keepalive_timeout_secs, MySQLTlsConfig::default())?;

    let listening = "127.0.0.1:0".parse::<SocketAddr>()?;
    let runnable_server = handler.start(listening).await?;
    let mut connection = create_connection(runnable_server.port(), false).await?;
    let result = connection.query_iter("SELECT 1, 2, 3;").await;

    assert!(result.is_ok());

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_connect_with_tls() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;

    let tcp_keepalive_timeout_secs = 120;
    let tls_config = MySQLTlsConfig::new(TEST_SERVER_CERT.to_string(), TEST_SERVER_KEY.to_string());
    let mut handler = MySQLHandler::create(tcp_keepalive_timeout_secs, tls_config)?;

    let listening = "127.0.0.1:0".parse::<SocketAddr>()?;
    let runnable_server = handler.start(listening).await?;

    let mut connection = create_connection(runnable_server.port(), true).await?;
    let result = connection.query_iter("SELECT 1, 2, 3;").await;

    assert!(result.is_ok());

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_rejected_session_with_sequence() -> anyhow::Result<()> {
    // TestFixture will create a default session, so we should limit the max_active_sessions to 2.
    let max_active_sessions = 2;
    let conf = ConfigBuilder::create()
        .max_active_sessions(max_active_sessions)
        .build();
    let _fixture = TestFixture::setup_with_config(&conf).await?;

    let tcp_keepalive_timeout_secs = 120;
    let mut handler = MySQLHandler::create(tcp_keepalive_timeout_secs, MySQLTlsConfig::default())?;

    let listening = "127.0.0.1:0".parse::<SocketAddr>()?;
    let listening = handler.start(listening).await?;

    {
        // Accepted connection
        let conn = create_connection(listening.port(), false).await?;
        let conn2 = create_connection(listening.port(), false).await?;

        // Rejected connection
        match create_connection(listening.port(), false).await {
            Ok(_) => panic!("Expected rejected connection"),
            Err(error) => {
                assert_eq!(error.code(), 1067);
                assert_eq!(
                    error.message(),
                    "Reject connection, cause: Server error: `ERROR HY000 (1815): Current active sessions (2) has exceeded the max_active_sessions limit (2)'"
                );
            }
        };

        drop(conn);
        drop(conn2);
    }

    // Wait for the connection to be destroyed
    tokio::time::sleep(Duration::from_secs(5)).await;
    // Accepted connection
    create_connection(listening.port(), false).await?;

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_rejected_session_with_parallel() -> anyhow::Result<()> {
    enum CreateServerResult {
        Accept,
        Rejected,
    }

    async fn connect_server(
        port: u16,
        start_barrier: Arc<Barrier>,
        destroy_barrier: Arc<Barrier>,
    ) -> CreateServerResult {
        start_barrier.wait().await;
        match create_connection(port, false).await {
            Ok(_conn) => {
                destroy_barrier.wait().await;
                CreateServerResult::Accept
            }
            Err(error) => {
                destroy_barrier.wait().await;
                assert_eq!(error.code(), 1067);
                assert_eq!(
                    error.message(),
                    "Reject connection, cause: Server error: `ERROR HY000 (1815): Current active sessions (2) has exceeded the max_active_sessions limit (2)'"
                );
                CreateServerResult::Rejected
            }
        }
    }

    // TestFixture will create a default session, so we should limit the max_active_sessions to 2.
    let max_active_sessions = 2;
    let conf = ConfigBuilder::create()
        .max_active_sessions(max_active_sessions)
        .build();
    let _fixture = TestFixture::setup_with_config(&conf).await?;

    let tcp_keepalive_timeout_secs = 120;
    let mut handler = MySQLHandler::create(tcp_keepalive_timeout_secs, MySQLTlsConfig::default())?;

    let listening = "127.0.0.1:0".parse::<SocketAddr>()?;
    let listening = handler.start(listening).await?;

    let start_barriers = Arc::new(Barrier::new(3));
    let destroy_barriers = Arc::new(Barrier::new(3));

    let runtime = Runtime::with_worker_threads(2, None)?;
    let mut join_handlers = Vec::with_capacity(3);
    for _ in 0..3 {
        let port = listening.port();
        let start_barrier = start_barriers.clone();
        let destroy_barrier = destroy_barriers.clone();

        join_handlers.push(runtime.spawn(connect_server(port, start_barrier, destroy_barrier)));
    }

    let mut accept = 0;
    let mut rejected = 0;
    for join_handler in join_handlers {
        match join_handler.await {
            Err(error) => panic!("Unexpected error: {:?}", error),
            Ok(CreateServerResult::Accept) => accept += 1,
            Ok(CreateServerResult::Rejected) => rejected += 1,
        }
    }

    assert_eq!(accept, 2);
    assert_eq!(rejected, 1);

    Ok(())
}

async fn create_connection(port: u16, with_tls: bool) -> Result<mysql_async::Conn> {
    let ssl_opts = if with_tls {
        Some(SslOpts::default().with_root_certs(vec![Path::new(TEST_CA_CERT).into()]))
    } else {
        None
    };
    let opts = mysql_async::OptsBuilder::default()
        .ip_or_hostname("localhost")
        .user(Some("root".to_string()))
        .tcp_port(port)
        .ssl_opts(ssl_opts);

    mysql_async::Conn::new(opts)
        .await
        .map_err_to_code(ErrorCode::UnknownException, || "Reject connection")
}

#[allow(dead_code)]
struct EmptyRow;

impl FromRow for EmptyRow {
    fn from_row_opt(_: Row) -> std::result::Result<Self, FromRowError>
    where Self: Sized {
        Ok(EmptyRow)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn test_unsigned_integer_column_flags() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;

    let tcp_keepalive_timeout_secs = 120;
    let mut handler = MySQLHandler::create(tcp_keepalive_timeout_secs, MySQLTlsConfig::default())?;

    let listening = "127.0.0.1:0".parse::<SocketAddr>()?;
    let runnable_server = handler.start(listening).await?;
    let mut connection = create_connection(runnable_server.port(), false).await?;

    // Test case: SELECT 32767 + 1
    // Due to constant folding and shrink_scalar, 32768 fits in UInt16.
    // The UNSIGNED_FLAG must be set so clients interpret it correctly.
    let mut result = connection.query_iter("SELECT 32767 + 1").await?;
    let columns = result.columns_ref();

    assert_eq!(columns.len(), 1);
    let col = &columns[0];
    // The type should be SHORT (INT16/UINT16 both map to MYSQL_TYPE_SHORT)
    assert_eq!(col.column_type(), ColumnType::MYSQL_TYPE_SHORT);
    // The UNSIGNED_FLAG must be set since shrink_scalar produces UInt16
    // The NOT_NULL_FLAG must be set since the expression is not nullable
    assert!(
        col.flags().contains(ColumnFlags::UNSIGNED_FLAG),
        "Expected UNSIGNED_FLAG to be set for UInt16 column, got flags: {:?}",
        col.flags()
    );
    assert!(
        col.flags().contains(ColumnFlags::NOT_NULL_FLAG),
        "Expected NOT_NULL_FLAG to be set for non-nullable column, got flags: {:?}",
        col.flags()
    );

    // Verify the value is correct and doesn't overflow
    let row: Option<Row> = result.next().await?;
    let value: (u32,) = mysql_async::from_row(row.unwrap());
    assert_eq!(value, (32768,));

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn test_null_column_flags() -> anyhow::Result<()> {
    let _fixture = TestFixture::setup().await?;

    let tcp_keepalive_timeout_secs = 120;
    let mut handler = MySQLHandler::create(tcp_keepalive_timeout_secs, MySQLTlsConfig::default())?;

    let listening = "127.0.0.1:0".parse::<SocketAddr>()?;
    let runnable_server = handler.start(listening).await?;
    let mut connection = create_connection(runnable_server.port(), false).await?;

    let mut result = connection.query_iter("SELECT NULL").await?;
    let columns = result.columns_ref();

    assert_eq!(columns.len(), 1);
    let col = &columns[0];
    assert_eq!(col.column_type(), ColumnType::MYSQL_TYPE_NULL);
    assert!(
        !col.flags().contains(ColumnFlags::NOT_NULL_FLAG),
        "Expected NOT_NULL_FLAG to be unset for NULL column, got flags: {:?}",
        col.flags()
    );

    let row: Option<Row> = result.next().await?;
    let value: (Option<u8>,) = mysql_async::from_row(row.unwrap());
    assert_eq!(value, (None,));

    Ok(())
}
