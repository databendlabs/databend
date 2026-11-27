// Copyright 2023 Datafuse Labs.
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

use std::fs;
use std::future::Future;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_cast::display::array_value_to_string;
use arrow_cast::pretty::pretty_format_batches;
use arrow_flight::Action;
use arrow_flight::FlightData;
use arrow_flight::FlightDescriptor;
use arrow_flight::Ticket;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::sql::ActionClosePreparedStatementRequest;
use arrow_flight::sql::Any;
use arrow_flight::sql::CommandPreparedStatementQuery;
use arrow_flight::sql::CommandPreparedStatementUpdate;
use arrow_flight::sql::ProstMessageExt;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::client::PreparedStatement;
use databend_common_base::runtime::Runtime;
use databend_common_config::InnerConfig;
use databend_common_config::UserAuthConfig;
use databend_common_config::UserConfig;
use databend_common_exception::Result;
use databend_common_meta_app::principal::PasswordHashMethod;
use databend_common_meta_app::schema::ListLockRevReq;
use databend_common_meta_app::schema::LockKey;
use databend_query::catalogs::Catalog;
use databend_query::servers::flight_sql::flight_sql_service::FlightSqlServiceImpl;
use databend_query::sessions::TableContextTableAccess;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestFixture;
use futures::TryStreamExt;
use goldenfile::Mint;
use hyper_util::rt::TokioIo;
use log::debug;
use prost::Message;
use tempfile::NamedTempFile;
use tokio::net::UnixListener;
use tokio::net::UnixStream;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::Code;
use tonic::Request;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use tonic::transport::Server;
use tower::service_fn;
use uuid::Uuid;

const TEST_USER: &str = "test_user";
const TEST_PASSWORD: &str = "test_password";

async fn client_with_uds(path: String) -> FlightSqlServiceClient<Channel> {
    let connector = service_fn(move |_| {
        let path = path.clone();
        async move {
            let s = UnixStream::connect(path).await?;
            Ok::<_, std::io::Error>(TokioIo::new(s))
        }
    });
    let channel = Endpoint::try_from("http://example.com")
        .unwrap()
        .connect_with_connector(connector)
        .await
        .unwrap();
    FlightSqlServiceClient::new(channel)
}

async fn run_query(
    client: &mut FlightSqlServiceClient<Channel>,
    sql: &str,
) -> std::result::Result<String, FlightError> {
    let mut stmt = client.prepare(sql.to_string(), None).await?;
    let res = if stmt.dataset_schema()?.fields.is_empty() {
        let affected_rows = client.execute_update(sql.to_string(), None).await?;
        affected_rows.to_string()
    } else {
        let flight_info = stmt.execute().await?;
        let ticket = flight_info.endpoint[0].ticket.as_ref().unwrap().clone();
        let flight_data = client.do_get(ticket).await?;
        let batches: Vec<RecordBatch> = flight_data.try_collect().await.unwrap();
        pretty_format_batches(batches.as_slice())
            .map_err(FlightError::from)?
            .to_string()
    };
    stmt.close().await?;
    Ok(res)
}

fn prepared_request<T>(
    client: &FlightSqlServiceClient<Channel>,
    message: T,
    authenticated: bool,
) -> Request<T> {
    let mut request = Request::new(message);
    if authenticated {
        let token = client.token().expect("client must be authenticated");
        request
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
    }
    request
}

async fn assert_prepared_put_handle_errors(client: &mut FlightSqlServiceClient<Channel>) {
    // Go through the real FlightService dispatcher: PeekableFlightDataStream cannot be
    // constructed directly, and both Query and Update commands must reject malformed UUIDs.
    for (bytes, authenticated, expected) in [
        (
            Uuid::nil().to_string().into_bytes(),
            true,
            Code::InvalidArgument,
        ),
        (Uuid::nil().as_bytes().to_vec(), true, Code::NotFound),
        // One malformed handle per handler is enough to verify authentication comes first.
        (
            Uuid::nil().to_string().into_bytes(),
            false,
            Code::Unauthenticated,
        ),
    ] {
        for command in [
            CommandPreparedStatementQuery {
                prepared_statement_handle: bytes.clone().into(),
            }
            .as_any(),
            CommandPreparedStatementUpdate {
                prepared_statement_handle: bytes.clone().into(),
            }
            .as_any(),
        ] {
            let data = FlightData {
                flight_descriptor: Some(FlightDescriptor::new_cmd(command.encode_to_vec())),
                ..Default::default()
            };
            let request = prepared_request(client, futures::stream::iter([data]), authenticated);
            let error = client
                .inner_mut()
                .do_put(request)
                .await
                .expect_err("an invalid or unknown prepared handle must not execute");
            assert_eq!(error.code(), expected, "{}: {error}", command.type_url);
        }
    }
}

async fn assert_prepared_statement_lifecycle(client: &mut FlightSqlServiceClient<Channel>) {
    let mut statement = client.prepare("SELECT 1".to_string(), None).await.unwrap();
    for authenticated in [false, true] {
        let expected = if authenticated {
            Code::InvalidArgument
        } else {
            Code::Unauthenticated
        };
        // UUID text is malformed here: prepared statement handles are opaque binary UUIDs.
        let handle = Uuid::nil().to_string().into_bytes();
        let action = Action {
            r#type: "ClosePreparedStatement".to_string(),
            body: ActionClosePreparedStatementRequest {
                prepared_statement_handle: handle.clone().into(),
            }
            .as_any()
            .encode_to_vec()
            .into(),
        };
        let request = prepared_request(client, action, authenticated);
        let error = client.inner_mut().do_action(request).await.unwrap_err();
        assert_eq!(error.code(), expected);
        let command = CommandPreparedStatementQuery {
            prepared_statement_handle: handle.into(),
        };
        let descriptor = FlightDescriptor::new_cmd(command.as_any().encode_to_vec());
        let request = prepared_request(client, descriptor, authenticated);
        let error = client
            .inner_mut()
            .get_flight_info(request)
            .await
            .unwrap_err();
        assert_eq!(error.code(), expected);
    }

    // The real SDK must still be able to use the statement after the rejected requests.
    let info = statement.execute().await.unwrap();
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    let mut malformed = Any::decode(ticket.ticket.as_ref()).unwrap();
    // An empty protobuf message gives FetchResults its default (empty) UUID string.
    malformed.value = Default::default();
    let request = prepared_request(
        client,
        Ticket {
            ticket: malformed.encode_to_vec().into(),
        },
        true,
    );
    let error = client.inner_mut().do_get(request).await.unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);

    statement.clone().close().await.unwrap();
    let error = statement.execute().await.unwrap_err();
    assert!(
        matches!(&error, FlightError::Tonic(status) if status.code() == Code::NotFound),
        "{error}"
    );
    let request = prepared_request(client, ticket, true);
    let error = client.inner_mut().do_get(request).await.unwrap_err();
    assert_eq!(error.code(), Code::NotFound);
    statement.close().await.unwrap(); // Closing an already removed statement is idempotent.
}

async fn wait_for_table_unlocked(
    catalog: &Arc<dyn Catalog>,
    request: &ListLockRevReq,
    message: &str,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !catalog
            .list_lock_revisions(request.clone())
            .await
            .unwrap()
            .is_empty()
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect(message);
}

async fn assert_prepared_update_lock(
    fixture: &TestFixture,
    client: &mut FlightSqlServiceClient<Channel>,
) {
    let table_name = format!(
        "{}.{}",
        fixture.default_db_name(),
        fixture.default_table_name()
    );
    let mut statement = client
        .prepare(format!("UPDATE {table_name} SET id = 2"), None)
        .await
        .unwrap();
    let ctx = fixture.new_query_ctx().await.unwrap();
    let catalog = ctx
        .get_catalog(&fixture.default_catalog_name())
        .await
        .unwrap();
    let table = fixture.latest_default_table().await.unwrap();
    let request = ListLockRevReq::new(LockKey::Table {
        tenant: fixture.default_tenant(),
        table_id: table.get_id(),
    });
    // Prepare binds the UPDATE to get its schema and drops the plan with its table lock: each
    // execution binds it again and takes the lock for that execution only.
    wait_for_table_unlocked(&catalog, &request, "prepare must not keep the table lock").await;

    client
        .execute_update("SET acquire_lock_timeout = 0".to_string(), None)
        .await
        .unwrap();
    // An open prepared UPDATE does not block another writer.
    assert_eq!(
        client
            .execute_update(format!("UPDATE {table_name} SET id = 3"), None)
            .await
            .unwrap(),
        1
    );
    for _ in 0..2 {
        assert_eq!(statement.execute_update().await.unwrap(), 1);
        wait_for_table_unlocked(
            &catalog,
            &request,
            "an execution must release its table lock",
        )
        .await;
    }
    statement.close().await.unwrap();
}

fn prepare_config() -> InnerConfig {
    let hash_method = PasswordHashMethod::DoubleSha1;
    let hash_value = hash_method.hash(TEST_PASSWORD.as_bytes());

    let user_config = UserConfig {
        name: TEST_USER.to_string(),
        auth: UserAuthConfig {
            auth_type: "double_sha1_password".to_string(),
            auth_string: Some(hex::encode(hash_value)),
        },
    };
    ConfigBuilder::create()
        .add_user(TEST_USER, user_config)
        .build()
}

/// Serves `service` on a Unix socket, runs `requests` with the socket path, then shuts it down.
fn serve<Fut: Future<Output = ()>>(
    service: Arc<FlightSqlServiceImpl>,
    requests: impl FnOnce(String) -> Fut,
) -> Result<()> {
    let runtime = Runtime::with_default_worker_threads(Some("flight-sql-test".to_string()))?;
    runtime.block_on(async move {
        let file = NamedTempFile::new().unwrap();
        let path = file.into_temp_path().to_str().unwrap().to_string();
        let _ = fs::remove_file(path.clone());

        let uds = UnixListener::bind(path.clone()).unwrap();
        let stream = UnixListenerStream::new(uds);

        // We would just listen on TCP, but it seems impossible to know when tonic is ready to serve
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let serve_future = Server::builder()
            .add_service(FlightServiceServer::from_arc(service))
            .serve_with_incoming_shutdown(stream, async { shutdown_rx.await.unwrap() });
        tokio::pin!(serve_future);

        tokio::select! {
            _ = &mut serve_future => panic!("server returned first"),
            _ = requests(path) => {
                debug!("Client finished!");
            }
        }
        shutdown_tx.send(()).unwrap();
        serve_future.await.unwrap();
        debug!("Server shutdown!");

        Ok(())
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_query() -> Result<()> {
    let fixture = TestFixture::setup_with_config(&prepare_config()).await?;
    fixture.create_default_database().await?;
    fixture.create_default_table().await?;
    fixture
        .execute_command(&format!(
            "INSERT INTO {}.{}(id) VALUES (1)",
            fixture.default_db_name(),
            fixture.default_table_name()
        ))
        .await?;

    let service = Arc::new(FlightSqlServiceImpl::create(
        &databend_common_version::BUILD_INFO,
    ));
    let fixture = &fixture;
    serve(service.clone(), |path| async move {
        let mut mint = Mint::new("tests/it/servers/flight_sql/testdata");
        let mut file = mint.new_goldenfile("query.txt").unwrap();

        let mut client = client_with_uds(path).await;
        let token = client.handshake(TEST_USER, TEST_PASSWORD).await.unwrap();

        debug!("Auth succeeded with token: {:?}", token);
        assert_prepared_put_handle_errors(&mut client).await;
        let cases = [
            "select 1, 'abc', 1.1, 1.1::float32 as c, 1::nullable(int) as b",
            "select [1, 2]",
            "select (1, 1.1)",
            "select {1: 11, 2: 22}",
            "show tables",
            "drop table if exists test1",
            "create table test1(a int, b string)",
            "insert into table test1(a, b) values (1, 'x'), (2, 'y')",
            "select * from test1",
        ];
        for case in cases {
            writeln!(file, "---------- Input ----------").unwrap();
            writeln!(file, "{}", case).unwrap();
            writeln!(file, "---------- Output ---------").unwrap();
            let res = match run_query(&mut client, case).await {
                Ok(s) => s,
                Err(e) => format!("{e:?}"),
            };
            writeln!(file, "{}", res).unwrap();
        }
        assert_prepared_statement_lifecycle(&mut client).await;
        assert_prepared_update_lock(fixture, &mut client).await;
        // Invalidate a real handshake token without waiting for its wall-clock TTL.
        let statement = client.prepare("SELECT 1".to_string(), None).await.unwrap();
        service.sessions.lock().remove(client.token().unwrap());
        let error = statement.close().await.unwrap_err();
        assert!(
            matches!(&error, FlightError::Tonic(status) if status.code() == Code::Unauthenticated),
            "{error}"
        );
    })
}

/// Executes `statement` and returns the first value of its result, as text.
async fn first_value(
    client: &mut FlightSqlServiceClient<Channel>,
    statement: &mut PreparedStatement<Channel>,
) -> String {
    let info = statement.execute().await.unwrap();
    let ticket = info.endpoint[0].ticket.clone().unwrap();
    let batches: Vec<RecordBatch> = client
        .do_get(ticket)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let batch = batches.iter().find(|batch| batch.num_rows() > 0).unwrap();
    array_value_to_string(batch.column(0), 0).unwrap()
}

/// Waits until `sql` would read a cached result in the session of `client`.
async fn wait_for_cached_result(client: &mut FlightSqlServiceClient<Channel>, sql: &str) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let mut explain = client
                .prepare(format!("EXPLAIN {sql}"), None)
                .await
                .unwrap();
            let plan = first_value(client, &mut explain).await;
            explain.close().await.unwrap();
            if plan == "ReadQueryResultCache" {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the result of the first execution must be cached");
}

/// A prepared statement is bound again when it executes, in the context that runs it: the
/// values binding resolves, and the result cache key they are added to, are those of the user
/// and roles at execution, not at prepare.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_prepared_statement_binds_on_execution() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    fixture.create_default_table().await?;
    let table = format!(
        "{}.{}",
        fixture.default_db_name(),
        fixture.default_table_name()
    );
    for sql in [
        format!("INSERT INTO {table}(id) VALUES (1)"),
        "CREATE ROLE flight_prepared_role".to_string(),
        "CREATE USER flight_prepared_a IDENTIFIED BY 'password'".to_string(),
        "CREATE USER flight_prepared_b IDENTIFIED BY 'password'".to_string(),
        "GRANT ROLE flight_prepared_role TO flight_prepared_a".to_string(),
        format!("GRANT SELECT ON {table} TO flight_prepared_a"),
        format!("GRANT SELECT ON {table} TO flight_prepared_b"),
    ] {
        fixture.execute_command(&sql).await?;
    }

    let service = Arc::new(FlightSqlServiceImpl::create(
        &databend_common_version::BUILD_INFO,
    ));
    let fixture = &fixture;
    serve(service, |path| async move {
        let mut user_a = client_with_uds(path.clone()).await;
        user_a
            .handshake("flight_prepared_a", "password")
            .await
            .unwrap();
        let mut user_b = client_with_uds(path).await;
        user_b
            .handshake("flight_prepared_b", "password")
            .await
            .unwrap();

        // Binding resolves IS_ROLE_IN_SESSION to a constant: after a REVOKE, a statement
        // prepared before it must not run with the value bound at prepare.
        let sql = "SELECT is_role_in_session('flight_prepared_role')";
        let mut in_role = user_a.prepare(sql.to_string(), None).await.unwrap();
        assert_eq!(first_value(&mut user_a, &mut in_role).await, "true");
        fixture
            .execute_command("REVOKE ROLE flight_prepared_role FROM flight_prepared_a")
            .await
            .unwrap();
        assert_eq!(first_value(&mut user_a, &mut in_role).await, "false");
        in_role.close().await.unwrap();

        // Binding adds the value of current_user() to the result cache key of its context: the
        // execution must use that key, or user b reads the result cached for user a.
        for client in [&mut user_a, &mut user_b] {
            for setting in [
                "SET enable_query_result_cache = 1",
                "SET query_result_cache_min_execute_secs = 0",
            ] {
                client
                    .execute_update(setting.to_string(), None)
                    .await
                    .unwrap();
            }
        }
        let sql = format!("SELECT current_user(), id FROM {table}");
        let mut by_a = user_a.prepare(sql.clone(), None).await.unwrap();
        assert_eq!(
            first_value(&mut user_a, &mut by_a).await,
            "'flight_prepared_a'@'%'"
        );
        wait_for_cached_result(&mut user_a, &sql).await;
        assert_eq!(
            first_value(&mut user_a, &mut by_a).await,
            "'flight_prepared_a'@'%'"
        );
        let mut by_b = user_b.prepare(sql, None).await.unwrap();
        assert_eq!(
            first_value(&mut user_b, &mut by_b).await,
            "'flight_prepared_b'@'%'"
        );
        by_a.close().await.unwrap();
        by_b.close().await.unwrap();
    })
}
