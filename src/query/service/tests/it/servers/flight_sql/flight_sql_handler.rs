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
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
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
use databend_common_base::runtime::Runtime;
use databend_common_config::InnerConfig;
use databend_common_config::UserAuthConfig;
use databend_common_config::UserConfig;
use databend_common_exception::Result;
use databend_common_meta_app::principal::PasswordHashMethod;
use databend_common_meta_app::schema::ListLockRevReq;
use databend_common_meta_app::schema::LockKey;
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

async fn assert_prepared_update_lock(
    fixture: &TestFixture,
    client: &mut FlightSqlServiceClient<Channel>,
) {
    let table_name = format!(
        "{}.{}",
        fixture.default_db_name(),
        fixture.default_table_name()
    );
    let statement = client
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
    let revisions = catalog.list_lock_revisions(request.clone()).await.unwrap();
    assert_eq!(
        revisions.len(),
        1,
        "prepare must hold a revision to release"
    );
    assert!(revisions[0].1.acquired_on.is_some());

    statement.close().await.unwrap();
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
    .expect("Close must release the prepared UPDATE's table lock");
    client
        .execute_update("SET acquire_lock_timeout = 0".to_string(), None)
        .await
        .unwrap();
    assert_eq!(
        client
            .execute_update(format!("UPDATE {table_name} SET id = 3"), None)
            .await
            .unwrap(),
        1
    );
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

    let runtime = Runtime::with_default_worker_threads(Some("flight-sql-test".to_string()))?;
    runtime.block_on(async {
        let file = NamedTempFile::new().unwrap();
        let path = file.into_temp_path().to_str().unwrap().to_string();
        let _ = fs::remove_file(path.clone());

        let uds = UnixListener::bind(path.clone()).unwrap();
        let stream = UnixListenerStream::new(uds);

        // We would just listen on TCP, but it seems impossible to know when tonic is ready to serve
        let service = Arc::new(FlightSqlServiceImpl::create(&databend_common_version::BUILD_INFO));
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let serve_future = Server::builder()
            .add_service(FlightServiceServer::from_arc(service.clone()))
            .serve_with_incoming_shutdown(stream, async { shutdown_rx.await.unwrap() });

        let request_future = async {
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
            assert_prepared_update_lock(&fixture, &mut client).await;
            // Invalidate a real handshake token without waiting for its wall-clock TTL.
            let statement = client.prepare("SELECT 1".to_string(), None).await.unwrap();
            service.sessions.lock().remove(client.token().unwrap());
            let error = statement.close().await.unwrap_err();
            assert!(matches!(&error, FlightError::Tonic(status) if status.code() == Code::Unauthenticated), "{error}");
        };
        tokio::pin!(serve_future);

        tokio::select! {
            _ = &mut serve_future => panic!("server returned first"),
            _ = request_future => {
                debug!("Client finished!");
            }
        }
        shutdown_tx.send(()).unwrap();
        serve_future.await.unwrap();
        debug!("Server shutdown!");

        Ok(())
    })
}
