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

use std::pin::Pin;
use std::slice::from_ref;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::StringArray;
use arrow_flight::Action;
use arrow_flight::ActionType;
use arrow_flight::Criteria;
use arrow_flight::Empty;
use arrow_flight::FlightData;
use arrow_flight::FlightDescriptor;
use arrow_flight::FlightEndpoint;
use arrow_flight::FlightInfo;
use arrow_flight::HandshakeRequest;
use arrow_flight::HandshakeResponse;
use arrow_flight::PollInfo;
use arrow_flight::PutResult;
use arrow_flight::SchemaResult;
use arrow_flight::Ticket;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field;
use arrow_schema::Schema;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::spawn;
use databend_common_catalog::table_context::TableContextTableAccess;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::Symbol;
use databend_common_expression::types::DataType;
use databend_common_expression::udf_client::UDFFlightClient;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::UserInfo;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use databend_common_sql::plans::UDFType;
use databend_common_users::UserApiProvider;
use databend_meta_client::types::MatchSeq;
use databend_query::interpreters::InterpreterFactory;
use databend_query::physical_plans::UdfFunctionDesc;
use databend_query::pipelines::processors::transforms::TransformUdfServer;
use databend_query::sql::Planner;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestFixture;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::transport::Server;

#[derive(Clone)]
enum MockMode {
    EchoString,
    Transport,
    ServerStatus(String),
    MalformedData,
    SchemaMismatch,
}

struct MockFlightService {
    mode: MockMode,
}

type FlightStream<T> = Pin<Box<dyn Stream<Item = std::result::Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl FlightService for MockFlightService {
    type HandshakeStream = FlightStream<HandshakeResponse>;

    async fn handshake(
        &self,
        _request: Request<tonic::Streaming<HandshakeRequest>>,
    ) -> std::result::Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake not supported"))
    }

    type ListFlightsStream = FlightStream<FlightInfo>;

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> std::result::Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list flights not supported"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        if matches!(self.mode, MockMode::SchemaMismatch | MockMode::EchoString) {
            let echo = matches!(self.mode, MockMode::EchoString);
            let schema = Schema::new(vec![
                Field::new(
                    "arg1",
                    if echo {
                        ArrowDataType::Utf8
                    } else {
                        ArrowDataType::Null
                    },
                    true,
                ),
                Field::new("result", ArrowDataType::Utf8, echo),
            ]);
            let descriptor = request.into_inner();
            let info = FlightInfo::new()
                .try_with_schema(&schema)
                .map_err(|e| Status::internal(e.to_string()))?
                .with_descriptor(descriptor)
                .with_endpoint(FlightEndpoint::new());
            Ok(Response::new(info))
        } else {
            Err(Status::unimplemented("get flight info not supported"))
        }
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll flight info not supported"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<SchemaResult>, Status> {
        match &self.mode {
            MockMode::SchemaMismatch => {
                let err = Status::internal("schema mismatch: expected Int32, got Utf8");
                Err(err)
            }
            _ => Err(Status::unimplemented("get schema not supported")),
        }
    }

    type DoGetStream = FlightStream<FlightData>;

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> std::result::Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get not supported"))
    }

    type DoPutStream = FlightStream<PutResult>;

    async fn do_put(
        &self,
        _request: Request<tonic::Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put not supported"))
    }

    type DoExchangeStream = FlightStream<FlightData>;

    async fn do_exchange(
        &self,
        _request: Request<tonic::Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
        let stream: FlightStream<FlightData> = match &self.mode {
            MockMode::Transport => Box::pin(stream::once(async {
                Err(Status::internal(
                    "h2 protocol error: error reading a body from connection",
                ))
            })),
            MockMode::ServerStatus(message) => {
                let message = message.clone();
                Box::pin(stream::once(async move {
                    Err(Status::invalid_argument(message))
                }))
            }
            MockMode::MalformedData => {
                let invalid = FlightData::default();
                Box::pin(stream::iter(vec![Ok(invalid)]))
            }
            MockMode::SchemaMismatch | MockMode::EchoString => {
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "result",
                    ArrowDataType::Utf8,
                    matches!(self.mode, MockMode::EchoString),
                )]));
                let batch =
                    RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(vec![
                        "hello",
                    ]))])
                    .expect("build record batch");
                let stream = stream::iter(vec![Ok(batch)]);
                let encoder = FlightDataEncoderBuilder::new()
                    .with_schema(schema)
                    .build(stream)
                    .map(|res| res.map_err(|e| Status::internal(e.to_string())));
                Box::pin(encoder)
            }
        };
        Ok(Response::new(stream))
    }

    type DoActionStream = FlightStream<arrow_flight::Result>;

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> std::result::Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action not supported"))
    }

    type ListActionsStream = FlightStream<ActionType>;

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions not supported"))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transport_error_returns_friendly_hint() -> Result<()> {
    let message = run_mock_exchange(MockMode::Transport).await?;
    assert!(
        message.contains("stopped responding before it finished"),
        "unexpected transport message: {message}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_status_error_includes_message() -> Result<()> {
    let message = run_mock_exchange(MockMode::ServerStatus(
        "remote validation failed".to_string(),
    ))
    .await?;
    assert!(
        message.contains("reported an error: remote validation failed"),
        "unexpected server status message: {message}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_data_returns_parse_hint() -> Result<()> {
    let message = run_mock_exchange(MockMode::MalformedData).await?;
    assert!(
        message.contains("could not parse"),
        "unexpected malformed data message: {message}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schema_mismatch_returns_schema_hint() -> Result<()> {
    let message = run_mock_exchange(MockMode::SchemaMismatch).await?;
    assert!(
        message.contains("return incorrect type"),
        "unexpected schema mismatch message: {message}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_access_creation_authorization_and_schema() -> Result<()> {
    let mut config = ConfigBuilder::create().config();
    config.query.common.enable_udf_server = true;
    config.query.common.udf_server_allow_insecure = true;
    config.query.common.udf_server_allow_list.clear();
    let fixture = TestFixture::setup_with_config(&config).await?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server = spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(MockFlightService {
                mode: MockMode::EchoString,
            }))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                let _ = shutdown_rx.await;
            })
            .await
    });
    let session = fixture.default_session();
    let mut admin = session.get_current_user()?;
    admin.grants.grant_role("account_admin".into());
    session.set_authed_user(admin.clone(), None).await?;
    session.set_current_role_checked("account_admin").await?;
    for sql in [
        format!("CREATE NETWORK RULE auth_rule TYPE = HOST_PORT MODE = EGRESS VALUE_LIST = ('{address}')"),
        "CREATE EXTERNAL ACCESS INTEGRATION auth_access ALLOWED_NETWORK_RULES = (auth_rule) ENABLED = TRUE".into(),
        "CREATE ROLE udf_developer".into(),
        "GRANT CREATE ON *.* TO ROLE udf_developer".into(),
    ] { fixture.execute_command(&sql).await?; }
    let mut developer = UserInfo::new("udf_developer", "%", AuthInfo::None);
    developer.grants.grant_role("udf_developer".into());
    session.set_authed_user(developer.clone(), None).await?;
    session.set_current_role_checked("udf_developer").await?;
    session.set_secondary_roles_checked(Some(vec![])).await?;
    let create = format!(
        "CREATE FUNCTION managed_echo(STRING) RETURNS STRING LANGUAGE python IMMUTABLE HANDLER = 'echo' ADDRESS = 'http://{address}' EXTERNAL_ACCESS_INTEGRATIONS = (auth_access)"
    );
    assert_eq!(
        fixture.execute_command(&create).await.unwrap_err().code(),
        ErrorCode::PERMISSION_DENIED
    );
    assert_eq!(
        fixture
            .execute_command("ALTER EXTERNAL ACCESS INTEGRATION auth_access SET ENABLED = FALSE")
            .await
            .unwrap_err()
            .code(),
        ErrorCode::PERMISSION_DENIED
    );
    session.set_authed_user(admin.clone(), None).await?;
    session.set_current_role_checked("account_admin").await?;
    fixture
        .execute_command("GRANT USAGE ON INTEGRATION auth_access TO ROLE udf_developer")
        .await?;
    session.set_authed_user(developer.clone(), None).await?;
    session.set_current_role_checked("udf_developer").await?;
    // Integration USAGE is additional authorization, not a replacement for
    // the existing SUPER requirement to create any UDF.
    assert_eq!(
        fixture.execute_command(&create).await.unwrap_err().code(),
        ErrorCode::PERMISSION_DENIED
    );
    session.set_authed_user(admin.clone(), None).await?;
    session.set_current_role_checked("account_admin").await?;
    fixture
        .execute_command("GRANT SUPER ON *.* TO ROLE udf_developer")
        .await?;
    fixture
        .execute_command("REVOKE USAGE ON INTEGRATION auth_access FROM ROLE udf_developer")
        .await?;
    session.set_authed_user(developer.clone(), None).await?;
    session.set_current_role_checked("udf_developer").await?;
    // Having SUPER does not remove the integration USAGE check either.
    assert_eq!(
        fixture.execute_command(&create).await.unwrap_err().code(),
        ErrorCode::PERMISSION_DENIED
    );
    session.set_authed_user(admin.clone(), None).await?;
    session.set_current_role_checked("account_admin").await?;
    fixture
        .execute_command("GRANT USAGE ON INTEGRATION auth_access TO ROLE udf_developer")
        .await?;
    session.set_authed_user(developer.clone(), None).await?;
    session.set_current_role_checked("udf_developer").await?;
    fixture.execute_command(&create).await?;
    // Integration authorization does not skip the original Flight schema check.
    let bad_schema = create.replace("managed_echo(STRING)", "bad_schema(INT)");
    assert!(
        fixture
            .execute_command(&bad_schema)
            .await
            .unwrap_err()
            .message()
            .contains("arg types mismatch")
    );
    // The legacy path still requires the old configured allow list.
    let legacy = create
        .replace("managed_echo", "legacy_echo")
        .replace(" EXTERNAL_ACCESS_INTEGRATIONS = (auth_access)", "");
    assert!(
        fixture
            .execute_command(&legacy)
            .await
            .unwrap_err()
            .message()
            .contains("udf_server_allow_list")
    );
    session.set_authed_user(admin.clone(), None).await?;
    session.set_current_role_checked("account_admin").await?;
    fixture
        .execute_command("REVOKE USAGE ON INTEGRATION auth_access FROM ROLE udf_developer")
        .await?;
    session.set_authed_user(developer, None).await?;
    session.set_current_role_checked("udf_developer").await?;
    assert_eq!(
        fixture
            .execute_command(&create.replace("managed_echo", "revoked_echo"))
            .await
            .unwrap_err()
            .code(),
        ErrorCode::PERMISSION_DENIED
    );
    // Revoking integration USAGE does not revoke UDF invocation rights.
    fixture
        .execute_command("SELECT managed_echo('hello')")
        .await?;
    let ctx = fixture.new_query_ctx().await?;
    let (plan, _) = Planner::new(ctx.clone())
        .plan_sql("SELECT managed_echo('hello')")
        .await?;
    session.set_authed_user(admin, None).await?;
    session.set_current_role_checked("account_admin").await?;
    fixture
        .execute_command("ALTER EXTERNAL ACCESS INTEGRATION auth_access SET ENABLED = FALSE")
        .await?;
    // Even IMMUTABLE calls with constant arguments must not be folded away.
    let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;
    let result = async {
        interpreter
            .execute(ctx)
            .await?
            .try_collect::<Vec<DataBlock>>()
            .await
    }
    .await;
    assert!(result.unwrap_err().message().contains("disabled"));
    let _ = shutdown_tx.send(());
    server.await.unwrap().unwrap();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_access_rechecks_already_built_transform() -> Result<()> {
    let mut config = ConfigBuilder::create().config();
    config.query.common.enable_udf_server = true;
    config.query.common.udf_server_allow_insecure = true;
    let fixture = TestFixture::setup_with_config(&config).await?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server = spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(MockFlightService {
                mode: MockMode::SchemaMismatch,
            }))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                let _ = shutdown_rx.await;
            })
            .await
    });
    fixture.execute_command(&format!("CREATE NETWORK RULE live_rule TYPE = HOST_PORT MODE = EGRESS VALUE_LIST = ('{address}')")).await?;
    fixture.execute_command("CREATE EXTERNAL ACCESS INTEGRATION live_access ALLOWED_NETWORK_RULES = (live_rule) ENABLED = TRUE").await?;
    let ctx = fixture.new_query_ctx().await?;
    let func = UdfFunctionDesc {
        external_access_integrations: vec!["live_access".into()],
        name: "policy_udf".into(),
        func_name: "mock_handler".into(),
        output_column: Symbol::new(1),
        arg_indices: vec![0],
        arg_exprs: vec!["NULL".into()],
        data_type: Box::new(DataType::String),
        headers: Default::default(),
        udf_type: UDFType::Server(format!("http://{address}")),
    };
    let endpoints = TransformUdfServer::init_endpoints(ctx.clone(), from_ref(&func))?;
    let semaphore = TransformUdfServer::init_semaphore(ctx.clone())?;
    let transform = Arc::new(Mutex::new(TransformUdfServer::new(
        ctx.clone(),
        vec![func],
        semaphore,
        endpoints,
    )?));
    // Use Databend's runtime: unlike plain Tokio worker threads, its workers
    // carry the fixture's named global registry (also needed by QueryContext).
    let runtime = GlobalIORuntime::instance();
    let run = || {
        let transform = transform.clone();
        runtime.spawn(async move {
            let input = DataBlock::new(vec![BlockEntry::from(Column::Null { len: 1 })], 1);
            transform.lock().await.transform(input).await
        })
    };
    assert_eq!(run().await??.num_columns(), 2);

    // Reuse exactly the same transform/endpoints: binding-time checks cannot
    // protect a plan that is already built when an administrator changes policy.
    fixture
        .execute_command("ALTER EXTERNAL ACCESS INTEGRATION live_access SET ENABLED = FALSE")
        .await?;
    let err = run().await?.unwrap_err();
    assert!(err.message().contains("disabled"), "{err}");
    fixture
        .execute_command("ALTER EXTERNAL ACCESS INTEGRATION live_access SET ENABLED = TRUE")
        .await?;
    fixture
        .execute_command("ALTER NETWORK RULE live_rule SET VALUE_LIST = ('denied.example.com:443')")
        .await?;
    let err = run().await?.unwrap_err();
    assert!(err.message().contains("not allowed"), "{err}");
    fixture
        .execute_command(&format!(
            "ALTER NETWORK RULE live_rule SET VALUE_LIST = ('{address}')"
        ))
        .await?;
    assert_eq!(run().await??.num_columns(), 2);
    // Removing metadata is fail-closed as well.
    let tenant = ctx.get_tenant();
    UserApiProvider::instance()
        .external_access_integration_api(&tenant)
        .remove("live_access", MatchSeq::GE(1))
        .await?;
    assert!(run().await?.is_err());
    let _ = shutdown_tx.send(());
    server.await.unwrap().unwrap();
    Ok(())
}

async fn run_mock_exchange(mode: MockMode) -> Result<String> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let incoming = TcpListenerStream::new(listener);

    let service = MockFlightService { mode };
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let server = databend_common_base::runtime::spawn(async move {
        let _ = Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.await;
            })
            .await;
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let endpoint =
        UDFFlightClient::build_endpoint(&format!("http://{}", address), 3, 3, "udf-client-test")?;
    let mut client = UDFFlightClient::connect("mock_udf", endpoint, 3, 1024).await?;

    let num_rows = 1;
    let args = vec![BlockEntry::from(Column::Null { len: num_rows })];
    let return_type = DataType::Null;
    let result = timeout(
        std::time::Duration::from_secs(5),
        client.do_exchange(
            "mock_udf",
            "mock_handler",
            Some(num_rows),
            args,
            &return_type,
        ),
    )
    .await
    .expect("do_exchange future timed out");

    let _ = shutdown_tx.send(());
    let _ = timeout(std::time::Duration::from_secs(5), server)
        .await
        .expect("server shutdown timed out");

    let err = result.expect_err("expected failure");
    Ok(err.message().to_string())
}
