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

use std::collections::BTreeMap;
use std::pin::Pin;
use std::slice::from_ref;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

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
use concurrent_queue::ConcurrentQueue;
use databend_common_base::runtime::CaptureLogSettings;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrackingPayloadExt;
use databend_common_base::runtime::spawn;
use databend_common_catalog::table_context::TableContextCluster;
use databend_common_catalog::table_context::TableContextQueryIdentity;
use databend_common_catalog::table_context::TableContextSettings;
use databend_common_catalog::table_context::TableContextTableAccess;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::Symbol;
use databend_common_expression::types::DataType;
use databend_common_expression::udf_client::UDFFlightClient;
use databend_common_expression::udf_client::UdfConnectResult;
use databend_common_expression::udf_client::UdfFailurePhase;
use databend_common_expression::udf_client::UdfFlightFailure;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::UDFDefinition;
use databend_common_meta_app::principal::UserInfo;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use databend_common_sql::plans::UDFType;
use databend_common_tracing::LogFormat;
use databend_common_tracing::UDF_PEER_LOG_TARGET;
use databend_common_tracing::init_logging;
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
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpSocket;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::sync::oneshot;
use tokio::time::sleep;
use tokio::time::timeout;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Code;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::transport::Certificate;
use tonic::transport::ClientTlsConfig;
use tonic::transport::Endpoint;
use tonic::transport::Identity;
use tonic::transport::Server;
use tonic::transport::ServerTlsConfig;

#[derive(Clone)]
enum MockMode {
    EchoString,
    Transport,
    ServerStatus(String),
    Unavailable,
    UnavailableOnce(Arc<AtomicBool>),
    MalformedData,
    SchemaMismatch,
    HoldAfterResponse,
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
        request: Request<tonic::Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
        if matches!(self.mode, MockMode::HoldAfterResponse) {
            // Receiving headers alone cannot trigger a response in the socket-loss test.
            let mut body = request.into_inner();
            while body.message().await?.is_some() {}
        }
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
            MockMode::Unavailable => return Err(Status::unavailable("private upstream message")),
            MockMode::UnavailableOnce(first) if first.swap(false, Ordering::Relaxed) => {
                return Err(Status::unavailable("private upstream message"));
            }
            MockMode::MalformedData => {
                let invalid = FlightData::default();
                Box::pin(stream::iter(vec![Ok(invalid)]))
            }
            MockMode::SchemaMismatch
            | MockMode::EchoString
            | MockMode::UnavailableOnce(_)
            | MockMode::HoldAfterResponse => {
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "result",
                    ArrowDataType::Utf8,
                    !matches!(self.mode, MockMode::SchemaMismatch),
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
                if matches!(self.mode, MockMode::HoldAfterResponse) {
                    Box::pin(encoder.chain(stream::pending()))
                } else {
                    Box::pin(encoder)
                }
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
    let failure = run_mock_exchange(MockMode::Transport).await?;
    assert_eq!(failure.phase, UdfFailurePhase::ReadResponseStream);
    assert_eq!(failure.grpc_code, Some(Code::Internal));
    // A transport-like message supplied by the remote side is not proof of a local transport error.
    assert_eq!(failure.status_origin, Some("unknown"));
    let message = failure.error.message();
    assert!(
        message.contains("stopped responding before it finished"),
        "unexpected transport message: {message}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_status_error_includes_message() -> Result<()> {
    let failure = run_mock_exchange(MockMode::ServerStatus(
        "remote validation failed".to_string(),
    ))
    .await?;
    assert_eq!(failure.phase, UdfFailurePhase::ReadResponseStream);
    assert_eq!(failure.grpc_code, Some(Code::InvalidArgument));
    let message = failure.error.message();
    assert!(
        message.contains("reported an error: remote validation failed"),
        "unexpected server status message: {message}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_data_returns_parse_hint() -> Result<()> {
    let failure = run_mock_exchange(MockMode::MalformedData).await?;
    assert_eq!(failure.phase, UdfFailurePhase::Decode);
    assert_eq!(failure.grpc_code, None);
    let message = failure.error.message();
    assert!(
        message.contains("could not parse"),
        "unexpected malformed data message: {message}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schema_mismatch_returns_schema_hint() -> Result<()> {
    let failure = run_mock_exchange(MockMode::SchemaMismatch).await?;
    assert_eq!(failure.phase, UdfFailurePhase::Decode);
    let message = failure.error.message();
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
    config.query.common.udf_server_allow_list = vec!["http://127.0.0.1".into()];
    assert!(!config.query.common.management_mode);
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
        format!("CREATE NETWORK RULE auth_rule TYPE = HOST_PORT MODE = EGRESS VALUE_LIST = ('{address}', 'localhost:{}')", address.port()),
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
    // Wrong schema makes an early remote check fail differently from authorization.
    let denied_schema = create.replace("managed_echo(STRING)", "managed_echo(INT)");
    assert_eq!(
        fixture
            .execute_command(&denied_schema)
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
    // Integration USAGE does not replace the existing SUPER requirement.
    assert_eq!(
        fixture
            .execute_command(&denied_schema)
            .await
            .unwrap_err()
            .code(),
        ErrorCode::PERMISSION_DENIED
    );
    // CREATE and integration USAGE do not authorize policy management.
    for sql in [
        "CREATE NETWORK RULE denied_rule TYPE = HOST_PORT MODE = EGRESS VALUE_LIST = ('denied.example.com:443')",
        "ALTER NETWORK RULE auth_rule SET VALUE_LIST = ('denied.example.com:443')",
        "DROP NETWORK RULE auth_rule",
        "CREATE EXTERNAL ACCESS INTEGRATION denied_access ALLOWED_NETWORK_RULES = (auth_rule) ENABLED = TRUE",
        "ALTER EXTERNAL ACCESS INTEGRATION auth_access SET ENABLED = FALSE",
        "DROP EXTERNAL ACCESS INTEGRATION auth_access",
        "CREATE OR REPLACE NETWORK RULE auth_rule TYPE = HOST_PORT VALUE_LIST = ('denied.example.com:443') MODE = EGRESS",
        "CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION auth_access ALLOWED_NETWORK_RULES = (auth_rule) ENABLED = TRUE",
        "ALTER NETWORK RULE IF EXISTS missing_rule SET VALUE_LIST = ('example.com:443')",
        "ALTER EXTERNAL ACCESS INTEGRATION IF EXISTS missing_access SET ENABLED = TRUE",
        "DROP INTEGRATION IF EXISTS auth_access",
    ] {
        assert_eq!(
            fixture.execute_command(sql).await.unwrap_err().code(),
            ErrorCode::PERMISSION_DENIED,
            "{sql}",
        );
    }
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
    // SUPER alone does not bypass integration USAGE, even before schema validation.
    assert_eq!(
        fixture
            .execute_command(&denied_schema)
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
    // Even SUPER + USAGE cannot escape a nonempty operator host allow list.
    let blocked_address = format!("http://localhost:{}", address.port());
    let denied = create
        .replace("managed_echo", "operator_denied_echo")
        .replace(&format!("http://{address}"), &blocked_address);
    assert!(
        fixture
            .execute_command(&denied)
            .await
            .unwrap_err()
            .message()
            .contains("udf_server_allow_list")
    );
    fixture.execute_command(&create).await?;
    // An already stored function must pass the same bound during SELECT binding.
    let tenant = fixture.new_query_ctx().await?.get_tenant();
    let api = UserApiProvider::instance();
    let original = api.get_udf(&tenant, "managed_echo").await?.unwrap();
    let mut blocked = original.clone();
    let UDFDefinition::UDFServer(definition) = &mut blocked.definition else {
        unreachable!()
    };
    definition.address = blocked_address.clone();
    api.update_udf(&tenant, blocked).await?;
    let ctx = fixture.new_query_ctx().await?;
    assert!(
        Planner::new(ctx)
            .plan_sql("SELECT managed_echo('hello')")
            .await
            .unwrap_err()
            .message()
            .contains("udf_server_allow_list")
    );
    api.update_udf(&tenant, original).await?;
    // SQL-managed policies do not skip the original Flight schema check.
    let bad_schema = create.replace("managed_echo(STRING)", "bad_schema(INT)");
    assert!(
        fixture
            .execute_command(&bad_schema)
            .await
            .unwrap_err()
            .message()
            .contains("arg types mismatch")
    );
    // The legacy path also obeys the operator host allow list.
    let legacy = create
        .replace("managed_echo", "legacy_echo")
        .replace(&format!("http://{address}"), &blocked_address)
        .replace(" EXTERNAL_ACCESS_INTEGRATIONS = (auth_access)", "");
    assert!(
        fixture
            .execute_command(&legacy)
            .await
            .unwrap_err()
            .message()
            .contains("udf_server_allow_list")
    );
    let alter = create.replacen("CREATE FUNCTION", "ALTER FUNCTION", 1);
    let denied_alter_schema = denied_schema.replacen("CREATE FUNCTION", "ALTER FUNCTION", 1);
    // ALTER retains its own privilege; SUPER alone is not a substitute for it.
    assert_eq!(
        fixture
            .execute_command(&denied_alter_schema)
            .await
            .unwrap_err()
            .code(),
        ErrorCode::PERMISSION_DENIED
    );
    session.set_authed_user(admin.clone(), None).await?;
    session.set_current_role_checked("account_admin").await?;
    fixture
        .execute_command("GRANT ALTER ON *.* TO ROLE udf_developer")
        .await?;
    fixture
        .execute_command("REVOKE SUPER ON *.* FROM ROLE udf_developer")
        .await?;
    session.set_authed_user(developer.clone(), None).await?;
    session.set_current_role_checked("udf_developer").await?;
    fixture.execute_command(&alter).await?;
    session.set_authed_user(admin.clone(), None).await?;
    session.set_current_role_checked("account_admin").await?;
    fixture
        .execute_command("REVOKE USAGE ON INTEGRATION auth_access FROM ROLE udf_developer")
        .await?;
    session.set_authed_user(developer, None).await?;
    session.set_current_role_checked("udf_developer").await?;
    assert_eq!(
        fixture
            .execute_command(&denied_alter_schema)
            .await
            .unwrap_err()
            .code(),
        ErrorCode::PERMISSION_DENIED
    );
    // Invocation requires neither SUPER nor integration USAGE.
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
    sleep(Duration::from_millis(500)).await;
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
async fn external_access_snowflake_ddl() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let tenant = fixture.new_query_ctx().await?.get_tenant();
    let provider = UserApiProvider::instance();
    let rules = provider.network_rule_api(&tenant);
    let integrations = provider.external_access_integration_api(&tenant);
    fixture.execute_command("CREATE NETWORK RULE quoted_rule MODE = EGRESS TYPE = HOST_PORT VALUE_LIST = ('example.com:443')").await?;
    fixture.execute_command("CREATE OR REPLACE NETWORK RULE quoted_rule TYPE = HOST_PORT VALUE_LIST = ('example.com:8443') MODE = EGRESS").await?;
    assert_eq!(
        rules
            .get("quoted_rule", MatchSeq::GE(1))
            .await?
            .data
            .value_list,
        vec!["example.com:8443"]
    );
    fixture.execute_command("CREATE NETWORK RULE IF NOT EXISTS quoted_rule TYPE = HOST_PORT MODE = EGRESS VALUE_LIST = ('ignored.example.com')").await?;
    assert_eq!(
        rules
            .get("quoted_rule", MatchSeq::GE(1))
            .await?
            .data
            .value_list,
        vec!["example.com:8443"]
    );
    for (name, quoted) in [
        ("QuotedAccess", r#""QuotedAccess""#),
        ("access with space", r#""access with space""#),
        ("access/r", r#""access/r""#),
        ("access\"r", r#""access""r""#),
        ("select", r#""select""#),
    ] {
        fixture.execute_command(&format!("CREATE EXTERNAL ACCESS INTEGRATION {quoted} ALLOWED_NETWORK_RULES = (quoted_rule) ENABLED = TRUE")).await?;
        fixture.execute_command(&format!("CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION {quoted} ENABLED = FALSE ALLOWED_NETWORK_RULES = (quoted_rule)")).await?;
        assert!(!integrations.get(name, MatchSeq::GE(1)).await?.data.enabled);
        fixture.execute_command(&format!("CREATE EXTERNAL ACCESS INTEGRATION IF NOT EXISTS {quoted} ALLOWED_NETWORK_RULES = (quoted_rule) ENABLED = TRUE")).await?;
        assert!(!integrations.get(name, MatchSeq::GE(1)).await?.data.enabled);
        fixture.execute_command(&format!("ALTER EXTERNAL ACCESS INTEGRATION IF EXISTS {quoted} SET ENABLED = TRUE ALLOWED_NETWORK_RULES = (quoted_rule)")).await?;
        assert!(integrations.get(name, MatchSeq::GE(1)).await?.data.enabled);
        fixture
            .execute_command(&format!("DROP INTEGRATION {quoted}"))
            .await?;
        assert!(integrations.get(name, MatchSeq::GE(1)).await.is_err());
        fixture
            .execute_command(&format!(
                "DROP EXTERNAL ACCESS INTEGRATION IF EXISTS {quoted}"
            ))
            .await?;
    }
    fixture
        .execute_command(
            "ALTER NETWORK RULE IF EXISTS quoted_rule SET VALUE_LIST = ('example.com:443')",
        )
        .await?;
    assert_eq!(
        rules
            .get("quoted_rule", MatchSeq::GE(1))
            .await?
            .data
            .value_list,
        vec!["example.com:443"]
    );
    fixture
        .execute_command(
            "ALTER NETWORK RULE IF EXISTS missing_rule SET VALUE_LIST = ('example.com:443')",
        )
        .await?;
    fixture.execute_command("ALTER EXTERNAL ACCESS INTEGRATION IF EXISTS missing_access SET ALLOWED_NETWORK_RULES = (missing_rule) ENABLED = FALSE").await?;
    fixture
        .execute_command("DROP INTEGRATION IF EXISTS missing_access")
        .await?;
    assert!(
        fixture
            .execute_command("ALTER EXTERNAL ACCESS INTEGRATION missing_access SET ENABLED = FALSE")
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_access_rechecks_already_built_transform() -> Result<()> {
    let mut config = ConfigBuilder::create().config();
    config.query.common.enable_udf_server = true;
    config.query.common.udf_server_allow_insecure = true;
    config.query.common.udf_server_allow_list = vec!["http://127.0.0.1".into()];
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
    // A received physical plan is checked against the worker's own config too.
    let mut blocked = func.clone();
    blocked.udf_type = UDFType::Server(format!("http://localhost:{}", address.port()));
    let blocked_endpoints = TransformUdfServer::init_endpoints(ctx.clone(), from_ref(&blocked))?;
    let err = TransformUdfServer::new(
        ctx.clone(),
        vec![blocked],
        semaphore.clone(),
        blocked_endpoints,
    )
    .err()
    .unwrap();
    assert!(err.message().contains("udf_server_allow_list"));
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
        .execute_command("CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION live_access ALLOWED_NETWORK_RULES = (live_rule) ENABLED = FALSE")
        .await?;
    sleep(Duration::from_millis(500)).await;
    let err = run().await?.unwrap_err();
    assert!(err.message().contains("disabled"), "{err}");
    fixture
        .execute_command("ALTER EXTERNAL ACCESS INTEGRATION live_access SET ENABLED = TRUE")
        .await?;
    fixture
        .execute_command("ALTER NETWORK RULE live_rule SET VALUE_LIST = ('denied.example.com:443')")
        .await?;
    sleep(Duration::from_millis(500)).await;
    let err = run().await?.unwrap_err();
    assert!(err.message().contains("not allowed"), "{err}");
    fixture
        .execute_command(&format!(
            "CREATE OR REPLACE NETWORK RULE live_rule TYPE = HOST_PORT VALUE_LIST = ('{address}') MODE = EGRESS"
        ))
        .await?;
    sleep(Duration::from_millis(500)).await;
    assert_eq!(run().await??.num_columns(), 2);
    // Removing metadata is fail-closed as well.
    let tenant = ctx.get_tenant();
    UserApiProvider::instance()
        .external_access_integration_api(&tenant)
        .remove("live_access", MatchSeq::GE(1))
        .await?;
    sleep(Duration::from_millis(500)).await;
    assert!(run().await?.is_err());
    let _ = shutdown_tx.send(());
    server.await.unwrap().unwrap();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn logs_each_unavailable_attempt_without_secrets() -> Result<()> {
    // TestFixture teardown disables the logger process-wide. Run this assertion in
    // isolation so other tests cannot disable logging in the middle of a retry.
    const CHILD_PROCESS: &str = "DATABEND_UDF_ATTEMPT_LOG_TEST_CHILD";
    let recover = match std::env::var(CHILD_PROCESS).as_deref() {
        Ok("recover") => true,
        Ok("terminal") => false,
        _ => {
            let name = concat!(
                module_path!(),
                "::logs_each_unavailable_attempt_without_secrets"
            );
            for case in ["terminal", "recover"] {
                let output = std::process::Command::new(std::env::current_exe()?)
                    .env(CHILD_PROCESS, case)
                    .args(["--exact", name.split_once("::").unwrap().1, "--nocapture"])
                    .output()?;
                let stdout = String::from_utf8_lossy(&output.stdout);
                assert!(
                    output.status.success() && stdout.contains("1 passed"),
                    "isolated UDF log test ({case}) failed:\n{stdout}\n{}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            return Ok(());
        }
    };

    let dir = tempfile::tempdir()?;
    let mut config = ConfigBuilder::create().config();
    config.log.file.dir = dir.path().to_str().unwrap().to_string();
    config.log.file.format = LogFormat::Text;
    config.log.file.level = format!("INFO,{UDF_PEER_LOG_TARGET}=WARN");
    // Own the guards so the test can flush the file sink before inspecting it.
    let log_guards = init_logging("udf_attempt_test", &config.log, BTreeMap::new());
    config.log.file.on = false;
    let fixture = TestFixture::setup_with_config(&config).await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings()
        .set_setting("external_server_request_retry_times".into(), "1".into())?;
    let query_id = ctx.get_id();
    let node_id = ctx.get_cluster().local_id.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let mode = if recover {
        MockMode::UnavailableOnce(Arc::new(AtomicBool::new(true)))
    } else {
        MockMode::Unavailable
    };
    let server = spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(MockFlightService { mode }))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let func = UdfFunctionDesc {
        external_access_integrations: vec![],
        name: "monitor_udf".into(),
        func_name: "mock_handler".into(),
        output_column: Symbol::new(1),
        arg_indices: vec![0],
        arg_exprs: vec!["NULL".into()],
        data_type: Box::new(DataType::Nullable(Box::new(DataType::String))),
        headers: [("authorization".into(), "secret-header-value".into())].into(),
        udf_type: UDFType::Server(format!(
            "http://{address}/secret-path?token=secret-url-token"
        )),
    };
    let endpoints = TransformUdfServer::init_endpoints(ctx.clone(), from_ref(&func))?;
    let semaphore = TransformUdfServer::init_semaphore(ctx.clone())?;
    let mut transform = TransformUdfServer::new(ctx, vec![func], semaphore, endpoints)?;
    let logs = Arc::new(ConcurrentQueue::unbounded());
    let mut payload = ThreadTracker::new_tracking_payload();
    payload.query_id = Some(query_id.clone());
    payload.capture_log_settings = Some(CaptureLogSettings::capture_query(
        log::LevelFilter::Info,
        logs.clone(),
    ));
    let result = GlobalIORuntime::instance()
        .spawn(payload.tracking(async move {
            let input = DataBlock::new(vec![BlockEntry::from(Column::Null { len: 1 })], 1);
            transform.transform(input).await
        }))
        .await?;

    let _ = shutdown_tx.send(());
    server.await.unwrap().unwrap();
    if recover {
        assert_eq!(result?.num_columns(), 2);
    } else {
        let error = result.expect_err("expected UNAVAILABLE");
        assert!(error.message().contains("service is currently unavailable"));
    }
    let logs: Vec<_> = std::iter::from_fn(|| logs.pop().ok()).collect();
    assert!(logs.iter().all(|line| !line.contains("udf_attempt_peer")));
    let failures: Vec<_> = logs
        .iter()
        .filter(|line| line.contains("event=\"udf_attempt_failure\""))
        .collect();
    assert_eq!(failures.len(), if recover { 1 } else { 2 }, "{failures:?}");
    let retry = failures[0];
    for line in &failures {
        assert!(line.contains(&format!("query_id=\"{query_id}\"")), "{line}");
        assert!(line.contains(&format!("node_id=\"{node_id}\"")), "{line}");
        assert!(line.contains("udf_name=\"monitor_udf\""), "{line}");
        assert!(line.contains("dns_result=\"not_applicable\""), "{line}");
        assert!(line.contains("tcp_result=\"ok\""), "{line}");
        assert!(line.contains("tls_result=\"not_applicable\""), "{line}");
        assert!(line.contains("redial_started=false"), "{line}");
        assert!(line.contains("phase=\"await_response\""), "{line}");
        assert!(line.contains("grpc_code=14"), "{line}");
        assert!(line.contains("status_origin=\"unknown\""), "{line}");
        assert!(line.contains("elapsed_ms="), "{line}");
        for secret in [
            "127.0.0.1",
            "initial_peer_ip",
            "endpoint_host",
            "secret-path",
            "secret-url-token",
            "secret-header-value",
            "private upstream message",
        ] {
            assert!(!line.contains(secret), "{line}");
        }
    }
    assert!(
        retry.contains("attempt=1 ") && retry.contains("will_retry=true"),
        "{retry}"
    );
    let call_id = |line: &str| {
        line.split_whitespace()
            .find(|field| field.starts_with("call_id="))
            .expect("missing call_id")
            .to_string()
    };
    let recoveries: Vec<_> = logs
        .iter()
        .filter(|line| line.contains("event=\"udf_call_recovered\""))
        .collect();
    if recover {
        assert_eq!(recoveries.len(), 1, "{recoveries:?}");
        let line = recoveries[0];
        assert!(line.contains(&format!("query_id=\"{query_id}\"")), "{line}");
        assert!(line.contains(&format!("node_id=\"{node_id}\"")), "{line}");
        assert!(line.contains("attempt=2"), "{line}");
        assert_eq!(call_id(retry), call_id(line));
    } else {
        let terminal = failures[1];
        assert!(
            terminal.contains("attempt=2 ") && terminal.contains("will_retry=false"),
            "{terminal}"
        );
        assert_eq!(call_id(retry), call_id(terminal));
        assert!(recoveries.is_empty(), "{recoveries:?}");
    }

    drop(log_guards);
    let file_logs = std::fs::read_dir(dir.path())?
        .map(|entry| std::fs::read_to_string(entry?.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    let mut peer_events: Vec<_> = file_logs
        .iter()
        .flat_map(|log| log.lines())
        .filter(|line| line.contains("event=\"udf_attempt_peer\""))
        .collect();
    peer_events.sort_by_key(|line| !line.contains("attempt=1 "));
    assert_eq!(peer_events.len(), failures.len(), "{peer_events:?}");
    for (index, line) in peer_events.iter().enumerate() {
        assert!(line.contains(&format!("query_id=\"{query_id}\"")), "{line}");
        assert!(line.contains(&call_id(retry)), "{line}");
        assert!(line.contains(&format!("attempt={} ", index + 1)), "{line}");
        assert!(line.contains("initial_peer_ip=\"127.0.0.1\""), "{line}");
        assert!(
            line.contains(&format!("endpoint_port={}", address.port())),
            "{line}"
        );
        assert!(!line.contains("secret-"), "{line}");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reports_dns_failure_before_tcp() -> Result<()> {
    // A DNS label longer than 63 bytes is rejected locally by the existing resolver.
    let host = format!("{}.invalid", "x".repeat(64));
    // Construct a plaintext endpoint directly so tonic's TLS name validation doesn't
    // reject the label before the real resolver sees it.
    let endpoint = Arc::new(
        Endpoint::from_shared(format!("http://{host}"))
            .unwrap()
            .connect_timeout(Duration::from_secs(3)),
    );
    let failure = UDFFlightClient::connect_with_diagnostics("test_udf", endpoint, 3, 1)
        .await
        .expect_err("an invalid DNS label cannot resolve");
    let dial = failure.connection.unwrap();
    assert_eq!(failure.phase, UdfFailurePhase::Connect);
    assert_eq!(dial.dns, UdfConnectResult::Failed);
    assert_eq!(dial.tcp, UdfConnectResult::NotStarted);
    assert_eq!(dial.initial_peer_ip, None);
    assert!(!failure.error.message().contains(&host));
    Ok(())
}

#[tokio::test]
async fn reports_tcp_refusal_without_a_peer() -> Result<()> {
    // Keep the port reserved, but do not listen on it.
    let socket = TcpSocket::new_v4()?;
    socket.bind("127.0.0.1:0".parse().unwrap())?;
    let address = socket.local_addr()?;
    let uri = format!("http://{address}/secret-path?token=secret-value");
    let endpoint = UDFFlightClient::build_endpoint(&uri, 3, 3, "udf-test")?;
    let failure = UDFFlightClient::connect_with_diagnostics("test_udf", endpoint, 3, 1)
        .await
        .expect_err("the TCP port is not listening");
    let dial = failure.connection.unwrap();
    assert_eq!(failure.phase, UdfFailurePhase::Connect);
    assert_eq!(dial.dns, UdfConnectResult::NotApplicable);
    assert_eq!(dial.tcp, UdfConnectResult::Failed);
    assert_eq!(dial.initial_peer_ip, None);
    assert_eq!(
        failure.io_error_kind,
        Some(std::io::ErrorKind::ConnectionRefused)
    );
    let message = failure.error.message();
    assert!(!message.contains("secret-"));
    assert!(!message.contains("127.0.0.1"));
    Ok(())
}

#[tokio::test]
async fn reports_tls_timeout_after_tcp() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let endpoint =
        UDFFlightClient::build_endpoint(&format!("https://{address}"), 1, 3, "udf-test")?;
    let connect = UDFFlightClient::connect_with_diagnostics("test_udf", endpoint, 1, 1);
    // Keep the accepted socket open without replying to ClientHello.
    let (failure, accepted) =
        tokio::join!(connect, timeout(Duration::from_secs(3), listener.accept()));
    let _socket = accepted.expect("client did not connect")?;
    let failure = failure.expect_err("the TLS peer did not respond");
    let dial = failure.connection.unwrap();
    assert_eq!(failure.phase, UdfFailurePhase::Connect);
    assert_eq!(dial.tcp, UdfConnectResult::Ok);
    assert_eq!(dial.tls, UdfConnectResult::Timeout);
    assert_eq!(dial.initial_peer_ip, Some(address.ip()));
    assert!(!format!("{dial:?}").contains("127.0.0.1"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reports_dns_and_tls_outcomes_for_the_actual_socket() -> Result<()> {
    let ca = include_str!("../../../../../../tests/certs/ca.pem");
    let cert = include_str!("../../../../../../tests/certs/server.pem");
    let key = include_str!("../../../../../../tests/certs/server.key");
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = Server::builder()
        .tls_config(ServerTlsConfig::new().identity(Identity::from_pem(cert, key)))
        .unwrap()
        .add_service(FlightServiceServer::new(MockFlightService {
            mode: MockMode::Unavailable,
        }));
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server = spawn(server.serve_with_incoming_shutdown(
        TcpListenerStream::new(listener),
        async move {
            let _ = shutdown_rx.await;
        },
    ));
    let endpoint = |domain: &str| {
        Arc::new(
            Endpoint::from_shared(format!("https://localhost:{}", address.port()))
                .unwrap()
                .connect_timeout(Duration::from_secs(3))
                .tls_config(
                    ClientTlsConfig::new()
                        .ca_certificate(Certificate::from_pem(ca))
                        .domain_name(domain),
                )
                .unwrap(),
        )
    };
    let client = UDFFlightClient::connect_with_diagnostics("test_udf", endpoint("localhost"), 3, 1)
        .await
        .expect("the test certificate must be valid for localhost");
    let dial = client.connection_diagnostics();
    assert_eq!(dial.dns, UdfConnectResult::Ok);
    assert_eq!(dial.tcp, UdfConnectResult::Ok);
    assert_eq!(dial.tls, UdfConnectResult::Ok);
    assert_eq!(dial.initial_peer_ip, Some(address.ip()));
    drop(client);

    let failure =
        UDFFlightClient::connect_with_diagnostics("test_udf", endpoint("bad-name.invalid"), 3, 1)
            .await
            .expect_err("certificate does not cover this name");
    let dial = failure.connection.unwrap();
    assert_eq!(failure.phase, UdfFailurePhase::Connect);
    assert_eq!(dial.dns, UdfConnectResult::Ok);
    assert_eq!(dial.tcp, UdfConnectResult::Ok);
    assert_eq!(dial.tls, UdfConnectResult::Failed);
    assert_eq!(dial.initial_peer_ip, Some(address.ip()));
    assert!(!failure.error.message().contains("bad-name.invalid"));

    let _ = shutdown_tx.send(());
    server.await.unwrap().unwrap();
    Ok(())
}

// Forward a real plaintext HTTP/2 connection until its first non-empty DATA frame.
// This deliberately drops the socket, rather than fabricating a gRPC Status.
async fn cut_flight_data(
    mut input: impl AsyncRead + Unpin,
    mut output: impl AsyncWrite + Unpin,
    after_forward: bool,
) -> std::io::Result<()> {
    loop {
        let mut header = [0; 9];
        input.read_exact(&mut header).await?;
        let size = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
        let mut data = vec![0; size];
        input.read_exact(&mut data).await?;
        let is_data = header[3] == 0 && size > 0;
        if is_data && !after_forward {
            return Ok(());
        }
        output.write_all(&header).await?;
        output.write_all(&data).await?;
        if is_data {
            return Ok(());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distinguishes_request_socket_loss_from_response_socket_loss() -> Result<()> {
    for (cut_response, expected) in [
        (false, UdfFailurePhase::AwaitResponse),
        (true, UdfFailurePhase::ReadResponseStream),
    ] {
        let backend = TcpListener::bind("127.0.0.1:0").await?;
        let backend_addr = backend.local_addr()?;
        let server = tokio::spawn(
            Server::builder()
                .add_service(FlightServiceServer::new(MockFlightService {
                    mode: MockMode::HoldAfterResponse,
                }))
                .serve_with_incoming(TcpListenerStream::new(backend)),
        );
        let proxy = TcpListener::bind("127.0.0.1:0").await?;
        let address = proxy.local_addr()?;
        let proxy = tokio::spawn(async move {
            let (client, _) = proxy.accept().await?;
            let backend = TcpStream::connect(backend_addr).await?;
            let (mut from_client, mut to_client) = client.into_split();
            let (mut from_server, mut to_server) = backend.into_split();
            if cut_response {
                tokio::select! {
                    result = cut_flight_data(&mut from_server, &mut to_client, true) => result,
                    result = tokio::io::copy(&mut from_client, &mut to_server) => result.map(|_| ()),
                }
            } else {
                let mut preface = [0; 24];
                from_client.read_exact(&mut preface).await?;
                to_server.write_all(&preface).await?;
                tokio::select! {
                    result = cut_flight_data(&mut from_client, &mut to_server, false) => result,
                    result = tokio::io::copy(&mut from_server, &mut to_client) => result.map(|_| ()),
                }
            }
        });
        let endpoint =
            UDFFlightClient::build_endpoint(&format!("http://{address}"), 3, 3, "udf-test")?;
        let mut client = UDFFlightClient::connect("mock_udf", endpoint, 3, 1024).await?;
        let failure = timeout(
            Duration::from_secs(5),
            client.do_exchange_with_diagnostics(
                "mock_udf",
                "mock_handler",
                Some(1),
                vec![BlockEntry::from(Column::Null { len: 1 })],
                &DataType::Null,
            ),
        )
        .await
        .expect("socket interruption did not reach the client")
        .expect_err("the connection was cut");
        proxy.await.unwrap()?;
        server.abort();
        let _ = server.await;
        assert_eq!(failure.phase, expected);
        assert_eq!(failure.connection.unwrap().tcp, UdfConnectResult::Ok);
        assert!(failure.grpc_code.is_some());
    }
    Ok(())
}

async fn run_mock_exchange(mode: MockMode) -> Result<UdfFlightFailure> {
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
        client.do_exchange_with_diagnostics(
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

    Ok(result.expect_err("expected failure"))
}
