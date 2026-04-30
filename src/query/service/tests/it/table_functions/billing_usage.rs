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
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;

use chrono::NaiveDate;
use databend_common_base::runtime;
use databend_common_catalog::session_type::SessionType;
use databend_common_cloud_control::pb::BillingError as PbBillingError;
use databend_common_cloud_control::pb::BillingUsageDailyRow;
use databend_common_cloud_control::pb::GetBillingUsageDailyRequest;
use databend_common_cloud_control::pb::GetBillingUsageDailyResponse;
use databend_common_cloud_control::pb::billing_service_server::BillingService;
use databend_common_cloud_control::pb::billing_service_server::BillingServiceServer;
use databend_common_config::InnerConfig;
use databend_common_expression::DataBlock;
use databend_common_expression::NumberScalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::types::DecimalScalar;
use databend_common_expression::types::DecimalSize;
use databend_common_meta_app::principal::UserInfo;
use databend_common_version::BUILD_INFO;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sql::Planner;
use databend_query::table_functions::TableFunctionFactory;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestFixture;
use futures::TryStreamExt;
use jsonb::OwnedJsonb;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::transport::Server;

#[derive(Clone, Default)]
struct BillingRequests {
    usage_daily: Arc<Mutex<Vec<GetBillingUsageDailyRequest>>>,
}

#[derive(Clone, Default)]
struct MockBillingServiceImpl {
    requests: BillingRequests,
    usage_daily_error: Option<PbBillingError>,
}

#[tonic::async_trait]
impl BillingService for MockBillingServiceImpl {
    async fn get_billing_usage_daily(
        &self,
        request: Request<GetBillingUsageDailyRequest>,
    ) -> std::result::Result<Response<GetBillingUsageDailyResponse>, Status> {
        self.requests
            .usage_daily
            .lock()
            .unwrap()
            .push(request.into_inner());

        if let Some(error) = self.usage_daily_error.clone() {
            return Ok(Response::new(GetBillingUsageDailyResponse {
                rows: vec![],
                error: Some(error),
            }));
        }

        Ok(Response::new(GetBillingUsageDailyResponse {
            rows: vec![BillingUsageDailyRow {
                usage_date: "2026-03-02".to_string(),
                usage_type: "compute".to_string(),
                service_type: "WAREHOUSE_METERING".to_string(),
                resource_name: "default".to_string(),
                usage: "2653".to_string(),
                usage_unit: "second".to_string(),
                rate: "".to_string(),
                rate_unit: "second".to_string(),
                usage_in_currency: "0.737".to_string(),
                currency: "¥".to_string(),
                tags: BTreeMap::from([("env".to_string(), "test".to_string())]),
                details: "{\"cluster_name\":\"cl-00000\",\"max_clusters\":1,\"size\":\"XSmall\"}"
                    .to_string(),
            }],
            error: None,
        }))
    }
}

fn extract_single_block(blocks: Vec<DataBlock>) -> DataBlock {
    let block = DataBlock::concat(&blocks).expect("concat blocks");
    assert_eq!(block.num_rows(), 1);
    block
}

fn date_days(date: &str) -> i32 {
    let date = NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap();
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    date.signed_duration_since(epoch).num_days() as i32
}

#[test]
fn test_billing_usage_daily_table_function_registered_only_with_cloud_control() {
    let config: InnerConfig = ConfigBuilder::create().build();
    let factory = TableFunctionFactory::create(&config);
    assert!(!factory.exists("billing_usage_daily"));

    let mut config: InnerConfig = ConfigBuilder::create().build();
    config.query.common.cloud_control_grpc_server_address =
        Some("http://127.0.0.1:65535".to_string());
    let factory = TableFunctionFactory::create(&config);
    assert!(factory.exists("billing_usage_daily"));
    assert!(factory.exists("BILLING_USAGE_DAILY"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_billing_usage_daily_table_function_via_mock_grpc() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let requests = BillingRequests::default();
    let mock = MockBillingServiceImpl {
        requests: requests.clone(),
        usage_daily_error: None,
    };

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server_handle = runtime::spawn(async move {
        Server::builder()
            .add_service(BillingServiceServer::new(mock))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let mut config: InnerConfig = ConfigBuilder::create().build();
    config.query.common.cloud_control_grpc_server_address = Some(format!("http://{addr}"));
    config.query.common.cloud_control_grpc_timeout = 5;

    let fixture = TestFixture::setup_with_config(&config).await?;

    let blocks = fixture
        .execute_query(
            "select usage_date, usage_type, service_type, resource_name, usage, rate, usage_in_currency, currency, tags, details \
             from billing_usage_daily(start_date => '2026-03-01', end_date => '2026-03-31')",
        )
        .await?
        .try_collect::<Vec<DataBlock>>()
        .await?;
    let block = extract_single_block(blocks);

    assert_eq!(
        block.get_by_offset(0).index(0).unwrap(),
        ScalarRef::Date(date_days("2026-03-02"))
    );
    assert_eq!(
        block.get_by_offset(1).index(0).unwrap(),
        ScalarRef::String("compute")
    );
    assert_eq!(
        block.get_by_offset(2).index(0).unwrap(),
        ScalarRef::String("WAREHOUSE_METERING")
    );
    assert_eq!(
        block.get_by_offset(3).index(0).unwrap(),
        ScalarRef::String("default")
    );
    assert_eq!(
        block.get_by_offset(4).index(0).unwrap(),
        ScalarRef::Decimal(DecimalScalar::Decimal128(
            2653,
            DecimalSize::new_unchecked(38, 0)
        ))
    );
    assert_eq!(block.get_by_offset(5).index(0).unwrap(), ScalarRef::Null);
    assert_eq!(
        block.get_by_offset(6).index(0).unwrap(),
        ScalarRef::Decimal(DecimalScalar::Decimal128(
            737_000_000_000,
            DecimalSize::new_unchecked(38, 12)
        ))
    );
    assert_eq!(
        block.get_by_offset(7).index(0).unwrap(),
        ScalarRef::String("¥")
    );

    let expected_tags = OwnedJsonb::from_str(r#"{"env":"test"}"#)?.to_vec();
    match block.get_by_offset(8).index(0).unwrap() {
        ScalarRef::Variant(bytes) => assert_eq!(bytes, expected_tags.as_slice()),
        other => panic!("unexpected scalar type for tags: {other:?}"),
    }

    let expected_details =
        OwnedJsonb::from_str(r#"{"cluster_name":"cl-00000","max_clusters":1,"size":"XSmall"}"#)?
            .to_vec();
    match block.get_by_offset(9).index(0).unwrap() {
        ScalarRef::Variant(bytes) => assert_eq!(bytes, expected_details.as_slice()),
        other => panic!("unexpected scalar type for details: {other:?}"),
    }

    let blocks = fixture
        .execute_query(
            "select count(), sum(usage), sum(usage_in_currency) \
             from billing_usage_daily(start_date => '2026-03-01', end_date => '2026-03-31') \
             where usage_date = to_date('2026-03-02') and usage = 2653 and usage_in_currency = 0.737 and rate is null",
        )
        .await?
        .try_collect::<Vec<DataBlock>>()
        .await?;
    let block = extract_single_block(blocks);
    assert_eq!(
        block.get_by_offset(0).index(0).unwrap(),
        ScalarRef::Number(NumberScalar::UInt64(1))
    );
    assert_eq!(
        block.get_by_offset(1).index(0).unwrap(),
        ScalarRef::Decimal(DecimalScalar::Decimal128(
            2653,
            DecimalSize::new_unchecked(38, 0)
        ))
    );
    assert_eq!(
        block.get_by_offset(2).index(0).unwrap(),
        ScalarRef::Decimal(DecimalScalar::Decimal128(
            737_000_000_000,
            DecimalSize::new_unchecked(38, 12)
        ))
    );

    let blocks = fixture
        .execute_query(
            "select tags:env::String, details:size::String, details:max_clusters::String \
             from billing_usage_daily(start_date => '2026-03-01', end_date => '2026-03-31') \
             where tags:env::String = 'test' and details:size::String = 'XSmall'",
        )
        .await?
        .try_collect::<Vec<DataBlock>>()
        .await?;
    let block = extract_single_block(blocks);

    assert_eq!(
        block.get_by_offset(0).index(0).unwrap(),
        ScalarRef::String("test")
    );
    assert_eq!(
        block.get_by_offset(1).index(0).unwrap(),
        ScalarRef::String("XSmall")
    );
    assert_eq!(
        block.get_by_offset(2).index(0).unwrap(),
        ScalarRef::String("1")
    );

    let usage_daily_requests = requests.usage_daily.lock().unwrap().clone();
    assert_eq!(usage_daily_requests.len(), 3);
    assert!(
        usage_daily_requests
            .iter()
            .all(|request| request.start_date == "2026-03-01")
    );
    assert!(
        usage_daily_requests
            .iter()
            .all(|request| request.end_date == "2026-03-31")
    );
    assert!(
        usage_daily_requests
            .iter()
            .all(|request| request.sql_user == "'root'@'%'")
    );
    assert!(!usage_daily_requests[0].tenant_id.is_empty());
    assert!(!usage_daily_requests[0].query_id.is_empty());

    let _ = shutdown_tx.send(());
    server_handle.await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_billing_usage_daily_table_function_surfaces_task_error() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let mock = MockBillingServiceImpl {
        requests: BillingRequests::default(),
        usage_daily_error: Some(PbBillingError {
            kind: "Internal".to_string(),
            message: "billing usage unavailable".to_string(),
            code: 500,
        }),
    };

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server_handle = runtime::spawn(async move {
        Server::builder()
            .add_service(BillingServiceServer::new(mock))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let mut config: InnerConfig = ConfigBuilder::create().build();
    config.query.common.cloud_control_grpc_server_address = Some(format!("http://{addr}"));
    config.query.common.cloud_control_grpc_timeout = 5;

    let fixture = TestFixture::setup_with_config(&config).await?;

    let stream = fixture
        .execute_query("select * from billing_usage_daily(start_date => '2026-03-01')")
        .await?;
    let err = stream
        .try_collect::<Vec<DataBlock>>()
        .await
        .expect_err("billing usage task error should fail query");
    assert!(err.message().contains("billing usage unavailable"));
    assert!(err.message().contains("Internal"));

    let _ = shutdown_tx.send(());
    server_handle.await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_billing_usage_daily_table_function_requires_super_privilege() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let requests = BillingRequests::default();
    let mock = MockBillingServiceImpl {
        requests: requests.clone(),
        usage_daily_error: None,
    };

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server_handle = runtime::spawn(async move {
        Server::builder()
            .add_service(BillingServiceServer::new(mock))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let mut config: InnerConfig = ConfigBuilder::create().build();
    config.query.common.cloud_control_grpc_server_address = Some(format!("http://{addr}"));
    config.query.common.cloud_control_grpc_timeout = 5;

    let fixture = TestFixture::setup_with_config(&config).await?;
    let session = fixture.new_session_with_type(SessionType::Dummy).await?;
    session
        .set_authed_user(UserInfo::new_no_auth("billing_viewer", "%"), None)
        .await?;

    let ctx = session.create_query_context(&BUILD_INFO).await?;
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner
        .plan_sql("select * from billing_usage_daily(start_date => '2026-03-01')")
        .await?;
    let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
    let stream = executor.execute(ctx).await?;
    let err = stream
        .try_collect::<Vec<DataBlock>>()
        .await
        .expect_err("billing usage should require SUPER privilege");

    assert!(err.message().contains("privilege [SUPER] is required"));
    assert!(err.message().contains("billing_usage_daily"));
    assert!(requests.usage_daily.lock().unwrap().is_empty());

    let _ = shutdown_tx.send(());
    server_handle.await??;
    Ok(())
}
