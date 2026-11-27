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
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use backon::ExponentialBuilder;
use backon::Retryable;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::udf_client::UDFFlightClient;
use databend_common_expression::udf_client::UdfFailurePhase;
use databend_common_expression::udf_client::UdfFlightFailure;
use databend_common_expression::udf_client::error_kind;
use databend_common_expression::udf_client::is_transport_error_message;
use databend_common_metrics::external_server::record_error_external;
use databend_common_metrics::external_server::record_retry_external;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use databend_common_sql::planner::UDFValidator;
use databend_common_tracing::UDF_PEER_LOG_TARGET;
use databend_common_users::UserApiProvider;
use tokio::sync::Semaphore;
use tonic::transport::Endpoint;
use uuid::Uuid;

use crate::physical_plans::UdfFunctionDesc;
use crate::sessions::QueryContext;
use crate::sessions::TableContextCluster;
use crate::sessions::TableContextQueryIdentity;
use crate::sessions::TableContextQueryInfo;
use crate::sessions::TableContextSettings;
use crate::sessions::TableContextTableAccess;

pub struct TransformUdfServer {
    ctx: Arc<QueryContext>,
    user_api: Arc<UserApiProvider>,
    funcs: Vec<UdfFunctionDesc>,
    connect_timeout: u64,
    // request batch rows is used to split the block into smaller blocks and improve concurrency performance.
    request_batch_rows: usize,
    // semaphore is used to control the total number of concurrent threads,
    // avoid the case of too many flight connections caused by the small batch rows.
    semaphore: Arc<Semaphore>,
    // key is the server address of udf, value is the endpoint.
    endpoints: BTreeMap<String, Arc<Endpoint>>,
    retry_times: usize,
}

impl TransformUdfServer {
    pub fn init_semaphore(ctx: Arc<QueryContext>) -> Result<Arc<Semaphore>> {
        let settings = ctx.get_settings();
        let request_max_threads = settings.get_external_server_request_max_threads()? as usize;
        let semaphore = Arc::new(Semaphore::new(request_max_threads));
        Ok(semaphore)
    }

    pub fn init_endpoints(
        ctx: Arc<QueryContext>,
        funcs: &[UdfFunctionDesc],
    ) -> Result<BTreeMap<String, Arc<Endpoint>>> {
        let settings = ctx.get_settings();
        let connect_timeout = settings.get_external_server_connect_timeout_secs()?;
        let request_timeout = settings.get_external_server_request_timeout_secs()?;
        let mut endpoints: BTreeMap<String, Arc<Endpoint>> = BTreeMap::new();
        for func in funcs.iter() {
            let server_addr = func.udf_type.as_server().unwrap();
            if endpoints.contains_key(server_addr) {
                continue;
            }
            let endpoint = UDFFlightClient::build_endpoint(
                server_addr,
                connect_timeout,
                request_timeout,
                &ctx.get_version().udf_client_user_agent(),
            )?;
            endpoints.insert(server_addr.clone(), endpoint);
        }
        Ok(endpoints)
    }

    pub fn new(
        ctx: Arc<QueryContext>,
        funcs: Vec<UdfFunctionDesc>,
        semaphore: Arc<Semaphore>,
        endpoints: BTreeMap<String, Arc<Endpoint>>,
    ) -> Result<Self> {
        let settings = ctx.get_settings();
        let connect_timeout = settings.get_external_server_connect_timeout_secs()?;
        let request_batch_rows = settings.get_external_server_request_batch_rows()? as usize;
        let retry_times = settings.get_external_server_request_retry_times()? as usize;
        // A received physical plan must also obey this executor's operator config.
        for func in &funcs {
            if !func.external_access_integrations.is_empty() {
                UDFValidator::is_udf_server_policy_allowed(func.udf_type.as_server().unwrap())?;
            }
        }

        Ok(Self {
            ctx,
            user_api: UserApiProvider::instance(),
            funcs,
            connect_timeout,
            request_batch_rows,
            semaphore,
            endpoints,
            retry_times,
        })
    }

    // data_block is spilt into multiple blocks, each block is processed by transform_inner
    async fn transform_inner(
        ctx: Arc<QueryContext>,
        user_api: Arc<UserApiProvider>,
        endpoint: Arc<Endpoint>,
        semaphore: Arc<Semaphore>,
        connect_timeout: u64,
        func: UdfFunctionDesc,
        mut data_block: DataBlock,
    ) -> std::result::Result<DataBlock, UdfFlightFailure> {
        // Must obtain the permit to execute, prevent too many connections being executed concurrently
        let permit = semaphore.acquire_owned().await.map_err(|e| {
            UdfFlightFailure::new(
                ErrorCode::Internal(format!("Udf transformer acquire permit failure. {}", e)),
                UdfFailurePhase::AcquirePermit,
            )
        })?;
        // Check immediately before every Flight request (including retries),
        // using the watch-backed policy view or authoritative KV fallback.
        if !func.external_access_integrations.is_empty() {
            user_api
                .validate_external_access(
                    &ctx.get_tenant(),
                    func.udf_type.as_server().unwrap(),
                    &func.external_access_integrations,
                )
                .await
                .map_err(|err| UdfFlightFailure::new(err, UdfFailurePhase::AccessCheck))?;
        }
        // construct input record_batch
        let num_rows = data_block.num_rows();
        let block_entries = func
            .arg_indices
            .iter()
            .map(|i| data_block.get_by_offset(*i).clone())
            .collect::<Vec<_>>();

        let client = UDFFlightClient::connect_with_diagnostics(
            &func.func_name,
            endpoint,
            connect_timeout,
            65536,
        )
        .await?;
        let connection = client.connection_diagnostics();
        let mut client = client
            .with_tenant(ctx.get_tenant().tenant_name())
            .and_then(|client| client.with_func_name(&func.name))
            .and_then(|client| client.with_handler_name(&func.func_name))
            .and_then(|client| client.with_query_id(&ctx.get_id()))
            .and_then(|client| client.with_headers(func.headers))
            .map_err(|err| {
                UdfFlightFailure::new(err, UdfFailurePhase::PrepareRequest)
                    .with_connection(connection)
            })?;

        let result = client
            .do_exchange_with_diagnostics(
                &func.name,
                &func.func_name,
                Some(num_rows),
                block_entries,
                &func.data_type,
            )
            .await?;

        data_block.add_entry(result.take_columns().pop().unwrap());

        drop(permit);
        Ok(data_block)
    }
}

struct UdfAttemptFailure {
    failure: UdfFlightFailure,
    attempt: usize,
    elapsed: Duration,
}

struct UdfCallLog<'a> {
    query_id: &'a str,
    node_id: &'a str,
    function: &'a str,
    handler: &'a str,
    endpoint: &'a Endpoint,
    call_id: Uuid,
}

impl UdfCallLog<'_> {
    fn log_recovery(&self, attempt: usize) {
        log::info!(
            event = "udf_call_recovered",
            query_id = self.query_id,
            node_id = self.node_id,
            udf_name = self.function,
            handler = self.handler,
            call_id:% = self.call_id,
            attempt = attempt;
            "UDF call recovered after retry"
        );
    }

    fn log_failure(&self, attempt: &UdfAttemptFailure, delay: Option<Duration>, kind: &str) {
        let failure = &attempt.failure;
        let dial = failure.connection.unwrap_or_default();
        let io_error_kind = failure.io_error_kind.map(|kind| format!("{kind:?}"));
        // This event is also visible to query log capture. Do not include network addresses,
        // full URIs, raw status messages, metadata, or data.
        log::warn!(
            event = "udf_attempt_failure",
            query_id = self.query_id,
            node_id = self.node_id,
            udf_name = self.function,
            handler = self.handler,
            phase = failure.phase.as_str(),
            dns_result = dial.dns.as_str(),
            tcp_result = dial.tcp.as_str(),
            tls_result = dial.tls.as_str(),
            redial_started = dial.redial_started,
            grpc_code = failure.grpc_code.map(|code| code as i32),
            status_origin = failure.status_origin,
            io_error_kind = io_error_kind.as_deref(),
            error_code = failure.error.code(),
            error_kind = kind,
            call_id:% = self.call_id,
            attempt = attempt.attempt,
            elapsed_ms = attempt.elapsed.as_secs_f64() * 1_000.0,
            retry_in_ms = delay.map(|delay| delay.as_secs_f64() * 1_000.0),
            will_retry = delay.is_some();
            "{}",
            if delay.is_some() { "Retry udf error" } else { "UDF attempt failed" }
        );
        if failure.connection.is_some()
            && log::log_enabled!(target: UDF_PEER_LOG_TARGET, log::Level::Warn)
        {
            let uri = self.endpoint.uri();
            let port = uri.port_u16().or(match uri.scheme_str() {
                Some("http") => Some(80),
                Some("https") => Some(443),
                _ => None,
            });
            let peer_ip = dial.initial_peer_ip.map(|ip| ip.to_string());
            log::warn!(
                target: UDF_PEER_LOG_TARGET,
                event = "udf_attempt_peer",
                query_id = self.query_id,
                node_id = self.node_id,
                call_id:% = self.call_id,
                attempt = attempt.attempt,
                endpoint_host = uri.host(),
                endpoint_port = port,
                initial_peer_ip = peer_ip.as_deref();
                "UDF initial dial peer"
            );
        }
    }
}

fn retry_on(err: &ErrorCode) -> bool {
    if err.code() == ErrorCode::PERMISSION_DENIED || err.code() == ErrorCode::INVALID_ARGUMENT {
        return false;
    }
    if err.code() == ErrorCode::U_D_F_DATA_ERROR {
        let message = err.message();
        // this means the server can't handle the request in 60s
        if is_transport_error_message(&message) {
            return false;
        }
    }
    true
}

#[async_trait::async_trait]
impl AsyncTransform for TransformUdfServer {
    const NAME: &'static str = "UdfTransform";

    #[async_backtrace::framed]
    async fn transform(&mut self, mut data_block: DataBlock) -> Result<DataBlock> {
        if data_block.is_empty() {
            for func in self.funcs.iter() {
                let builder = ColumnBuilder::repeat_default(&func.data_type, 0);
                let entry = BlockEntry::Column(builder.build());
                data_block.add_entry(entry);
            }
            return Ok(data_block);
        }
        let rows = data_block.num_rows();
        let batch_rows = self.request_batch_rows;
        let retry_times = self.retry_times;
        let mut batch_blocks: Vec<DataBlock> = (0..rows)
            .step_by(batch_rows)
            .map(|start| data_block.slice(start..start + batch_rows.min(rows - start)))
            .collect();
        for func in self.funcs.iter() {
            let server_addr = func.udf_type.as_server().unwrap();
            let endpoint = self.endpoints.get(server_addr).unwrap();
            let tasks: Vec<_> = batch_blocks
                .into_iter()
                .map(|mini_batch| {
                    databend_common_base::runtime::spawn({
                        let ctx = self.ctx.clone();
                        let user_api = self.user_api.clone();
                        let endpoint = endpoint.clone();
                        let connect_timeout = self.connect_timeout;
                        let semaphore = self.semaphore.clone();
                        let func = func.clone();
                        async move {
                            let query_id = ctx.get_id();
                            let cluster = ctx.get_cluster();
                            let call_log = UdfCallLog {
                                query_id: &query_id,
                                node_id: &cluster.local_id,
                                function: &func.name,
                                handler: &func.func_name,
                                endpoint: &endpoint,
                                call_id: Uuid::new_v4(),
                            };
                            let mut attempt = 0;
                            let f = || {
                                attempt += 1;
                                let attempt = attempt;
                                let start = Instant::now();
                                let future = Self::transform_inner(
                                    ctx.clone(),
                                    user_api.clone(),
                                    endpoint.clone(),
                                    semaphore.clone(),
                                    connect_timeout,
                                    func.clone(),
                                    mini_batch.clone(),
                                );
                                async move {
                                    future.await.map_err(|failure| UdfAttemptFailure {
                                        failure,
                                        attempt,
                                        elapsed: start.elapsed(),
                                    })
                                }
                            };
                            let backoff = ExponentialBuilder::default()
                                .with_min_delay(Duration::from_millis(50))
                                .with_factor(2.0)
                                .with_max_delay(Duration::from_secs(30))
                                .with_max_times(retry_times);

                            let result = f
                                .retry(backoff)
                                .when(|err| retry_on(&err.failure.error))
                                .notify(|err, dur| {
                                    Profile::record_usize_profile(
                                        ProfileStatisticsName::ExternalServerRetryCount,
                                        1,
                                    );
                                    let kind = error_kind(&err.failure.error.message());
                                    record_retry_external(func.name.clone(), kind);
                                    call_log.log_failure(err, Some(dur), kind);
                                })
                                .await;
                            match result {
                                Ok(block) => {
                                    if attempt > 1 {
                                        call_log.log_recovery(attempt);
                                    }
                                    Ok(block)
                                }
                                Err(err) => {
                                    let kind = error_kind(&err.failure.error.message());
                                    call_log.log_failure(&err, None, kind);
                                    Err(err.failure.error)
                                }
                            }
                        }
                    })
                })
                .collect();

            let task_results = futures::future::join_all(tasks).await;
            batch_blocks = task_results
                .into_iter()
                .map(|b| b.unwrap())
                .map(|result| {
                    result.inspect_err(|err| {
                        record_error_external(func.name.clone(), error_kind(&err.message()));
                    })
                })
                .collect::<Result<Vec<_>>>()?;
        }
        data_block = DataBlock::concat(&batch_blocks)?;
        Ok(data_block)
    }
}
