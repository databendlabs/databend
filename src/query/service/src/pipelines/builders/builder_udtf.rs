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

use backon::ExponentialBuilder;
use backon::Retryable;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::udf_client::error_kind;
use databend_common_expression::udf_client::UDFFlightClient;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_metrics::external_server::record_retry_external;
use databend_common_pipeline::sources::AsyncSource;
use tokio::sync::Semaphore;
use tonic::transport::Endpoint;

pub struct UdtfServerSource {
    ctx: Arc<dyn TableContext>,

    func: UdtfFunctionDesc,
    connect_timeout: u64,
    semaphore: Arc<Semaphore>,
    endpoint: Arc<Endpoint>,
    retry_times: usize,

    done: bool,
}

impl UdtfServerSource {
    pub fn init_semaphore(ctx: Arc<dyn TableContext>) -> Result<Arc<Semaphore>> {
        let settings = ctx.get_settings();
        let request_max_threads = settings.get_external_server_request_max_threads()? as usize;
        let semaphore = Arc::new(Semaphore::new(request_max_threads));
        Ok(semaphore)
    }

    pub fn init_endpoints(
        ctx: Arc<dyn TableContext>,
        func: &UdtfFunctionDesc,
    ) -> Result<Arc<Endpoint>> {
        let settings = ctx.get_settings();
        let connect_timeout = settings.get_external_server_connect_timeout_secs()?;
        let request_timeout = settings.get_external_server_request_timeout_secs()?;

        let endpoint = UDFFlightClient::build_endpoint(
            &func.server,
            connect_timeout,
            request_timeout,
            &ctx.get_version().udf_client_user_agent(),
        )?;

        Ok(endpoint)
    }

    pub fn new(
        ctx: Arc<dyn TableContext>,
        func: UdtfFunctionDesc,
        semaphore: Arc<Semaphore>,
        endpoint: Arc<Endpoint>,
    ) -> Result<Self> {
        let settings = ctx.get_settings();
        let connect_timeout = settings.get_external_server_connect_timeout_secs()?;
        let retry_times = settings.get_external_server_request_retry_times()? as usize;

        Ok(Self {
            ctx,
            func,
            connect_timeout,
            semaphore,
            endpoint,
            retry_times,
            done: false,
        })
    }

    async fn source_inner(
        ctx: Arc<dyn TableContext>,
        endpoint: Arc<Endpoint>,
        semaphore: Arc<Semaphore>,
        connect_timeout: u64,
        func: UdtfFunctionDesc,
    ) -> Result<DataBlock> {
        // Must obtain the permit to execute, prevent too many connections being executed concurrently
        let permit = semaphore.acquire_owned().await.map_err(|e| {
            ErrorCode::Internal(format!("Udtf transformer acquire permit failure. {}", e))
        })?;
        // construct input record_batch
        let mut client =
            UDFFlightClient::connect(&func.func_name, endpoint, connect_timeout, 65536)
                .await?
                .with_tenant(ctx.get_tenant().tenant_name())?
                .with_func_name(&func.name)?
                .with_handler_name(&func.func_name)?
                .with_query_id(&ctx.get_id())?
                .with_headers(func.headers)?;

        let args = func
            .args
            .into_iter()
            .filter(|(_, ty)| ty.remove_nullable() != DataType::StageLocation)
            .map(|(scalar, ty)| BlockEntry::Const(scalar, ty, 1))
            .collect();

        debug_assert!(func.return_ty.is_tuple());
        let result = client
            .do_exchange(&func.name, &func.func_name, 1, args, &func.return_ty)
            .await?;

        drop(permit);
        Ok(result)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UdtfFunctionDesc {
    pub name: String,
    pub func_name: String,
    pub return_ty: DataType,
    pub args: Vec<(Scalar, DataType)>,
    pub headers: BTreeMap<String, String>,
    pub server: String,
}

fn retry_on(err: &databend_common_exception::ErrorCode) -> bool {
    if err.code() == ErrorCode::U_D_F_DATA_ERROR {
        let message = err.message();
        // this means the server can't handle the request in 60s
        if message.contains("h2 protocol error") {
            return false;
        }
    }
    true
}

#[async_trait::async_trait]
impl AsyncSource for UdtfServerSource {
    const NAME: &'static str = "UdtfServerSource";
    const SKIP_EMPTY_DATA_BLOCK: bool = true;

    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.done {
            return Ok(None);
        }
        let ctx = self.ctx.clone();
        let endpoint = self.endpoint.clone();
        let connect_timeout = self.connect_timeout;
        let semaphore = self.semaphore.clone();
        let func = self.func.clone();
        let name = func.name.clone();

        let f = {
            move || {
                Self::source_inner(
                    ctx.clone(),
                    endpoint.clone(),
                    semaphore.clone(),
                    connect_timeout,
                    func.clone(),
                )
            }
        };
        let backoff = ExponentialBuilder::default()
            .with_min_delay(Duration::from_millis(50))
            .with_factor(2.0)
            .with_max_delay(Duration::from_secs(30))
            .with_max_times(self.retry_times);

        let data_block = f
            .retry(backoff)
            .when(retry_on)
            .notify(move |err, dur| {
                Profile::record_usize_profile(ProfileStatisticsName::ExternalServerRetryCount, 1);
                record_retry_external(name.clone(), error_kind(&err.message()));
                log::warn!("Retry udtf error: {:?} after {:?}", err.message(), dur);
            })
            .await?;

        self.done = true;
        Ok(Some(data_block))
    }
}
