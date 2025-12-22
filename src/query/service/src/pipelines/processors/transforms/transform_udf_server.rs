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
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::udf_client::UDFFlightClient;
use databend_common_expression::udf_client::error_kind;
use databend_common_expression::udf_client::is_transport_error_message;
use databend_common_metrics::external_server::record_error_external;
use databend_common_metrics::external_server::record_retry_external;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use tokio::sync::Semaphore;
use tonic::transport::Endpoint;

use crate::physical_plans::UdfFunctionDesc;
use crate::sessions::QueryContext;

pub struct TransformUdfServer {
    ctx: Arc<QueryContext>,
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

        Ok(Self {
            ctx,
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
        endpoint: Arc<Endpoint>,
        semaphore: Arc<Semaphore>,
        connect_timeout: u64,
        func: UdfFunctionDesc,
        mut data_block: DataBlock,
    ) -> Result<DataBlock> {
        // Must obtain the permit to execute, prevent too many connections being executed concurrently
        let permit = semaphore.acquire_owned().await.map_err(|e| {
            ErrorCode::Internal(format!("Udf transformer acquire permit failure. {}", e))
        })?;
        // construct input record_batch
        let num_rows = data_block.num_rows();
        let block_entries = func
            .arg_indices
            .iter()
            .map(|i| data_block.get_by_offset(*i).clone())
            .collect::<Vec<_>>();

        let mut client =
            UDFFlightClient::connect(&func.func_name, endpoint, connect_timeout, 65536)
                .await?
                .with_tenant(ctx.get_tenant().tenant_name())?
                .with_func_name(&func.name)?
                .with_handler_name(&func.func_name)?
                .with_query_id(&ctx.get_id())?
                .with_headers(func.headers)?;

        let result = client
            .do_exchange(
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

fn retry_on(err: &databend_common_exception::ErrorCode) -> bool {
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
                        let endpoint = endpoint.clone();
                        let connect_timeout = self.connect_timeout;
                        let semaphore = self.semaphore.clone();
                        let func = func.clone();
                        let name = func.name.clone();

                        let f = {
                            move || {
                                Self::transform_inner(
                                    ctx.clone(),
                                    endpoint.clone(),
                                    semaphore.clone(),
                                    connect_timeout,
                                    func.clone(),
                                    mini_batch.clone(),
                                )
                            }
                        };
                        let backoff = ExponentialBuilder::default()
                            .with_min_delay(Duration::from_millis(50))
                            .with_factor(2.0)
                            .with_max_delay(Duration::from_secs(30))
                            .with_max_times(self.retry_times);

                        f.retry(backoff).when(retry_on).notify(move |err, dur| {
                            Profile::record_usize_profile(
                                ProfileStatisticsName::ExternalServerRetryCount,
                                1,
                            );
                            record_retry_external(name.clone(), error_kind(&err.message()));
                            log::warn!("Retry udf error: {:?} after {:?}", err.message(), dur);
                        })
                    })
                })
                .collect();

            let task_results = futures::future::join_all(tasks).await;
            batch_blocks = task_results
                .into_iter()
                .map(|b| b.unwrap())
                .map(|b| match b {
                    Ok(b) => Ok(b),
                    Err(err) => {
                        record_error_external(func.name.clone(), error_kind(&err.message()));
                        Err(err)
                    }
                })
                .collect::<Result<Vec<_>>>()?;
        }
        data_block = DataBlock::concat(&batch_blocks)?;
        Ok(data_block)
    }
}
