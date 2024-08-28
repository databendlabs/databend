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

use std::sync::Arc;

use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::udf_client::UDFFlightClient;
use databend_common_expression::variant_transform::contains_variant;
use databend_common_expression::variant_transform::transform_variant;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_pipeline_transforms::processors::AsyncRetry;
use databend_common_pipeline_transforms::processors::AsyncRetryWrapper;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use databend_common_pipeline_transforms::processors::RetryStrategy;
use databend_common_sql::executor::physical_plans::UdfFunctionDesc;

use crate::sessions::QueryContext;

pub struct TransformUdfServer {
    ctx: Arc<QueryContext>,
    funcs: Vec<UdfFunctionDesc>,
    connect_timeout: u64,
    request_timeout: u64,
    request_bacth_rows: u64,
    retry_times: u64,
}

impl TransformUdfServer {
    pub fn new_retry_wrapper(
        ctx: Arc<QueryContext>,
        funcs: Vec<UdfFunctionDesc>,
    ) -> Result<AsyncRetryWrapper<Self>> {
        let settings = ctx.get_settings();
        let connect_timeout = settings.get_external_server_connect_timeout_secs()?;
        let request_timeout = settings.get_external_server_request_timeout_secs()?;
        let request_bacth_rows = settings.get_external_server_request_batch_rows()?;
        let retry_times = settings.get_external_server_request_retry_times()?;

        let s = Self {
            ctx,
            funcs,

            connect_timeout,
            request_timeout,
            request_bacth_rows,
            retry_times,
        };
        Ok(AsyncRetryWrapper::create(s))
    }
}

impl AsyncRetry for TransformUdfServer {
    fn retry_on(&self, _err: &databend_common_exception::ErrorCode) -> bool {
        true
    }

    fn retry_strategy(&self) -> RetryStrategy {
        RetryStrategy {
            retry_times: self.retry_times as usize,
            retry_sleep_duration: Some(tokio::time::Duration::from_millis(500)),
        }
    }

    fn retry_hook(&self) {
        Profile::record_usize_profile(ProfileStatisticsName::ExternalServerRetryCount, 1);
    }
}

#[async_trait::async_trait]
impl AsyncTransform for TransformUdfServer {
    const NAME: &'static str = "UdfTransform";

    #[async_backtrace::framed]
    async fn transform(&mut self, mut data_block: DataBlock) -> Result<DataBlock> {
        for func in &self.funcs {
            let server_addr = func.udf_type.as_server().unwrap();
            // construct input record_batch
            let num_rows = data_block.num_rows();
            let block_entries = func
                .arg_indices
                .iter()
                .map(|i| {
                    let arg = data_block.get_by_offset(*i).clone();
                    if contains_variant(&arg.data_type) {
                        let new_arg = BlockEntry::new(
                            arg.data_type.clone(),
                            transform_variant(&arg.value, true)?,
                        );
                        Ok(new_arg)
                    } else {
                        Ok(arg)
                    }
                })
                .collect::<Result<Vec<_>>>()?;

            let fields = block_entries
                .iter()
                .enumerate()
                .map(|(idx, arg)| DataField::new(&format!("arg{}", idx + 1), arg.data_type.clone()))
                .collect::<Vec<_>>();
            let data_schema = DataSchema::new(fields);

            let input_batch = DataBlock::new(block_entries, num_rows)
                .to_record_batch_with_dataschema(&data_schema)
                .map_err(|err| ErrorCode::from_string(format!("{err}")))?;

            let mut client = UDFFlightClient::connect(
                server_addr,
                self.connect_timeout,
                self.request_timeout,
                self.request_bacth_rows,
            )
            .await?
            .with_tenant(self.ctx.get_tenant().tenant_name())?
            .with_func_name(&func.func_name)?
            .with_query_id(&self.ctx.get_id())?;

            let result_batch = client.do_exchange(&func.func_name, input_batch).await?;
            let schema = DataSchema::try_from(&(*result_batch.schema()))?;
            let (result_block, result_schema) =
                DataBlock::from_record_batch(&schema, &result_batch).map_err(|err| {
                    ErrorCode::UDFDataError(format!(
                        "Cannot convert arrow record batch to data block: {err}"
                    ))
                })?;

            let result_fields = result_schema.fields();
            if result_fields.is_empty() || result_block.is_empty() {
                return Err(ErrorCode::EmptyDataFromServer(
                    "Get empty data from UDF Server",
                ));
            }

            if result_fields[0].data_type() != &*func.data_type {
                return Err(ErrorCode::UDFSchemaMismatch(format!(
                    "UDF server return incorrect type, expected: {}, but got: {}",
                    func.data_type,
                    result_fields[0].data_type()
                )));
            }
            if result_block.num_rows() != num_rows {
                return Err(ErrorCode::UDFDataError(format!(
                    "UDF server should return {} rows, but it returned {} rows",
                    num_rows,
                    result_block.num_rows()
                )));
            }

            let col = if contains_variant(&func.data_type) {
                let value = transform_variant(&result_block.get_by_offset(0).value, false)?;
                BlockEntry {
                    data_type: result_fields[0].data_type().clone(),
                    value,
                }
            } else {
                result_block.get_by_offset(0).clone()
            };

            data_block.add_column(col);
        }
        Ok(data_block)
    }
}
