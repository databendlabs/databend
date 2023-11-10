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

use std::any::Any;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::udf_client::UDFFlightClient;
use common_expression::variant_transform::contains_variant;
use common_expression::variant_transform::transform_variant;
use common_expression::BlockEntry;
use common_expression::DataBlock;
use common_expression::DataField;
use common_expression::DataSchema;
use common_expression::FunctionContext;
use common_sql::executor::physical_plans::UdfFunctionDesc;

use crate::pipelines::processors::Event;
use crate::pipelines::processors::InputPort;
use crate::pipelines::processors::OutputPort;
use crate::pipelines::processors::Processor;

pub struct TransformUdf {
    func_ctx: FunctionContext,
    funcs: Vec<UdfFunctionDesc>,

    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    input_data_block: Option<DataBlock>,
    output_data_block: Option<DataBlock>,
}

impl TransformUdf {
    pub fn try_create(
        func_ctx: FunctionContext,
        funcs: Vec<UdfFunctionDesc>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
    ) -> Result<Box<dyn Processor>> {
        Ok(Box::new(Self {
            func_ctx,
            funcs,
            input,
            output,
            input_data_block: None,
            output_data_block: None,
        }))
    }
}

#[async_trait::async_trait]
impl Processor for TransformUdf {
    fn name(&self) -> String {
        "UdfTransform".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data_block.take() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.input_data_block.is_some() {
            return Ok(Event::Async);
        }

        if self.input.has_data() {
            self.input_data_block = Some(self.input.pull_data().unwrap()?);
            return Ok(Event::Async);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if let Some(mut data_block) = self.input_data_block.take() {
            let connect_timeout = self.func_ctx.external_server_connect_timeout_secs;
            let request_timeout = self.func_ctx.external_server_request_timeout_secs;
            for func in &self.funcs {
                // construct input record_batch
                let num_rows = data_block.num_rows();
                let block_entries = func
                    .arg_indices
                    .iter()
                    .map(|i| data_block.get_by_offset(*i).clone())
                    .collect::<Vec<_>>();

                let fields = block_entries
                    .iter()
                    .enumerate()
                    .map(|(idx, arg)| {
                        DataField::new(&format!("arg{}", idx + 1), arg.data_type.clone())
                    })
                    .collect::<Vec<_>>();
                let data_schema = DataSchema::new(fields);

                let input_batch = DataBlock::new(block_entries, num_rows)
                    .to_record_batch(&data_schema)
                    .map_err(|err| ErrorCode::from_string(format!("{err}")))?;

                let mut client =
                    UDFFlightClient::connect(&func.server_addr, connect_timeout, request_timeout)
                        .await?;
                let result_batch = client.do_exchange(&func.func_name, input_batch).await?;

                let (result_block, result_schema) = DataBlock::from_record_batch(&result_batch)
                    .map_err(|err| {
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
            self.output_data_block = Some(data_block.clone())
        }

        Ok(())
    }
}
