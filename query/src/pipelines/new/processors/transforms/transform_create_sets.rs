// Copyright 2022 Datafuse Labs.
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

use common_base::base::tokio::task::JoinHandle;
use common_base::base::Runtime;
use common_base::base::TrySpawn;
use common_base::infallible::Mutex;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::Expression;
use common_planners::PlanNode;
use common_planners::SelectPlan;

use crate::interpreters::SelectInterpreter;
use crate::pipelines::new::executor::PipelinePullingExecutor;
use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::Event;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::Processor;
use crate::sessions::QueryContext;

pub struct TransformCreateSets {
    initialized: bool,
    schema: DataSchemaRef,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    input_data: Option<DataBlock>,
    output_data: Option<DataBlock>,

    sub_queries_result: Vec<DataValue>,
    sub_queries_puller: Arc<SubQueriesPuller>,

    runtime: Arc<Runtime>,
    sub_queries_res_handle: Option<JoinHandle<Result<Vec<DataValue>>>>,
}

impl TransformCreateSets {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        schema: DataSchemaRef,
        puller: Arc<SubQueriesPuller>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(TransformCreateSets {
            schema,
            input,
            output,
            input_data: None,
            initialized: false,
            sub_queries_result: vec![],
            sub_queries_puller: puller,
            output_data: None,
            sub_queries_res_handle: None,
            runtime: Arc::new(Runtime::with_worker_threads(1, None)?),
        })))
    }
}

#[async_trait::async_trait]
impl Processor for TransformCreateSets {
    fn name(&self) -> &'static str {
        "TransformCreateSets"
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if !self.initialized {
            let sub_queries_puller = self.sub_queries_puller.clone();
            self.sub_queries_res_handle = Some(
                self.runtime
                    .spawn(async move { sub_queries_puller.execute_sub_queries().await }),
            );
            return Ok(Event::Async);
        }

        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(output_data) = self.output_data.take() {
            self.output.push_data(Ok(output_data));
            return Ok(Event::NeedConsume);
        }

        if self.input_data.is_some() {
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.input.has_data() {
            self.input_data = Some(self.input.pull_data().unwrap()?);
            return Ok(Event::Sync);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(input_data) = self.input_data.take() {
            let num_rows = input_data.num_rows();
            let mut new_columns = input_data.columns().to_vec();
            let start_index = self.schema.fields().len() - self.sub_queries_result.len();

            for (index, result) in self.sub_queries_result.iter().enumerate() {
                let data_type = self.schema.field(start_index + index).data_type();
                let col = data_type.create_constant_column(result, num_rows)?;
                new_columns.push(col);
            }

            self.output_data = Some(DataBlock::create(self.schema.clone(), new_columns));
        }

        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        if let Some(handler) = self.sub_queries_res_handle.take() {
            self.sub_queries_result = match handler.await {
                Ok(Ok(res)) => Ok(res),
                Ok(Err(error_code)) => Err(error_code),
                Err(join_error) => {
                    if !join_error.is_panic() {
                        return Err(ErrorCode::TokioError("Join handler is canceled."));
                    }

                    let panic_error = join_error.into_panic();
                    return match panic_error.downcast_ref::<&'static str>() {
                        None => match panic_error.downcast_ref::<String>() {
                            None => Err(ErrorCode::PanicError("Sorry, unknown panic message")),
                            Some(message) => Err(ErrorCode::PanicError(message.to_string())),
                        },
                        Some(message) => Err(ErrorCode::PanicError(message.to_string())),
                    };
                }
            }?;

            self.initialized = true;
        }

        Ok(())
    }
}

pub struct SubQueriesPuller {
    ctx: Arc<QueryContext>,
    expressions: Vec<Expression>,
    sub_queries_result: Mutex<Vec<DataValue>>,
}

impl SubQueriesPuller {
    pub fn create(ctx: Arc<QueryContext>, exprs: Vec<Expression>) -> Arc<SubQueriesPuller> {
        Arc::new(SubQueriesPuller {
            ctx,
            expressions: exprs,
            sub_queries_result: Mutex::new(vec![]),
        })
    }

    async fn receive_subquery(&self, plan: SelectPlan) -> Result<DataValue> {
        let schema = plan.schema();
        let subquery_ctx = QueryContext::create_from(self.ctx.clone());
        let async_runtime = subquery_ctx.get_storage_runtime();

        let interpreter = SelectInterpreter::try_create(subquery_ctx, plan)?;
        let query_pipeline = interpreter.create_new_pipeline().await?;
        let mut query_executor =
            PipelinePullingExecutor::try_create(async_runtime, query_pipeline)?;

        let mut columns = Vec::with_capacity(schema.fields().len());

        for _ in schema.fields() {
            columns.push(Vec::new())
        }

        query_executor.start();
        while let Some(data_block) = query_executor.pull_data()? {
            #[allow(clippy::needless_range_loop)]
            for column_index in 0..data_block.num_columns() {
                let column = data_block.column(column_index);
                let mut values = column.to_values();
                columns[column_index].append(&mut values)
            }
        }

        let mut struct_fields = Vec::with_capacity(columns.len());

        for values in columns {
            struct_fields.push(DataValue::Array(values))
        }

        match struct_fields.len() {
            1 => Ok(struct_fields.remove(0)),
            _ => Ok(DataValue::Struct(struct_fields)),
        }
    }

    async fn receive_scalar_subquery(&self, plan: SelectPlan) -> Result<DataValue> {
        let schema = plan.schema();
        let subquery_ctx = QueryContext::create_from(self.ctx.clone());
        let async_runtime = subquery_ctx.get_storage_runtime();

        let interpreter = SelectInterpreter::try_create(subquery_ctx, plan)?;
        let query_pipeline = interpreter.create_new_pipeline().await?;

        let mut query_executor =
            PipelinePullingExecutor::try_create(async_runtime, query_pipeline)?;

        query_executor.start();
        match query_executor.pull_data()? {
            None => Err(ErrorCode::ScalarSubqueryBadRows(
                "Scalar subquery result set must be one row.",
            )),
            Some(data_block) => {
                if data_block.num_rows() != 1 {
                    return Err(ErrorCode::ScalarSubqueryBadRows(
                        "Scalar subquery result set must be one row.",
                    ));
                }

                let mut columns = Vec::with_capacity(schema.fields().len());

                for column_index in 0..data_block.num_columns() {
                    let column = data_block.column(column_index);
                    columns.push(column.get(0));
                }

                match columns.len() {
                    1 => Ok(columns.remove(0)),
                    _ => Ok(DataValue::Struct(columns)),
                }
            }
        }
    }

    pub async fn execute_sub_queries(&self) -> Result<Vec<DataValue>> {
        for sub_query_expr in &self.expressions {
            match sub_query_expr {
                Expression::Subquery { query_plan, .. } => match query_plan.as_ref() {
                    PlanNode::Select(select_plan) => {
                        let select_plan = select_plan.clone();
                        let subquery_res = self.receive_subquery(select_plan).await?;
                        let mut sub_queries_result = self.sub_queries_result.lock();
                        sub_queries_result.push(subquery_res);
                    }
                    _ => {
                        return Err(ErrorCode::LogicalError(
                            "Subquery must be select plan. It's a bug.",
                        ));
                    }
                },
                Expression::ScalarSubquery { query_plan, .. } => match query_plan.as_ref() {
                    PlanNode::Select(select_plan) => {
                        let select_plan = select_plan.clone();
                        let query_result = self.receive_scalar_subquery(select_plan).await?;
                        let mut sub_queries_result = self.sub_queries_result.lock();
                        sub_queries_result.push(query_result);
                    }
                    _ => {
                        return Err(ErrorCode::LogicalError(
                            "Subquery must be select plan. It's a bug.",
                        ));
                    }
                },
                _ => {
                    return Err(ErrorCode::LogicalError(
                        "Expression must be Subquery or ScalarSubquery",
                    ));
                }
            };
        }

        let sub_queries_result = self.sub_queries_result.lock();
        Ok(sub_queries_result.to_owned())
    }
}
