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

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::Mutex;
use common_planners::Expression;
use common_planners::PlanNode;
use common_planners::SelectPlan;

use crate::interpreters::SelectInterpreter;
use crate::pipelines::new::executor::PipelinePullingExecutor;
use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::transforms::transform::Transform;
use crate::pipelines::new::processors::transforms::transform::Transformer;
use crate::sessions::QueryContext;

pub struct TransformCreateSets {
    initialized: bool,
    schema: DataSchemaRef,
    sub_queries_result: Vec<DataValue>,
    sub_queries_puller: Arc<SubQueriesPuller>,
}

impl TransformCreateSets {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        schema: DataSchemaRef,
        puller: Arc<SubQueriesPuller>,
    ) -> Result<ProcessorPtr> {
        Ok(Transformer::create(input, output, TransformCreateSets {
            schema,
            initialized: false,
            sub_queries_result: vec![],
            sub_queries_puller: puller,
        }))
    }
}

impl Transform for TransformCreateSets {
    const NAME: &'static str = "TransformCreateSets";

    fn transform(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        if data_block.is_empty() {
            return Ok(DataBlock::empty_with_schema(self.schema.clone()));
        }

        if !self.initialized {
            self.initialized = true;
            self.sub_queries_result = self.sub_queries_puller.execute_sub_queries()?;
        }

        let num_rows = data_block.num_rows();
        let mut new_columns = data_block.columns().to_vec();
        let start_index = self.schema.fields().len() - self.sub_queries_result.len();

        for (index, result) in self.sub_queries_result.iter().enumerate() {
            let data_type = self.schema.field(start_index + index).data_type();
            let col = data_type.create_constant_column(result, num_rows)?;
            new_columns.push(col);
        }

        Ok(DataBlock::create(self.schema.clone(), new_columns))
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

    fn receive_subquery(&self, plan: SelectPlan) -> Result<DataValue> {
        let schema = plan.schema();
        let subquery_ctx = QueryContext::create_from(self.ctx.clone());
        let async_runtime = subquery_ctx.get_storage_runtime();

        let interpreter = SelectInterpreter::try_create(subquery_ctx, plan)?;
        let query_pipeline = interpreter.create_new_pipeline()?;
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

    fn receive_scalar_subquery(&self, plan: SelectPlan) -> Result<DataValue> {
        let schema = plan.schema();
        let subquery_ctx = QueryContext::create_from(self.ctx.clone());
        let async_runtime = subquery_ctx.get_storage_runtime();

        let interpreter = SelectInterpreter::try_create(subquery_ctx, plan)?;
        let query_pipeline = interpreter.create_new_pipeline()?;

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

    pub fn execute_sub_queries(&self) -> Result<Vec<DataValue>> {
        let mut sub_queries_result = self.sub_queries_result.lock();

        if sub_queries_result.is_empty() {
            for sub_query_expr in &self.expressions {
                match sub_query_expr {
                    Expression::Subquery { query_plan, .. } => match query_plan.as_ref() {
                        PlanNode::Select(select_plan) => {
                            let select_plan = select_plan.clone();
                            sub_queries_result.push(self.receive_subquery(select_plan)?);
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
                            sub_queries_result.push(self.receive_scalar_subquery(select_plan)?);
                        }
                        _ => {
                            return Err(ErrorCode::LogicalError(
                                "Subquery must be select plan. It's a bug.",
                            ));
                        }
                    },
                    _ => {
                        return Result::Err(ErrorCode::LogicalError(
                            "Expression must be Subquery or ScalarSubquery",
                        ));
                    }
                };
            }
        }

        Ok(sub_queries_result.to_owned())
    }
}
