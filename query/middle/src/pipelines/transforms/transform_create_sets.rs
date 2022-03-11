// Copyright 2021 Datafuse Labs.
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

use common_base::tokio::task::JoinHandle;
use common_base::TrySpawn;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_infallible::Mutex;
use common_planners::Expression;
use common_streams::SendableDataBlockStream;
use common_streams::SubQueriesStream;
use common_tracing::tracing;
use futures::future::join_all;
use futures::future::BoxFuture;
use futures::future::JoinAll;
use futures::future::Shared;
use futures::Future;
use futures::FutureExt;
use futures::StreamExt;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::Pipeline;
use crate::pipelines::processors::PipelineBuilder;
use crate::pipelines::processors::Processor;
use crate::sessions::QueryContext;

pub struct CreateSetsTransform {
    ctx: Arc<QueryContext>,
    schema: DataSchemaRef,
    input: Arc<dyn Processor>,
    sub_queries_puller: Arc<Mutex<SubQueriesPuller<'static>>>,
}

impl CreateSetsTransform {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        schema: DataSchemaRef,
        sub_queries_puller: Arc<Mutex<SubQueriesPuller<'static>>>,
    ) -> Result<CreateSetsTransform> {
        Ok(CreateSetsTransform {
            ctx,
            schema,
            sub_queries_puller,
            input: Arc::new(EmptyProcessor::create()),
        })
    }

    fn execute_sub_queries(&self) -> Result<impl Future<Output = Result<Vec<DataValue>>>> {
        let join_all = self.execute_sub_queries_impl()?;

        Ok(async move {
            let sub_queries_res = join_all.await;
            let mut execute_res = Vec::with_capacity(sub_queries_res.len());

            // TODO: maybe it's a map(Result::flatten)?
            for subquery_res in sub_queries_res {
                match subquery_res {
                    Ok(Ok(data)) => execute_res.push(data),
                    Ok(Err(error)) => return Err(error),
                    Err(error) => {
                        return Err(ErrorCode::TokioError(format!(
                            "Cannot join all sub queries. cause: {}",
                            error
                        )));
                    }
                };
            }

            Ok(execute_res)
        })
    }

    fn execute_sub_queries_impl(&self) -> Result<JoinAll<JoinHandle<SubqueryData>>> {
        let context = self.ctx.clone();
        let mut data_puller = self.sub_queries_puller.lock();

        let mut join_tasks = vec![];
        for index in 0..data_puller.sub_queries_num() {
            let future = data_puller.take_subquery_data(index)?;
            join_tasks.push(context.try_spawn(future)?);
        }

        Ok(join_all(join_tasks))
    }
}

#[async_trait::async_trait]
impl Processor for CreateSetsTransform {
    fn name(&self) -> &str {
        "CreateSetsTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn Processor>) -> Result<()> {
        self.input = input;
        Ok(())
    }

    fn inputs(&self) -> Vec<Arc<dyn Processor>> {
        vec![self.input.clone()]
    }

    fn as_any(&'_ self) -> &dyn Any {
        self
    }

    #[tracing::instrument(level = "debug", name = "create_sets_execute", skip(self))]
    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let data = self.execute_sub_queries()?.await;

        Ok(Box::pin(SubQueriesStream::create(
            self.schema.clone(),
            self.input.execute().await?,
            data?,
        )))
    }
}

type SubqueryData = Result<DataValue>;
type SharedFuture<'a> = Shared<BoxFuture<'a, SubqueryData>>;

pub struct SubQueriesPuller<'a> {
    ctx: Arc<QueryContext>,
    expressions: Vec<Expression>,
    sub_queries: Vec<SharedFuture<'a>>,
}

impl<'a> SubQueriesPuller<'a> {
    pub fn create(
        ctx: Arc<QueryContext>,
        expressions: Vec<Expression>,
    ) -> Arc<Mutex<SubQueriesPuller<'a>>> {
        let expression_len = expressions.len();
        Arc::new(Mutex::new(SubQueriesPuller {
            ctx,
            expressions,
            sub_queries: Vec::with_capacity(expression_len),
        }))
    }

    pub fn sub_queries_num(&mut self) -> usize {
        self.expressions.len()
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub fn take_subquery_data(
        &mut self,
        pos: usize,
    ) -> Result<impl Future<Output = SubqueryData> + 'a> {
        if self.sub_queries.is_empty() {
            self.init()?;
        }

        Ok(self.sub_queries[pos].clone())
    }

    fn init(&mut self) -> Result<()> {
        for query_expression in &self.expressions {
            let subquery_ctx = QueryContext::create_from(self.ctx.clone());

            match query_expression {
                Expression::Subquery { query_plan, .. } => {
                    let plan = query_plan.as_ref().clone();
                    let builder = PipelineBuilder::create(subquery_ctx);
                    let pipeline = builder.build(&plan)?;
                    let shared_future = Self::receive_subquery_res(plan.schema(), pipeline);
                    self.sub_queries.push(shared_future);
                }
                Expression::ScalarSubquery { query_plan, .. } => {
                    let plan = query_plan.as_ref().clone();
                    let builder = PipelineBuilder::create(subquery_ctx);
                    let pipeline = builder.build(&plan)?;
                    let shared_future = Self::receive_scalar_subquery_res(pipeline);
                    self.sub_queries.push(shared_future);
                }
                _ => {
                    return Result::Err(ErrorCode::LogicalError(
                        "Expression must be Subquery or ScalarSubquery",
                    ))
                }
            };
        }

        Ok(())
    }

    fn receive_subquery_res(schema: DataSchemaRef, mut pipeline: Pipeline) -> SharedFuture<'a> {
        let subquery_future = async move {
            let mut stream = pipeline.execute().await?;
            let mut columns = Vec::with_capacity(schema.fields().len());

            for field in schema.fields() {
                let data_type = field.data_type().clone();
                columns.push((data_type, Vec::new()))
            }

            while let Some(data_block) = stream.next().await {
                let data_block = data_block?;

                #[allow(clippy::needless_range_loop)]
                for column_index in 0..data_block.num_columns() {
                    let col = data_block.column(column_index);
                    let mut values = col.to_values();
                    columns[column_index].1.append(&mut values)
                }
            }

            let mut struct_fields = Vec::with_capacity(columns.len());

            for (_, values) in columns {
                struct_fields.push(DataValue::Array(values))
            }

            match struct_fields.len() {
                1 => Ok(struct_fields.remove(0)),
                _ => Ok(DataValue::Struct(struct_fields)),
            }
        };

        subquery_future.boxed().shared()
    }

    fn receive_scalar_subquery_res(mut pipeline: Pipeline) -> SharedFuture<'a> {
        let subquery_future = async move {
            let mut stream = pipeline.execute().await?;

            let mut columns = None;
            while let Some(data_block) = stream.next().await {
                let data_block = data_block?;

                if data_block.num_rows() != 1 || columns.is_some() {
                    return Err(ErrorCode::ScalarSubqueryBadRows(
                        "Scalar subquery result set must be one row.",
                    ));
                }

                let mut columns_data = Vec::with_capacity(data_block.num_columns());
                for column in data_block.columns() {
                    match column.len() {
                        1 => columns_data.push(column.get(0)),
                        _ => {
                            return Err(ErrorCode::ScalarSubqueryBadRows(
                                "Scalar subquery result set must be one row.",
                            ))
                        }
                    }
                }

                columns = Some(columns_data)
            }

            match columns {
                Some(mut data) if data.len() == 1 => Ok(data.remove(0)),
                Some(data) => Ok(DataValue::Struct(data)),
                None => Err(ErrorCode::ScalarSubqueryBadRows(
                    "Scalar subquery result set must be one row.",
                )),
            }
        };

        subquery_future.boxed().shared()
    }
}
