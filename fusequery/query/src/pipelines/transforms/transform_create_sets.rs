use std::sync::Arc;
use crate::pipelines::processors::{Processor, Pipeline, EmptyProcessor, PipelineBuilder};
use std::collections::HashMap;
use common_exception::{Result, ErrorCode};
use common_streams::{SendableDataBlockStream, SubQueriesStream};
use crate::sessions::{FuseQueryContextRef, FuseQueryContext};
use std::any::Any;
use common_infallible::Mutex;
use futures::{FutureExt, Future, StreamExt};
use futures::future::{Shared, BoxFuture, JoinAll};
use futures::channel::oneshot::Receiver;
use common_datavalues::columns::DataColumn;
use common_runtime::tokio::macros::support::{Pin, Poll};
use common_datavalues::{DataValue, DataSchemaRef};
use common_planners::Expression;
use common_datablocks::DataBlock;
use common_runtime::tokio::task::JoinHandle;
use futures::future::join_all;

pub struct CreateSetsTransform {
    ctx: FuseQueryContextRef,
    schema: DataSchemaRef,
    input: Arc<dyn Processor>,
    sub_queries_puller: Arc<Mutex<SubQueriesPuller<'static>>>,
}

impl CreateSetsTransform {
    pub fn try_create(
        ctx: FuseQueryContextRef,
        schema: DataSchemaRef,
        sub_queries_puller: Arc<Mutex<SubQueriesPuller<'static>>>,
    ) -> Result<CreateSetsTransform>
    {
        Ok(CreateSetsTransform {
            ctx,
            schema,
            sub_queries_puller,
            input: Arc::new(EmptyProcessor::create()),
        })
    }

    fn execute_sub_queries(&self) -> Result<impl Future<Output=Result<Vec<DataValue>>>> {
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
                            "Cannot join all sub queries. cause: {}", error
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
            join_tasks.push(context.execute_task(future)?);
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
        Ok(self.input = input)
    }

    fn inputs(&self) -> Vec<Arc<dyn Processor>> {
        vec![self.input.clone()]
    }

    fn as_any(&'_ self) -> &dyn Any {
        self
    }

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
    ctx: FuseQueryContextRef,
    expressions: Vec<Expression>,
    sub_queries: Vec<SharedFuture<'a>>,
}

impl<'a> SubQueriesPuller<'a> {
    pub fn create(
        ctx: FuseQueryContextRef,
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
        self.sub_queries.len()
    }

    pub fn take_subquery_data(&mut self, pos: usize) -> Result<impl Future<Output=SubqueryData> + 'a> {
        if self.sub_queries.is_empty() {
            self.init()?;
        }

        Ok(self.sub_queries[pos].clone())
    }

    fn init(&mut self) -> Result<()> {
        for query_expression in &self.expressions {
            let subquery_ctx = FuseQueryContext::new(self.ctx.clone());

            match query_expression {
                Expression::Subquery { name, query_plan } => {
                    let subquery_plan = (**query_plan).clone();
                    let builder = PipelineBuilder::create(subquery_ctx, subquery_plan);
                    let shared_future = Self::receive_subquery_res(builder.build()?);
                    self.sub_queries.push(shared_future);
                }
                Expression::ScalarSubquery { name, query_plan } => {
                    let subquery_plan = (**query_plan).clone();
                    let builder = PipelineBuilder::create(subquery_ctx, subquery_plan);
                    let shared_future = Self::receive_scalar_subquery_res(builder.build()?);
                    self.sub_queries.push(shared_future);
                }
                _ => return Result::Err(ErrorCode::LogicalError("Expression must be Subquery or ScalarSubquery"))
            };
        };

        Ok(())
    }

    fn receive_subquery_res(mut pipeline: Pipeline) -> SharedFuture<'a> {
        let subquery_future = async move {
            let mut stream = pipeline.execute().await?;

            let mut columns = Vec::new();
            while let Some(data_block) = stream.next().await {
                let data_block = data_block?;

                if columns.is_empty() {
                    for index in 0..data_block.num_columns() {
                        columns.push((data_block.column(index).data_type(), Vec::new()));
                    }
                }

                for column_index in 0..data_block.num_columns() {
                    let series = data_block.column(column_index).to_array()?;
                    let mut values = series.to_values()?;
                    columns[column_index].1.append(&mut values)
                }
            }

            let mut struct_fields = Vec::with_capacity(columns.len());

            for (data_type, values) in columns {
                struct_fields.push(DataValue::List(Some(values), data_type))
            }

            match struct_fields.len() {
                1 => Ok(struct_fields.remove(0)),
                _ => Ok(DataValue::Struct(struct_fields))
            }
        };

        subquery_future.boxed().shared()
    }

    fn receive_scalar_subquery_res(mut pipeline: Pipeline) -> SharedFuture<'a> {
        let subquery_future = async move {
            let mut stream = pipeline.execute().await?;

            if let Some(data_block) = stream.next().await {
                let data_block = data_block?;

                if data_block.num_rows() > 1 {
                    return Err(ErrorCode::ScalarSubqueryHasMoreRows(
                        "Scalar subquery result set must be one row."
                    ));
                }

                if data_block.num_columns() == 1 {
                    // match data_block
                }
            }

            // match stream.next().await {
            //     None =>
            // }
            Ok(DataValue::UInt64(Some(1)))
        };

        subquery_future.boxed().shared()
    }
}
