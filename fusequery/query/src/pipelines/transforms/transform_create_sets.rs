use std::sync::Arc;
use crate::pipelines::processors::{Processor, Pipeline, EmptyProcessor, PipelineBuilder};
use std::collections::HashMap;
use common_exception::{Result, ErrorCode};
use common_streams::SendableDataBlockStream;
use crate::sessions::{FuseQueryContextRef, FuseQueryContext};
use std::any::Any;
use common_infallible::Mutex;
use futures::{FutureExt, Future, StreamExt};
use futures::future::{Shared, BoxFuture};
use futures::channel::oneshot::Receiver;
use common_datavalues::columns::DataColumn;
use common_runtime::tokio::macros::support::{Pin, Poll};
use common_datavalues::{DataValue, DataSchemaRef};
use common_planners::Expression;
use common_datablocks::DataBlock;

pub struct CreateSetsTransform {
    ctx: FuseQueryContextRef,
    input: Arc<dyn Processor>,
    sub_queries_puller: Arc<Mutex<SubQueriesPuller<'static>>>,
}

impl CreateSetsTransform {
    pub fn try_create(
        ctx: FuseQueryContextRef,
        sub_queries_puller: Arc<Mutex<SubQueriesPuller<'static>>>,
    ) -> Result<CreateSetsTransform>
    {
        Ok(CreateSetsTransform {
            ctx,
            sub_queries_puller,
            input: Arc::new(EmptyProcessor::create()),
        })
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

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let mut sub_queries_puller = self.sub_queries_puller.lock();

        self.ctx.execute_task(async {});
        Err(ErrorCode::UnImplement(""))
    }
}

type SubqueryData = Result<Arc<DataColumn>>;
type SharedFuture<'a> = Shared<BoxFuture<'a, SubqueryData>>;

pub struct SubQueriesPuller<'a> {
    ctx: FuseQueryContextRef,
    expressions: Vec<Expression>,
    sub_queries: HashMap<String, SharedFuture<'a>>,
}

impl<'a> SubQueriesPuller<'a> {
    pub fn create(
        ctx: FuseQueryContextRef,
        expressions: Vec<Expression>,
    ) -> Arc<Mutex<SubQueriesPuller<'a>>> {
        Arc::new(Mutex::new(SubQueriesPuller {
            ctx,
            expressions: expressions,
            sub_queries: HashMap::new(),
        }))
    }

    fn init(&mut self) -> Result<()> {
        for query_expression in &self.expressions {
            let subquery_ctx = FuseQueryContext::new(self.ctx.clone());

            match query_expression {
                Expression::Subquery { name, query_plan } => {
                    let subquery_plan = (**query_plan).clone();
                    let builder = PipelineBuilder::create(subquery_ctx, subquery_plan);
                    let shared_future = Self::receive_subquery_res(query_plan.schema(), builder.build()?);
                    self.sub_queries.insert(name.clone(), shared_future);
                }
                Expression::ScalarSubquery { name, query_plan } => {
                    let subquery_plan = (**query_plan).clone();
                    let builder = PipelineBuilder::create(subquery_ctx, subquery_plan);
                    let shared_future = Self::receive_scalar_subquery_res(builder.build()?);
                    self.sub_queries.insert(name.clone(), shared_future);
                }
                _ => return Result::Err(ErrorCode::LogicalError("Expression must be Subquery or ScalarSubquery"))
            };
        };

        Ok(())
    }

    pub fn get_subquery_data(&mut self, name: String) -> Result<impl Future<Output=SubqueryData>> {
        if self.sub_queries.is_empty() {
            self.init()?;
        }

        match self.sub_queries.get(&name) {
            None => Err(ErrorCode::LogicalError("Cannot find subquery.")),
            Some(future) => Ok(future.clone())
        }
    }

    fn receive_subquery_res(schema: DataSchemaRef, mut pipeline: Pipeline) -> SharedFuture<'a> {
        let subquery_future = async move {
            let mut stream = pipeline.execute().await?;

            // TODO: concat to list field
            let list = Vec::new();
            while let Some(data_block) = stream.next().await {
                let data_block = data_block?;
                for column_index in 0..data_block.num_columns() {
                    let series = data_block.column(column_index).to_array()?;

                    if let DataValue::List(Some(values), _) = series.compact() {}
                }
            }
            Ok(Arc::new(DataColumn::Constant(DataValue::UInt64(Some(1)), 1)))
        };

        subquery_future.boxed().shared()
    }

    fn receive_scalar_subquery_res(mut pipeline: Pipeline) -> SharedFuture<'a> {
        let subquery_future = async move {
            let mut stream = pipeline.execute().await?;

            if let Some(data_block) = stream.next().await {
                let data_block = data_block?;

                if data_block.num_rows() > 1 {
                    return Err(ErrorCode::ScalarSubqueryHashMoreRows(
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
            Ok(Arc::new(DataColumn::Constant(DataValue::UInt64(Some(1)), 1)))
        };

        subquery_future.boxed().shared()
    }
}

