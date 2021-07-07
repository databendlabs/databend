use std::sync::Arc;
use crate::pipelines::processors::{Processor, Pipeline, EmptyProcessor, PipelineBuilder};
use std::collections::HashMap;
use common_exception::{Result, ErrorCode};
use common_streams::SendableDataBlockStream;
use crate::sessions::{FuseQueryContextRef, FuseQueryContext};
use std::any::Any;
use common_infallible::Mutex;
use futures::{FutureExt, Future};
use futures::future::{Shared, BoxFuture};
use futures::channel::oneshot::Receiver;
use common_datavalues::columns::DataColumn;
use common_runtime::tokio::macros::support::{Pin, Poll};
use common_datavalues::DataValue;
use common_planners::Expression;

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
        Err(ErrorCode::UnImplement(""))
    }
}

pub struct SubQueriesPuller<'a> {
    ctx: FuseQueryContextRef,
    expressions: Vec<Expression>,
    sub_queries: HashMap<String, Shared<BoxFuture<'a, Arc<DataColumn>>>>,
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
                    let shared_future = Self::receive_subquery_res(builder.build()?);
                    self.sub_queries.insert(name.clone(), shared_future);
                }
                Expression::ScalarSubquery { name, query_plan } => {
                    let subquery_plan = (**query_plan).clone();
                    let builder = PipelineBuilder::create(subquery_ctx, subquery_plan);
                    let shared_future = Self::receive_subquery_res(builder.build()?);
                    self.sub_queries.insert(name.clone(), shared_future);
                }
                _ => return Result::Err(ErrorCode::LogicalError("Expression must be Subquery or ScalarSubquery"))
            };
        };

        Ok(())
    }

    fn receive_subquery_res(mut pipeline: Pipeline) -> Shared<BoxFuture<'a, Arc<DataColumn>>> {
        let subquery_future = async move {
            let stream = pipeline.execute().await;
            Arc::new(DataColumn::Constant(DataValue::UInt64(Some(1)), 1))
        };

        subquery_future.boxed().shared()
    }
}

