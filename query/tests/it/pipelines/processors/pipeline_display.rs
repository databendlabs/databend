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

use common_base::tokio;
use common_exception::Result;
use databend_query::pipelines::processors::*;
use databend_query::sql::PlanParser;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_pipeline_display() -> Result<()> {
    let ctx = crate::tests::create_query_context()?;

    let query = "\
        EXPLAIN PIPELINE SELECT \
            sum(number + 1) + 2 AS sumx \
        FROM numbers_mt(80000) \
        WHERE (number + 1) = 4 limit 1\
    ";

    let plan = PlanParser::parse(ctx.clone(), query).await?;
    let pipeline_builder = PipelineBuilder::create(ctx);
    let pipeline = pipeline_builder.build(plan.input(0).as_ref())?;
    let expect = "LimitTransform × 1 processor\
    \n  ProjectionTransform × 1 processor\
    \n    ExpressionTransform × 1 processor\
    \n      AggregatorFinalTransform × 1 processor\
    \n        Merge (AggregatorPartialTransform × 8 processors) to (AggregatorFinalTransform × 1)\
    \n          AggregatorPartialTransform × 8 processors\
    \n            ExpressionTransform × 8 processors\
    \n              FilterTransform × 8 processors\
    \n                SourceTransform × 8 processors";
    let actual = format!("{:?}", pipeline);
    assert_eq!(expect, actual);
    Ok(())
}
