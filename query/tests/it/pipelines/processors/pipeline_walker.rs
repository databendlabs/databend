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
async fn test_pipeline_walker() -> Result<()> {
    let ctx = crate::tests::create_query_context()?;

    let query = "\
        SELECT sum(number + 1) + 2 AS sumx \
        FROM numbers_mt(80000) \
        WHERE (number + 1) = 4 \
        LIMIT 1\
    ";

    let plan = PlanParser::parse(ctx.clone(), query).await?;
    let pipeline_builder = PipelineBuilder::create(ctx);
    let pipeline = pipeline_builder.build(&plan)?;

    // PreOrder.
    {
        let mut actual: Vec<String> = vec![];
        pipeline.walk_preorder(|pipe| {
            let processor = pipe.processor_by_index(0).clone();
            actual.push(processor.name().to_string() + " x " + &*format!("{}", pipe.nums()));
            Result::Ok(true)
        })?;

        let expect = vec![
            "LimitTransform x 1".to_string(),
            "ProjectionTransform x 1".to_string(),
            "ExpressionTransform x 1".to_string(),
            "AggregatorFinalTransform x 1".to_string(),
            "MergeProcessor x 1".to_string(),
            "AggregatorPartialTransform x 8".to_string(),
            "ExpressionTransform x 8".to_string(),
            "FilterTransform x 8".to_string(),
            "SourceTransform x 8".to_string(),
        ];
        assert_eq!(expect, actual);
    }

    // PostOrder.
    {
        let mut actual: Vec<String> = vec![];
        pipeline.walk_postorder(|pipe| {
            let processor = pipe.processor_by_index(0).clone();
            actual.push(processor.name().to_string() + " x " + &*format!("{}", pipe.nums()));
            Result::Ok(true)
        })?;

        let expect = vec![
            "SourceTransform x 8".to_string(),
            "FilterTransform x 8".to_string(),
            "ExpressionTransform x 8".to_string(),
            "AggregatorPartialTransform x 8".to_string(),
            "MergeProcessor x 1".to_string(),
            "AggregatorFinalTransform x 1".to_string(),
            "ExpressionTransform x 1".to_string(),
            "ProjectionTransform x 1".to_string(),
            "LimitTransform x 1".to_string(),
        ];
        assert_eq!(expect, actual);
    }
    Ok(())
}
