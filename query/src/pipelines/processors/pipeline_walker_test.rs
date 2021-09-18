// Copyright 2020 Datafuse Labs.
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

use common_exception::Result;
use common_runtime::tokio;
use pretty_assertions::assert_eq;

use crate::pipelines::processors::*;
use crate::sql::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_pipeline_walker() -> Result<()> {
    let ctx = crate::tests::try_create_context()?;

    let plan = PlanParser::create(ctx.clone()).build_from_sql(
        "select sum(number+1)+2 as sumx from numbers_mt(80000) where (number+1)=4 limit 1",
    )?;
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
