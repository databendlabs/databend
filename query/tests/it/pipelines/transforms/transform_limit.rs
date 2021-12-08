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

use std::sync::Arc;

use common_base::tokio;
use common_exception::Result;
use common_planners::*;
use databend_query::pipelines::processors::*;
use databend_query::pipelines::transforms::*;
use futures::TryStreamExt;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_transform_limit() -> Result<()> {
    let testcases = vec![
        ((Some(2), 0), vec![
            "+--------+",
            "| number |",
            "+--------+",
            "| 7      |",
            "| 6      |",
            "+--------+",
        ]),
        ((Some(2), 2), vec![
            "+--------+",
            "| number |",
            "+--------+",
            "| 4      |",
            "| 5      |",
            "+--------+",
        ]),
        ((None, 4), vec![
            "+--------+",
            "| number |",
            "+--------+",
            "| 0      |",
            "| 1      |",
            "| 2      |",
            "| 3      |",
            "+--------+",
        ]),
    ];

    for ((limit, offset), expected) in testcases {
        let ctx = crate::tests::create_query_context()?;
        let test_source = crate::tests::NumberTestData::create(ctx.clone());

        let mut pipeline = Pipeline::create(ctx.clone());

        let a = test_source.number_source_transform_for_test(8)?;
        pipeline.add_source(Arc::new(a))?;

        pipeline.merge_processor()?;

        if let PlanNode::Limit(plan) = PlanBuilder::create(test_source.number_schema_for_test()?)
            .limit_offset(limit, offset)?
            .build()?
        {
            pipeline.add_simple_transform(|| {
                Ok(Box::new(LimitTransform::try_create(plan.n, plan.offset)?))
            })?;
        }

        let stream = pipeline.execute().await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let block = &result[0];
        assert_eq!(block.num_columns(), 1);

        common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    Ok(())
}
