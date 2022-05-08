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

use common_base::base::tokio;
use common_exception::Result;
use databend_query::pipelines::processors::*;
use databend_query::pipelines::transforms::SourceTransform;
use futures::TryStreamExt;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn transform_source_test() -> Result<()> {
    let ctx = crate::tests::create_query_context().await?;
    let test_source = crate::tests::NumberTestData::create(ctx.clone());

    let mut pipeline = Pipeline::create(ctx.clone());
    let mut partitions = vec![];

    let source_plan = test_source.number_read_source_plan_for_test(1)?;
    partitions.extend(source_plan.parts.clone());
    pipeline.add_source(Arc::new(SourceTransform::try_create(
        ctx.clone(),
        source_plan,
    )?))?;

    let source_plan = test_source.number_read_source_plan_for_test(1)?;
    partitions.extend(source_plan.parts.clone());
    pipeline.add_source(Arc::new(SourceTransform::try_create(
        ctx.clone(),
        source_plan,
    )?))?;

    ctx.try_set_partitions(partitions)?;
    pipeline.merge_processor()?;

    let stream = pipeline.execute().await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 1);

    let expected = vec![
        "+--------+",
        "| number |",
        "+--------+",
        "| 0      |",
        "| 0      |",
        "+--------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}
