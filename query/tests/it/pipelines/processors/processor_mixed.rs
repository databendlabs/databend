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
use databend_query::pipelines::processors::*;
use futures::TryStreamExt;
use pretty_assertions::assert_eq;

use crate::tests;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_processor_mixed() -> Result<()> {
    let ctx = crate::tests::create_query_context()?;
    let test_source = tests::NumberTestData::create(ctx.clone());

    let mut pipeline = Pipeline::create(ctx.clone());

    let source = test_source.number_source_transform_for_test(6)?;
    pipeline.add_source(Arc::new(source))?;
    pipeline.mixed_processor(4)?;

    let pip = pipeline.last_pipe()?;

    assert_eq!(pip.nums(), 4);
    let stream = pipeline.execute().await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 1);

    let expected = vec![
        "+--------+",
        "| number |",
        "+--------+",
        "| 0      |",
        "| 1      |",
        "| 2      |",
        "| 3      |",
        "| 4      |",
        "| 5      |",
        "+--------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_processor_mixed2() -> Result<()> {
    let ctx = crate::tests::create_query_context()?;
    let test_source = tests::NumberTestData::create(ctx.clone());

    let m = 5;
    let n = 2;
    let mut processor0 = MixedProcessor::create(ctx, n);
    for i in 0..m {
        let source = test_source.number_source_transform_for_test(i + 1)?;
        processor0.connect_to(Arc::new(source))?;
    }
    let processor1 = processor0.share()?;

    let stream0 = processor0.execute().await?;
    let blocks0 = stream0.try_collect::<Vec<_>>().await?;

    let stream1 = processor1.execute().await?;
    let blocks1 = stream1.try_collect::<Vec<_>>().await?;

    assert_eq!(blocks0.len(), 3);
    assert_eq!(blocks1.len(), 2);
    Ok(())
}
