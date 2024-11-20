// Copyright 2021 Datafuse Labs
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

use databend_common_expression::types::ArgType;
use databend_query::pipelines::processors::transforms::sort::add_range_shuffle_exchange;

use super::*;

fn create_pipeline(
    ctx: Arc<QueryContext>,
    data: Vec<Vec<DataBlock>>,
    k: usize,
) -> Result<(Arc<QueryPipelineExecutor>, Receiver<Result<DataBlock>>)> {
    let mut pipeline = Pipeline::create();

    let source_pipe = create_source_pipe(ctx, data)?;
    pipeline.add_pipe(source_pipe);

    let schema = DataSchemaRefExt::create(vec![DataField::new("a", Int32Type::data_type())]);
    let sort_desc = Arc::new(vec![SortColumnDescription {
        offset: 0,
        asc: true,
        nulls_first: true,
    }]);
    add_range_shuffle_exchange(&mut pipeline, schema, sort_desc, k)?;

    pipeline.resize(1, false)?;

    let (mut rx, sink_pipe) = create_sink_pipe(1)?;
    let rx = rx.pop().unwrap();
    pipeline.add_pipe(sink_pipe);
    pipeline.set_max_threads(3);

    let settings = ExecutorSettings {
        query_id: Arc::new("".to_string()),
        max_execute_time_in_seconds: Default::default(),
        enable_queries_executor: true,
        max_threads: 8,
        executor_node_id: "".to_string(),
    };
    let executor = QueryPipelineExecutor::create(pipeline, settings)?;
    Ok((executor, rx))
}

/// Returns (input, expected)
fn basic_test_data(limit: Option<usize>) -> (Vec<Vec<DataBlock>>, DataBlock) {
    let data = vec![
        vec![vec![1, 2, 3, 4], vec![4, 5, 6, 7]],
        vec![vec![1, 1, 1, 1], vec![1, 10, 100, 2000]],
        vec![vec![0, 2, 4, 5]],
    ];

    prepare_multi_input_and_result(data, limit)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_simple() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let k = 3;
    let (data, _expected) = basic_test_data(None);
    let (executor, mut rx) = create_pipeline(ctx, data, k)?;

    executor.execute()?;

    let mut got: Vec<DataBlock> = Vec::new();
    while !rx.is_empty() {
        got.push(rx.recv().await.unwrap()?);
        println!("{}", got.last().unwrap());
    }

    // check_result(got, expected);

    Ok(())
}
