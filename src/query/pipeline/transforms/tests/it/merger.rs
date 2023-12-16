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

use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_base::base::tokio;
use databend_common_exception::Result;
use databend_common_expression::block_debug::pretty_format_blocks;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::FromData;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline_transforms::processors::sort::HeapMerger;
use databend_common_pipeline_transforms::processors::sort::SimpleRows;
use databend_common_pipeline_transforms::processors::sort::SortedStream;
use itertools::Itertools;
use rand::rngs::ThreadRng;
use rand::Rng;

struct TestStream {
    data: VecDeque<DataBlock>,
    rng: ThreadRng,
}

unsafe impl Send for TestStream {}

impl TestStream {
    fn new(data: VecDeque<DataBlock>) -> Self {
        Self {
            data,
            rng: rand::thread_rng(),
        }
    }
}

impl SortedStream for TestStream {
    fn next(&mut self) -> Result<(Option<(DataBlock, Column)>, bool)> {
        // To simulate the real scenario, we randomly decide whether the stream is pending or not.
        let pending = self.rng.gen_bool(0.5);
        if pending {
            Ok((None, true))
        } else {
            Ok((
                self.data.pop_front().map(|b| {
                    let col = b.get_last_column().clone();
                    (b, col)
                }),
                false,
            ))
        }
    }
}

type TestMerger = HeapMerger<SimpleRows<Int32Type>, TestStream>;

fn prepare_input_and_result(
    data: Vec<Vec<Vec<i32>>>,
    limit: Option<usize>,
) -> (Vec<Vec<DataBlock>>, DataBlock) {
    let input = data
        .clone()
        .into_iter()
        .map(|v| {
            v.into_iter()
                .map(|v| DataBlock::new_from_columns(vec![Int32Type::from_data(v)]))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let result = data
        .into_iter()
        .flatten()
        .flatten()
        .sorted()
        .take(limit.unwrap_or(usize::MAX))
        .collect::<Vec<_>>();
    let result = DataBlock::new_from_columns(vec![Int32Type::from_data(result)]);

    (input, result)
}

/// Returns (input, expected)
fn basic_test_data(limit: Option<usize>) -> (Vec<Vec<DataBlock>>, DataBlock) {
    let data = vec![
        vec![vec![1, 2, 3, 4], vec![4, 5, 6, 7]],
        vec![vec![1, 1, 1, 1], vec![1, 10, 100, 2000]],
        vec![vec![0, 2, 4, 5]],
    ];

    prepare_input_and_result(data, limit)
}

/// Returns (input, expected, batch_size, num_merge)
fn random_test_data(rng: &mut ThreadRng) -> (Vec<Vec<DataBlock>>, DataBlock, Option<usize>) {
    let random_batch_size = rng.gen_range(1..=10);
    let random_num_streams = rng.gen_range(5..=10);

    let random_data = (0..random_num_streams)
        .map(|_| {
            let random_num_blocks = rng.gen_range(1..=10);
            let mut data = (0..random_batch_size * random_num_blocks)
                .map(|_| rng.gen_range(0..=1000))
                .collect::<Vec<_>>();
            data.sort();
            data.chunks(random_batch_size)
                .map(|v| v.to_vec())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    let num_rows = random_data
        .iter()
        .map(|v| v.iter().map(|v| v.len()).sum::<usize>())
        .sum::<usize>();
    let limit = rng.gen_range(0..=num_rows);
    let (input, expected) = prepare_input_and_result(random_data, Some(limit));
    (input, expected, Some(limit))
}

fn create_test_merger(input: Vec<Vec<DataBlock>>, limit: Option<usize>) -> TestMerger {
    let schema = DataSchemaRefExt::create(vec![DataField::new(
        "a",
        DataType::Number(NumberDataType::Int32),
    )]);
    let sort_desc = Arc::new(vec![SortColumnDescription {
        offset: 0,
        asc: true,
        nulls_first: true,
        is_nullable: false,
    }]);
    let streams = input
        .into_iter()
        .map(|v| TestStream::new(v.into_iter().collect::<VecDeque<_>>()))
        .collect::<Vec<_>>();

    TestMerger::create(schema, streams, sort_desc, 4, limit)
}

fn check_result(result: Vec<DataBlock>, expected: DataBlock) {
    if expected.is_empty() {
        if !result.is_empty() && !DataBlock::concat(&result).unwrap().is_empty() {
            panic!(
                "\nexpected empty block, but got:\n {}",
                pretty_format_blocks(&result).unwrap()
            )
        }
        return;
    }

    let result_rows = result.iter().map(|v| v.num_rows()).sum::<usize>();
    let result = pretty_format_blocks(&result).unwrap();
    let expected_rows = expected.num_rows();
    let expected = pretty_format_blocks(&[expected]).unwrap();
    assert_eq!(
        expected, result,
        "\nexpected (num_rows = {}):\n{}\nactual (num_rows = {}):\n{}",
        expected_rows, expected, result_rows, result
    );
}

fn test(mut merger: TestMerger, expected: DataBlock) -> Result<()> {
    let mut result = Vec::new();

    while !merger.is_finished() {
        if let Some(block) = merger.next_block()? {
            result.push(block);
        }
    }

    check_result(result, expected);

    Ok(())
}

async fn async_test(mut merger: TestMerger, expected: DataBlock) -> Result<()> {
    let mut result = Vec::new();

    while !merger.is_finished() {
        if let Some(block) = merger.async_next_block().await? {
            result.push(block);
        }
    }
    check_result(result, expected);

    Ok(())
}

fn test_basic(limit: Option<usize>) -> Result<()> {
    let (input, expected) = basic_test_data(limit);
    let merger = create_test_merger(input, limit);
    test(merger, expected)
}

async fn async_test_basic(limit: Option<usize>) -> Result<()> {
    let (input, expected) = basic_test_data(limit);
    let merger = create_test_merger(input, limit);
    async_test(merger, expected).await
}

#[test]
fn test_basic_with_limit() -> Result<()> {
    test_basic(None)?;
    test_basic(Some(0))?;
    test_basic(Some(1))?;
    test_basic(Some(5))?;
    test_basic(Some(20))?;
    test_basic(Some(21))?;
    test_basic(Some(1000000))
}

#[tokio::test(flavor = "multi_thread")]
async fn async_test_basic_with_limit() -> Result<()> {
    async_test_basic(None).await?;
    async_test_basic(Some(0)).await?;
    async_test_basic(Some(1)).await?;
    async_test_basic(Some(5)).await?;
    async_test_basic(Some(20)).await?;
    async_test_basic(Some(21)).await?;
    async_test_basic(Some(1000000)).await
}

#[test]
fn test_fuzz() -> Result<()> {
    let mut rng = rand::thread_rng();

    for _ in 0..10 {
        let (input, expected, limit) = random_test_data(&mut rng);
        let merger = create_test_merger(input, limit);
        test(merger, expected)?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuzz_async() -> Result<()> {
    let mut rng = rand::thread_rng();

    for _ in 0..10 {
        let (input, expected, limit) = random_test_data(&mut rng);
        let merger = create_test_merger(input, limit);
        async_test(merger, expected).await?;
    }

    Ok(())
}
