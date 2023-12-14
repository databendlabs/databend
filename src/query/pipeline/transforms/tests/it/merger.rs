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

use common_exception::Result;
use common_expression::block_debug::pretty_format_blocks;
use common_expression::types::DataType;
use common_expression::types::Int32Type;
use common_expression::types::NumberDataType;
use common_expression::DataBlock;
use common_expression::DataField;
use common_expression::DataSchemaRefExt;
use common_expression::FromData;
use common_expression::SortColumnDescription;
use common_pipeline_transforms::processors::sort::HeapMerger;
use common_pipeline_transforms::processors::sort::SimpleRows;
use common_pipeline_transforms::processors::sort::SortedStream;
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
    fn next(&mut self) -> Result<(Option<DataBlock>, bool)> {
        // To simulate the real scenario, we randomly decide whether the stream is pending or not.
        let pending = self.rng.gen_bool(0.5);
        if pending {
            Ok((None, true))
        } else {
            Ok((self.data.pop_front(), false))
        }
    }
}

type TestMerger = HeapMerger<SimpleRows<Int32Type>, TestStream>;

fn prepare_input_and_result(data: Vec<Vec<Vec<i32>>>) -> (Vec<Vec<DataBlock>>, DataBlock) {
    let input = data
        .clone()
        .into_iter()
        .map(|v| {
            v.into_iter()
                .map(|v| DataBlock::new_from_columns(vec![Int32Type::from_data(v)]))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut result = data.into_iter().flatten().flatten().collect::<Vec<_>>();
    result.sort();
    let result = DataBlock::new_from_columns(vec![Int32Type::from_data(result)]);

    (input, result)
}

/// Returns (input, expected)
fn basic_test_data() -> (Vec<Vec<DataBlock>>, DataBlock) {
    let data = vec![
        vec![vec![1, 2, 3, 4], vec![4, 5, 6, 7]],
        vec![vec![1, 1, 1, 1], vec![1, 10, 100, 2000]],
        vec![vec![0, 2, 4, 5]],
    ];

    prepare_input_and_result(data)
}

/// Returns (input, expected, batch_size, num_merge)
fn random_test_data(rng: &mut ThreadRng) -> (Vec<Vec<DataBlock>>, DataBlock) {
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

    prepare_input_and_result(random_data)
}

fn create_test_merger(input: Vec<Vec<DataBlock>>) -> TestMerger {
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

    TestMerger::create(schema, streams, sort_desc, 4, true)
}

fn test(mut merger: TestMerger, expected: DataBlock) -> Result<()> {
    let mut result = Vec::new();

    while let (Some(block), pending) = merger.next_block()? {
        if pending {
            continue;
        }
        result.push(block);
    }

    let result_rows = result.iter().map(|v| v.num_rows()).sum::<usize>();
    let result = pretty_format_blocks(&result).unwrap();
    let expected_rows = expected.num_rows();
    let expected = pretty_format_blocks(&[expected]).unwrap();
    assert_eq!(
        expected, result,
        "expected (num_rows = {}):\n{}\nactual (num_rows = {}):\n{}",
        expected_rows, expected, result_rows, result
    );

    Ok(())
}

#[test]
fn test_basic() -> Result<()> {
    let (input, expected) = basic_test_data();
    let merger = create_test_merger(input);
    test(merger, expected)
}

#[test]
fn test_fuzz() -> Result<()> {
    let mut rng = rand::thread_rng();

    for _ in 0..10 {
        let (input, expected) = random_test_data(&mut rng);
        let merger = create_test_merger(input);
        test(merger, expected)?;
    }

    Ok(())
}
