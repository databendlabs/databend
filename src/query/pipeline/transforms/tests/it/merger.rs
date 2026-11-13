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

use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::block_debug::pretty_format_blocks;
use databend_common_expression::types::Int32Type;
use databend_common_pipeline_transforms::sorts::core::Merger;
use databend_common_pipeline_transforms::sorts::core::SimpleRowsAsc;
use databend_common_pipeline_transforms::sorts::core::SortedStream;
use databend_common_pipeline_transforms::sorts::core::algorithm::HeapSort;
use databend_common_pipeline_transforms::sorts::core::algorithm::LoserTreeSort;
use databend_common_pipeline_transforms::sorts::core::algorithm::SortAlgorithm;
use itertools::Itertools;
use rand::Rng;
use rand::rngs::ThreadRng;
use rand::seq::SliceRandom;

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

#[test]
fn test_flush_selected_prefix_with_pending_refills() -> Result<()> {
    let (input, expected) = prepare_input_and_result(
        vec![
            (0..100).map(|i| vec![i * 2]).collect(),
            (0..100).map(|i| vec![i * 2 + 1]).collect(),
        ],
        None,
    );
    let streams = input
        .into_iter()
        .map(|blocks| TestStream::new(blocks.into()))
        .collect();
    let mut merger =
        Merger::<TestLoserTreeSort, _>::new(streams, 1024, None).with_flush_before_refill();
    let mut output = Vec::new();
    while !merger.is_finished() {
        if let Some(block) = merger.next_block()? {
            assert!(block.num_rows() < 1024);
            output.push(block);
        }
    }
    let actual = DataBlock::concat(&output)?;
    assert_eq!(actual.num_rows(), expected.num_rows());
    assert_eq!(actual.get_last_column(), expected.get_last_column());
    Ok(())
}

#[test]
fn test_flushes_prefix_before_missing_head() -> Result<()> {
    // Selecting 0 exhausts route 0. Materialize it before requesting its
    // next head; never select around an unknown head.
    let (input, _) = prepare_input_and_result(
        vec![vec![vec![0], vec![2, 4, 6, 8]], vec![vec![1, 3, 5]]],
        None,
    );
    let streams = input
        .into_iter()
        .map(|blocks| TestStream::new(blocks.into()))
        .collect();
    let mut merger =
        Merger::<TestLoserTreeSort, _>::new(streams, 1024, None).with_flush_before_refill();
    let prefix = loop {
        if let Some(block) = merger.next_block()? {
            break block;
        }
    };
    assert!(!prefix.is_empty());
    // Recovery requires the selected prefix to have been materialized.
    assert!(merger.into_remaining_streams().is_ok());
    Ok(())
}

#[test]
fn test_recover_unconsumed_merge_suffixes() -> Result<()> {
    let (input, expected) = prepare_input_and_result(
        vec![vec![vec![0, 2, 4], vec![6, 8]], vec![vec![1, 3, 5], vec![
            7, 9,
        ]]],
        None,
    );
    let streams = input
        .into_iter()
        .map(|blocks| TestStream::new(blocks.into()))
        .collect();
    let mut merger = Merger::<TestLoserTreeSort, _>::new(streams, 3, None);
    let prefix = loop {
        if let Some(block) = merger.next_block()? {
            break block;
        }
    };
    let recovered = merger.into_remaining_streams()?;
    let streams = recovered
        .into_iter()
        .map(|recovered| {
            let mut stream = recovered.stream;
            if let Some((block, _)) = recovered.head {
                stream.data.push_front(block);
            }
            stream
        })
        .collect();
    let mut suffix = Merger::<TestLoserTreeSort, _>::new(streams, 3, None);
    let mut output = vec![prefix];
    while !suffix.is_finished() {
        if let Some(block) = suffix.next_block()? {
            output.push(block);
        }
    }
    let actual = DataBlock::concat(&output)?;
    assert_eq!(actual.num_rows(), expected.num_rows());
    assert_eq!(actual.get_last_column(), expected.get_last_column());
    Ok(())
}

type TestMerger<A> = Merger<A, TestStream>;
type TestHeapSort = HeapSort<SimpleRowsAsc<Int32Type>>;
type TestLoserTreeSort = LoserTreeSort<SimpleRowsAsc<Int32Type>>;

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

fn create_test_merger<A: SortAlgorithm>(
    input: Vec<Vec<DataBlock>>,
    limit: Option<usize>,
) -> TestMerger<A> {
    create_test_merger_with_batch::<A>(input, 4, limit)
}

fn create_test_merger_with_batch<A: SortAlgorithm>(
    input: Vec<Vec<DataBlock>>,
    batch_rows: usize,
    limit: Option<usize>,
) -> TestMerger<A> {
    let streams = input
        .into_iter()
        .map(|v| TestStream::new(v.into_iter().collect::<VecDeque<_>>()))
        .collect::<Vec<_>>();

    TestMerger::<A>::new(streams, batch_rows, limit)
}

/// Returns (input, expected, limit).
///
/// Produces inputs with *long* runs of rows that are `<=` the next cursor, so the
/// merger's batch detection (linear → last-row check → galloping → binary search) is
/// exercised well past the 16-row linear budget. Blocks are large and the value
/// domain is narrow (`0..=key_range`). Streams come in three shapes:
///   - dense:    full-range values (interleaves with everyone)
///   - narrow:   values restricted to a small sub-range (long runs vs. others)
///   - disjoint: values above everyone else's range (whole-block drains)
fn long_run_test_data(rng: &mut ThreadRng) -> (Vec<Vec<DataBlock>>, DataBlock, Option<usize>) {
    let num_streams = rng.gen_range(2..=16);
    // 64 rows exercises linear → last-row → galloping (1 probe) → binary search;
    // 300 rows reaches the third galloping probe (18 → 34 → 67 → 132 → 261).
    let block_rows = *[64usize, 300].choose(rng).unwrap();
    let key_range = rng.gen_range(1..=40i32);

    let data = (0..num_streams)
        .map(|s| {
            let num_blocks = rng.gen_range(1..=3);
            let (lo, hi) = match s % 3 {
                0 => (0, key_range),
                1 => {
                    let lo = rng.gen_range(0..=key_range);
                    (lo, (lo + rng.gen_range(0..=3)).min(key_range))
                }
                _ => (key_range + 1, key_range + 1 + rng.gen_range(0..=5)),
            };
            let mut v: Vec<i32> = (0..num_blocks * block_rows)
                .map(|_| rng.gen_range(lo..=hi))
                .collect();
            v.sort_unstable();
            v.chunks(block_rows).map(|c| c.to_vec()).collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    let total: usize = data.iter().flatten().map(|b| b.len()).sum();
    let limit = if rng.gen_bool(0.3) {
        Some(rng.gen_range(0..=total))
    } else {
        None
    };
    let (input, expected) = prepare_input_and_result(data, limit);
    (input, expected, limit)
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

fn test<A: SortAlgorithm>(mut merger: TestMerger<A>, expected: DataBlock) -> Result<()> {
    let mut result = Vec::new();

    while !merger.is_finished() {
        if let Some(block) = merger.next_block()? {
            result.push(block);
        }
    }

    check_result(result, expected);

    Ok(())
}

async fn async_test<A: SortAlgorithm>(
    mut merger: TestMerger<A>,
    expected: DataBlock,
) -> Result<()> {
    let mut result = Vec::new();

    while !merger.is_finished() {
        if let Some(block) = merger.next_block()? {
            result.push(block);
        }
    }
    check_result(result, expected);

    Ok(())
}

fn test_basic(limit: Option<usize>) -> Result<()> {
    let (input, expected) = basic_test_data(limit);
    let merger = create_test_merger::<TestHeapSort>(input, limit);
    test(merger, expected)?;

    let (input, expected) = basic_test_data(limit);
    let merger = create_test_merger::<TestLoserTreeSort>(input, limit);
    test(merger, expected)
}

async fn async_test_basic(limit: Option<usize>) -> Result<()> {
    let (input, expected) = basic_test_data(limit);
    let merger = create_test_merger::<HeapSort<SimpleRowsAsc<Int32Type>>>(input, limit);
    async_test(merger, expected).await?;

    let (input, expected) = basic_test_data(limit);
    let merger = create_test_merger::<LoserTreeSort<SimpleRowsAsc<Int32Type>>>(input, limit);
    async_test(merger, expected).await
}

#[test]
fn test_basic_with_limit() -> anyhow::Result<()> {
    test_basic(None)?;
    test_basic(Some(0))?;
    test_basic(Some(1))?;
    test_basic(Some(5))?;
    test_basic(Some(20))?;
    test_basic(Some(21))?;
    test_basic(Some(1000000))?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn async_test_basic_with_limit() -> anyhow::Result<()> {
    async_test_basic(None).await?;
    async_test_basic(Some(0)).await?;
    async_test_basic(Some(1)).await?;
    async_test_basic(Some(5)).await?;
    async_test_basic(Some(20)).await?;
    async_test_basic(Some(21)).await?;
    async_test_basic(Some(1000000)).await?;
    Ok(())
}

#[test]
fn test_fuzz() -> anyhow::Result<()> {
    let mut rng = rand::thread_rng();

    for _ in 0..10 {
        let (input, expected, limit) = random_test_data(&mut rng);
        let merger = create_test_merger::<TestHeapSort>(input, limit);
        test(merger, expected)?;
    }

    for _ in 0..10 {
        let (input, expected, limit) = random_test_data(&mut rng);
        let merger = create_test_merger::<TestLoserTreeSort>(input, limit);
        test(merger, expected)?;
    }

    Ok(())
}

#[test]
fn test_long_runs() -> anyhow::Result<()> {
    let mut rng = rand::thread_rng();

    for _ in 0..20 {
        let (input, expected, limit) = long_run_test_data(&mut rng);
        // Output batch sizes spanning "inside the linear budget" to "larger than a
        // block", so `row_index_limit` clamps at every phase of batch detection.
        for batch_rows in [7usize, 17, 100, 1024] {
            let merger =
                create_test_merger_with_batch::<TestHeapSort>(input.clone(), batch_rows, limit);
            test(merger, expected.clone())?;

            let merger = create_test_merger_with_batch::<TestLoserTreeSort>(
                input.clone(),
                batch_rows,
                limit,
            );
            test(merger, expected.clone())?;
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuzz_async() -> anyhow::Result<()> {
    let mut rng = rand::thread_rng();

    for _ in 0..10 {
        let (input, expected, limit) = random_test_data(&mut rng);
        let merger = create_test_merger::<TestHeapSort>(input, limit);
        async_test(merger, expected).await?;
    }

    for _ in 0..10 {
        let (input, expected, limit) = random_test_data(&mut rng);
        let merger = create_test_merger::<TestLoserTreeSort>(input, limit);
        async_test(merger, expected).await?;
    }

    Ok(())
}
