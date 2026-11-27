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

fn main() {
    divan::main();
}

/// Merges sorted string streams with the `Merger` of the sort pipeline, as the final merge of
/// `ORDER BY s` does.
#[divan::bench_group(max_time = 1)]
mod merge_string {
    use std::collections::VecDeque;

    use databend_common_exception::Result;
    use databend_common_expression::Column;
    use databend_common_expression::DataBlock;
    use databend_common_expression::FromData;
    use databend_common_expression::types::Int64Type;
    use databend_common_expression::types::StringType;
    use databend_common_pipeline_transforms::sorts::core::Merger;
    use databend_common_pipeline_transforms::sorts::core::Rows;
    use databend_common_pipeline_transforms::sorts::core::SimpleRowsAsc;
    use databend_common_pipeline_transforms::sorts::core::SimpleRowsDesc;
    use databend_common_pipeline_transforms::sorts::core::SortedStream;
    use databend_common_pipeline_transforms::sorts::core::algorithm::HeapSort;
    use databend_common_pipeline_transforms::sorts::core::algorithm::LoserTreeSort;
    use databend_common_pipeline_transforms::sorts::core::algorithm::SortAlgorithm;
    use divan::counter::ItemsCount;
    use rand::Rng;
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    const STREAMS: usize = 8;
    const BLOCKS_PER_STREAM: usize = 4;
    const BLOCK_ROWS: usize = 16_384;
    const OUTPUT_ROWS: usize = 65_536;
    const SHAPES: [&str; 4] = ["short", "url", "distinct", "low_card"];

    #[derive(Clone)]
    struct BlockStream {
        blocks: VecDeque<DataBlock>,
    }

    impl SortedStream for BlockStream {
        fn next(&mut self) -> Result<(Option<(DataBlock, Column)>, bool)> {
            Ok((
                self.blocks.pop_front().map(|block| {
                    let keys = block.get_last_column().clone();
                    (block, keys)
                }),
                false,
            ))
        }
    }

    /// `short`: inlined strings (<= 12 bytes) sharing a 4-byte prefix.
    /// `url`: long strings sharing a long prefix, decided by the bytes after it.
    /// `distinct`: long strings whose first 4 bytes mostly differ.
    /// `low_card`: inlined strings with 50 distinct values, so rows come in runs of equal keys.
    fn value(shape: &str, rng: &mut StdRng) -> String {
        match shape {
            "short" => format!("id_{:08}", rng.gen_range(0..100_000_000)),
            "url" => format!(
                "https://www.example.com/{:012}",
                rng.gen_range(0..1_000_000_000_000u64)
            ),
            "distinct" => {
                let len = rng.gen_range(16..=32);
                (0..len).map(|_| rng.gen_range('a'..='z')).collect()
            }
            "low_card" => format!("city_{:03}", rng.gen_range(0..50)),
            _ => unreachable!(),
        }
    }

    /// Streams of blocks sorted by their last column, with an `Int64` payload column.
    fn streams(shape: &str, asc: bool) -> Vec<BlockStream> {
        let mut rng = StdRng::seed_from_u64(0x5eed);
        (0..STREAMS)
            .map(|_| {
                let mut keys = (0..BLOCKS_PER_STREAM * BLOCK_ROWS)
                    .map(|_| value(shape, &mut rng))
                    .collect::<Vec<_>>();
                keys.sort_unstable();
                if !asc {
                    keys.reverse();
                }
                let blocks = keys
                    .chunks(BLOCK_ROWS)
                    .map(|keys| {
                        DataBlock::new_from_columns(vec![
                            Int64Type::from_data((0..keys.len() as i64).collect::<Vec<_>>()),
                            StringType::from_data(keys.to_vec()),
                        ])
                    })
                    .collect();
                BlockStream { blocks }
            })
            .collect()
    }

    fn merge<A: SortAlgorithm>(streams: Vec<BlockStream>) -> usize {
        let mut merger = Merger::<A, _>::new(streams, OUTPUT_ROWS, None);
        let mut rows = 0;
        while let Some(block) = merger.next_block().unwrap() {
            rows += block.num_rows();
        }
        rows
    }

    fn bench_merge<A: SortAlgorithm>(bencher: divan::Bencher, shape: &str) {
        let streams = streams(shape, <A::Rows as Rows>::IS_ASC_COLUMN);
        bencher
            .counter(ItemsCount::new(STREAMS * BLOCKS_PER_STREAM * BLOCK_ROWS))
            .with_inputs(|| streams.clone())
            .bench_local_values(merge::<A>);
    }

    #[divan::bench(args = SHAPES)]
    fn loser_tree_asc(bencher: divan::Bencher, shape: &str) {
        bench_merge::<LoserTreeSort<SimpleRowsAsc<StringType>>>(bencher, shape);
    }

    #[divan::bench(args = SHAPES)]
    fn loser_tree_desc(bencher: divan::Bencher, shape: &str) {
        bench_merge::<LoserTreeSort<SimpleRowsDesc<StringType>>>(bencher, shape);
    }

    #[divan::bench(args = SHAPES)]
    fn heap_asc(bencher: divan::Bencher, shape: &str) {
        bench_merge::<HeapSort<SimpleRowsAsc<StringType>>>(bencher, shape);
    }
}

/// Filters a string column by a TopN boundary, as `TopNCandidates::sift_unsorted` does for every
/// block once the candidates are full.
#[divan::bench_group(max_time = 1)]
mod topn_boundary {
    use databend_common_expression::FromData;
    use databend_common_expression::Scalar;
    use databend_common_expression::types::StringType;
    use databend_common_pipeline_transforms::sorts::core::Rows;
    use databend_common_pipeline_transforms::sorts::core::SimpleRowsAsc;
    use databend_common_pipeline_transforms::sorts::core::SimpleRowsDesc;
    use divan::counter::ItemsCount;

    const ROWS: usize = 65_536;
    const SHAPES: [&str; 3] = ["short", "url", "distinct"];

    /// `short`: inlined strings (<= 12 bytes) sharing a 4-byte prefix.
    /// `url`: long strings sharing a long prefix, decided by the bytes after it.
    /// `distinct`: long strings whose first 4 bytes mostly differ.
    fn value(shape: &str, row: usize) -> String {
        let key = (row as u32).wrapping_mul(2_654_435_761) as usize % ROWS;
        match shape {
            "short" => format!("id_{key:08}"),
            "url" => format!("https://www.example.com/{key:012}"),
            "distinct" => format!("{:08x}_with_a_long_suffix", key * 65_521),
            _ => unreachable!(),
        }
    }

    fn bench_filter<R: Rows>(bencher: divan::Bencher, shape: &str) {
        let column = StringType::from_data((0..ROWS).map(|row| value(shape, row)).collect());
        let rows = R::from_column(&column).unwrap();
        // About one row in a hundred passes, as after the candidates fill up.
        let mut values = (0..ROWS).map(|row| value(shape, row)).collect::<Vec<_>>();
        values.sort();
        let bound = if R::IS_ASC_COLUMN {
            values[ROWS / 100].clone()
        } else {
            values[ROWS - ROWS / 100].clone()
        };
        let bound = Scalar::String(bound);
        bencher
            .counter(ItemsCount::new(ROWS))
            .bench_local(|| rows.rows_le_bound(&bound));
    }

    #[divan::bench(args = SHAPES)]
    fn asc(bencher: divan::Bencher, shape: &str) {
        bench_filter::<SimpleRowsAsc<StringType>>(bencher, shape);
    }

    #[divan::bench(args = SHAPES)]
    fn desc(bencher: divan::Bencher, shape: &str) {
        bench_filter::<SimpleRowsDesc<StringType>>(bencher, shape);
    }
}

/// `row_number() OVER (PARTITION BY k)` over sorted blocks, which finds every partition end by
/// comparing each row with the partition start.
#[divan::bench_group(max_time = 1)]
mod window_partition {
    use databend_common_expression::Column;
    use databend_common_expression::DataBlock;
    use databend_common_expression::FromData;
    use databend_common_expression::FunctionContext;
    use databend_common_expression::types::Int64Type;
    use databend_common_expression::types::StringType;
    use databend_common_pipeline::core::Event;
    use databend_common_pipeline::core::InputPort;
    use databend_common_pipeline::core::OutputPort;
    use databend_common_pipeline::core::Processor;
    use databend_common_pipeline::core::port::connect;
    use databend_common_pipeline_transforms::window::FrameBound;
    use databend_common_pipeline_transforms::window::TransformWindow;
    use databend_common_pipeline_transforms::window::WindowFunctionInfo;
    use divan::counter::ItemsCount;

    const BLOCKS: usize = 8;
    const BLOCK_ROWS: usize = 65_536;
    const PARTITION_ROWS: usize = 1_000;
    const SHAPES: [&str; 4] = ["short", "url", "distinct", "int64"];

    /// The key of partition `p`. `short`: inlined strings sharing a 4-byte prefix. `url`: long
    /// strings sharing a long prefix. `distinct`: long strings whose first 4 bytes differ.
    fn key(shape: &str, p: usize) -> String {
        match shape {
            "short" => format!("id_{p:08}"),
            "url" => format!("https://www.example.com/{p:012}"),
            "distinct" => format!("{:08x}_with_a_long_suffix", p * 65_521),
            _ => unreachable!(),
        }
    }

    fn blocks(shape: &str) -> Vec<DataBlock> {
        (0..BLOCKS)
            .map(|b| {
                let partitions = (0..BLOCK_ROWS).map(|r| (b * BLOCK_ROWS + r) / PARTITION_ROWS);
                let keys: Column = match shape {
                    "int64" => Int64Type::from_data(partitions.map(|p| p as i64).collect()),
                    shape => StringType::from_data(partitions.map(|p| key(shape, p)).collect()),
                };
                DataBlock::new_from_columns(vec![keys])
            })
            .collect()
    }

    fn run(blocks: Vec<DataBlock>) -> usize {
        let input = InputPort::create();
        let output = OutputPort::create();
        let mut transform = TransformWindow::try_create_rows(
            input.clone(),
            output.clone(),
            WindowFunctionInfo::RowNumber,
            vec![0],
            vec![],
            (FrameBound::Preceding(None), FrameBound::CurrentRow),
            FunctionContext::default(),
        )
        .unwrap();
        let upstream = OutputPort::create();
        let downstream = InputPort::create();
        unsafe {
            connect(&input, &upstream);
            connect(&downstream, &output);
        }
        downstream.set_need_data();

        let mut blocks = blocks.into_iter();
        let mut rows = 0;
        loop {
            match transform.event().unwrap() {
                Event::NeedData => match blocks.next() {
                    Some(block) => upstream.push_data(Ok(block)),
                    None => upstream.finish(),
                },
                Event::Sync => transform.process().unwrap(),
                Event::NeedConsume => {
                    rows += downstream.pull_data().unwrap().unwrap().num_rows();
                    downstream.set_need_data();
                }
                Event::Finished => return rows,
                Event::Async => unreachable!(),
            }
        }
    }

    #[divan::bench(args = SHAPES)]
    fn row_number(bencher: divan::Bencher, shape: &str) {
        let blocks = blocks(shape);
        bencher
            .counter(ItemsCount::new(BLOCKS * BLOCK_ROWS))
            .with_inputs(|| blocks.clone())
            .bench_local_values(run);
    }
}
