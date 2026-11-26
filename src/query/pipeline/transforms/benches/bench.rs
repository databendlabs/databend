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
