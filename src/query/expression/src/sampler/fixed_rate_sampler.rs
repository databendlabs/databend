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

use rand::Rng;
use rate_sampling::Sampling;

use crate::BlockIndex;
use crate::DataBlock;

pub struct FixedRateSampler<R: Rng> {
    columns: Vec<usize>,
    block_size: usize,

    indices: VecDeque<Vec<BlockIndex>>,
    sparse_blocks: Vec<DataBlock>,
    pub dense_blocks: Vec<DataBlock>,

    core: Sampling<R>,
    s: usize,
}

impl<R: Rng> FixedRateSampler<R> {
    /// Constructs a new `FixedRateSampler<R>`.
    /// expectation: Average number of rows to sample one
    pub fn new(
        columns: Vec<usize>,
        block_size: usize,
        expectation: usize,
        deviation: usize,
        rng: R,
    ) -> Option<Self> {
        let mut core = Sampling::new_expectation(expectation, deviation, rng)?;
        let s = core.search();
        Some(Self {
            columns,
            block_size,
            indices: VecDeque::new(),
            sparse_blocks: Vec::new(),
            dense_blocks: Vec::new(),
            core,
            s,
        })
    }

    pub fn add_block(&mut self, data: DataBlock) -> bool {
        let rows = data.num_rows();
        assert!(rows > 0);
        let block_idx = self.sparse_blocks.len() as u32;
        let change = self.add_indices(rows, block_idx);
        if change {
            let columns = self
                .columns
                .iter()
                .map(|&offset| data.get_by_offset(offset).to_owned())
                .collect::<Vec<_>>();
            self.sparse_blocks.push(DataBlock::new(columns, rows));
        }
        change
    }

    fn add_indices(&mut self, rows: usize, block_idx: u32) -> bool {
        let mut change = false;
        let mut cur: usize = 0;

        while rows - cur > self.s {
            change = true;
            cur += self.s;
            match self.indices.back_mut() {
                Some(back) if back.len() < self.block_size => back.push((block_idx, cur as u32)),
                _ => {
                    let mut v = Vec::with_capacity(self.block_size);
                    v.push((block_idx, cur as u32));
                    self.indices.push_back(v)
                }
            }
            self.s = self.core.search();
        }

        self.s -= rows - cur;
        change
    }

    pub fn compact_blocks(&mut self, is_final: bool) {
        if self.sparse_blocks.is_empty() {
            return;
        }

        while self
            .indices
            .front()
            .is_some_and(|indices| indices.len() == self.block_size)
        {
            let indices = self.indices.pop_front().unwrap();
            let block = DataBlock::take_blocks(&self.sparse_blocks, &indices, indices.len());
            self.dense_blocks.push(block)
        }

        let Some(mut indices) = self.indices.pop_front() else {
            self.sparse_blocks.clear();
            return;
        };
        debug_assert!(self.indices.is_empty());

        if is_final {
            let block = DataBlock::take_blocks(&self.sparse_blocks, &indices, indices.len());
            self.sparse_blocks.clear();
            self.dense_blocks.push(block);
            return;
        }

        if self.sparse_blocks.len() == 1 {
            self.indices.push_back(indices);
            return;
        }
        let block = DataBlock::take_blocks(&self.sparse_blocks, &indices, indices.len());
        self.sparse_blocks.clear();
        for (i, index) in indices.iter_mut().enumerate() {
            index.0 = 0;
            index.1 = i as u32;
        }
        self.indices.push_back(indices);
        self.sparse_blocks.push(block);
    }

    pub fn memory_size(self) -> usize {
        self.sparse_blocks.iter().map(|b| b.memory_size()).sum()
    }

    pub fn num_rows(&self) -> usize {
        self.indices.len()
    }
}

mod rate_sampling {
    use std::ops::RangeInclusive;

    use rand::Rng;

    pub struct Sampling<R: Rng> {
        range: RangeInclusive<usize>,
        r: R,
    }

    impl<R: Rng> Sampling<R> {
        #[allow(dead_code)]
        pub fn new(range: RangeInclusive<usize>, r: R) -> Self {
            Self { range, r }
        }

        pub fn new_expectation(expectation: usize, deviation: usize, r: R) -> Option<Self> {
            if expectation < deviation && usize::MAX - expectation >= deviation {
                None
            } else {
                Some(Self {
                    range: expectation - deviation..=expectation + deviation,
                    r,
                })
            }
        }

        pub fn search(&mut self) -> usize {
            self.r.gen_range(self.range.clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    use super::*;
    use crate::types::Int32Type;
    use crate::utils::FromData;

    #[test]
    fn test_add_indices() {
        let rng = StdRng::seed_from_u64(0);
        let mut core = Sampling::new(3..=6, rng);
        let s = core.search();
        let mut sampler = FixedRateSampler {
            columns: vec![0],
            block_size: 65536,
            indices: VecDeque::new(),
            sparse_blocks: Vec::new(),
            dense_blocks: Vec::new(),
            core,
            s,
        };

        sampler.add_indices(15, 0);

        let want: Vec<_> = vec![(0, 6), (0, 9), (0, 14)];
        assert_eq!(Some(&want), sampler.indices.front());
        assert_eq!(3, sampler.s);

        sampler.add_indices(20, 1);

        let want = vec![(0, 6), (0, 9), (0, 14), (1, 3), (1, 9), (1, 15), (1, 18)];
        assert_eq!(Some(&want), sampler.indices.front());
        assert_eq!(1, sampler.s);
    }

    #[test]
    fn test_compact_blocks() {
        let rng = StdRng::seed_from_u64(0);

        let sparse_blocks = vec![
            DataBlock::new_from_columns(vec![Int32Type::from_data(vec![1, 2, 3, 4, 5])]),
            DataBlock::new_from_columns(vec![Int32Type::from_data(vec![6, 7, 8, 9, 10])]),
        ];

        let indices = VecDeque::from(vec![vec![(0, 1, 1), (0, 2, 1), (1, 0, 1)], vec![
            (1, 1, 1),
            (1, 2, 1),
        ]]);

        {
            let core = Sampling::new(3..=6, rng.clone());
            let mut sampler = FixedRateSampler {
                columns: vec![0],
                block_size: 3,
                indices: indices.clone(),
                sparse_blocks: sparse_blocks.clone(),
                dense_blocks: Vec::new(),
                core,
                s: 0,
            };

            sampler.compact_blocks(false);

            assert_eq!(Some(&vec![(0, 0, 1), (0, 1, 1)]), sampler.indices.front());
            assert_eq!(
                &Int32Type::from_data(vec![7, 8]),
                sampler.sparse_blocks[0].get_last_column()
            );
            assert_eq!(
                &Int32Type::from_data(vec![2, 3, 6]),
                sampler.dense_blocks[0].get_last_column()
            );

            sampler.compact_blocks(true);
            assert!(sampler.indices.is_empty());
            assert!(sampler.sparse_blocks.is_empty());
        }

        {
            let core = Sampling::new(3..=6, rng.clone());
            let mut sampler = FixedRateSampler {
                columns: vec![0],
                block_size: 3,
                indices: indices.clone(),
                sparse_blocks: sparse_blocks.clone(),
                dense_blocks: Vec::new(),
                core,
                s: 0,
            };

            sampler.compact_blocks(true);

            assert!(sampler.indices.is_empty());
            assert_eq!(
                &Int32Type::from_data(vec![2, 3, 6]),
                sampler.dense_blocks[0].get_last_column()
            );
            assert_eq!(
                &Int32Type::from_data(vec![7, 8]),
                sampler.dense_blocks[1].get_last_column()
            );
        }

        {
            let indices = VecDeque::from(vec![vec![(0, 1, 1), (0, 2, 1), (1, 0, 1)], vec![
                (1, 1, 1),
                (1, 2, 1),
                (1, 3, 1),
            ]]);

            let core = Sampling::new(3..=6, rng.clone());
            let mut sampler = FixedRateSampler {
                columns: vec![0],
                block_size: 3,
                indices: indices.clone(),
                sparse_blocks: sparse_blocks.clone(),
                dense_blocks: Vec::new(),
                core,
                s: 0,
            };

            sampler.compact_blocks(false);

            assert!(sampler.indices.is_empty());
            assert_eq!(
                &Int32Type::from_data(vec![2, 3, 6]),
                sampler.dense_blocks[0].get_last_column()
            );
            assert_eq!(
                &Int32Type::from_data(vec![7, 8, 9]),
                sampler.dense_blocks[1].get_last_column()
            );
        }
    }
}
