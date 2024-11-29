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

use rand::Rng;
use rate_sampling::Sampling;

use super::fixed_size_sampler::compact_blocks;
use crate::BlockRowIndex;
use crate::DataBlock;

pub struct FixedRateSimpler<R: Rng> {
    columns: Vec<usize>,
    block_size: usize,

    blocks: Vec<DataBlock>,
    indices: Vec<BlockRowIndex>,
    core: Sampling<R>,

    s: usize,
}

impl<R: Rng> FixedRateSimpler<R> {
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
            blocks: Vec::new(),
            indices: Vec::new(),
            core,
            s,
        })
    }

    pub fn add_block(&mut self, data: DataBlock) -> bool {
        let rows = data.num_rows();
        assert!(rows > 0);
        let block_idx = self.blocks.len() as u32;
        let change = self.add_indices(rows, block_idx);
        if change {
            let columns = self
                .columns
                .iter()
                .map(|&offset| data.get_by_offset(offset).to_owned())
                .collect::<Vec<_>>();
            self.blocks.push(DataBlock::new(columns, rows));
        }
        change
    }

    fn add_indices(&mut self, rows: usize, block_idx: u32) -> bool {
        let mut change = false;
        let mut cur: usize = 0;

        while rows - cur > self.s {
            change = true;
            cur += self.s;
            self.indices.push((block_idx, cur as u32, 1));
            self.s = self.core.search();
        }

        self.s -= rows - cur;
        change
    }

    pub fn compact_blocks(&mut self) {
        compact_blocks(&mut self.indices, &mut self.blocks, self.block_size)
    }

    pub fn memory_size(self) -> usize {
        self.blocks.iter().map(|b| b.memory_size()).sum()
    }

    pub fn take_blocks(&mut self) -> Vec<DataBlock> {
        std::mem::take(&mut self.blocks)
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
        pub fn new(range: RangeInclusive<usize>, r: R) -> Self {
            Self { range, r }
        }

        pub fn new_expectation(expectation: usize, deviation: usize, r: R) -> Option<Self> {
            if expectation >= deviation && usize::MAX - expectation >= deviation {
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
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    use super::*;

    #[test]
    fn test_add_indices() {
        let rng = StdRng::seed_from_u64(0);
        let mut core = Sampling::new(3..=6, rng);
        let s = core.search();
        let mut simpler = FixedRateSimpler {
            columns: vec![0],
            block_size: 65536,
            blocks: Vec::new(),
            indices: Vec::new(),
            core,
            s,
        };

        simpler.add_indices(15, 0);

        let want: Vec<BlockRowIndex> = vec![(0, 6, 1), (0, 9, 1), (0, 14, 1)];
        assert_eq!(&want, &simpler.indices);
        assert_eq!(3, simpler.s);

        simpler.add_indices(20, 1);

        let want: Vec<BlockRowIndex> = vec![
            (0, 6, 1),
            (0, 9, 1),
            (0, 14, 1),
            (1, 3, 1),
            (1, 9, 1),
            (1, 15, 1),
            (1, 18, 1),
        ];
        assert_eq!(&want, &simpler.indices);
        assert_eq!(1, simpler.s);
    }
}
