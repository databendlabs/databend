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

mod reservoir_sampling;

use std::collections::HashSet;

use rand::Rng;
use reservoir_sampling::AlgoL;

use crate::BlockRowIndex;
use crate::DataBlock;

pub struct Simpler<R: Rng> {
    columns: Vec<usize>,
    k: usize,
    block_size: usize,

    blocks: Vec<DataBlock>,
    indices: Vec<BlockRowIndex>,
    core: AlgoL<R>,

    s: usize,
}

impl<R: Rng> Simpler<R> {
    pub fn new(columns: Vec<usize>, block_size: usize, k: usize, rng: R) -> Self {
        let core = AlgoL::new(k.try_into().unwrap(), rng);
        Self {
            columns,
            blocks: Vec::new(),
            indices: Vec::with_capacity(k),
            k,
            block_size,
            core,
            s: usize::MAX,
        }
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
            if self.blocks.len() > self.k {
                self.compact_blocks()
            }
        }
        change
    }

    fn add_indices(&mut self, rows: usize, block_idx: u32) -> bool {
        let mut change = false;
        let mut cur: usize = 0;
        if self.indices.len() < self.k {
            if rows + self.indices.len() <= self.k {
                for i in 0..rows {
                    self.indices.push((block_idx, i as u32, 1));
                }
                if self.indices.len() == self.k {
                    self.s = self.core.search()
                }
                return true;
            }
            while self.indices.len() < self.k {
                self.indices.push((block_idx, cur as u32, 1));
                cur += 1;
            }
            self.s = self.core.search();
            change = true;
        }

        while rows - cur > self.s {
            change = true;
            cur += self.s;
            self.indices[self.core.pos()] = (block_idx, cur as u32, 1);
            self.core.update_w();
            self.s = self.core.search();
        }

        self.s -= rows - cur;
        change
    }

    pub fn compact_indices(&mut self) {
        let used_set: HashSet<_> = self.indices.iter().map(|&(b, _, _)| b).collect();
        if used_set.len() == self.blocks.len() {
            return;
        }

        let mut used: Vec<_> = used_set.iter().cloned().collect();
        used.sort();

        self.indices = self
            .indices
            .drain(..)
            .map(|(b, r, c)| (used.binary_search(&b).unwrap() as u32, r, c))
            .collect();

        self.blocks = self
            .blocks
            .drain(..)
            .enumerate()
            .filter_map(|(i, block)| {
                if used_set.contains(&(i as u32)) {
                    Some(block)
                } else {
                    None
                }
            })
            .collect();
    }

    pub fn compact_blocks(&mut self) {
        self.blocks = self
            .indices
            .chunks_mut(self.block_size)
            .enumerate()
            .map(|(i, indices)| {
                let rows = indices.len();
                let block = DataBlock::take_blocks(&self.blocks, indices, rows);

                for (j, (b, r, _)) in indices.iter_mut().enumerate() {
                    *b = i as u32;
                    *r = j as u32;
                }

                block
            })
            .collect::<Vec<_>>();
    }

    pub fn memory_size(self) -> usize {
        self.blocks.iter().map(|b| b.memory_size()).sum()
    }

    pub fn take_blocks(&mut self) -> Vec<DataBlock> {
        std::mem::take(&mut self.blocks)
    }

    pub fn k(&self) -> usize {
        self.k
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    use super::*;

    #[test]
    fn test_add_indeces() {
        let rng = StdRng::seed_from_u64(0);
        let k = 5;
        let core = AlgoL::new(k.try_into().unwrap(), rng);
        let mut simpler = Simpler {
            columns: vec![0],
            k,
            block_size: 65536,
            blocks: Vec::new(),
            indices: Vec::new(),
            core,
            s: usize::MAX,
        };

        simpler.add_indices(15, 0);

        let want: Vec<BlockRowIndex> =
            vec![(0, 10, 1), (0, 1, 1), (0, 2, 1), (0, 8, 1), (0, 12, 1)];
        assert_eq!(&want, &simpler.indices);
        assert_eq!(0, simpler.s);

        simpler.add_indices(20, 1);

        let want: Vec<BlockRowIndex> = vec![(1, 0, 1), (0, 1, 1), (1, 6, 1), (0, 8, 1), (1, 9, 1)];
        assert_eq!(&want, &simpler.indices);
        assert_eq!(1, simpler.s);
    }
}
