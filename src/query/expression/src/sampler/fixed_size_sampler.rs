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

use std::collections::HashSet;

use rand::Rng;
use reservoir_sampling::AlgoL;

use crate::BlockIndex;
use crate::DataBlock;

pub struct FixedSizeSampler<R: Rng> {
    columns: Vec<usize>,
    k: usize,
    block_size: usize,

    blocks: Vec<DataBlock>,
    indices: Vec<BlockIndex>,
    core: AlgoL<R>,

    s: usize,
}

impl<R: Rng> FixedSizeSampler<R> {
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
                    self.indices.push((block_idx, i as u32));
                }
                if self.indices.len() == self.k {
                    self.s = self.core.search()
                }
                return true;
            }
            while self.indices.len() < self.k {
                self.indices.push((block_idx, cur as u32));
                cur += 1;
            }
            self.s = self.core.search();
            change = true;
        }

        while rows - cur > self.s {
            change = true;
            cur += self.s;
            self.indices[self.core.pos()] = (block_idx, cur as u32);
            self.core.update_w();
            self.s = self.core.search();
        }

        self.s -= rows - cur;
        change
    }

    pub fn compact_indices(&mut self) {
        compact_indices(&mut self.indices, &mut self.blocks)
    }

    pub fn compact_blocks(&mut self) {
        self.blocks = self
            .indices
            .chunks_mut(self.block_size)
            .enumerate()
            .map(|(i, indices)| {
                let rows = indices.len();
                let block = DataBlock::take_blocks(&self.blocks, indices, rows);

                for (j, (b, r)) in indices.iter_mut().enumerate() {
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

fn compact_indices(indices: &mut Vec<BlockIndex>, blocks: &mut Vec<DataBlock>) {
    let used_set: HashSet<_> = indices.iter().map(|&(b, _)| b).collect();
    if used_set.len() == blocks.len() {
        return;
    }

    let mut used: Vec<_> = used_set.iter().cloned().collect();
    used.sort();

    *indices = indices
        .drain(..)
        .map(|(b, r)| (used.binary_search(&b).unwrap() as u32, r))
        .collect();

    *blocks = blocks
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

mod reservoir_sampling {
    use std::num::NonZeroUsize;

    use rand::Rng;

    /// An implementation of Algorithm `L` (https://en.wikipedia.org/wiki/Reservoir_sampling#An_optimal_algorithm)
    pub struct AlgoL<R: Rng> {
        k: usize,
        w: f64,

        r: R,
    }

    impl<R: Rng> AlgoL<R> {
        pub fn new(k: NonZeroUsize, r: R) -> Self {
            let mut al = Self {
                k: k.into(),
                w: 1.0,
                r,
            };
            al.update_w();
            al
        }

        pub fn search(&mut self) -> usize {
            let s = (self.rng().log2() / (1.0 - self.w).log2()).floor() + 1.0;
            if s.is_normal() {
                s as usize
            } else {
                usize::MAX
            }
        }

        pub fn pos(&mut self) -> usize {
            self.r.sample(rand::distributions::Uniform::new(0, self.k))
        }

        pub fn update_w(&mut self) {
            self.w *= (self.rng().log2() / self.k as f64).exp2(); // rng ^ (1/k)
        }

        fn rng(&mut self) -> f64 {
            self.r.sample(rand::distributions::Open01)
        }
    }

    #[cfg(test)]
    mod tests {
        use rand::SeedableRng;
        use rand::rngs::StdRng;

        use super::*;

        #[test]
        fn test_algo_l() {
            let rng = StdRng::seed_from_u64(0);
            let mut sample = vec![0_u64; 10];

            let mut al = AlgoL::new(10.try_into().unwrap(), rng);
            for (i, v) in sample.iter_mut().enumerate() {
                *v = i as u64
            }

            let mut i = 9;
            loop {
                i += al.search();
                if i < 100 {
                    sample[al.pos()] = i as u64;
                    al.update_w()
                } else {
                    break;
                }
            }

            let want: Vec<u64> = vec![69, 49, 53, 83, 4, 72, 88, 38, 45, 27];
            assert_eq!(want, sample)
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    use super::*;

    #[test]
    fn test_add_indices() {
        let rng = StdRng::seed_from_u64(0);
        let k = 5;
        let core = AlgoL::new(k.try_into().unwrap(), rng);
        let mut sampler = FixedSizeSampler {
            columns: vec![0],
            k,
            block_size: 65536,
            blocks: Vec::new(),
            indices: Vec::new(),
            core,
            s: usize::MAX,
        };

        sampler.add_indices(15, 0);

        let want: Vec<BlockIndex> = vec![(0, 10), (0, 1), (0, 2), (0, 8), (0, 12)];
        assert_eq!(&want, &sampler.indices);
        assert_eq!(0, sampler.s);

        sampler.add_indices(20, 1);

        let want: Vec<BlockIndex> = vec![(1, 0), (0, 1), (1, 6), (0, 8), (1, 9)];
        assert_eq!(&want, &sampler.indices);
        assert_eq!(1, sampler.s);
    }
}
