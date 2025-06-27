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

use databend_common_expression::DataBlock;
use rand::prelude::SliceRandom;
use rand::prelude::SmallRng;
use rand::SeedableRng;

pub struct RangeBoundSampler {
    offset: usize,
    sample_size: usize,
    rng: SmallRng,

    values: Vec<(u64, Vec<Vec<u8>>)>,
}

impl RangeBoundSampler {
    pub fn new(offset: usize, sample_size: usize, seed: u64) -> Self {
        let rng = SmallRng::seed_from_u64(seed);
        Self {
            offset,
            sample_size,
            rng,
            values: vec![],
        }
    }
}

impl RangeBoundSampler {
    pub fn add_block(&mut self, data: &DataBlock) {
        let rows = data.num_rows();
        assert!(rows > 0);
        let column = data.get_by_offset(self.offset).to_column();

        let sample_size = std::cmp::min(self.sample_size, rows);
        let mut indices = (0..rows).collect::<Vec<_>>();
        indices.shuffle(&mut self.rng);
        let sampled_indices = &indices[..sample_size];

        let column = column.as_binary().unwrap();
        let sample_values = sampled_indices
            .iter()
            .map(|i| unsafe { column.index_unchecked(*i) }.to_vec())
            .collect::<Vec<_>>();
        self.values.push((rows as u64, sample_values));
    }

    pub fn sample_values(&mut self) -> Vec<(u64, Vec<Vec<u8>>)> {
        std::mem::take(&mut self.values)
    }
}
