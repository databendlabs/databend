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

use std::marker::PhantomData;

use databend_common_expression::types::ArgType;
use databend_common_expression::types::ValueType;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use rand::prelude::SliceRandom;
use rand::prelude::SmallRng;
use rand::SeedableRng;

pub struct RangeBoundSampler<T>
where T: ValueType
{
    offset: usize,
    sample_size: usize,
    rng: SmallRng,

    values: Vec<(u64, Vec<Scalar>)>,
    _t: PhantomData<T>,
}

impl<T> RangeBoundSampler<T>
where T: ValueType
{
    pub fn new(offset: usize, sample_size: usize, seed: u64) -> Self {
        let rng = SmallRng::seed_from_u64(seed);
        Self {
            offset,
            sample_size,
            rng,
            values: vec![],
            _t: PhantomData,
        }
    }
}

impl<T> RangeBoundSampler<T>
where
    T: ArgType,
    T::Scalar: Ord + Send,
{
    pub fn add_block(&mut self, data: &DataBlock) {
        let rows = data.num_rows();
        assert!(rows > 0);
        let column = data.get_by_offset(self.offset).to_column(rows);

        let sample_size = std::cmp::min(self.sample_size, rows);
        let mut indices = (0..rows).collect::<Vec<_>>();
        indices.shuffle(&mut self.rng);
        let sampled_indices = &indices[..sample_size];

        let column = T::try_downcast_column(&column).unwrap();
        let sample_values = sampled_indices
            .iter()
            .map(|i| {
                T::upcast_scalar(T::to_owned_scalar(unsafe {
                    T::index_column_unchecked(&column, *i)
                }))
            })
            .collect::<Vec<_>>();
        self.values.push((rows as u64, sample_values));
    }

    pub fn sample_values(&mut self) -> Vec<(u64, Vec<Scalar>)> {
        std::mem::take(&mut self.values)
    }
}
