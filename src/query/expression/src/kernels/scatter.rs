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

use databend_common_base::vec_ext::VecExt;
use databend_common_exception::Result;

use crate::DataBlock;

impl DataBlock {
    pub fn scatter<I>(&self, indices: &[I], scatter_size: usize) -> Result<Vec<Self>>
    where I: databend_common_column::types::Index {
        if indices.is_empty() {
            let mut result = Vec::with_capacity(scatter_size);
            result.push(self.clone());
            for _ in 1..scatter_size {
                result.push(self.slice(0..0));
            }
            return Ok(result);
        }

        let scatter_indices = Self::divide_indices_by_scatter_size(indices, scatter_size);

        let mut results = Vec::with_capacity(scatter_size);
        for indices in scatter_indices.iter().take(scatter_size) {
            let block = self.take_with_optimize_size(indices.as_slice())?;
            results.push(block);
        }

        Ok(results)
    }

    pub fn divide_indices_by_scatter_size<I>(indices: &[I], scatter_size: usize) -> Vec<Vec<u32>>
    where I: databend_common_column::types::Index {
        let mut scatter_indices: Vec<Vec<u32>> = Vec::with_capacity(scatter_size);
        unsafe {
            let mut scatter_num_rows = vec![0usize; scatter_size];
            for index in indices.iter() {
                *scatter_num_rows.get_unchecked_mut(index.to_usize()) += 1;
            }
            for num_rows in scatter_num_rows.iter().take(scatter_size) {
                scatter_indices.push(Vec::with_capacity(*num_rows));
            }

            for (i, index) in indices.iter().enumerate() {
                scatter_indices[index.to_usize()].push_unchecked(i as u32);
            }
        }
        scatter_indices
    }
}
