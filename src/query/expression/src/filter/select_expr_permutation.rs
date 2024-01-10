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

#[derive(Clone, Debug)]
pub struct FilterPermutation {
    can_reorder: bool,
    observe: bool,
    keep_better: bool,
    swap_idx: usize,
    runtime_sum: u64,
    runtime_count: u64,
    swap_possibility: Vec<u32>,
    permutation: Vec<usize>,
    random_number_border: u64,
}

impl FilterPermutation {
    pub fn new(num_filter_exprs: usize, can_reorder: bool) -> Self {
        Self {
            can_reorder,
            observe: false,
            keep_better: false,
            swap_idx: 0,
            runtime_sum: 0,
            runtime_count: 0,
            swap_possibility: vec![100; num_filter_exprs - 1],
            permutation: (0..num_filter_exprs).collect(),
            random_number_border: 100 * (num_filter_exprs - 1) as u64,
        }
    }

    pub fn get(&self, idx: usize) -> usize {
        if self.can_reorder {
            self.permutation[idx]
        } else {
            idx
        }
    }

    pub fn add_statistics(&mut self, runtime: u64) {
        if !self.can_reorder {
            return;
        }

        // Whether there is a swap to be observed.
        if self.observe {
            let last_runtime = self.runtime_sum / self.runtime_count;

            // Observe the last swap, if runtime decreased, keep swap, else reverse swap.
            if last_runtime <= runtime {
                // Reverse swap because runtime didn't decrease, we don't update runtime_sum and runtime_count
                // and we will continue to use the old runtime_sum and runtime_count in next iteration.
                self.permutation.swap(self.swap_idx, self.swap_idx + 1);
                // Decrease swap possibility, but make sure there is always a small possibility left.
                if self.swap_possibility[self.swap_idx] > 1 {
                    self.swap_possibility[self.swap_idx] /= 2;
                }
            } else {
                // Keep swap because runtime decreased, reset possibility.
                self.swap_possibility[self.swap_idx] = 100;
                // Reset runtime_sum and runtime_count.
                self.runtime_sum = runtime;
                self.runtime_count = 1;
                // This is a better swap, we keep it at least once.
                self.keep_better = true;
            }

            self.observe = false;
        } else {
            let random_number = rand::thread_rng().gen_range(0..self.random_number_border);
            // We will swap the filter expression at index swap_idx and swap_idx + 1.
            self.swap_idx = (random_number / 100) as usize;
            let possibility = random_number - 100 * self.swap_idx as u64;

            // Check if swap is going to happen.
            if !self.keep_better && self.swap_possibility[self.swap_idx] > possibility as u32 {
                // Swap.
                self.permutation.swap(self.swap_idx, self.swap_idx + 1);
                // Observe whether this swap is a better swap.
                self.observe = true;
            }

            // Don't need to keep this permutation anymore.
            self.keep_better = false;
            // Update runtime_sum and runtime_count.
            self.runtime_sum += runtime;
            self.runtime_count += 1;
        }
    }
}
