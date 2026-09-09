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

use std::num::NonZeroUsize;

use rand::Rng;
use reservoir_sampling::AlgoL;

/// A fixed-capacity owning reservoir sampler using Vitter's Algorithm L.
///
/// Like Spark's `reservoirSampleAndCount`, the reservoir owns at most `k` values and also tracks
/// the input cardinality. Algorithm L replaces Spark's per-row Algorithm R decision with an exact
/// skip calculation, avoiding work for rows that cannot enter the reservoir. Input block boundaries
/// do not affect the resulting sample.
pub struct FixedSizeSampler<T, R: Rng> {
    samples: Vec<T>,
    k: usize,
    rows_seen: usize,
    // Zero-based global stream index selected next by Algorithm L.
    next_sample: Option<usize>,
    core: AlgoL<R>,
}

impl<T, R: Rng> FixedSizeSampler<T, R> {
    pub fn new(k: usize, rng: R) -> Self {
        let k = NonZeroUsize::new(k).expect("sample size must be greater than zero");
        Self {
            samples: Vec::with_capacity(k.get()),
            k: k.get(),
            rows_seen: 0,
            next_sample: None,
            core: AlgoL::new(k, rng),
        }
    }

    /// Consider one logical block while preserving the same result as one continuous row stream.
    ///
    /// `value_at` is evaluated only for rows entering the reservoir: every row during the initial
    /// fill, then only the rows selected by Algorithm L.
    pub fn add_block<F>(&mut self, rows: usize, mut value_at: F)
    where F: FnMut(usize) -> T {
        let start = self.rows_seen;
        let end = start.checked_add(rows).expect("sample row count overflow");
        let mut row = 0;

        if self.samples.len() < self.k {
            let take = (self.k - self.samples.len()).min(rows);
            self.samples.extend((0..take).map(&mut value_at));
            row = take;

            if self.samples.len() == self.k {
                self.next_sample = (self.k - 1).checked_add(self.core.search());
            }
        }

        while let Some(sample_index) = self.next_sample {
            if sample_index >= end {
                break;
            }
            debug_assert!(sample_index >= start + row);
            row = sample_index - start;
            let slot = self.core.pos();
            self.samples[slot] = value_at(row);

            self.core.update_w();
            self.next_sample = sample_index.checked_add(self.core.search());
        }

        self.rows_seen = end;
    }

    pub fn rows_seen(&self) -> usize {
        self.rows_seen
    }

    pub fn into_samples(self) -> Vec<T> {
        self.samples
    }
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
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use rand::SeedableRng;
    use rand::rngs::StdRng;

    use super::*;

    fn sample_in_blocks(block_sizes: &[usize]) -> FixedSizeSampler<usize, StdRng> {
        let mut sampler = FixedSizeSampler::new(10, StdRng::seed_from_u64(0));
        let mut offset = 0;
        for &rows in block_sizes {
            sampler.add_block(rows, |row| offset + row);
            offset += rows;
        }
        sampler
    }

    #[test]
    fn test_algorithm_l_known_sample() {
        let sampler = sample_in_blocks(&[100]);
        assert_eq!(sampler.samples, [69, 49, 53, 83, 4, 72, 88, 38, 45, 27]);
        assert_eq!(sampler.rows_seen(), 100);
    }

    #[test]
    fn test_algorithm_l_block_adapter_matches_stream_reference() {
        const ROWS: usize = 4096;
        const CHUNK_PATTERN: [usize; 8] = [0, 1, 2, 17, 0, 31, 127, 509];

        for seed in 0..32 {
            for k in [1, 2, 5, 64] {
                let mut expected = (0..k).collect::<Vec<_>>();
                let mut core = AlgoL::new(k.try_into().unwrap(), StdRng::seed_from_u64(seed));
                let mut sample_index = k - 1;
                loop {
                    let Some(next) = sample_index.checked_add(core.search()) else {
                        break;
                    };
                    sample_index = next;
                    if sample_index >= ROWS {
                        break;
                    }
                    expected[core.pos()] = sample_index;
                    core.update_w();
                }

                let mut partitioned = FixedSizeSampler::new(k, StdRng::seed_from_u64(seed));
                let mut offset = 0;
                for chunk in CHUNK_PATTERN.into_iter().cycle() {
                    let rows = chunk.min(ROWS - offset);
                    partitioned.add_block(rows, |row| offset + row);
                    offset += rows;
                    if offset == ROWS {
                        break;
                    }
                }

                assert_eq!(partitioned.rows_seen(), ROWS, "seed={seed}, k={k}");
                assert_eq!(partitioned.samples, expected, "seed={seed}, k={k}");
            }
        }
    }

    #[test]
    fn test_reservoir_capacity_is_strict() {
        let mut sampler = FixedSizeSampler::new(5, StdRng::seed_from_u64(11));
        sampler.add_block(10_000, |row| row);
        assert_eq!(sampler.samples.len(), 5);
        assert!(sampler.samples.iter().all(|value| *value < 10_000));
    }

    #[test]
    fn test_input_smaller_than_reservoir_is_preserved() {
        let mut sampler = FixedSizeSampler::new(5, StdRng::seed_from_u64(11));
        sampler.add_block(3, |row| row);
        assert_eq!(sampler.samples, [0, 1, 2]);
        assert_eq!(sampler.rows_seen(), 3);
    }

    #[test]
    fn test_skipped_rows_are_not_materialized() {
        let materialized = Cell::new(0);
        let mut sampler = FixedSizeSampler::new(5, StdRng::seed_from_u64(19));
        sampler.add_block(10_000, |row| {
            materialized.set(materialized.get() + 1);
            row
        });
        assert_eq!(sampler.samples.len(), 5);
        assert!(materialized.get() < 100);
    }
}
