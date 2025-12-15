// Copyright Qdrant
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

use feistel_permutation_rs::DefaultBuildHasher;
use feistel_permutation_rs::Permutation;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::SmallRng;

pub const QUANTILE_SAMPLE_SIZE: usize = 100_000;

pub(crate) fn find_min_max_from_iter<'a>(
    iter: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone,
) -> (f32, f32) {
    iter.fold((f32::MAX, f32::MIN), |(mut min, mut max), vector| {
        for &value in vector.as_ref() {
            if value < min {
                min = value;
            }
            if value > max {
                max = value;
            }
        }
        (min, max)
    })
}

pub(crate) fn find_quantile_interval<'a>(
    vector_data: impl Iterator<Item = impl AsRef<[f32]> + 'a> + Clone,
    dim: usize,
    count: usize,
    quantile: f32,
) -> Option<(f32, f32)> {
    if count < 127 || quantile >= 1.0 {
        return None;
    }

    let slice_size = std::cmp::min(count, QUANTILE_SAMPLE_SIZE);
    let mut rng = SmallRng::from_entropy();
    let seed: u64 = rng.gen();
    let permutor = Permutation::new(count as u64, seed, DefaultBuildHasher::new());
    let mut selected_vectors: Vec<usize> = permutor
        .iter()
        .map(|i| i as usize)
        .take(slice_size)
        .collect();

    selected_vectors.sort_unstable();

    let mut data_slice = Vec::with_capacity(slice_size * dim);
    let mut selected_index: usize = 0;
    for (vector_index, vector_data) in vector_data.into_iter().enumerate() {
        if vector_index == selected_vectors[selected_index] {
            data_slice.extend_from_slice(vector_data.as_ref());
            selected_index += 1;
            if selected_index == slice_size {
                break;
            }
        }
    }

    let data_slice_len = data_slice.len();
    if data_slice_len < 4 {
        return None;
    }

    let cut_index = std::cmp::min(
        (data_slice_len - 1) / 2,
        (slice_size as f32 * (1.0 - quantile) / 2.0) as usize,
    );
    let cut_index = std::cmp::max(cut_index, 1);
    let comparator = |a: &f32, b: &f32| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal);
    let (selected_values, _, _) =
        data_slice.select_nth_unstable_by(data_slice_len - cut_index, comparator);
    let (_, _, selected_values) = selected_values.select_nth_unstable_by(cut_index, comparator);

    if selected_values.len() < 2 {
        return None;
    }

    let selected_values = [selected_values];
    Some(find_min_max_from_iter(
        selected_values.iter().map(|v| &v[..]),
    ))
}
