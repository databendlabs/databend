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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

pub const SAMPLE_SIZE: usize = 5_000;

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
    stopped: &AtomicBool,
) -> Result<Option<(f32, f32)>> {
    if count < 127 || quantile >= 1.0 {
        return Ok(None);
    }

    let selected_vectors = take_random_vectors(vector_data, count, SAMPLE_SIZE, stopped)?;
    let selected_vectors_count = selected_vectors.len();

    let mut data_slice: Vec<f32> = Vec::with_capacity(selected_vectors_count * dim);
    for vector in &selected_vectors {
        if stopped.load(Ordering::Relaxed) {
            return Err(ErrorCode::Internal("check process stopped error"));
        }
        data_slice.extend_from_slice(vector.as_ref());
    }

    let data_slice_len = data_slice.len();
    if data_slice_len < 4 {
        return Ok(None);
    }

    let cut_index = std::cmp::min(
        (data_slice_len - 1) / 2,
        (selected_vectors_count as f32 * (1.0 - quantile) / 2.0) as usize,
    );
    let cut_index = std::cmp::max(cut_index, 1);
    let comparator = |a: &f32, b: &f32| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal);
    let (selected_values, _, _) =
        data_slice.select_nth_unstable_by(data_slice_len - cut_index, comparator);
    let (_, _, selected_values) = selected_values.select_nth_unstable_by(cut_index, comparator);

    if selected_values.len() < 2 {
        return Ok(None);
    }

    let selected_values = [selected_values];
    Ok(Some(find_min_max_from_iter(
        selected_values.iter().map(|v| &v[..]),
    )))
}

// Take a random subset of vectors from the input iterator.
fn take_random_vectors<'a>(
    vector_data: impl Iterator<Item = impl AsRef<[f32]> + 'a>,
    count: usize,
    sample_size: usize,
    stopped: &AtomicBool,
) -> Result<Vec<impl AsRef<[f32]> + 'a>> {
    let slice_size = std::cmp::min(count, sample_size);
    let mut selected_vectors: Vec<usize> =
        rand::seq::index::sample(&mut rand::thread_rng(), count, slice_size).into_vec();
    selected_vectors.sort_unstable();

    let mut data_slice = Vec::with_capacity(slice_size);
    let mut selected_index: usize = 0;
    for (vector_index, vector_data) in vector_data.into_iter().enumerate() {
        if stopped.load(Ordering::Relaxed) {
            return Err(ErrorCode::Internal("check process stopped error"));
        }

        if vector_index == selected_vectors[selected_index] {
            data_slice.push(vector_data);
            selected_index += 1;
            if selected_index == slice_size {
                break;
            }
        }
    }

    Ok(data_slice)
}

#[cfg(test)]
mod tests {
    use rand::Rng as _;
    use rand::SeedableRng as _;
    use rand::rngs::StdRng;

    use super::*;

    /// The interval of uniform `[0, 1)` data at `quantile` is
    /// `((1-q)/2, 1-(1-q)/2)`, whichever vectors the sample picks. The cut is
    /// `(1-q)/2` of the sampled *vectors* at each end, so one dimension makes
    /// it the quantile of the values.
    #[test]
    fn test_quantile_interval() {
        const COUNT: usize = 20_000;
        const DIM: usize = 1;

        let mut rng = StdRng::seed_from_u64(42);
        let data: Vec<Vec<f32>> = (0..COUNT)
            .map(|_| (0..DIM).map(|_| rng.r#gen::<f32>()).collect())
            .collect();

        for quantile in [0.9_f32, 0.99] {
            let (min, max) =
                find_quantile_interval(data.iter(), DIM, COUNT, quantile, &AtomicBool::new(false))
                    .unwrap()
                    .expect("enough vectors for a quantile interval");
            let acc = 0.02;
            let min_result = (1.0 - quantile) / 2.0;
            let max_result = 1.0 - min_result;
            assert!(
                (min - min_result).abs() < acc,
                "quantile {quantile}: min {min}, expected ~{min_result}"
            );
            assert!(
                (max - max_result).abs() < acc,
                "quantile {quantile}: max {max}, expected ~{max_result}"
            );
        }
    }

    /// Too few vectors, or the whole range, use min/max instead of a sample.
    #[test]
    fn test_quantile_interval_falls_back_to_the_whole_range() {
        let data: Vec<Vec<f32>> = (0..126).map(|i| vec![i as f32]).collect();
        let stopped = AtomicBool::new(false);
        assert_eq!(
            find_quantile_interval(data.iter(), 1, data.len(), 0.9, &stopped).unwrap(),
            None
        );
        let data: Vec<Vec<f32>> = (0..200).map(|i| vec![i as f32]).collect();
        assert_eq!(
            find_quantile_interval(data.iter(), 1, data.len(), 1.0, &stopped).unwrap(),
            None
        );
    }

    /// The sample reads the vectors in order, each at most once, and stops at
    /// the last sampled one.
    #[test]
    fn test_take_random_vectors() {
        const COUNT: usize = 1_000;
        let data: Vec<Vec<f32>> = (0..COUNT).map(|i| vec![i as f32]).collect();
        let stopped = AtomicBool::new(false);

        let sample = take_random_vectors(data.iter(), COUNT, 100, &stopped).unwrap();
        assert_eq!(sample.len(), 100);
        let indexes: Vec<usize> = sample.iter().map(|v| v.as_ref()[0] as usize).collect();
        assert!(
            indexes.windows(2).all(|pair| pair[0] < pair[1]),
            "{indexes:?}"
        );

        // A sample larger than the data takes every vector.
        let all = take_random_vectors(data.iter(), COUNT, 2 * COUNT, &stopped).unwrap();
        assert_eq!(all.len(), COUNT);

        stopped.store(true, Ordering::Relaxed);
        assert!(take_random_vectors(data.iter(), COUNT, 100, &stopped).is_err());
    }
}
