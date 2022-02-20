// Copyright 2021 Datafuse Labs.
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

#[macro_use]
extern crate criterion;
use std::sync::Arc;

use common_arrow::arrow::array::*;
use common_arrow::arrow::buffer::Buffer;
use common_arrow::arrow::types::NativeType;
use common_datavalues::prelude::*;
use common_exception::Result;
use criterion::Criterion;

fn add_benchmark(c: &mut Criterion) {
    let size = 1048576;

    let array = create_primitive_array::<i32>(size, 0.2);
    let values = array.values();

    c.bench_function("from_iter", |b| {
        b.iter(|| criterion::black_box(from_iter(&values)))
    });

    c.bench_function("from_builder", |b| {
        b.iter(|| criterion::black_box(from_builder(&values)))
    });
}

fn from_iter(values: &Buffer<i32>) -> Result<Box<dyn Array>> {
    let it = (0..values.len()).map(|i| i32::abs(i as i32) as u32);

    let arr = unsafe { PrimitiveArray::from_trusted_len_values_iter_unchecked(it) };
    Ok(Box::new(arr))
}

fn from_builder(values: &Buffer<i32>) -> Result<Arc<dyn Column>> {
    let it = (0..values.len()).map(|i| i32::abs(i as i32) as u32);

    Ok(Arc::new(ColumnBuilder::<u32>::from_iterator(it)))
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);

use rand::distributions::Distribution;
use rand::distributions::Standard;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;
/// Returns fixed seedable RNG
pub fn seedable_rng() -> StdRng {
    StdRng::seed_from_u64(42)
}
/// Creates an random (but fixed-seeded) array of a given size and null density
pub fn create_primitive_array<T>(size: usize, null_density: f32) -> PrimitiveArray<T>
where
    T: NativeType,
    Standard: Distribution<T>,
{
    let mut rng = seedable_rng();

    (0..size)
        .map(|_| {
            if rng.gen::<f32>() < null_density {
                None
            } else {
                Some(rng.gen())
            }
        })
        .collect::<PrimitiveArray<T>>()
}

/// Creates a new [`PrimitiveArray`] from random values with a pre-set seed.
pub fn create_primitive_array_with_seed<T>(
    size: usize,
    null_density: f32,
    seed: u64,
) -> PrimitiveArray<T>
where
    T: NativeType,
    Standard: Distribution<T>,
{
    let mut rng = StdRng::seed_from_u64(seed);

    (0..size)
        .map(|_| {
            if rng.gen::<f32>() < null_density {
                None
            } else {
                Some(rng.gen())
            }
        })
        .collect::<PrimitiveArray<T>>()
}
