// Copyright 2020-2022 Jorge C. Leitão
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

//! Utilities for benchmarking

use rand::distributions::Alphanumeric;
use rand::distributions::Distribution;
use rand::distributions::Standard;
use rand::rngs::StdRng;
use rand::Rng;
use rand::SeedableRng;

use crate::arrow::array::*;
use crate::arrow::offset::Offset;
use crate::arrow::types::NativeType;

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

/// Creates an random (but fixed-seeded) array of a given size and null density
pub fn create_boolean_array(size: usize, null_density: f32, true_density: f32) -> BooleanArray
where Standard: Distribution<bool> {
    let mut rng = seedable_rng();
    (0..size)
        .map(|_| {
            if rng.gen::<f32>() < null_density {
                None
            } else {
                let value = rng.gen::<f32>() < true_density;
                Some(value)
            }
        })
        .collect()
}

/// Creates an random (but fixed-seeded) [`Utf8Array`] of a given length, number of characters and null density.
pub fn create_string_array<O: Offset>(
    length: usize,
    size: usize,
    null_density: f32,
    seed: u64,
) -> Utf8Array<O> {
    let mut rng = StdRng::seed_from_u64(seed);

    (0..length)
        .map(|_| {
            if rng.gen::<f32>() < null_density {
                None
            } else {
                let value = (&mut rng)
                    .sample_iter(&Alphanumeric)
                    .take(size)
                    .map(char::from)
                    .collect::<String>();
                Some(value)
            }
        })
        .collect()
}
