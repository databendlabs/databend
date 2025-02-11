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

/// Mask the least significant `num_bits` of `x`.
fn mask_bits(x: u64, num_bits: usize) -> u64 {
    x & ((1u64 << num_bits) - 1)
}

/// Apply Feistel network round to the least significant `num_bits` part of `x`.
fn feistel_round(x: u64, num_bits: usize, seed: u64, round: usize) -> u64 {
    let num_bits_left_half = num_bits / 2;
    let num_bits_right_half = num_bits - num_bits_left_half;

    let left_half = mask_bits(x >> num_bits_right_half, num_bits_left_half);
    let right_half = mask_bits(x, num_bits_right_half);

    let new_left_half = right_half;

    let mut state = std::hash::DefaultHasher::new();
    state.write_u64(right_half);
    state.write_u64(seed);
    state.write_usize(round);
    let new_right_half = left_half ^ mask_bits(state.finish(), num_bits_left_half);

    (new_left_half << num_bits_left_half) ^ new_right_half
}

/// Apply Feistel network with `num_rounds` to the least significant `num_bits` part of `x`.
fn feistel_network(x: u64, num_bits: usize, seed: u64, num_rounds: usize) -> u64 {
    let mut bits = mask_bits(x, num_bits);
    for i in 0..num_rounds {
        bits = feistel_round(bits, num_bits, seed, i);
    }
    (x & !((1u64 << num_bits) - 1)) ^ bits
}

/// Pseudorandom permutation within the set of numbers with the same log2(x).
pub fn transform(x: u64, seed: u64) -> u64 {
    // Keep 0 and 1 as is.
    if x == 0 || x == 1 {
        return x;
    }

    // Pseudorandom permutation of two elements.
    if x == 2 || x == 3 {
        return x ^ (seed & 1);
    }

    let num_leading_zeros = x.leading_zeros() as usize;
    feistel_network(x, 64 - num_leading_zeros - 1, seed, 4)
}

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_2_arg::<StringType, StringType, BooleanType, _, _>(
        "eq",
        |_, d1, d2| d1.domain_eq(d2),
        vectorize_string_cmp(|cmp| cmp == Ordering::Equal),
    );
}
