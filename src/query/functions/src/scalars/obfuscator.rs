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

use std::hash::Hasher;

use databend_common_expression::types::*;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::with_signed_integer_mapped_type;
use databend_common_expression::FunctionDomain;

use crate::FunctionRegistry;

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
    let bits = (0..num_rounds).fold(mask_bits(x, num_bits), |bits, i| {
        feistel_round(bits, num_bits, seed, i)
    });
    (x & !((1u64 << num_bits) - 1)) ^ bits
}

macro_rules! impl_transform {
    ($T:ty) => {
        impl Transform for $T {
            /// Pseudorandom permutation within the set of numbers with the same log2(x).
            fn transform(self, seed: u64) -> Self {
                let x = self;
                match self {
                    // Keep 0 and 1 as is.
                    0 | 1 | -1 => x,
                    // Pseudorandom permutation of two elements.
                    2 | 3 => x ^ (seed as Self & 1),
                    -2 | -3 => -(-x ^ (seed as Self & 1)),
                    0.. => {
                        let num_bits = 64 - 1 - x.leading_zeros() as usize;
                        feistel_network(x as u64, num_bits, seed, 4) as Self
                    }
                    Self::MIN => {
                        let num_bits = 64 - 1 - x.leading_zeros() as usize;
                        let v = feistel_network(Self::MAX as u64 + 1, num_bits, seed, 4);
                        -(mask_bits(v, num_bits) as Self)
                    }
                    Self::MIN..0 => {
                        let x = -x as u64;
                        let num_bits = 64 - 1 - x.leading_zeros() as usize;
                        -(feistel_network(x, num_bits, seed, 4) as Self)
                    }
                }
            }
        }
    };
}

trait Transform {
    fn transform(self, seed: u64) -> Self;
}

impl_transform!(i8);
impl_transform!(i16);
impl_transform!(i32);
impl_transform!(i64);

pub fn register_feistel(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_2_arg::<UInt64Type, UInt64Type, UInt64Type, _, _>(
        "feistel_obfuscate",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<UInt64Type, UInt64Type, UInt64Type>(|x, seed, output, _| {
            // Pseudorandom permutation within the set of numbers with the same log2(x).
            let v = match x {
                // Keep 0 and 1 as is.
                0 | 1 => x,
                // Pseudorandom permutation of two elements.
                2 | 3 => x ^ (seed & 1),
                _ => {
                    let num_bits = 64 - 1 - x.leading_zeros() as usize;
                    feistel_network(x, num_bits, seed, 4)
                }
            };
            output.push(v);
        }),
    );

    for num_type in ALL_SIGNED_INTEGER_TYPES {
        with_signed_integer_mapped_type!(|NUM_TYPE| match num_type {
            NumberDataType::NUM_TYPE => {
                registry.register_passthrough_nullable_2_arg::<NumberType<NUM_TYPE>, UInt64Type, NumberType<NUM_TYPE>, _, _>(
                    "feistel_obfuscate",
                    |_, _, _| FunctionDomain::Full,
                    vectorize_with_builder_2_arg::<NumberType<NUM_TYPE>, UInt64Type, NumberType<NUM_TYPE>>(|x, seed, output, _| {
                        output.push(x.transform(seed));
                    }),
                );
            }
            _ => unreachable!(),
        })
    }

    registry.register_passthrough_nullable_2_arg::<Float32Type, UInt64Type, Float32Type, _, _>(
        "feistel_obfuscate",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<Float32Type, UInt64Type, Float32Type>(
            |x, seed, output, _| {
                const MANTISSA_NUM_BITS: usize = 23;
                let v =
                    f32::from_bits(
                        feistel_network(x.0.to_bits() as u64, MANTISSA_NUM_BITS, seed, 4) as u32,
                    )
                    .into();
                output.push(v);
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<Float64Type, UInt64Type, Float64Type, _, _>(
        "feistel_obfuscate",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<Float64Type, UInt64Type, Float64Type>(
            |x, seed, output, _| {
                const MANTISSA_NUM_BITS: usize = 52;
                let v = f64::from_bits(feistel_network(x.0.to_bits(), MANTISSA_NUM_BITS, seed, 4))
                    .into();
                output.push(v);
            },
        ),
    );
}

#[cfg(test)]
mod tests {
    #[test]
    fn xx() {
        let x = i64::MAX as u64 + 1;
        println!("{:?}", x.to_le_bytes());
        let num_leading_zeros = x.leading_zeros() as usize;
        println!("{num_leading_zeros}");
        let x = feistel_network(x, 64 - 1 - num_leading_zeros, 0, 4);
        println!("{x} {}", i64::MAX);
        // println!(x & ((1 << 63) - 1))
    }
}
