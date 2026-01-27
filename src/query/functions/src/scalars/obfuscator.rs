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

use databend_common_base::obfuscator::CodePoint;
use databend_common_base::obfuscator::Histogram;
use databend_common_base::obfuscator::NGramHash;
use databend_common_base::obfuscator::Table;
use databend_common_base::obfuscator::generate;
use databend_common_column::buffer::Buffer;
use databend_common_expression::Column;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::Value;
use databend_common_expression::types::map::KvPair;
use databend_common_expression::types::*;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::with_signed_integer_mapped_type;

struct ColumnHistogram {
    total: u32,
    count_end: u32,
    codes: Buffer<u32>,
    weights: Buffer<u32>,
}

impl Histogram<'_> for ColumnHistogram {
    fn sample(&self, random: u64, end_multiplier: f64) -> Option<CodePoint> {
        let range = self.total + (self.count_end as f64 * end_multiplier) as u32;
        if range == 0 {
            return None;
        }

        let mut random = random as u32 % range;
        self.codes
            .iter()
            .zip(self.weights.iter())
            .find(|(_, weighted)| {
                if random <= **weighted {
                    true
                } else {
                    random -= **weighted;
                    false
                }
            })
            .map(|(code, _)| *code)
    }

    fn is_empty(&self) -> bool {
        self.total == 0 && self.count_end == 0
    }
}

#[derive(Clone)]
struct ColumnTable {
    hash: Buffer<NGramHash>,
    total: Buffer<u32>,
    count_end: Buffer<u32>,
    buckets: ArrayColumn<KvPair<UInt32Type, UInt32Type>>,
}

impl Table<'_, ColumnHistogram> for ColumnTable {
    fn get(&self, context_hash: &NGramHash) -> Option<ColumnHistogram> {
        let row = self.hash.binary_search(context_hash).ok()?;
        let bucket = self.buckets.index(row).unwrap();
        Some(ColumnHistogram {
            total: self.total[row],
            count_end: self.count_end[row],
            codes: bucket.keys,
            weights: bucket.values,
        })
    }
}

impl ColumnTable {
    fn try_new(column: Column) -> Option<Self> {
        let Column::Tuple(mut tuple) = column else {
            return None;
        };
        if tuple.len() != 4 {
            return None;
        }
        let buckets = tuple.pop()?;
        let buckets = MapType::<UInt32Type, UInt32Type>::try_downcast_column(&buckets).ok()?;
        let Some(Column::Number(NumberColumn::UInt32(count_end))) = tuple.pop() else {
            return None;
        };
        let Some(Column::Number(NumberColumn::UInt32(total))) = tuple.pop() else {
            return None;
        };
        let Some(Column::Number(NumberColumn::UInt32(hash))) = tuple.pop() else {
            return None;
        };

        Some(ColumnTable {
            hash,
            total,
            count_end,
            buckets,
        })
    }
}

#[derive(Clone, Copy, Debug, serde::Deserialize)]
struct MarkovModelParameters {
    order: usize,
    sliding_window_size: usize,
}

pub fn register(registry: &mut FunctionRegistry) {
    register_feistel(registry);

    registry.register_passthrough_nullable_4_arg::<ArrayType<GenericType<0>>,StringType,UInt64Type,StringType,StringType,_,_>(
        "markov_generate",
         |_,_,_,_,_|FunctionDomain::MayThrow,
         move |model_arg, params_arg, seed, determinator, ctx| {
            let generics = ctx.generics.to_vec();

            let input_all_scalars =
                model_arg.is_scalar() && params_arg.is_scalar() && determinator.is_scalar();
            let process_rows = if input_all_scalars { 1 } else { ctx.num_rows };

            let mut output = StringType::create_builder(process_rows, &generics);

            let table = model_arg
                .as_scalar()
                .and_then(|model| ColumnTable::try_new(model.clone()));
            let params = params_arg
                .as_scalar()
                .and_then(|params| serde_json::from_str::<MarkovModelParameters>(params).ok());

            let mut code_points = Vec::new();
            for index in 0..process_rows {
                if let Some(validity) = &ctx.validity
                    && !validity.get_bit(index) {
                        output.commit_row();
                        continue;
                    }

                let table = match &table {
                    Some(table) => table.clone(),
                    None => {
                        let model = unsafe { model_arg.index_unchecked(index) };
                        let Some(table) = ColumnTable::try_new(model.clone()) else {
                            let expect = DataType::Array(Box::new(DataType::Tuple(vec![
                                UInt32Type::data_type(),                        // hash
                                UInt32Type::data_type(),                        // total
                                UInt32Type::data_type(),                        // count_end
                                MapType::<UInt32Type, UInt32Type>::data_type(), // buckets
                            ])));
                            let got = DataType::Array(Box::new(model.data_type()));
                            ctx.set_error(
                                output.len(),
                                format!("invalid model expect {expect:?}, but got {got:?}"),
                            );
                            output.commit_row();
                            continue;
                        };
                        table
                    }
                };

                let MarkovModelParameters {
                    order,
                    sliding_window_size,
                } = match params {
                    Some(params) => params,
                    None => {
                        let params = unsafe { params_arg.index_unchecked(index) };
                        let Ok(params) = serde_json::from_str::<MarkovModelParameters>(params) else {
                            ctx.set_error(output.len(), "invalid params");
                            output.commit_row();
                            continue;
                        };
                        if params.order == 0 {
                            ctx.set_error(output.len(), "invalid order");
                            output.commit_row();
                            continue;
                        }
                        if params.sliding_window_size == 0 {
                            ctx.set_error(output.len(), "invalid sliding_window_size");
                            output.commit_row();
                            continue;
                        }
                        params
                    }
                };

                let seed = unsafe { seed.index_unchecked(index) };
                let determinator = unsafe { determinator.index_unchecked(index) };
                let desired_size = determinator.chars().count();
                let mut writer = vec![0; determinator.len() * 2];

                match generate(
                    &table,
                    order,
                    seed,
                    &mut writer,
                    desired_size,
                    sliding_window_size,
                    determinator.as_bytes(),
                    &mut code_points,
                ) {
                    Some(n) => {
                        writer.truncate(n);
                        output.put_and_commit(std::str::from_utf8(&writer).unwrap());
                    }
                    None => {
                        ctx.set_error(output.len(), "logical error in markov model");
                        output.commit_row();
                    }
                }
            }
            if input_all_scalars {
                Value::Scalar(StringType::build_scalar(output))
            } else {
                Value::Column(StringType::build_column(output))
            }
        }
    );
}

fn mask(num_bits: u32) -> u64 {
    (1u64 << num_bits) - 1
}

/// Mask the least significant `num_bits` of `x`.
fn mask_bits(x: u64, num_bits: u32) -> u64 {
    x & mask(num_bits)
}

/// Apply Feistel network with `num_rounds` to the least significant `num_bits` part of `x`.
fn feistel_network(x: u64, num_bits: u32, seed: u64, num_rounds: usize) -> u64 {
    (0..num_rounds).fold(mask_bits(x, num_bits), |bits, round| {
        let num_bits_left_half = num_bits / 2;
        let num_bits_right_half = num_bits - num_bits_left_half;

        let left_half = mask_bits(bits >> num_bits_right_half, num_bits_left_half);
        let right_half = mask_bits(bits, num_bits_right_half);

        let mut state = std::hash::DefaultHasher::new();
        state.write_u64(right_half);
        state.write_u64(seed);
        state.write_usize(round);

        (right_half << num_bits_left_half)
            ^ (left_half ^ mask_bits(state.finish(), num_bits_left_half))
    }) ^ (x & !mask(num_bits))
}

macro_rules! impl_transform {
    ($T:ty) => {
        impl Transform for $T {
            /// Pseudorandom permutation within the set of numbers with the same log2(x).
            fn transform(self, seed: u64) -> Self {
                let x = self;
                match self {
                    // Keep 0 and 1 as is.
                    -1..=1 => x,
                    // Pseudorandom permutation of two elements.
                    2..=3 => x ^ (seed as Self & 1),
                    -3..=-2 => -(-x ^ (seed as Self & 1)),
                    4.. => {
                        let num_bits = Self::BITS - 1 - x.leading_zeros();
                        feistel_network(x as u64, num_bits, seed, 4) as Self
                    }
                    Self::MIN => x,
                    Self::MIN..=-4 => {
                        let x = -x as u64;
                        let num_bits = 64 - 1 - x.leading_zeros();
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

impl Transform for u64 {
    fn transform(self, seed: u64) -> Self {
        // Pseudorandom permutation within the set of numbers with the same log2(x).
        let x = self;
        match x {
            // Keep 0 and 1 as is.
            0 | 1 => x,
            // Pseudorandom permutation of two elements.
            2 | 3 => x ^ (seed & 1),
            _ => {
                let num_bits = 64 - 1 - x.leading_zeros();
                feistel_network(x, num_bits, seed, 4)
            }
        }
    }
}

pub fn register_feistel(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_2_arg::<UInt64Type, UInt64Type, UInt64Type, _, _>(
        "feistel_obfuscate",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<UInt64Type, UInt64Type, UInt64Type>(|x, seed, output, _| {
            output.push(x.transform(seed));
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
                const MANTISSA_NUM_BITS: u32 = 23;
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
                const MANTISSA_NUM_BITS: u32 = 52;
                let v = f64::from_bits(feistel_network(x.0.to_bits(), MANTISSA_NUM_BITS, seed, 4))
                    .into();
                output.push(v);
            },
        ),
    );
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestCaseResult;
    use proptest::test_runner::TestRunner;

    use super::Transform;

    #[test]
    fn test_transform() -> TestCaseResult {
        let mut runner = TestRunner::default();

        {
            let input = (prop::num::u64::ANY, prop::num::u64::ANY);
            for _ in 0..1000 {
                let (x, seed) = input.new_tree(&mut runner).unwrap().current();
                let got = Transform::transform(x, seed);
                prop_assert_eq!(x > 0, got > 0, "u64 sign not match x:{} seed:{}", x, seed);
                prop_assert_eq!(
                    x.leading_zeros(),
                    got.leading_zeros(),
                    "u64 log2 not match x:{} seed:{}",
                    x,
                    seed
                );
            }
        }

        {
            let input = (prop::num::i64::ANY, prop::num::u64::ANY);
            for _ in 0..1000 {
                let (x, seed) = input.new_tree(&mut runner).unwrap().current();
                let got = Transform::transform(x, seed);
                prop_assert_eq!(x > 0, got > 0, "i64 sign not match x:{} seed:{}", x, seed);
                prop_assert_eq!(
                    (x as f64).abs().log2() as u32,
                    (got as f64).abs().log2() as u32,
                    "i8 log2 not match x:{} seed:{}",
                    x,
                    seed
                );
            }
        }

        {
            let input = (prop::num::i32::ANY, prop::num::u64::ANY);
            for _ in 0..1000 {
                let (x, seed) = input.new_tree(&mut runner).unwrap().current();
                let got = Transform::transform(x, seed);
                prop_assert_eq!(x > 0, got > 0, "i32 sign not match x:{} seed:{}", x, seed);
                prop_assert_eq!(
                    (x as f64).abs().log2() as u32,
                    (got as f64).abs().log2() as u32,
                    "i8 log2 not match x:{} seed:{}",
                    x,
                    seed
                );
            }
        }

        {
            let input = (prop::num::i16::ANY, prop::num::u64::ANY);
            for _ in 0..1000 {
                let (x, seed) = input.new_tree(&mut runner).unwrap().current();
                let got = Transform::transform(x, seed);
                prop_assert_eq!(x > 0, got > 0, "i16 sign not match x:{} seed:{}", x, seed);
                prop_assert_eq!(
                    (x as f64).abs().log2() as u32,
                    (got as f64).abs().log2() as u32,
                    "i8 log2 not match x:{} seed:{}",
                    x,
                    seed
                );
            }
        }

        {
            let input = (prop::num::i8::ANY, prop::num::u64::ANY);
            for _ in 0..1000 {
                let (x, seed) = input.new_tree(&mut runner).unwrap().current();
                let got = Transform::transform(x, seed);
                prop_assert_eq!(x > 0, got > 0, "i8 sign not match x:{} seed:{}", x, seed);
                prop_assert_eq!(
                    (x as f64).abs().log2() as u32,
                    (got as f64).abs().log2() as u32,
                    "i8 log2 not match x:{} seed:{}",
                    x,
                    seed
                );
            }
        }

        Ok(())
    }
}
