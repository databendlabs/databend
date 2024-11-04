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

#![allow(clippy::unnecessary_cast)]

use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;

use databend_common_expression::types::decimal::DecimalType;
use databend_common_expression::types::number::NumberScalar;
use databend_common_expression::types::number::F32;
use databend_common_expression::types::number::F64;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::BitmapType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::NumberClass;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::VariantType;
use databend_common_expression::types::ALL_INTEGER_TYPES;
use databend_common_expression::types::ALL_NUMBER_CLASSES;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::with_integer_mapped_type;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::Scalar;
use ethnum::i256;
use md5::Digest;
use md5::Md5 as Md5Hasher;
use naive_cityhash::cityhash64_with_seed;
use num_traits::AsPrimitive;
use twox_hash::XxHash32;
use twox_hash::XxHash64;

use crate::scalars::string::vectorize_string_to_string;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_aliases("siphash64", &["siphash"]);
    registry.register_aliases("sha", &["sha1"]);

    register_simple_domain_type_hash::<VariantType>(registry);
    register_simple_domain_type_hash::<StringType>(registry);
    register_simple_domain_type_hash::<DateType>(registry);
    register_simple_domain_type_hash::<TimestampType>(registry);
    register_simple_domain_type_hash::<BooleanType>(registry);
    register_simple_domain_type_hash::<BitmapType>(registry);

    for ty in ALL_NUMBER_CLASSES {
        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberClass::NUM_TYPE => {
                register_simple_domain_type_hash::<NumberType<NUM_TYPE>>(registry);
            }
            NumberClass::Decimal128 => {
                register_simple_domain_type_hash::<DecimalType<i128>>(registry);
            }
            NumberClass::Decimal256 => {
                register_simple_domain_type_hash::<DecimalType<i256>>(registry);
            }
        });
    }

    registry.register_passthrough_nullable_1_arg::<StringType, StringType, _, _>(
        "md5",
        |_, _| FunctionDomain::MayThrow,
        vectorize_string_to_string(
            |col| col.data().len() * 32,
            |val, output, ctx| {
                // TODO md5 lib doesn't allow encode into buffer...
                let old_len = output.data.len();
                output.data.resize(old_len + 32, 0);
                if let Err(err) = hex::encode_to_slice(
                    Md5Hasher::digest(val).as_slice(),
                    &mut output.data[old_len..],
                ) {
                    ctx.set_error(output.len(), err.to_string());
                }
                output.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, StringType, _, _>(
        "sha",
        |_, _| FunctionDomain::MayThrow,
        vectorize_string_to_string(
            |col| col.data().len() * 40,
            |val, output, ctx| {
                let old_len = output.data.len();
                output.data.resize(old_len + 40, 0);
                // TODO sha1 lib doesn't allow encode into buffer...
                let mut m = ::sha1::Sha1::new();
                sha1::digest::Update::update(&mut m, val.as_bytes());

                if let Err(err) =
                    hex::encode_to_slice(m.finalize().as_slice(), &mut output.data[old_len..])
                {
                    ctx.set_error(output.len(), err.to_string());
                }
                output.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, StringType, _, _>(
        "blake3",
        |_, _| FunctionDomain::MayThrow,
        vectorize_string_to_string(
            |col| col.data().len() * 64,
            |val, output, ctx| {
                let old_len = output.data.len();
                output.data.resize(old_len + 64, 0);
                if let Err(err) = hex::encode_to_slice(
                    blake3::hash(val.as_bytes()).as_bytes(),
                    &mut output.data[old_len..],
                ) {
                    ctx.set_error(output.len(), err.to_string());
                }
                output.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<StringType, NumberType<u64>, StringType, _, _>(
        "sha2",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<StringType, NumberType<u64>, StringType>(
            |val, l, output, ctx| {
                let l: u64 = l.as_();
                let res = match l {
                    224 => {
                        let mut h = sha2::Sha224::new();
                        sha2::digest::Update::update(&mut h, val.as_bytes());
                        format!("{:x}", h.finalize())
                    }
                    256 | 0 => {
                        let mut h = sha2::Sha256::new();
                        sha2::digest::Update::update(&mut h, val.as_bytes());
                        format!("{:x}", h.finalize())
                    }
                    384 => {
                        let mut h = sha2::Sha384::new();
                        sha2::digest::Update::update(&mut h, val.as_bytes());
                        format!("{:x}", h.finalize())
                    }
                    512 => {
                        let mut h = sha2::Sha512::new();
                        sha2::digest::Update::update(&mut h, val.as_bytes());
                        format!("{:x}", h.finalize())
                    }
                    v => {
                        ctx.set_error(
                            output.len(),
                            format!(
                                "Expected [0, 224, 256, 384, 512] as sha2 encode options, but got {}",
                                v
                            ),
                        );
                        String::new()
                    },
                };
                output.put_str(&res);
                output.commit_row();
            },
        ),
    );
}

fn register_simple_domain_type_hash<T: ArgType>(registry: &mut FunctionRegistry)
where for<'a> T::ScalarRef<'a>: DFHash {
    registry.register_passthrough_nullable_1_arg::<T, NumberType<u64>, _, _>(
        "siphash64",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<T, NumberType<u64>>(|val, output, _| {
            let mut hasher = DefaultHasher::default();
            DFHash::hash(&val, &mut hasher);
            output.push(hasher.finish());
        }),
    );

    registry.register_passthrough_nullable_1_arg::<T, NumberType<u64>, _, _>(
        "xxhash64",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<T, NumberType<u64>>(|val, output, _| {
            let mut hasher = XxHash64::default();
            DFHash::hash(&val, &mut hasher);
            output.push(hasher.finish());
        }),
    );

    registry.register_passthrough_nullable_1_arg::<T, NumberType<u32>, _, _>(
        "xxhash32",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<T, NumberType<u32>>(|val, output, _| {
            let mut hasher = XxHash32::default();
            DFHash::hash(&val, &mut hasher);
            output.push(hasher.finish().try_into().unwrap());
        }),
    );

    for num_type in ALL_INTEGER_TYPES {
        with_integer_mapped_type!(|NUM_TYPE| match num_type {
            NumberDataType::NUM_TYPE => {
                registry
                        .register_passthrough_nullable_2_arg::<T, NumberType<NUM_TYPE>, NumberType<u64>, _, _>(
                            "city64withseed",
                                    |_, _, _| FunctionDomain::Full,
                            vectorize_with_builder_2_arg::<T, NumberType<NUM_TYPE>, NumberType<u64>>(
                                |val, l, output, _| {
                                    let mut hasher = CityHasher64::with_seed(l as u64);
                                    DFHash::hash(&val, &mut hasher);
                                    output.push(hasher.finish());
                                },
                            ),
                        );
            }
            _ => unreachable!(),
        });
    }

    registry.register_passthrough_nullable_2_arg::<T, NumberType<F32>, NumberType<u64>, _, _>(
        "city64withseed",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<T, NumberType<F32>, NumberType<u64>>(|val, l, output, _| {
            let mut hasher = CityHasher64::with_seed(l.0 as u64);
            DFHash::hash(&val, &mut hasher);
            output.push(hasher.finish());
        }),
    );

    registry.register_passthrough_nullable_2_arg::<T, NumberType<F64>, NumberType<u64>, _, _>(
        "city64withseed",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<T, NumberType<F64>, NumberType<u64>>(|val, l, output, _| {
            let mut hasher = CityHasher64::with_seed(l.0 as u64);
            DFHash::hash(&val, &mut hasher);
            output.push(hasher.finish());
        }),
    );
}

struct CityHasher64 {
    seed: u64,
    value: u64,
}

impl CityHasher64 {
    fn with_seed(s: u64) -> Self {
        Self { seed: s, value: 0 }
    }
}

impl Hasher for CityHasher64 {
    fn finish(&self) -> u64 {
        self.value
    }

    fn write(&mut self, bytes: &[u8]) {
        self.value = cityhash64_with_seed(bytes, self.seed);
    }
}

pub trait DFHash {
    fn hash<H: Hasher>(&self, state: &mut H);
}

macro_rules! integer_impl {
    ([], $( { $S: ident} ),*) => {
        $(
            impl DFHash for $S {
                #[inline]
                fn hash<H: Hasher>(&self, state: &mut H) {
                    Hash::hash(self, state);
                }
            }
        )*
    }
}

#[macro_export]
macro_rules! for_all_integer_types{
    ($macro:tt $(, $x:tt)*) => {
        $macro! {
            [$($x),*],
            { i8 },
            { i16 },
            { i32 },
            { i64 },
            { u8 },
            { u16 },
            { u32 },
            { u64 },
            { i128 }
        }
    };
}

for_all_integer_types! { integer_impl }

impl DFHash for i256 {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(self.0.as_slice(), state);
    }
}

impl DFHash for F32 {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        let u = self.to_bits();
        Hash::hash(&u, state);
    }
}

impl DFHash for F64 {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        let u = self.to_bits();
        Hash::hash(&u, state);
    }
}

impl<'a> DFHash for &'a [u8] {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash_slice(self, state);
    }
}

impl<'a> DFHash for &'a str {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash_slice(self.as_bytes(), state);
    }
}

impl DFHash for [u8] {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash_slice(self, state);
    }
}

impl DFHash for str {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash_slice(self.as_bytes(), state);
    }
}

impl DFHash for bool {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash(self, state);
    }
}

impl DFHash for Scalar {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Scalar::Boolean(v) => DFHash::hash(v, state),
            Scalar::Number(t) => with_number_mapped_type!(|NUM_TYPE| match t {
                NumberScalar::NUM_TYPE(v) => {
                    DFHash::hash(v, state);
                }
            }),
            Scalar::Binary(vals) | Scalar::Variant(vals) => {
                DFHash::hash(vals.as_slice(), state);
            }
            Scalar::String(vals) => {
                DFHash::hash(vals.as_str(), state);
            }
            _ => {}
        }
    }
}
