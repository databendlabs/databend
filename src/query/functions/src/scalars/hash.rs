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

use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;

use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::Scalar;
use databend_common_expression::types::ALL_INTEGER_TYPES;
use databend_common_expression::types::ALL_NUMBER_CLASSES;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::BitmapType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::GenericType;
use databend_common_expression::types::NumberClass;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::VariantType;
use databend_common_expression::types::i256;
use databend_common_expression::types::number::F32;
use databend_common_expression::types::number::F64;
use databend_common_expression::types::number::NumberScalar;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::with_integer_mapped_type;
use databend_common_expression::with_number_mapped_type;
use databend_functions_scalar_decimal::HashFunction;
use databend_functions_scalar_decimal::HashFunctionWithSeed;
use databend_functions_scalar_decimal::register_decimal_hash;
use databend_functions_scalar_decimal::register_decimal_hash_with_seed;
use md5::Digest;
use md5::Md5 as Md5Hasher;
use naive_cityhash::cityhash64_with_seed;
use num_traits::AsPrimitive;
use twox_hash::XxHash32;
use twox_hash::XxHash64;

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
                struct Siphash64;
                impl HashFunction for Siphash64 {
                    type Hasher = DefaultHasher;
                    fn name() -> &'static str {
                        "siphash64"
                    }
                }
                register_decimal_hash::<Siphash64>(registry);

                struct XxHash64Func;
                impl HashFunction for XxHash64Func {
                    type Hasher = XxHash64;
                    fn name() -> &'static str {
                        "xxhash64"
                    }
                }
                register_decimal_hash::<XxHash64Func>(registry);

                struct XxHash32Func;
                impl HashFunction for XxHash32Func {
                    type Hasher = XxHash32;
                    const IS_HASH_32: bool = true;
                    fn name() -> &'static str {
                        "xxhash32"
                    }
                }

                register_decimal_hash::<XxHash32Func>(registry);
                register_decimal_hash_with_seed::<CityHasher64>(registry);
            }
            NumberClass::Decimal256 => {}
        });
    }

    // Could be used in exchange
    registry
        .scalar_builder("siphash64")
        .function()
        .typed_1_arg::<GenericType<0>, NumberType<u64>>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<
            GenericType<0>,
            NumberType<u64>,
        >(|val, output, _| {
            let mut hasher = DefaultHasher::default();
            DFHash::hash(&val.to_owned(), &mut hasher);
            output.push(hasher.finish());
        }))
        .register();

    registry
        .scalar_builder("md5")
        .function()
        .typed_1_arg::<StringType, StringType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::MayThrow)
        .vectorized(vectorize_with_builder_1_arg::<StringType, StringType>(
            |val, output, ctx| {
                output.row_buffer.resize(32, 0);
                if let Err(err) =
                    hex::encode_to_slice(Md5Hasher::digest(val), &mut output.row_buffer)
                {
                    ctx.set_error(output.len(), err.to_string());
                }
                output.commit_row();
            },
        ))
        .register();

    registry
        .scalar_builder("sha")
        .function()
        .typed_1_arg::<StringType, StringType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::MayThrow)
        .vectorized(vectorize_with_builder_1_arg::<StringType, StringType>(
            |val, output, ctx| {
                output.row_buffer.resize(40, 0);
                let mut m = ::sha1::Sha1::new();
                sha1::digest::Update::update(&mut m, val.as_bytes());

                if let Err(err) = hex::encode_to_slice(m.finalize(), &mut output.row_buffer) {
                    ctx.set_error(output.len(), err.to_string());
                }
                output.commit_row();
            },
        ))
        .register();

    registry
        .scalar_builder("blake3")
        .function()
        .typed_1_arg::<StringType, StringType>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::MayThrow)
        .vectorized(vectorize_with_builder_1_arg::<StringType, StringType>(
            |val, output, ctx| {
                output.row_buffer.resize(64, 0);
                if let Err(err) = hex::encode_to_slice(
                    blake3::hash(val.as_bytes()).as_bytes(),
                    &mut output.row_buffer,
                ) {
                    ctx.set_error(output.len(), err.to_string());
                }
                output.commit_row();
            },
        ))
        .register();

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
                output.put_and_commit(res);
            },
        ),
    );
}

fn register_simple_domain_type_hash<T: ArgType>(registry: &mut FunctionRegistry)
where for<'a> T::ScalarRef<'a>: DFHash {
    registry
        .scalar_builder("siphash64")
        .function()
        .typed_1_arg::<T, NumberType<u64>>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<T, NumberType<u64>>(
            |val, output, _| {
                let mut hasher = DefaultHasher::default();
                DFHash::hash(&val, &mut hasher);
                output.push(hasher.finish());
            },
        ))
        .register();

    registry
        .scalar_builder("xxhash64")
        .function()
        .typed_1_arg::<T, NumberType<u64>>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<T, NumberType<u64>>(
            |val, output, _| {
                let mut hasher = XxHash64::default();
                DFHash::hash(&val, &mut hasher);
                output.push(hasher.finish());
            },
        ))
        .register();

    registry
        .scalar_builder("xxhash32")
        .function()
        .typed_1_arg::<T, NumberType<u32>>()
        .passthrough_nullable()
        .calc_domain(|_, _| FunctionDomain::Full)
        .vectorized(vectorize_with_builder_1_arg::<T, NumberType<u32>>(
            |val, output, _| {
                let mut hasher = XxHash32::default();
                DFHash::hash(&val, &mut hasher);
                output.push(hasher.finish().try_into().unwrap());
            },
        ))
        .register();

    #[allow(clippy::unnecessary_cast)]
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

pub struct CityHasher64 {
    seed: u64,
    value: u64,
}

impl HashFunctionWithSeed for CityHasher64 {
    fn name() -> &'static str {
        "city64withseed"
    }

    fn with_seed(seed: u64) -> Self {
        CityHasher64::with_seed(seed)
    }
}

impl CityHasher64 {
    pub fn with_seed(s: u64) -> Self {
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
        Hash::hash(self.0.0.as_slice(), state);
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

impl DFHash for &[u8] {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash_slice(self, state);
    }
}

impl DFHash for &str {
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
