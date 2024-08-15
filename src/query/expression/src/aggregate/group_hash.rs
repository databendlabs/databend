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

use ethnum::i256;
use ordered_float::OrderedFloat;

use crate::types::decimal::DecimalType;
use crate::types::geometry::GeometryType;
use crate::types::AnyType;
use crate::types::ArgType;
use crate::types::BinaryType;
use crate::types::BitmapType;
use crate::types::BooleanType;
use crate::types::DataType;
use crate::types::DateType;
use crate::types::DecimalDataType;
use crate::types::NumberDataType;
use crate::types::NumberType;
use crate::types::StringType;
use crate::types::TimestampType;
use crate::types::ValueType;
use crate::types::VariantType;
use crate::with_number_mapped_type;
use crate::Column;
use crate::InputColumns;
use crate::ScalarRef;

const NULL_HASH_VAL: u64 = 0xd1cefa08eb382d69;

pub fn group_hash_columns(cols: InputColumns, values: &mut [u64]) {
    debug_assert!(!cols.is_empty());
    let mut iter = cols.iter();
    combine_group_hash_column::<true>(iter.next().unwrap(), values);
    for col in iter {
        combine_group_hash_column::<false>(col, values);
    }
}

pub fn group_hash_columns_slice(cols: &[Column], values: &mut [u64]) {
    debug_assert!(!cols.is_empty());
    let mut iter = cols.iter();
    combine_group_hash_column::<true>(iter.next().unwrap(), values);
    for col in iter {
        combine_group_hash_column::<false>(col, values);
    }
}

pub fn combine_group_hash_column<const IS_FIRST: bool>(c: &Column, values: &mut [u64]) {
    match c.data_type() {
        DataType::Null => {}
        DataType::EmptyArray => {}
        DataType::EmptyMap => {}
        DataType::Number(v) => with_number_mapped_type!(|NUM_TYPE| match v {
            NumberDataType::NUM_TYPE => {
                combine_group_hash_type_column::<IS_FIRST, NumberType<NUM_TYPE>>(c, values)
            }
        }),
        DataType::Decimal(v) => match v {
            DecimalDataType::Decimal128(_) => {
                combine_group_hash_type_column::<IS_FIRST, DecimalType<i128>>(c, values)
            }
            DecimalDataType::Decimal256(_) => {
                combine_group_hash_type_column::<IS_FIRST, DecimalType<i256>>(c, values)
            }
        },
        DataType::Boolean => combine_group_hash_type_column::<IS_FIRST, BooleanType>(c, values),
        DataType::Timestamp => combine_group_hash_type_column::<IS_FIRST, TimestampType>(c, values),
        DataType::Date => combine_group_hash_type_column::<IS_FIRST, DateType>(c, values),
        DataType::Binary => combine_group_hash_string_column::<IS_FIRST, BinaryType>(c, values),
        DataType::String => combine_group_hash_string_column::<IS_FIRST, StringType>(c, values),
        DataType::Bitmap => combine_group_hash_string_column::<IS_FIRST, BitmapType>(c, values),
        DataType::Variant => combine_group_hash_string_column::<IS_FIRST, VariantType>(c, values),
        DataType::Geometry => combine_group_hash_string_column::<IS_FIRST, GeometryType>(c, values),
        DataType::Nullable(_) => {
            let col = c.as_nullable().unwrap();
            if IS_FIRST {
                combine_group_hash_column::<IS_FIRST>(&col.column, values);
                for (val, ok) in values.iter_mut().zip(col.validity.iter()) {
                    if !ok {
                        *val = NULL_HASH_VAL;
                    }
                }
            } else {
                let mut values2 = vec![0; c.len()];
                combine_group_hash_column::<true>(&col.column, &mut values2);

                for ((x, val), ok) in values2
                    .iter()
                    .zip(values.iter_mut())
                    .zip(col.validity.iter())
                {
                    if ok {
                        *val = (*val).wrapping_mul(NULL_HASH_VAL) ^ *x;
                    } else {
                        *val = (*val).wrapping_mul(NULL_HASH_VAL) ^ NULL_HASH_VAL;
                    }
                }
            }
        }
        DataType::Generic(_) => unreachable!(),
        _ => combine_group_hash_type_column::<IS_FIRST, AnyType>(c, values),
    }
}

fn combine_group_hash_type_column<const IS_FIRST: bool, T: ValueType>(
    col: &Column,
    values: &mut [u64],
) where
    for<'a> T::ScalarRef<'a>: AggHash,
{
    let c = T::try_downcast_column(col).unwrap();
    if IS_FIRST {
        for (x, val) in T::iter_column(&c).zip(values.iter_mut()) {
            *val = x.agg_hash();
        }
    } else {
        for (x, val) in T::iter_column(&c).zip(values.iter_mut()) {
            *val = (*val).wrapping_mul(NULL_HASH_VAL) ^ x.agg_hash();
        }
    }
}

fn combine_group_hash_string_column<const IS_FIRST: bool, T: ArgType>(
    col: &Column,
    values: &mut [u64],
) where
    for<'a> T::ScalarRef<'a>: AsRef<[u8]>,
{
    let c = T::try_downcast_column(col).unwrap();
    if IS_FIRST {
        for (x, val) in T::iter_column(&c).zip(values.iter_mut()) {
            *val = x.as_ref().agg_hash();
        }
    } else {
        for (x, val) in T::iter_column(&c).zip(values.iter_mut()) {
            *val = (*val).wrapping_mul(NULL_HASH_VAL) ^ x.as_ref().agg_hash();
        }
    }
}

pub trait AggHash {
    fn agg_hash(&self) -> u64;
}

// MIT License
// Copyright (c) 2018-2021 Martin Ankerl
// https://github.com/martinus/robin-hood-hashing/blob/3.11.5/LICENSE
// Rewrite using chatgpt

impl AggHash for [u8] {
    fn agg_hash(&self) -> u64 {
        const M: u64 = 0xc6a4a7935bd1e995;
        const SEED: u64 = 0xe17a1465;
        const R: u64 = 47;

        let mut h = SEED ^ (self.len() as u64).wrapping_mul(M);
        let n_blocks = self.len() / 8;

        for i in 0..n_blocks {
            let mut k = unsafe { (&self[i * 8] as *const u8 as *const u64).read_unaligned() };

            k = k.wrapping_mul(M);
            k ^= k >> R;
            k = k.wrapping_mul(M);

            h ^= k;
            h = h.wrapping_mul(M);
        }

        let data8 = &self[n_blocks * 8..];
        for (i, &value) in data8.iter().enumerate() {
            h ^= (value as u64) << (8 * (data8.len() - i - 1));
        }

        h ^= h >> R;
        h = h.wrapping_mul(M);
        h ^= h >> R;

        h
    }
}

macro_rules! impl_agg_hash_for_primitive_types {
    ($t: ty) => {
        impl AggHash for $t {
            #[inline(always)]
            fn agg_hash(&self) -> u64 {
                let mut x = *self as u64;
                x ^= x >> 32;
                x = x.wrapping_mul(0xd6e8feb86659fd93);
                x ^= x >> 32;
                x = x.wrapping_mul(0xd6e8feb86659fd93);
                x ^= x >> 32;
                x
            }
        }
    };
}

impl_agg_hash_for_primitive_types!(u8);
impl_agg_hash_for_primitive_types!(i8);
impl_agg_hash_for_primitive_types!(u16);
impl_agg_hash_for_primitive_types!(i16);
impl_agg_hash_for_primitive_types!(u32);
impl_agg_hash_for_primitive_types!(i32);
impl_agg_hash_for_primitive_types!(u64);
impl_agg_hash_for_primitive_types!(i64);

impl AggHash for bool {
    fn agg_hash(&self) -> u64 {
        *self as u64
    }
}

impl AggHash for i128 {
    fn agg_hash(&self) -> u64 {
        self.to_le_bytes().agg_hash()
    }
}

impl AggHash for i256 {
    fn agg_hash(&self) -> u64 {
        self.to_le_bytes().agg_hash()
    }
}

impl AggHash for OrderedFloat<f32> {
    #[inline(always)]
    fn agg_hash(&self) -> u64 {
        if self.is_nan() {
            f32::NAN.to_bits().agg_hash()
        } else {
            self.to_bits().agg_hash()
        }
    }
}

impl AggHash for OrderedFloat<f64> {
    #[inline(always)]
    fn agg_hash(&self) -> u64 {
        if self.is_nan() {
            f64::NAN.to_bits().agg_hash()
        } else {
            self.to_bits().agg_hash()
        }
    }
}

impl AggHash for ScalarRef<'_> {
    #[inline(always)]
    fn agg_hash(&self) -> u64 {
        self.to_string().as_bytes().agg_hash()
    }
}
