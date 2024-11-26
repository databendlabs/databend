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

use databend_common_base::base::OrderedFloat;
use databend_common_column::bitmap::Bitmap;
use databend_common_column::buffer::Buffer;
use databend_common_column::types::Index;
use databend_common_exception::Result;
use ethnum::i256;

use crate::types::AnyType;
use crate::types::ArgType;
use crate::types::BinaryType;
use crate::types::BitmapType;
use crate::types::BooleanType;
use crate::types::DataType;
use crate::types::DateType;
use crate::types::DecimalColumn;
use crate::types::DecimalDataType;
use crate::types::DecimalScalar;
use crate::types::DecimalType;
use crate::types::GeographyType;
use crate::types::GeometryType;
use crate::types::NumberColumn;
use crate::types::NumberDataType;
use crate::types::NumberScalar;
use crate::types::NumberType;
use crate::types::StringType;
use crate::types::TimestampType;
use crate::types::ValueType;
use crate::types::VariantType;
use crate::visitor::ValueVisitor;
use crate::with_decimal_type;
use crate::with_number_mapped_type;
use crate::with_number_type;
use crate::Column;
use crate::InputColumns;
use crate::Scalar;
use crate::ScalarRef;
use crate::Value;

const NULL_HASH_VAL: u64 = 0xd1cefa08eb382d69;

pub fn group_hash_columns(cols: InputColumns, values: &mut [u64]) {
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
        DataType::Geography => {
            combine_group_hash_string_column::<IS_FIRST, GeographyType>(c, values)
        }
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

pub fn group_hash_value_spread<I: Index>(
    indices: &[I],
    value: Value<AnyType>,
    first: bool,
    target: &mut [u64],
) -> Result<()> {
    if first {
        let mut v = IndexHashVisitor::<true, _>::new(indices, target);
        v.visit_value(value)
    } else {
        let mut v = IndexHashVisitor::<false, _>::new(indices, target);
        v.visit_value(value)
    }
}

struct IndexHashVisitor<'a, 'b, const IS_FIRST: bool, I>
where I: Index
{
    indices: &'a [I],
    target: &'b mut [u64],
}

impl<'a, 'b, const IS_FIRST: bool, I> IndexHashVisitor<'a, 'b, IS_FIRST, I>
where I: Index
{
    fn new(indices: &'a [I], target: &'b mut [u64]) -> Self {
        Self { indices, target }
    }
}

impl<'a, 'b, const IS_FIRST: bool, I> ValueVisitor for IndexHashVisitor<'a, 'b, IS_FIRST, I>
where I: Index
{
    fn visit_scalar(&mut self, scalar: Scalar) -> Result<()> {
        let hash = match scalar {
            Scalar::EmptyArray | Scalar::EmptyMap => return Ok(()),
            Scalar::Null => NULL_HASH_VAL,
            Scalar::Number(v) => with_number_type!(|NUM_TYPE| match v {
                NumberScalar::NUM_TYPE(v) => v.agg_hash(),
            }),
            Scalar::Decimal(v) => match v {
                DecimalScalar::Decimal128(v, _) => v.agg_hash(),
                DecimalScalar::Decimal256(v, _) => v.agg_hash(),
            },
            Scalar::Timestamp(v) => v.agg_hash(),
            Scalar::Date(v) => v.agg_hash(),
            Scalar::Boolean(v) => v.agg_hash(),
            Scalar::Binary(v) => v.agg_hash(),
            Scalar::String(v) => v.as_bytes().agg_hash(),
            Scalar::Variant(v) => v.agg_hash(),
            Scalar::Geometry(v) => v.agg_hash(),
            Scalar::Geography(v) => v.0.agg_hash(),
            v => v.as_ref().agg_hash(),
        };
        self.visit_indices(|_| hash)
    }

    fn visit_null(&mut self, _len: usize) -> Result<()> {
        Ok(())
    }

    fn visit_empty_array(&mut self, _len: usize) -> Result<()> {
        Ok(())
    }

    fn visit_empty_map(&mut self, _len: usize) -> Result<()> {
        Ok(())
    }

    fn visit_any_number(&mut self, column: crate::types::NumberColumn) -> Result<()> {
        with_number_type!(|NUM_TYPE| match column {
            NumberColumn::NUM_TYPE(buffer) => {
                let buffer = buffer.as_ref();
                self.visit_indices(|i| buffer[i.to_usize()].agg_hash())
            }
        })
    }

    fn visit_timestamp(&mut self, buffer: Buffer<i64>) -> Result<()> {
        self.visit_number(buffer)
    }

    fn visit_date(&mut self, buffer: Buffer<i32>) -> Result<()> {
        self.visit_number(buffer)
    }

    fn visit_any_decimal(&mut self, column: DecimalColumn) -> Result<()> {
        with_decimal_type!(|DECIMAL_TYPE| match column {
            DecimalColumn::DECIMAL_TYPE(buffer, _) => {
                let buffer = buffer.as_ref();
                self.visit_indices(|i| buffer[i.to_usize()].agg_hash())
            }
        })
    }

    fn visit_binary(&mut self, column: crate::types::BinaryColumn) -> Result<()> {
        self.visit_indices(|i| column.index(i.to_usize()).unwrap().agg_hash())
    }

    fn visit_variant(&mut self, column: crate::types::BinaryColumn) -> Result<()> {
        self.visit_binary(column)
    }

    fn visit_bitmap(&mut self, column: crate::types::BinaryColumn) -> Result<()> {
        self.visit_binary(column)
    }

    fn visit_string(&mut self, column: crate::types::StringColumn) -> Result<()> {
        self.visit_indices(|i| column.index(i.to_usize()).unwrap().as_bytes().agg_hash())
    }

    fn visit_boolean(&mut self, bitmap: Bitmap) -> Result<()> {
        self.visit_indices(|i| bitmap.get(i.to_usize()).unwrap().agg_hash())
    }

    fn visit_geometry(&mut self, column: crate::types::BinaryColumn) -> Result<()> {
        self.visit_binary(column)
    }

    fn visit_geography(&mut self, column: crate::types::GeographyColumn) -> Result<()> {
        self.visit_binary(column.0)
    }

    fn visit_nullable(&mut self, column: Box<crate::types::NullableColumn<AnyType>>) -> Result<()> {
        let indices = self
            .indices
            .iter()
            .cloned()
            .filter(|&i| {
                let i = i.to_usize();
                let ok = column.validity.get(i).unwrap();
                if !ok {
                    let val = &mut self.target[i];
                    if IS_FIRST {
                        *val = NULL_HASH_VAL;
                    } else {
                        *val = merge_hash(*val, NULL_HASH_VAL);
                    }
                }
                ok
            })
            .collect::<Vec<_>>();
        if IS_FIRST {
            let mut v = IndexHashVisitor::<true, _>::new(&indices, self.target);
            v.visit_column(column.column)
        } else {
            let mut v = IndexHashVisitor::<false, _>::new(&indices, self.target);
            v.visit_column(column.column)
        }
    }

    fn visit_typed_column<T: ValueType>(&mut self, column: T::Column) -> Result<()> {
        self.visit_indices(|i| {
            let x = T::upcast_scalar(T::to_owned_scalar(
                T::index_column(&column, i.to_usize()).unwrap(),
            ));
            x.as_ref().agg_hash()
        })
    }
}

impl<'a, 'b, const IS_FIRST: bool, I> IndexHashVisitor<'a, 'b, IS_FIRST, I>
where I: Index
{
    fn visit_indices<F>(&mut self, do_hash: F) -> Result<()>
    where F: Fn(&I) -> u64 {
        self.visit_indices_update(|i, val| {
            let hash = do_hash(i);
            if IS_FIRST {
                *val = hash;
            } else {
                *val = merge_hash(*val, hash);
            }
        })
    }

    fn visit_indices_update<F>(&mut self, update: F) -> Result<()>
    where F: Fn(&I, &mut u64) {
        for i in self.indices {
            let val = &mut self.target[i.to_usize()];
            update(i, val);
        }
        Ok(())
    }
}

fn merge_hash(a: u64, b: u64) -> u64 {
    a.wrapping_mul(NULL_HASH_VAL) ^ b
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

#[cfg(test)]
mod tests {
    use databend_common_column::bitmap::Bitmap;

    use super::*;
    use crate::types::ArgType;
    use crate::types::Int32Type;
    use crate::types::NullableColumn;
    use crate::types::NullableType;
    use crate::types::StringType;
    use crate::BlockEntry;
    use crate::DataBlock;
    use crate::FromData;
    use crate::Scalar;
    use crate::Value;

    fn merge_hash_slice(ls: &[u64]) -> u64 {
        ls.iter().cloned().reduce(merge_hash).unwrap()
    }

    #[test]
    fn test_value_spread() -> Result<()> {
        let data = DataBlock::new(
            vec![
                BlockEntry::new(
                    Int32Type::data_type(),
                    Value::Column(Int32Type::from_data(vec![3, 1, 2, 2, 4, 3, 7, 0, 3])),
                ),
                BlockEntry::new(
                    StringType::data_type(),
                    Value::Scalar(Scalar::String("a".to_string())),
                ),
                BlockEntry::new(
                    Int32Type::data_type(),
                    Value::Column(Int32Type::from_data(vec![3, 1, 3, 2, 2, 3, 4, 3, 3])),
                ),
                BlockEntry::new(
                    StringType::data_type(),
                    Value::Column(StringType::from_data(vec![
                        "a", "b", "c", "d", "e", "f", "g", "h", "i",
                    ])),
                ),
            ],
            9,
        );
        data.check_valid()?;

        {
            let mut target = vec![0; data.num_rows()];
            for (i, entry) in data.columns().iter().enumerate() {
                let indices = [0, 3, 8];
                group_hash_value_spread(&indices, entry.value.to_owned(), i == 0, &mut target)?;
            }

            assert_eq!(
                [
                    merge_hash_slice(&[
                        3.agg_hash(),
                        b"a".agg_hash(),
                        3.agg_hash(),
                        b"a".agg_hash(),
                    ]),
                    0,
                    0,
                    merge_hash_slice(&[
                        2.agg_hash(),
                        b"a".agg_hash(),
                        2.agg_hash(),
                        b"d".agg_hash(),
                    ]),
                    0,
                    0,
                    0,
                    0,
                    merge_hash_slice(&[
                        3.agg_hash(),
                        b"a".agg_hash(),
                        3.agg_hash(),
                        b"i".agg_hash(),
                    ]),
                ]
                .as_slice(),
                &target
            );
        }

        {
            let c = Int32Type::from_data(vec![3, 1, 2]);
            let c = NullableColumn::<AnyType>::new(c, Bitmap::from([true, true, false]));
            let nc = NullableType::<AnyType>::upcast_column(c);

            let indices = [0, 1, 2];
            let mut target = vec![0; 3];
            group_hash_value_spread(
                &indices,
                Value::<AnyType>::Column(nc.clone()),
                true,
                &mut target,
            )?;

            assert_eq!(
                [
                    merge_hash_slice(&[3.agg_hash()]),
                    merge_hash_slice(&[1.agg_hash()]),
                    merge_hash_slice(&[NULL_HASH_VAL]),
                ]
                .as_slice(),
                &target
            );

            let c = Int32Type::from_data(vec![2, 4, 3]);
            group_hash_value_spread(&indices, Value::<AnyType>::Column(c), false, &mut target)?;

            assert_eq!(
                [
                    merge_hash_slice(&[3.agg_hash(), 2.agg_hash()]),
                    merge_hash_slice(&[1.agg_hash(), 4.agg_hash()]),
                    merge_hash_slice(&[NULL_HASH_VAL, 3.agg_hash()]),
                ]
                .as_slice(),
                &target
            );

            group_hash_value_spread(&indices, Value::<AnyType>::Column(nc), false, &mut target)?;

            assert_eq!(
                [
                    merge_hash_slice(&[3.agg_hash(), 2.agg_hash(), 3.agg_hash()]),
                    merge_hash_slice(&[1.agg_hash(), 4.agg_hash(), 1.agg_hash()]),
                    merge_hash_slice(&[NULL_HASH_VAL, 3.agg_hash(), NULL_HASH_VAL]),
                ]
                .as_slice(),
                &target
            );
        }
        Ok(())
    }
}
