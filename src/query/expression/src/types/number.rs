// Copyright 2022 Datafuse Labs.
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

use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Range;

use common_arrow::arrow::buffer::Buffer;
use common_arrow::arrow::types::NativeType;
use num_traits::NumCast;
use ordered_float::OrderedFloat;

use crate::property::Domain;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::util::buffer_into_mut;
use crate::values::Column;
use crate::values::Scalar;
use crate::ScalarRef;

pub trait Number: Copy + Debug + Clone + PartialEq + 'static {
    type Storage: NumCast + Copy + From<Self::OrderedStorage> + NativeType;
    type OrderedStorage: NumCast + Copy + From<Self::Storage> + Ord;

    const MIN: Self::Storage;
    const MAX: Self::Storage;

    fn data_type() -> DataType;
    fn try_downcast_scalar(scalar: &ScalarRef) -> Option<Self::Storage>;
    fn try_downcast_column(col: &Column) -> Option<Buffer<Self::Storage>>;
    fn try_downcast_domain(domain: &Domain) -> Option<NumberDomain<Self>>;
    fn upcast_scalar(scalar: Self::Storage) -> Scalar;
    fn upcast_column(col: Buffer<Self::Storage>) -> Column;
    fn upcast_domain(domain: NumberDomain<Self>) -> Domain;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NumberType<T: Number>(PhantomData<T>);

pub type Int8Type = NumberType<i8>;
pub type Int16Type = NumberType<i16>;
pub type Int32Type = NumberType<i32>;
pub type Int64Type = NumberType<i64>;
pub type UInt8Type = NumberType<u8>;
pub type UInt16Type = NumberType<u16>;
pub type UInt32Type = NumberType<u32>;
pub type UInt64Type = NumberType<u64>;
pub type Float32Type = NumberType<f32>;
pub type Float64Type = NumberType<f64>;

impl<Num: Number> ValueType for NumberType<Num> {
    type Scalar = Num::Storage;
    type ScalarRef<'a> = Num::Storage;
    type Column = Buffer<Num::Storage>;
    type Domain = NumberDomain<Num>;
    type ColumnIterator<'a> = std::iter::Cloned<std::slice::Iter<'a, Num::Storage>>;
    type ColumnBuilder = Vec<Num::Storage>;

    fn to_owned_scalar<'a>(scalar: Self::ScalarRef<'a>) -> Self::Scalar {
        scalar
    }

    fn to_scalar_ref<'a>(scalar: &'a Self::Scalar) -> Self::ScalarRef<'a> {
        *scalar
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        Num::try_downcast_scalar(scalar)
    }

    fn try_downcast_column<'a>(col: &'a Column) -> Option<Self::Column> {
        Num::try_downcast_column(col)
    }

    fn try_downcast_domain(domain: &Domain) -> Option<NumberDomain<Num>> {
        Num::try_downcast_domain(domain)
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        Num::upcast_scalar(scalar)
    }

    fn upcast_column(col: Self::Column) -> Column {
        Num::upcast_column(col)
    }

    fn upcast_domain(domain: NumberDomain<Num>) -> Domain {
        Num::upcast_domain(domain)
    }

    fn column_len<'a>(col: &'a Self::Column) -> usize {
        col.len()
    }

    fn index_column<'a>(col: &'a Self::Column, index: usize) -> Option<Self::ScalarRef<'a>> {
        col.get(index).cloned()
    }

    unsafe fn index_column_unchecked<'a>(
        col: &'a Self::Column,
        index: usize,
    ) -> Self::ScalarRef<'a> {
        *col.get_unchecked(index)
    }

    fn slice_column<'a>(col: &'a Self::Column, range: Range<usize>) -> Self::Column {
        col.clone().slice(range.start, range.end - range.start)
    }

    fn iter_column<'a>(col: &'a Self::Column) -> Self::ColumnIterator<'a> {
        col.iter().cloned()
    }

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        buffer_into_mut(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::Scalar) {
        builder.push(item);
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        builder.push(Num::Storage::default());
    }

    fn append_builder(builder: &mut Self::ColumnBuilder, other_builder: &Self::ColumnBuilder) {
        builder.extend_from_slice(other_builder);
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.into()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        assert_eq!(builder.len(), 1);
        builder[0]
    }
}

impl<Num: Number> ArgType for NumberType<Num> {
    fn data_type() -> DataType {
        Num::data_type()
    }

    fn create_builder(capacity: usize, _generics: &GenericMap) -> Self::ColumnBuilder {
        Vec::with_capacity(capacity)
    }

    fn column_from_vec(vec: Vec<Self::Scalar>, _generics: &GenericMap) -> Self::Column {
        vec.into()
    }

    fn column_from_iter(iter: impl Iterator<Item = Self::Scalar>, _: &GenericMap) -> Self::Column {
        iter.collect()
    }

    fn column_from_ref_iter<'a>(
        iter: impl Iterator<Item = Self::ScalarRef<'a>>,
        _: &GenericMap,
    ) -> Self::Column {
        iter.collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NumberDomain<T: Number> {
    pub min: T::Storage,
    pub max: T::Storage,
}

// #[derive(Debug, Copy, Clone, PartialEq, Eq)]
// pub enum IntegerType {
//     u8(u8),
//     u16(u16),
//     u32(u32),
//     u64(u64),
//     i8(i8),
//     i16(i16),
//     i32(i32),
//     i64(i64),
// }
//
// macro_rules! int_trait_impl {
//     ($name:ident for $($t:ty)*) => ($(
//         impl $name for IntegerType::$t {
//
//             type Storage = $t;
//             type OrderedStorage = $t;
//
//             const MIN: Self::Storage = $t::MIN;
//             const MAX: Self::Storage = $t::MAX;
//
//             let type_str = stringify!($t);
//             let type_len = type_str.len();
//             // i8 -> int8, u32 -> uint32, ...
//             let integer_letter = if type_str.chars().nth(0).unwrap() == 'i' {
//                 let mut s = String::with_capacity(type_len+2);
//                 s.insert_str(0, "int"),
//                 s.insert_str(3, &type_str[1..])
//                 s
//             } else {
//                 let mut s = String::with_capacity(type_len+3);
//                 s.insert_str(0, "uint");
//                 s.insert_str(4, &type_str[1..])
//                 s
//             }
//
//             let
//
//             fn data_type() -> DataType {
//                 DataType::Int64
//             }
//
//             fn try_downcast_scalar(scalar: &ScalarRef) -> Option<Self::Storage> {
//                 scalar.as_int64().cloned()
//             }
//
//             fn try_downcast_column(col: &Column) -> Option<Buffer<Self::Storage>> {
//                 col.as_int64().cloned()
//             }
//
//             fn try_downcast_domain(domain: &Domain) -> Option<NumberDomain<Self>> {
//                 domain.as_int64().cloned()
//             }
//
//             fn upcast_scalar(scalar: Self::Storage) -> Scalar {
//                 Scalar::Int64(scalar)
//             }
//
//             fn upcast_column(col: Buffer<Self::Storage>) -> Column {
//                 Column::Int64(col)
//             }
//
//             fn upcast_domain(domain: NumberDomain<Self>) -> Domain {
//                 Domain::Int64(domain)
//             }
//         }
//     )*)
// }
// int_trait_impl!(Number for usize u8 u16 u32 u64 isize i8 i16 i32 i64);

impl Number for u8 {
    type Storage = u8;
    type OrderedStorage = u8;

    const MIN: Self::Storage = u8::MIN;
    const MAX: Self::Storage = u8::MAX;

    fn data_type() -> DataType {
        DataType::UInt8
    }

    fn try_downcast_scalar(scalar: &ScalarRef) -> Option<Self::Storage> {
        scalar.as_u_int8().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Buffer<Self::Storage>> {
        col.as_u_int8().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<NumberDomain<Self>> {
        domain.as_u_int8().cloned()
    }

    fn upcast_scalar(scalar: Self::Storage) -> Scalar {
        Scalar::UInt8(scalar)
    }

    fn upcast_column(col: Buffer<Self::Storage>) -> Column {
        Column::UInt8(col)
    }

    fn upcast_domain(domain: NumberDomain<Self>) -> Domain {
        Domain::UInt8(domain)
    }
}

impl Number for u16 {
    type Storage = u16;
    type OrderedStorage = u16;

    const MIN: Self::Storage = u16::MIN;
    const MAX: Self::Storage = u16::MAX;

    fn data_type() -> DataType {
        DataType::UInt16
    }

    fn try_downcast_scalar(scalar: &ScalarRef) -> Option<Self::Storage> {
        scalar.as_u_int16().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Buffer<Self::Storage>> {
        col.as_u_int16().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<NumberDomain<Self>> {
        domain.as_u_int16().cloned()
    }

    fn upcast_scalar(scalar: Self::Storage) -> Scalar {
        Scalar::UInt16(scalar)
    }

    fn upcast_column(col: Buffer<Self::Storage>) -> Column {
        Column::UInt16(col)
    }

    fn upcast_domain(domain: NumberDomain<Self>) -> Domain {
        Domain::UInt16(domain)
    }
}

impl Number for u32 {
    type Storage = u32;
    type OrderedStorage = u32;

    const MIN: Self::Storage = u32::MIN;
    const MAX: Self::Storage = u32::MAX;

    fn data_type() -> DataType {
        DataType::UInt32
    }

    fn try_downcast_scalar(scalar: &ScalarRef) -> Option<Self::Storage> {
        scalar.as_u_int32().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Buffer<Self::Storage>> {
        col.as_u_int32().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<NumberDomain<Self>> {
        domain.as_u_int32().cloned()
    }

    fn upcast_scalar(scalar: Self::Storage) -> Scalar {
        Scalar::UInt32(scalar)
    }

    fn upcast_column(col: Buffer<Self::Storage>) -> Column {
        Column::UInt32(col)
    }

    fn upcast_domain(domain: NumberDomain<Self>) -> Domain {
        Domain::UInt32(domain)
    }
}

impl Number for u64 {
    type Storage = u64;
    type OrderedStorage = u64;

    const MIN: Self::Storage = u64::MIN;
    const MAX: Self::Storage = u64::MAX;

    fn data_type() -> DataType {
        DataType::UInt64
    }

    fn try_downcast_scalar(scalar: &ScalarRef) -> Option<Self::Storage> {
        scalar.as_u_int64().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Buffer<Self::Storage>> {
        col.as_u_int64().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<NumberDomain<Self>> {
        domain.as_u_int64().cloned()
    }

    fn upcast_scalar(scalar: Self::Storage) -> Scalar {
        Scalar::UInt64(scalar)
    }

    fn upcast_column(col: Buffer<Self::Storage>) -> Column {
        Column::UInt64(col)
    }

    fn upcast_domain(domain: NumberDomain<Self>) -> Domain {
        Domain::UInt64(domain)
    }
}

impl Number for i8 {
    type Storage = i8;
    type OrderedStorage = i8;

    const MIN: Self::Storage = i8::MIN;
    const MAX: Self::Storage = i8::MAX;

    fn data_type() -> DataType {
        DataType::Int8
    }

    fn try_downcast_scalar(scalar: &ScalarRef) -> Option<Self::Storage> {
        scalar.as_int8().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Buffer<Self::Storage>> {
        col.as_int8().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<NumberDomain<Self>> {
        domain.as_int8().cloned()
    }

    fn upcast_scalar(scalar: Self::Storage) -> Scalar {
        Scalar::Int8(scalar)
    }

    fn upcast_column(col: Buffer<Self::Storage>) -> Column {
        Column::Int8(col)
    }

    fn upcast_domain(domain: NumberDomain<Self>) -> Domain {
        Domain::Int8(domain)
    }
}

impl Number for i16 {
    type Storage = i16;
    type OrderedStorage = i16;

    const MIN: Self::Storage = i16::MIN;
    const MAX: Self::Storage = i16::MAX;

    fn data_type() -> DataType {
        DataType::Int16
    }

    fn try_downcast_scalar(scalar: &ScalarRef) -> Option<Self::Storage> {
        scalar.as_int16().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Buffer<Self::Storage>> {
        col.as_int16().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<NumberDomain<Self>> {
        domain.as_int16().cloned()
    }

    fn upcast_scalar(scalar: Self::Storage) -> Scalar {
        Scalar::Int16(scalar)
    }

    fn upcast_column(col: Buffer<Self::Storage>) -> Column {
        Column::Int16(col)
    }

    fn upcast_domain(domain: NumberDomain<Self>) -> Domain {
        Domain::Int16(domain)
    }
}

impl Number for i32 {
    type Storage = i32;
    type OrderedStorage = i32;

    const MIN: Self::Storage = i32::MIN;
    const MAX: Self::Storage = i32::MAX;

    fn data_type() -> DataType {
        DataType::Int32
    }

    fn try_downcast_scalar(scalar: &ScalarRef) -> Option<Self::Storage> {
        scalar.as_int32().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Buffer<Self::Storage>> {
        col.as_int32().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<NumberDomain<Self>> {
        domain.as_int32().cloned()
    }

    fn upcast_scalar(scalar: Self::Storage) -> Scalar {
        Scalar::Int32(scalar)
    }

    fn upcast_column(col: Buffer<Self::Storage>) -> Column {
        Column::Int32(col)
    }

    fn upcast_domain(domain: NumberDomain<Self>) -> Domain {
        Domain::Int32(domain)
    }
}

impl Number for i64 {
    type Storage = i64;
    type OrderedStorage = i64;

    const MIN: Self::Storage = i64::MIN;
    const MAX: Self::Storage = i64::MAX;

    fn data_type() -> DataType {
        DataType::Int64
    }

    fn try_downcast_scalar(scalar: &ScalarRef) -> Option<Self::Storage> {
        scalar.as_int64().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Buffer<Self::Storage>> {
        col.as_int64().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<NumberDomain<Self>> {
        domain.as_int64().cloned()
    }

    fn upcast_scalar(scalar: Self::Storage) -> Scalar {
        Scalar::Int64(scalar)
    }

    fn upcast_column(col: Buffer<Self::Storage>) -> Column {
        Column::Int64(col)
    }

    fn upcast_domain(domain: NumberDomain<Self>) -> Domain {
        Domain::Int64(domain)
    }
}

impl Number for f32 {
    type Storage = f32;
    type OrderedStorage = OrderedFloat<f32>;

    const MIN: Self::Storage = f32::NEG_INFINITY;
    const MAX: Self::Storage = f32::INFINITY;

    fn data_type() -> DataType {
        DataType::Float32
    }

    fn try_downcast_scalar(scalar: &ScalarRef) -> Option<Self::Storage> {
        scalar.as_float32().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Buffer<Self::Storage>> {
        col.as_float32().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<NumberDomain<Self>> {
        domain.as_float32().cloned()
    }

    fn upcast_scalar(scalar: Self::Storage) -> Scalar {
        Scalar::Float32(scalar)
    }

    fn upcast_column(col: Buffer<Self::Storage>) -> Column {
        Column::Float32(col)
    }

    fn upcast_domain(domain: NumberDomain<Self>) -> Domain {
        Domain::Float32(domain)
    }
}

impl Number for f64 {
    type Storage = f64;
    type OrderedStorage = OrderedFloat<f64>;

    const MIN: Self::Storage = f64::NEG_INFINITY;
    const MAX: Self::Storage = f64::INFINITY;

    fn data_type() -> DataType {
        DataType::Float64
    }

    fn try_downcast_scalar(scalar: &ScalarRef) -> Option<Self::Storage> {
        scalar.as_float64().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Buffer<Self::Storage>> {
        col.as_float64().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<NumberDomain<Self>> {
        domain.as_float64().cloned()
    }

    fn upcast_scalar(scalar: Self::Storage) -> Scalar {
        Scalar::Float64(scalar)
    }

    fn upcast_column(col: Buffer<Self::Storage>) -> Column {
        Column::Float64(col)
    }

    fn upcast_domain(domain: NumberDomain<Self>) -> Domain {
        Domain::Float64(domain)
    }
}

pub fn overflow_cast<T: Number, U: Number>(src: T::Storage) -> (U::Storage, bool) {
    let src: T::OrderedStorage = src.into();
    let dest_min: T::OrderedStorage = num_traits::cast(U::MIN).unwrap_or(T::MIN).into();
    let dest_max: T::OrderedStorage = num_traits::cast(U::MAX).unwrap_or(T::MAX).into();
    let src_clamp: T::OrderedStorage = src.clamp(dest_min, dest_max);
    let overflowing = src != src_clamp;
    // The number must be within the range that `U` can represent after clamping, therefore
    // it's safe to unwrap.
    let dest: U::OrderedStorage = num_traits::cast(src_clamp).unwrap();

    (dest.into(), overflowing)
}
