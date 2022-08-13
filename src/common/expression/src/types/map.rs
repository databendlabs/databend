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

use std::marker::PhantomData;
use std::ops::Range;

use common_arrow::arrow::trusted_len::TrustedLen;

use super::ArrayType;
use super::StringType;
use crate::property::Domain;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::values::Column;
use crate::values::Scalar;
use crate::ScalarRef;

// Structuarally equals to `Tuple(K, V)`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvPair<K: ValueType, V: ValueType>(PhantomData<(K, V)>);

impl<K: ValueType, V: ValueType> ValueType for KvPair<K, V> {
    type Scalar = (K::Scalar, V::Scalar);
    type ScalarRef<'a> = (K::ScalarRef<'a>, V::ScalarRef<'a>);
    type Column = KvColumn<K, V>;
    type Domain = ();
    type ColumnIterator<'a> = KvIterator<'a, K, V>;
    type ColumnBuilder = KvColumnBuilder<K, V>;

    fn to_owned_scalar<'a>((k, v): Self::ScalarRef<'a>) -> Self::Scalar {
        (K::to_owned_scalar(k), V::to_owned_scalar(v))
    }

    fn to_scalar_ref<'a>((k, v): &'a Self::Scalar) -> Self::ScalarRef<'a> {
        (K::to_scalar_ref(k), V::to_scalar_ref(v))
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        match scalar {
            ScalarRef::Tuple(fields) => Some((
                K::try_downcast_scalar(&fields[0])?,
                V::try_downcast_scalar(&fields[1])?,
            )),
            _ => None,
        }
    }

    fn try_downcast_column<'a>(col: &'a Column) -> Option<Self::Column> {
        match col {
            Column::Tuple { fields, .. } => Some(KvColumn {
                keys: K::try_downcast_column(&fields[0])?,
                values: V::try_downcast_column(&fields[1])?,
            }),
            _ => None,
        }
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        match domain {
            Domain::Undefined => Some(()),
            _ => None,
        }
    }

    fn upcast_scalar((k, v): Self::Scalar) -> Scalar {
        Scalar::Tuple(vec![K::upcast_scalar(k), V::upcast_scalar(v)])
    }

    fn upcast_column(col: Self::Column) -> Column {
        Column::Tuple {
            len: col.len(),
            fields: vec![K::upcast_column(col.keys), V::upcast_column(col.values)],
        }
    }

    fn upcast_domain((): Self::Domain) -> Domain {
        Domain::Undefined
    }

    fn column_len<'a>(col: &'a Self::Column) -> usize {
        col.len()
    }

    fn index_column<'a>(col: &'a Self::Column, index: usize) -> Option<Self::ScalarRef<'a>> {
        col.index(index)
    }

    unsafe fn index_column_unchecked<'a>(
        col: &'a Self::Column,
        index: usize,
    ) -> Self::ScalarRef<'a> {
        col.index_unchecked(index)
    }

    fn slice_column<'a>(col: &'a Self::Column, range: Range<usize>) -> Self::Column {
        col.slice(range)
    }

    fn iter_column<'a>(col: &'a Self::Column) -> Self::ColumnIterator<'a> {
        col.iter()
    }

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        KvColumnBuilder::from_column(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>) {
        builder.push(item);
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        builder.push_default();
    }

    fn append_builder(builder: &mut Self::ColumnBuilder, other_builder: &Self::ColumnBuilder) {
        builder.append(other_builder);
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.build()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        builder.build_scalar()
    }
}

impl<K: ArgType, V: ArgType> ArgType for KvPair<K, V> {
    fn data_type() -> DataType {
        DataType::Tuple(vec![K::data_type(), V::data_type()])
    }

    fn create_builder(capacity: usize, generics: &GenericMap) -> Self::ColumnBuilder {
        KvColumnBuilder::with_capacity(capacity, generics)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct KvColumn<K: ValueType, V: ValueType> {
    pub keys: K::Column,
    pub values: V::Column,
}

impl<K: ValueType, V: ValueType> KvColumn<K, V> {
    pub fn len(&self) -> usize {
        K::column_len(&self.keys)
    }

    pub fn index(&self, index: usize) -> Option<(K::ScalarRef<'_>, V::ScalarRef<'_>)> {
        Some((
            K::index_column(&self.keys, index)?,
            V::index_column(&self.values, index)?,
        ))
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    pub unsafe fn index_unchecked(&self, index: usize) -> (K::ScalarRef<'_>, V::ScalarRef<'_>) {
        (
            K::index_column_unchecked(&self.keys, index),
            V::index_column_unchecked(&self.values, index),
        )
    }

    fn slice(&self, range: Range<usize>) -> Self {
        KvColumn {
            keys: K::slice_column(&self.keys, range.clone()),
            values: V::slice_column(&self.values, range),
        }
    }

    fn iter(&self) -> KvIterator<K, V> {
        KvIterator {
            keys: K::iter_column(&self.keys),
            values: V::iter_column(&self.values),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct KvColumnBuilder<K: ValueType, V: ValueType> {
    pub keys: K::ColumnBuilder,
    pub values: V::ColumnBuilder,
}

impl<K: ValueType, V: ValueType> KvColumnBuilder<K, V> {
    pub fn from_column(col: KvColumn<K, V>) -> Self {
        Self {
            keys: K::column_to_builder(col.keys),
            values: V::column_to_builder(col.values),
        }
    }

    pub fn len(&self) -> usize {
        K::builder_len(&self.keys)
    }

    pub fn push(&mut self, (k, v): (K::ScalarRef<'_>, V::ScalarRef<'_>)) {
        K::push_item(&mut self.keys, k);
        V::push_item(&mut self.values, v);
    }

    pub fn push_default(&mut self) {
        K::push_default(&mut self.keys);
        V::push_default(&mut self.values);
    }

    pub fn append(&mut self, other: &Self) {
        K::append_builder(&mut self.keys, &other.keys);
        V::append_builder(&mut self.values, &other.values);
    }

    pub fn build(self) -> KvColumn<K, V> {
        KvColumn {
            keys: K::build_column(self.keys),
            values: V::build_column(self.values),
        }
    }

    pub fn build_scalar(self) -> (K::Scalar, V::Scalar) {
        (K::build_scalar(self.keys), V::build_scalar(self.values))
    }
}

impl<K: ArgType, V: ArgType> KvColumnBuilder<K, V> {
    pub fn with_capacity(capacity: usize, generics: &GenericMap) -> Self {
        Self {
            keys: K::create_builder(capacity, generics),
            values: V::create_builder(capacity, generics),
        }
    }
}

pub struct KvIterator<'a, K: ValueType, V: ValueType> {
    keys: K::ColumnIterator<'a>,
    values: V::ColumnIterator<'a>,
}

impl<'a, K: ValueType, V: ValueType> Iterator for KvIterator<'a, K, V> {
    type Item = (K::ScalarRef<'a>, V::ScalarRef<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        Some((self.keys.next()?, self.values.next()?))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        assert_eq!(self.keys.size_hint(), self.values.size_hint());
        self.keys.size_hint()
    }
}

unsafe impl<'a, K: ValueType, V: ValueType> TrustedLen for KvIterator<'a, K, V> {}

// Structuarally equals to `Array(Tuple(K, V))` but treated distinct from `Array(Tuple(K, V))`
// in unification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapType<T: ValueType>(PhantomData<T>);

pub type MapInternal<T> = ArrayType<KvPair<StringType, T>>;

impl<T: ValueType> ValueType for MapType<T> {
    type Scalar = <MapInternal<T> as ValueType>::Scalar;
    type ScalarRef<'a> = <MapInternal<T> as ValueType>::ScalarRef<'a>;
    type Column = <MapInternal<T> as ValueType>::Column;
    type Domain = <MapInternal<T> as ValueType>::Domain;
    type ColumnIterator<'a> = <MapInternal<T> as ValueType>::ColumnIterator<'a>;
    type ColumnBuilder = <MapInternal<T> as ValueType>::ColumnBuilder;

    fn to_owned_scalar<'a>(scalar: Self::ScalarRef<'a>) -> Self::Scalar {
        <MapInternal<T> as ValueType>::to_owned_scalar(scalar)
    }

    fn to_scalar_ref<'a>(scalar: &'a Self::Scalar) -> Self::ScalarRef<'a> {
        <MapInternal<T> as ValueType>::to_scalar_ref(scalar)
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        <MapInternal<T> as ValueType>::try_downcast_scalar(scalar)
    }

    fn try_downcast_column<'a>(col: &'a Column) -> Option<Self::Column> {
        <MapInternal<T> as ValueType>::try_downcast_column(col)
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        <MapInternal<T> as ValueType>::try_downcast_domain(domain)
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        <MapInternal<T> as ValueType>::upcast_scalar(scalar)
    }

    fn upcast_column(col: Self::Column) -> Column {
        <MapInternal<T> as ValueType>::upcast_column(col)
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        <MapInternal<T> as ValueType>::upcast_domain(domain)
    }

    fn column_len<'a>(col: &'a Self::Column) -> usize {
        <MapInternal<T> as ValueType>::column_len(col)
    }

    fn index_column<'a>(col: &'a Self::Column, index: usize) -> Option<Self::ScalarRef<'a>> {
        <MapInternal<T> as ValueType>::index_column(col, index)
    }

    unsafe fn index_column_unchecked<'a>(
        col: &'a Self::Column,
        index: usize,
    ) -> Self::ScalarRef<'a> {
        <MapInternal<T> as ValueType>::index_column_unchecked(col, index)
    }

    fn slice_column<'a>(col: &'a Self::Column, range: Range<usize>) -> Self::Column {
        <MapInternal<T> as ValueType>::slice_column(col, range)
    }

    fn iter_column<'a>(col: &'a Self::Column) -> Self::ColumnIterator<'a> {
        <MapInternal<T> as ValueType>::iter_column(col)
    }

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        <MapInternal<T> as ValueType>::column_to_builder(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        <MapInternal<T> as ValueType>::builder_len(builder)
    }

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>) {
        <MapInternal<T> as ValueType>::push_item(builder, item)
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        <MapInternal<T> as ValueType>::push_default(builder)
    }

    fn append_builder(builder: &mut Self::ColumnBuilder, other_builder: &Self::ColumnBuilder) {
        <MapInternal<T> as ValueType>::append_builder(builder, other_builder)
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        <MapInternal<T> as ValueType>::build_column(builder)
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        <MapInternal<T> as ValueType>::build_scalar(builder)
    }
}

impl<T: ArgType> ArgType for MapType<T> {
    fn data_type() -> DataType {
        DataType::Map(Box::new(T::data_type()))
    }

    fn create_builder(capacity: usize, generics: &GenericMap) -> Self::ColumnBuilder {
        <MapInternal<T> as ArgType>::create_builder(capacity, generics)
    }
}
