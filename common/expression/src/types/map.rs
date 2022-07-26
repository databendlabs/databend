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

// Structuaral equals to `Tuple(K, V)`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvPair<K: ValueType, V: ValueType>(PhantomData<(K, V)>);

impl<K: ValueType, V: ValueType> ValueType for KvPair<K, V> {
    type Scalar = (K::Scalar, V::Scalar);
    type ScalarRef<'a> = (K::ScalarRef<'a>, V::ScalarRef<'a>);
    type Column = KvColumn<K, V>;
    type Domain = ();

    fn to_owned_scalar<'a>((k, v): Self::ScalarRef<'a>) -> Self::Scalar {
        (K::to_owned_scalar(k), V::to_owned_scalar(v))
    }

    fn to_scalar_ref<'a>((k, v): &'a Self::Scalar) -> Self::ScalarRef<'a> {
        (K::to_scalar_ref(k), V::to_scalar_ref(v))
    }
}

impl<K: ArgType, V: ArgType> ArgType for KvPair<K, V> {
    type ColumnIterator<'a> = KvIterator<'a, K, V>;
    type ColumnBuilder = KvColumnBuilder<K, V>;

    fn data_type() -> DataType {
        DataType::Tuple(vec![K::data_type(), V::data_type()])
    }

    fn try_downcast_scalar<'a>(scalar: &'a Scalar) -> Option<Self::ScalarRef<'a>> {
        match scalar {
            Scalar::Tuple(fields) => Some((
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

    fn full_domain(_: &GenericMap) -> Self::Domain {}

    fn column_len<'a>(col: &'a Self::Column) -> usize {
        col.len()
    }

    fn index_column<'a>(col: &'a Self::Column, index: usize) -> Option<Self::ScalarRef<'a>> {
        col.index(index)
    }

    fn slice_column<'a>(col: &'a Self::Column, range: Range<usize>) -> Self::Column {
        col.slice(range)
    }

    fn iter_column<'a>(col: &'a Self::Column) -> Self::ColumnIterator<'a> {
        col.iter()
    }

    fn create_builder(capacity: usize, generics: &GenericMap) -> Self::ColumnBuilder {
        KvColumnBuilder::with_capacity(capacity, generics)
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

#[derive(Debug, Clone, PartialEq)]
pub struct KvColumn<K: ValueType, V: ValueType> {
    pub keys: K::Column,
    pub values: V::Column,
}

impl<K: ArgType, V: ArgType> KvColumn<K, V> {
    pub fn len(&self) -> usize {
        K::column_len(&self.keys)
    }

    pub fn index(&self, index: usize) -> Option<(K::ScalarRef<'_>, V::ScalarRef<'_>)> {
        Some((
            K::index_column(&self.keys, index)?,
            V::index_column(&self.values, index)?,
        ))
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

pub struct KvColumnBuilder<K: ArgType, V: ArgType> {
    pub keys: K::ColumnBuilder,
    pub values: V::ColumnBuilder,
}

impl<K: ArgType, V: ArgType> KvColumnBuilder<K, V> {
    pub fn with_capacity(capacity: usize, generics: &GenericMap) -> Self {
        Self {
            keys: K::create_builder(capacity, generics),
            values: V::create_builder(capacity, generics),
        }
    }

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

pub struct KvIterator<'a, K: ArgType, V: ArgType> {
    keys: K::ColumnIterator<'a>,
    values: V::ColumnIterator<'a>,
}

impl<'a, K: ArgType, V: ArgType> Iterator for KvIterator<'a, K, V> {
    type Item = (K::ScalarRef<'a>, V::ScalarRef<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        Some((self.keys.next()?, self.values.next()?))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        assert_eq!(self.keys.size_hint(), self.values.size_hint());
        self.keys.size_hint()
    }
}

unsafe impl<'a, K: ArgType, V: ArgType> TrustedLen for KvIterator<'a, K, V> {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapType<T: ArgType>(PhantomData<T>);

pub type MapInternal<T> = ArrayType<KvPair<StringType, T>>;

impl<T: ArgType> ValueType for MapType<T> {
    type Scalar = <MapInternal<T> as ValueType>::Scalar;
    type ScalarRef<'a> = <MapInternal<T> as ValueType>::ScalarRef<'a>;
    type Column = <MapInternal<T> as ValueType>::Column;
    type Domain = <MapInternal<T> as ValueType>::Domain;

    fn to_owned_scalar<'a>(scalar: Self::ScalarRef<'a>) -> Self::Scalar {
        <MapInternal<T> as ValueType>::to_owned_scalar(scalar)
    }

    fn to_scalar_ref<'a>(scalar: &'a Self::Scalar) -> Self::ScalarRef<'a> {
        <MapInternal<T> as ValueType>::to_scalar_ref(scalar)
    }
}

impl<T: ArgType> ArgType for MapType<T> {
    type ColumnIterator<'a> = <MapInternal<T> as ArgType>::ColumnIterator<'a>;
    type ColumnBuilder = <MapInternal<T> as ArgType>::ColumnBuilder;

    fn data_type() -> DataType {
        DataType::Map(Box::new(T::data_type()))
    }

    fn try_downcast_scalar<'a>(scalar: &'a Scalar) -> Option<Self::ScalarRef<'a>> {
        <MapInternal<T> as ArgType>::try_downcast_scalar(scalar)
    }

    fn try_downcast_column<'a>(col: &'a Column) -> Option<Self::Column> {
        <MapInternal<T> as ArgType>::try_downcast_column(col)
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        <MapInternal<T> as ArgType>::try_downcast_domain(domain)
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        <MapInternal<T> as ArgType>::upcast_scalar(scalar)
    }

    fn upcast_column(col: Self::Column) -> Column {
        <MapInternal<T> as ArgType>::upcast_column(col)
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        <MapInternal<T> as ArgType>::upcast_domain(domain)
    }

    fn full_domain(generics: &GenericMap) -> Self::Domain {
        <MapInternal<T> as ArgType>::full_domain(generics)
    }

    fn column_len<'a>(col: &'a Self::Column) -> usize {
        <MapInternal<T> as ArgType>::column_len(col)
    }

    fn index_column<'a>(col: &'a Self::Column, index: usize) -> Option<Self::ScalarRef<'a>> {
        <MapInternal<T> as ArgType>::index_column(col, index)
    }

    fn slice_column<'a>(col: &'a Self::Column, range: Range<usize>) -> Self::Column {
        <MapInternal<T> as ArgType>::slice_column(col, range)
    }

    fn iter_column<'a>(col: &'a Self::Column) -> Self::ColumnIterator<'a> {
        <MapInternal<T> as ArgType>::iter_column(col)
    }

    fn create_builder(capacity: usize, generics: &GenericMap) -> Self::ColumnBuilder {
        <MapInternal<T> as ArgType>::create_builder(capacity, generics)
    }

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        <MapInternal<T> as ArgType>::column_to_builder(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        <MapInternal<T> as ArgType>::builder_len(builder)
    }

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>) {
        <MapInternal<T> as ArgType>::push_item(builder, item)
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        <MapInternal<T> as ArgType>::push_default(builder)
    }

    fn append_builder(builder: &mut Self::ColumnBuilder, other_builder: &Self::ColumnBuilder) {
        <MapInternal<T> as ArgType>::append_builder(builder, other_builder)
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        <MapInternal<T> as ArgType>::build_column(builder)
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        <MapInternal<T> as ArgType>::build_scalar(builder)
    }
}
