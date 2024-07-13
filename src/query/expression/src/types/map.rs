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

use std::marker::PhantomData;
use std::ops::Range;

use databend_common_arrow::arrow::trusted_len::TrustedLen;

use super::ArrayType;
use super::DecimalSize;
use crate::property::Domain;
use crate::types::array::ArrayColumn;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::values::Column;
use crate::values::Scalar;
use crate::ColumnBuilder;
use crate::ScalarRef;

// Structurally equals to `Tuple(K, V)`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvPair<K: ValueType, V: ValueType>(PhantomData<(K, V)>);

impl<K: ValueType, V: ValueType> ValueType for KvPair<K, V> {
    type Scalar = (K::Scalar, V::Scalar);
    type ScalarRef<'a> = (K::ScalarRef<'a>, V::ScalarRef<'a>);
    type Column = KvColumn<K, V>;
    type Domain = (K::Domain, V::Domain);
    type ColumnIterator<'a> = KvIterator<'a, K, V>;
    type ColumnBuilder = KvColumnBuilder<K, V>;

    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: Self::ScalarRef<'long>) -> Self::ScalarRef<'short> {
        (K::upcast_gat(long.0), V::upcast_gat(long.1))
    }

    fn to_owned_scalar((k, v): Self::ScalarRef<'_>) -> Self::Scalar {
        (K::to_owned_scalar(k), V::to_owned_scalar(v))
    }

    fn to_scalar_ref((k, v): &Self::Scalar) -> Self::ScalarRef<'_> {
        (K::to_scalar_ref(k), V::to_scalar_ref(v))
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        match scalar {
            ScalarRef::Tuple(fields) if fields.len() == 2 => Some((
                K::try_downcast_scalar(&fields[0])?,
                V::try_downcast_scalar(&fields[1])?,
            )),
            _ => None,
        }
    }

    fn try_downcast_column(col: &Column) -> Option<Self::Column> {
        match col {
            Column::Tuple(fields) if fields.len() == 2 => Some(KvColumn {
                keys: K::try_downcast_column(&fields[0])?,
                values: V::try_downcast_column(&fields[1])?,
            }),
            _ => None,
        }
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        match domain {
            Domain::Tuple(fields) if fields.len() == 2 => Some((
                K::try_downcast_domain(&fields[0])?,
                V::try_downcast_domain(&fields[1])?,
            )),
            _ => None,
        }
    }

    fn try_downcast_builder(_builder: &mut ColumnBuilder) -> Option<&mut Self::ColumnBuilder> {
        None
    }

    fn try_downcast_owned_builder<'a>(_builder: ColumnBuilder) -> Option<Self::ColumnBuilder> {
        None
    }

    fn try_upcast_column_builder(
        _builder: Self::ColumnBuilder,
        _decimal_size: Option<DecimalSize>,
    ) -> Option<ColumnBuilder> {
        None
    }

    fn upcast_scalar((k, v): Self::Scalar) -> Scalar {
        Scalar::Tuple(vec![K::upcast_scalar(k), V::upcast_scalar(v)])
    }

    fn upcast_column(col: Self::Column) -> Column {
        Column::Tuple(vec![
            K::upcast_column(col.keys),
            V::upcast_column(col.values),
        ])
    }

    fn upcast_domain((k, v): Self::Domain) -> Domain {
        Domain::Tuple(vec![K::upcast_domain(k), V::upcast_domain(v)])
    }

    fn column_len(col: &Self::Column) -> usize {
        col.len()
    }

    fn index_column(col: &Self::Column, index: usize) -> Option<Self::ScalarRef<'_>> {
        col.index(index)
    }

    unsafe fn index_column_unchecked(col: &Self::Column, index: usize) -> Self::ScalarRef<'_> {
        col.index_unchecked(index)
    }

    fn slice_column(col: &Self::Column, range: Range<usize>) -> Self::Column {
        col.slice(range)
    }

    fn iter_column(col: &Self::Column) -> Self::ColumnIterator<'_> {
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

    fn push_item_repeat(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>, n: usize) {
        builder.push_repeat(item, n)
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        builder.push_default();
    }

    fn append_column(builder: &mut Self::ColumnBuilder, other_builder: &Self::Column) {
        builder.append_column(other_builder);
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.build()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        builder.build_scalar()
    }

    fn scalar_memory_size((k, v): &Self::ScalarRef<'_>) -> usize {
        K::scalar_memory_size(k) + V::scalar_memory_size(v)
    }

    fn column_memory_size(col: &Self::Column) -> usize {
        col.memory_size()
    }
}

impl<K: ArgType, V: ArgType> ArgType for KvPair<K, V> {
    fn data_type() -> DataType {
        DataType::Tuple(vec![K::data_type(), V::data_type()])
    }

    fn full_domain() -> Self::Domain {
        (K::full_domain(), V::full_domain())
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

    pub fn iter(&self) -> KvIterator<K, V> {
        KvIterator {
            keys: K::iter_column(&self.keys),
            values: V::iter_column(&self.values),
        }
    }

    pub fn memory_size(&self) -> usize {
        K::column_memory_size(&self.keys) + V::column_memory_size(&self.values)
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

    pub fn push_repeat(&mut self, (k, v): (K::ScalarRef<'_>, V::ScalarRef<'_>), n: usize) {
        K::push_item_repeat(&mut self.keys, k, n);
        V::push_item_repeat(&mut self.values, v, n);
    }

    pub fn push_default(&mut self) {
        K::push_default(&mut self.keys);
        V::push_default(&mut self.values);
    }

    pub fn append_column(&mut self, other: &KvColumn<K, V>) {
        K::append_column(&mut self.keys, &other.keys);
        V::append_column(&mut self.values, &other.values);
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

// Structurally equals to `Array(Tuple(K, V))` but treated distinct from `Array(Tuple(K, V))`
// in unification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapType<K: ValueType, V: ValueType>(PhantomData<(K, V)>);

pub type MapInternal<K, V> = ArrayType<KvPair<K, V>>;

impl<K: ValueType, V: ValueType> ValueType for MapType<K, V> {
    type Scalar = <MapInternal<K, V> as ValueType>::Scalar;
    type ScalarRef<'a> = <MapInternal<K, V> as ValueType>::ScalarRef<'a>;
    type Column = <MapInternal<K, V> as ValueType>::Column;
    type Domain = Option<(K::Domain, V::Domain)>;
    type ColumnIterator<'a> = <MapInternal<K, V> as ValueType>::ColumnIterator<'a>;
    type ColumnBuilder = <MapInternal<K, V> as ValueType>::ColumnBuilder;

    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: Self::ScalarRef<'long>) -> Self::ScalarRef<'short> {
        <MapInternal<K, V> as ValueType>::upcast_gat(long)
    }

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        <MapInternal<K, V> as ValueType>::to_owned_scalar(scalar)
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        <MapInternal<K, V> as ValueType>::to_scalar_ref(scalar)
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        match scalar {
            ScalarRef::Map(array) => KvPair::<K, V>::try_downcast_column(array),
            _ => None,
        }
    }

    fn try_downcast_column(col: &Column) -> Option<Self::Column> {
        ArrayColumn::try_downcast(col.as_map()?)
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        match domain {
            Domain::Map(Some(domain)) => Some(Some(KvPair::<K, V>::try_downcast_domain(domain)?)),
            Domain::Map(None) => Some(None),
            _ => None,
        }
    }

    fn try_downcast_builder(builder: &mut ColumnBuilder) -> Option<&mut Self::ColumnBuilder> {
        <MapInternal<K, V> as ValueType>::try_downcast_builder(builder)
    }

    fn try_downcast_owned_builder<'a>(builder: ColumnBuilder) -> Option<Self::ColumnBuilder> {
        <MapInternal<K, V> as ValueType>::try_downcast_owned_builder(builder)
    }

    fn try_upcast_column_builder(
        builder: Self::ColumnBuilder,
        decimal_size: Option<DecimalSize>,
    ) -> Option<ColumnBuilder> {
        <MapInternal<K, V> as ValueType>::try_upcast_column_builder(builder, decimal_size)
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        Scalar::Map(KvPair::<K, V>::upcast_column(scalar))
    }

    fn upcast_column(col: Self::Column) -> Column {
        Column::Map(Box::new(col.upcast()))
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        Domain::Map(
            domain.map(|domain| Box::new(<KvPair<K, V> as ValueType>::upcast_domain(domain))),
        )
    }

    fn column_len(col: &Self::Column) -> usize {
        <MapInternal<K, V> as ValueType>::column_len(col)
    }

    fn index_column(col: &Self::Column, index: usize) -> Option<Self::ScalarRef<'_>> {
        <MapInternal<K, V> as ValueType>::index_column(col, index)
    }

    #[inline(always)]
    unsafe fn index_column_unchecked(col: &Self::Column, index: usize) -> Self::ScalarRef<'_> {
        <MapInternal<K, V> as ValueType>::index_column_unchecked(col, index)
    }

    fn slice_column(col: &Self::Column, range: Range<usize>) -> Self::Column {
        <MapInternal<K, V> as ValueType>::slice_column(col, range)
    }

    fn iter_column(col: &Self::Column) -> Self::ColumnIterator<'_> {
        <MapInternal<K, V> as ValueType>::iter_column(col)
    }

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        <MapInternal<K, V> as ValueType>::column_to_builder(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        <MapInternal<K, V> as ValueType>::builder_len(builder)
    }

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>) {
        <MapInternal<K, V> as ValueType>::push_item(builder, item)
    }

    fn push_item_repeat(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>, n: usize) {
        <MapInternal<K, V> as ValueType>::push_item_repeat(builder, item, n)
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        <MapInternal<K, V> as ValueType>::push_default(builder)
    }

    fn append_column(builder: &mut Self::ColumnBuilder, other: &Self::Column) {
        <MapInternal<K, V> as ValueType>::append_column(builder, other)
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        <MapInternal<K, V> as ValueType>::build_column(builder)
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        <MapInternal<K, V> as ValueType>::build_scalar(builder)
    }

    fn scalar_memory_size(scalar: &Self::ScalarRef<'_>) -> usize {
        <MapInternal<K, V> as ValueType>::scalar_memory_size(scalar)
    }

    fn column_memory_size(col: &Self::Column) -> usize {
        <MapInternal<K, V> as ValueType>::column_memory_size(col)
    }
}

impl<K: ArgType, V: ArgType> ArgType for MapType<K, V> {
    fn data_type() -> DataType {
        let inner_ty = DataType::Tuple(vec![K::data_type(), V::data_type()]);
        DataType::Map(Box::new(inner_ty))
    }

    fn full_domain() -> Self::Domain {
        Some((K::full_domain(), V::full_domain()))
    }

    fn create_builder(capacity: usize, generics: &GenericMap) -> Self::ColumnBuilder {
        <MapInternal<K, V> as ArgType>::create_builder(capacity, generics)
    }
}
