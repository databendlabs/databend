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

use std::cmp::Ordering;
use std::iter::TrustedLen;
use std::marker::PhantomData;
use std::ops::Range;

use databend_common_exception::Result;

use super::array::ArrayColumn;
use super::array::ArrayColumnBuilderMut;
use super::column_type_error;
use super::domain_type_error;
use super::scalar_type_error;
use super::AccessType;
use super::ArgType;
use super::ArrayType;
use super::BuilderExt;
use super::DataType;
use super::GenericMap;
use super::ReturnType;
use super::Scalar;
use super::ScalarRef;
use super::ValueType;
use crate::property::Domain;
use crate::values::Column;
use crate::ColumnBuilder;

// Structurally equals to `Tuple(K, V)`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvPair<K, V>(PhantomData<(K, V)>);

impl<K: AccessType, V: AccessType> AccessType for KvPair<K, V> {
    type Scalar = (K::Scalar, V::Scalar);
    type ScalarRef<'a> = (K::ScalarRef<'a>, V::ScalarRef<'a>);
    type Column = KvColumn<K, V>;
    type Domain = (K::Domain, V::Domain);
    type ColumnIterator<'a> = KvIterator<'a, K, V>;

    fn to_owned_scalar((k, v): Self::ScalarRef<'_>) -> Self::Scalar {
        (K::to_owned_scalar(k), V::to_owned_scalar(v))
    }

    fn to_scalar_ref((k, v): &Self::Scalar) -> Self::ScalarRef<'_> {
        (K::to_scalar_ref(k), V::to_scalar_ref(v))
    }

    fn try_downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Result<Self::ScalarRef<'a>> {
        match scalar {
            ScalarRef::Tuple(fields) if fields.len() == 2 => Ok((
                K::try_downcast_scalar(&fields[0])?,
                V::try_downcast_scalar(&fields[1])?,
            )),
            _ => Err(scalar_type_error::<Self>(scalar)),
        }
    }

    fn try_downcast_column(col: &Column) -> Result<Self::Column> {
        match col {
            Column::Tuple(fields) if fields.len() == 2 => Ok(KvColumn {
                keys: K::try_downcast_column(&fields[0])?,
                values: V::try_downcast_column(&fields[1])?,
            }),
            _ => Err(column_type_error::<Self>(col)),
        }
    }

    fn try_downcast_domain(domain: &Domain) -> Result<Self::Domain> {
        match domain {
            Domain::Tuple(fields) if fields.len() == 2 => Ok((
                K::try_downcast_domain(&fields[0])?,
                V::try_downcast_domain(&fields[1])?,
            )),
            _ => Err(domain_type_error::<Self>(domain)),
        }
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

    fn scalar_memory_size((k, v): &Self::ScalarRef<'_>) -> usize {
        K::scalar_memory_size(k) + V::scalar_memory_size(v)
    }

    fn column_memory_size(col: &Self::Column) -> usize {
        col.memory_size()
    }

    #[inline(always)]
    fn compare(
        (lhs_k, lhs_v): Self::ScalarRef<'_>,
        (rhs_k, rhs_v): Self::ScalarRef<'_>,
    ) -> Ordering {
        match K::compare(lhs_k, rhs_k) {
            Ordering::Equal => V::compare(lhs_v, rhs_v),
            ord => ord,
        }
    }
}

impl<K: ValueType, V: ValueType> ValueType for KvPair<K, V> {
    type ColumnBuilder = KvColumnBuilder<K, V>;
    type ColumnBuilderMut<'a> = KvColumnBuilderMut<'a, K, V>;

    fn upcast_scalar_with_type(scalar: Self::Scalar, data_type: &DataType) -> Scalar {
        match data_type {
            DataType::Tuple(fields) if fields.len() == 2 => {
                let (key_type, value_type) = (&fields[0], &fields[1]);
                Scalar::Tuple(vec![
                    K::upcast_scalar_with_type(scalar.0, key_type),
                    V::upcast_scalar_with_type(scalar.1, value_type),
                ])
            }
            _ => unreachable!(),
        }
    }

    fn upcast_domain_with_type(domain: Self::Domain, data_type: &DataType) -> Domain {
        match data_type {
            DataType::Tuple(fields) if fields.len() == 2 => {
                let (key_type, value_type) = (&fields[0], &fields[1]);
                Domain::Tuple(vec![
                    K::upcast_domain_with_type(domain.0, key_type),
                    V::upcast_domain_with_type(domain.1, value_type),
                ])
            }
            _ => unreachable!(),
        }
    }

    fn upcast_column_with_type(col: Self::Column, data_type: &DataType) -> Column {
        match data_type {
            DataType::Tuple(fields) if fields.len() == 2 => {
                let (key_type, value_type) = (&fields[0], &fields[1]);
                Column::Tuple(vec![
                    K::upcast_column_with_type(col.keys, key_type),
                    V::upcast_column_with_type(col.values, value_type),
                ])
            }
            _ => unreachable!(),
        }
    }

    fn downcast_builder(builder: &mut ColumnBuilder) -> Self::ColumnBuilderMut<'_> {
        match builder {
            ColumnBuilder::Tuple(kv) => {
                let [k, v] = kv.as_mut_array().unwrap();
                KvColumnBuilderMut {
                    keys: K::downcast_builder(k),
                    values: V::downcast_builder(v),
                }
            }
            _ => unreachable!(),
        }
    }

    fn try_upcast_column_builder(
        _builder: Self::ColumnBuilder,
        _data_type: &DataType,
    ) -> Option<ColumnBuilder> {
        None
    }

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        KvColumnBuilder::from_column(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn builder_len_mut(builder: &Self::ColumnBuilderMut<'_>) -> usize {
        K::builder_len_mut(&builder.keys)
    }

    fn push_item_mut(builder: &mut Self::ColumnBuilderMut<'_>, item: Self::ScalarRef<'_>) {
        builder.push(item);
    }

    fn push_item_repeat_mut(
        builder: &mut Self::ColumnBuilderMut<'_>,
        item: Self::ScalarRef<'_>,
        n: usize,
    ) {
        builder.push_repeat(item, n);
    }

    fn push_default_mut(builder: &mut Self::ColumnBuilderMut<'_>) {
        builder.push_default();
    }

    fn append_column_mut(builder: &mut Self::ColumnBuilderMut<'_>, other: &Self::Column) {
        builder.append_column(other);
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

    fn full_domain() -> Self::Domain {
        (K::full_domain(), V::full_domain())
    }
}

impl<K: ReturnType, V: ReturnType> ReturnType for KvPair<K, V> {
    fn create_builder(capacity: usize, generics: &GenericMap) -> Self::ColumnBuilder {
        KvColumnBuilder::with_capacity(capacity, generics)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct KvColumn<K: AccessType, V: AccessType> {
    pub keys: K::Column,
    pub values: V::Column,
}

impl<K: AccessType, V: AccessType> KvColumn<K, V> {
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
    pub(crate) keys: K::ColumnBuilder,
    pub(crate) values: V::ColumnBuilder,
}

#[derive(Debug)]
pub struct KvColumnBuilderMut<'a, K: ValueType, V: ValueType> {
    keys: K::ColumnBuilderMut<'a>,
    values: V::ColumnBuilderMut<'a>,
}

impl<'a, K: ValueType, V: ValueType> From<&'a mut KvColumnBuilder<K, V>>
    for KvColumnBuilderMut<'a, K, V>
{
    fn from(value: &'a mut KvColumnBuilder<K, V>) -> Self {
        Self {
            keys: (&mut value.keys).into(),
            values: (&mut value.values).into(),
        }
    }
}

impl<K: ValueType, V: ValueType> BuilderExt<KvPair<K, V>> for KvColumnBuilderMut<'_, K, V> {
    fn len(&self) -> usize {
        self.keys.len()
    }

    fn push_item(&mut self, item: <KvPair<K, V> as AccessType>::ScalarRef<'_>) {
        self.push(item);
    }

    fn push_repeat(&mut self, item: <KvPair<K, V> as AccessType>::ScalarRef<'_>, n: usize) {
        self.push_repeat(item, n);
    }

    fn push_default(&mut self) {
        self.push_default();
    }

    fn append_column(&mut self, other: &<KvPair<K, V> as AccessType>::Column) {
        self.append_column(other);
    }
}

impl<K: ValueType, V: ValueType> KvColumnBuilder<K, V> {
    pub fn as_mut(&mut self) -> KvColumnBuilderMut<'_, K, V> {
        self.into()
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

    pub fn push(&mut self, item: (K::ScalarRef<'_>, V::ScalarRef<'_>)) {
        self.as_mut().push(item);
    }

    pub fn push_repeat(&mut self, item: (K::ScalarRef<'_>, V::ScalarRef<'_>), n: usize) {
        self.as_mut().push_repeat(item, n);
    }

    pub fn push_default(&mut self) {
        self.as_mut().push_default();
    }

    pub fn append_column(&mut self, other: &KvColumn<K, V>) {
        self.as_mut().append_column(other);
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

impl<'a, K: ValueType, V: ValueType> KvColumnBuilderMut<'a, K, V> {
    pub fn push(&mut self, (k, v): (K::ScalarRef<'_>, V::ScalarRef<'_>)) {
        K::push_item_mut(&mut self.keys, k);
        V::push_item_mut(&mut self.values, v);
    }

    pub fn push_repeat(&mut self, (k, v): (K::ScalarRef<'_>, V::ScalarRef<'_>), n: usize) {
        K::push_item_repeat_mut(&mut self.keys, k, n);
        V::push_item_repeat_mut(&mut self.values, v, n);
    }

    pub fn push_default(&mut self) {
        K::push_default_mut(&mut self.keys);
        V::push_default_mut(&mut self.values);
    }

    pub fn append_column(&mut self, other: &KvColumn<K, V>) {
        K::append_column_mut(&mut self.keys, &other.keys);
        V::append_column_mut(&mut self.values, &other.values);
    }
}

impl<K: ReturnType, V: ReturnType> KvColumnBuilder<K, V> {
    pub fn with_capacity(capacity: usize, generics: &GenericMap) -> Self {
        Self {
            keys: K::create_builder(capacity, generics),
            values: V::create_builder(capacity, generics),
        }
    }
}

pub struct KvIterator<'a, K: AccessType, V: AccessType> {
    keys: K::ColumnIterator<'a>,
    values: V::ColumnIterator<'a>,
}

impl<'a, K: AccessType, V: AccessType> Iterator for KvIterator<'a, K, V> {
    type Item = (K::ScalarRef<'a>, V::ScalarRef<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        Some((self.keys.next()?, self.values.next()?))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        assert_eq!(self.keys.size_hint(), self.values.size_hint());
        self.keys.size_hint()
    }
}

unsafe impl<K: AccessType, V: AccessType> TrustedLen for KvIterator<'_, K, V> {}

// Structurally equals to `Array(Tuple(K, V))` but treated distinct from `Array(Tuple(K, V))`
// in unification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapType<K, V>(PhantomData<(K, V)>);

pub type MapInternal<K, V> = ArrayType<KvPair<K, V>>;

impl<K: AccessType, V: AccessType> AccessType for MapType<K, V> {
    type Scalar = <MapInternal<K, V> as AccessType>::Scalar;
    type ScalarRef<'a> = <MapInternal<K, V> as AccessType>::ScalarRef<'a>;
    type Column = <MapInternal<K, V> as AccessType>::Column;
    type Domain = Option<(K::Domain, V::Domain)>;
    type ColumnIterator<'a> = <MapInternal<K, V> as AccessType>::ColumnIterator<'a>;

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        MapInternal::<K, V>::to_owned_scalar(scalar)
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        MapInternal::<K, V>::to_scalar_ref(scalar)
    }

    fn try_downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Result<Self::ScalarRef<'a>> {
        match scalar {
            ScalarRef::Map(array) => KvPair::<K, V>::try_downcast_column(array),
            _ => Err(scalar_type_error::<Self>(scalar)),
        }
    }

    fn try_downcast_column(col: &Column) -> Result<Self::Column> {
        let map = col.as_map().ok_or_else(|| column_type_error::<Self>(col))?;
        ArrayColumn::try_downcast(map)
    }

    fn try_downcast_domain(domain: &Domain) -> Result<Self::Domain> {
        match domain {
            Domain::Map(Some(domain)) => Ok(Some(KvPair::<K, V>::try_downcast_domain(domain)?)),
            Domain::Map(None) => Ok(None),
            _ => Err(domain_type_error::<Self>(domain)),
        }
    }

    fn column_len(col: &Self::Column) -> usize {
        MapInternal::<K, V>::column_len(col)
    }

    fn index_column(col: &Self::Column, index: usize) -> Option<Self::ScalarRef<'_>> {
        MapInternal::<K, V>::index_column(col, index)
    }

    #[inline(always)]
    unsafe fn index_column_unchecked(col: &Self::Column, index: usize) -> Self::ScalarRef<'_> {
        MapInternal::<K, V>::index_column_unchecked(col, index)
    }

    fn slice_column(col: &Self::Column, range: Range<usize>) -> Self::Column {
        MapInternal::<K, V>::slice_column(col, range)
    }

    fn iter_column(col: &Self::Column) -> Self::ColumnIterator<'_> {
        MapInternal::<K, V>::iter_column(col)
    }

    fn scalar_memory_size(scalar: &Self::ScalarRef<'_>) -> usize {
        MapInternal::<K, V>::scalar_memory_size(scalar)
    }

    fn column_memory_size(col: &Self::Column) -> usize {
        MapInternal::<K, V>::column_memory_size(col)
    }

    #[inline(always)]
    fn compare(lhs: Self::ScalarRef<'_>, rhs: Self::ScalarRef<'_>) -> std::cmp::Ordering {
        MapInternal::<K, V>::compare(lhs, rhs)
    }
}

impl<K: ValueType, V: ValueType> ValueType for MapType<K, V> {
    type ColumnBuilder = <MapInternal<K, V> as ValueType>::ColumnBuilder;
    type ColumnBuilderMut<'a> = ArrayColumnBuilderMut<'a, KvPair<K, V>>;

    fn upcast_scalar_with_type(scalar: Self::Scalar, data_type: &DataType) -> Scalar {
        let data_type = data_type.as_map().unwrap();
        Scalar::Map(KvPair::<K, V>::upcast_column_with_type(scalar, data_type))
    }

    fn upcast_domain_with_type(domain: Self::Domain, data_type: &DataType) -> Domain {
        let data_type = data_type.as_map().unwrap();
        Domain::Map(
            domain
                .map(|domain| Box::new(KvPair::<K, V>::upcast_domain_with_type(domain, data_type))),
        )
    }

    fn upcast_column_with_type(col: Self::Column, data_type: &DataType) -> Column {
        let data_type = DataType::Array(data_type.as_map().unwrap().to_owned());
        Column::Map(Box::new(col.upcast(&data_type)))
    }

    fn downcast_builder(builder: &mut ColumnBuilder) -> Self::ColumnBuilderMut<'_> {
        let any_array = builder.as_map_mut().unwrap();
        ArrayColumnBuilderMut {
            builder: KvPair::<K, V>::downcast_builder(&mut any_array.builder),
            offsets: &mut any_array.offsets,
        }
    }

    fn try_upcast_column_builder(
        builder: Self::ColumnBuilder,
        data_type: &DataType,
    ) -> Option<ColumnBuilder> {
        MapInternal::<K, V>::try_upcast_column_builder(builder, data_type)
    }

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        MapInternal::<K, V>::column_to_builder(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        MapInternal::<K, V>::builder_len(builder)
    }

    fn builder_len_mut(builder: &Self::ColumnBuilderMut<'_>) -> usize {
        MapInternal::<K, V>::builder_len_mut(builder)
    }

    fn push_item_mut(builder: &mut Self::ColumnBuilderMut<'_>, item: Self::ScalarRef<'_>) {
        MapInternal::<K, V>::push_item_mut(builder, item)
    }

    fn push_item_repeat_mut(
        builder: &mut Self::ColumnBuilderMut<'_>,
        item: Self::ScalarRef<'_>,
        n: usize,
    ) {
        MapInternal::<K, V>::push_item_repeat_mut(builder, item, n)
    }

    fn push_default_mut(builder: &mut Self::ColumnBuilderMut<'_>) {
        MapInternal::<K, V>::push_default_mut(builder)
    }

    fn append_column_mut(builder: &mut Self::ColumnBuilderMut<'_>, other: &Self::Column) {
        MapInternal::<K, V>::append_column_mut(builder, other)
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        MapInternal::<K, V>::build_column(builder)
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        MapInternal::<K, V>::build_scalar(builder)
    }
}

impl<K: ValueType, V: ValueType> BuilderExt<MapType<K, V>>
    for ArrayColumnBuilderMut<'_, KvPair<K, V>>
{
    fn len(&self) -> usize {
        self.len()
    }

    fn push_item(&mut self, item: <MapType<K, V> as AccessType>::ScalarRef<'_>) {
        self.push(item);
    }

    fn push_repeat(&mut self, item: <MapType<K, V> as AccessType>::ScalarRef<'_>, n: usize) {
        self.push_repeat(&item, n);
    }

    fn push_default(&mut self) {
        self.push_default();
    }

    fn append_column(&mut self, other: &<MapType<K, V> as AccessType>::Column) {
        self.append_column(other);
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
}

impl<K: ReturnType, V: ReturnType> ReturnType for MapType<K, V> {
    fn create_builder(capacity: usize, generics: &GenericMap) -> Self::ColumnBuilder {
        MapInternal::<K, V>::create_builder(capacity, generics)
    }
}
