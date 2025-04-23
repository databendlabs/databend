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
use std::ops::Range;

use geozero::wkb::FromWkb;
use geozero::wkb::WkbDialect;
use geozero::wkt::Ewkt;

use super::binary::BinaryColumn;
use super::binary::BinaryColumnBuilder;
use super::binary::BinaryColumnIter;
use crate::property::Domain;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::DecimalSize;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::values::Column;
use crate::values::Scalar;
use crate::values::ScalarRef;
use crate::ColumnBuilder;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeometryType;

impl ValueType for GeometryType {
    type Scalar = Vec<u8>;
    type ScalarRef<'a> = &'a [u8];
    type Column = BinaryColumn;
    type Domain = ();
    type ColumnIterator<'a> = BinaryColumnIter<'a>;
    type ColumnBuilder = BinaryColumnBuilder;

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        scalar.to_vec()
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        scalar
    }

    fn try_downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Option<Self::ScalarRef<'a>> {
        scalar.as_geometry().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Self::Column> {
        col.as_geometry().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        if domain.is_undefined() {
            Some(())
        } else {
            None
        }
    }

    fn try_downcast_builder(builder: &mut ColumnBuilder) -> Option<&mut Self::ColumnBuilder> {
        match builder {
            ColumnBuilder::Geometry(builder) => Some(builder),
            _ => None,
        }
    }

    fn try_downcast_owned_builder(builder: ColumnBuilder) -> Option<Self::ColumnBuilder> {
        match builder {
            ColumnBuilder::Geometry(builder) => Some(builder),
            _ => None,
        }
    }

    fn try_upcast_column_builder(
        builder: Self::ColumnBuilder,
        _decimal_size: Option<DecimalSize>,
    ) -> Option<ColumnBuilder> {
        Some(ColumnBuilder::Geometry(builder))
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        Scalar::Geometry(scalar)
    }

    fn upcast_column(col: Self::Column) -> Column {
        Column::Geometry(col)
    }

    fn upcast_domain(_domain: Self::Domain) -> Domain {
        Domain::Undefined
    }

    fn column_len(col: &Self::Column) -> usize {
        col.len()
    }

    fn index_column(col: &Self::Column, index: usize) -> Option<Self::ScalarRef<'_>> {
        col.index(index)
    }

    #[inline(always)]
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
        BinaryColumnBuilder::from_column(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>) {
        builder.put_slice(item);
        builder.commit_row();
    }

    fn push_item_repeat(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>, n: usize) {
        builder.push_repeat(item, n)
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        builder.commit_row();
    }

    fn append_column(builder: &mut Self::ColumnBuilder, other_builder: &Self::Column) {
        builder.append_column(other_builder)
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.build()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        builder.build_scalar()
    }

    fn scalar_memory_size(scalar: &Self::ScalarRef<'_>) -> usize {
        scalar.len()
    }

    fn column_memory_size(col: &Self::Column) -> usize {
        col.memory_size()
    }

    #[inline(always)]
    fn compare(lhs: Self::ScalarRef<'_>, rhs: Self::ScalarRef<'_>) -> Ordering {
        lhs.cmp(rhs)
    }
}

impl ArgType for GeometryType {
    fn data_type() -> DataType {
        DataType::Geometry
    }

    fn full_domain() -> Self::Domain {}

    fn create_builder(capacity: usize, _: &GenericMap) -> Self::ColumnBuilder {
        BinaryColumnBuilder::with_capacity(capacity, 0)
    }
}

pub(crate) fn compare_geometry(left: &[u8], right: &[u8]) -> Option<Ordering> {
    let mut left_data = std::io::Cursor::new(left);
    let mut right_data = std::io::Cursor::new(right);
    let lres = Ewkt::from_wkb(&mut left_data, WkbDialect::Ewkb);
    let rres = Ewkt::from_wkb(&mut right_data, WkbDialect::Ewkb);
    match (lres, rres) {
        (Ok(lval), Ok(rval)) => lval.0.partial_cmp(&rval.0),
        (Ok(_), Err(_)) => Some(Ordering::Greater),
        (Err(_), Ok(_)) => Some(Ordering::Less),
        (Err(_), Err(_)) => Some(left.cmp(right)),
    }
}
