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

mod coord;
mod geo_trait;
mod linestring;
mod point;
mod utils;

use std::ops::Range;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_arrow::arrow::array::Array;
use databend_common_arrow::arrow::datatypes::DataType as ArrowDataType;
use databend_common_exception::Result;
use enum_as_inner::EnumAsInner;
use serde::Deserialize;
use serde::Serialize;

use crate::types::decimal::Decimal;
use crate::types::geo::geo_trait::AsArrow;
use crate::types::geo::geo_trait::GeometryColumnAccessor;
use crate::types::geo::linestring::LineStringColumn;
use crate::types::geo::linestring::LineStringColumnBuilder;
use crate::types::geo::linestring::LineStringColumnIterator;
use crate::types::geo::linestring::LineStringScalar;
use crate::types::geo::point::PointColumn;
use crate::types::geo::point::PointColumnBuilder;
use crate::types::geo::point::PointColumnIterator;
use crate::types::geo::point::PointScalar;
use crate::types::geo::utils::line_string_data_type;
use crate::types::geo::utils::point_data_type;
use crate::types::GeometryDataType::LineString;
use crate::types::GeometryDataType::Point;
use crate::types::ValueType;
use crate::Column;
use crate::ColumnBuilder;
use crate::Domain;
use crate::Scalar;
use crate::ScalarRef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeometryType;

impl ValueType for GeometryType {
    type Scalar = GeometryScalar;
    type ScalarRef<'a> = GeometryScalar;
    type Column = GeometryColumn;
    type Domain = ();
    type ColumnIterator<'a> = GeometryColumnIterator<'a>;
    type ColumnBuilder = GeometryColumnBuilder;

    fn upcast_gat<'short, 'long: 'short>(long: Self::ScalarRef<'long>) -> Self::ScalarRef<'short> {
        long
    }

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        scalar
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        *scalar
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        match scalar {
            ScalarRef::Geometry(scalar) => Some(*scalar),
            _ => None,
        }
    }

    fn try_downcast_column(col: &Column) -> Option<Self::Column> {
        match col {
            Column::Geometry(column) => Some(column.clone()),
            _ => None,
        }
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        todo!()
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

    fn try_upcast_column_builder(builder: Self::ColumnBuilder) -> Option<ColumnBuilder> {
        Some(ColumnBuilder::Geometry(builder))
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        Scalar::Geometry(scalar)
    }

    fn upcast_column(col: Self::Column) -> Column {
        Column::Geometry(col)
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        todo!()
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
        GeometryColumnBuilder::from_column(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>) {
        builder.push(item)
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        builder.push_default()
    }

    fn append_column(builder: &mut Self::ColumnBuilder, other: &Self::Column) {
        builder.append_column(other)
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.build()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        builder.build_scalar()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, EnumAsInner)]
pub enum GeometryDataType {
    Point,
    LineString,
}

impl GeometryDataType {
    pub fn to_arrow2_type(&self) -> ArrowDataType {
        use GeometryDataType::*;
        match self {
            Point => point_data_type(),
            LineString => line_string_data_type(),
        }
    }

    pub fn extension_name(&self) -> &'static str {
        use GeometryDataType::*;
        match self {
            Point => "geoarrow.point",
            LineString => "geoarrow.linestring",
        }
    }

    pub fn default_scalar(&self) -> GeometryScalar {
        use GeometryDataType::*;
        match self {
            Point => GeometryScalar::Point(PointScalar::default()),
            LineString => todo!(),
        }
    }
}

#[derive(Clone, PartialEq, EnumAsInner)]
pub enum GeometryColumn {
    Point(PointColumn),
    LineString(LineStringColumn),
}

impl GeometryColumn {
    pub fn len(&self) -> usize {
        match self {
            GeometryColumn::Point(p) => p.len(),
            GeometryColumn::LineString(ls) => ls.len(),
        }
    }

    pub fn index(&self, index: usize) -> Option<GeometryScalar> {
        crate::with_geometry_type!(|GEO_TYPE| match self {
            GeometryColumn::GEO_TYPE(g) => g.get(index).map(|s| GeometryScalar::GEO_TYPE(s)),
        })
    }

    pub fn index_unchecked(&self, index: usize) -> GeometryScalar {
        crate::with_geometry_type!(|GEO_TYPE| match self {
            GeometryColumn::GEO_TYPE(g) =>
                GeometryScalar::GEO_TYPE(unsafe { g.get_unchecked(index) }),
        })
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        crate::with_geometry_type!(|GEO_TYPE| match self {
            GeometryColumn::GEO_TYPE(g) => GeometryColumn::GEO_TYPE(g.slice(range)),
        })
    }

    pub fn memory_size(&self) -> usize {
        crate::with_geometry_type!(|GEO_TYPE| match self {
            GeometryColumn::GEO_TYPE(g) => g.memroy_size(),
        })
    }

    pub fn serialize_size(&self) -> usize {
        crate::with_geometry_type!(|GEO_TYPE| match self {
            GeometryColumn::GEO_TYPE(g) => g.memroy_size(),
        })
    }

    pub fn iter(&self) -> GeometryColumnIterator {
        crate::with_geometry_type!(|GEO_TYPE| match self {
            GeometryColumn::GEO_TYPE(g) => GeometryColumnIterator::GEO_TYPE(g.iter()),
        })
    }
}

impl AsArrow for GeometryColumn {
    fn as_arrow(&self, arrow_type: ArrowDataType) -> Box<dyn Array> {
        match self {
            GeometryColumn::Point(p) => p.as_arrow(arrow_type),
            GeometryColumn::LineString(ls) => ls.as_arrow(arrow_type),
        }
    }
}

// Geometry Scalar
// TODO(ariesdevil): add serde support
#[derive(Clone, Copy, PartialEq, Eq, EnumAsInner)]
pub enum GeometryScalar {
    Point(PointScalar),
    LineString(LineStringScalar),
}

impl GeometryScalar {
    pub fn memory_size(&self) -> usize {
        use GeometryScalar::*;
        match self {
            Point(p) => 2 * 8,
            LineString(ls) => todo!(),
        }
    }

    pub fn data_type(&self) -> GeometryDataType {
        crate::with_geometry_type!(|GEO_TYPE| match self {
            GeometryScalar::GEO_TYPE(_) => GeometryDataType::GEO_TYPE,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, EnumAsInner)]
pub enum GeometryColumnBuilder {
    Point(PointColumnBuilder),
    LineString(LineStringColumnBuilder),
}

impl GeometryColumnBuilder {
    pub fn from_column(col: GeometryColumn) -> Self {
        match col {
            GeometryColumn::Point(p) => {
                GeometryColumnBuilder::Point(PointColumnBuilder::from_column(p))
            }
            GeometryColumn::LineString(ls) => {
                GeometryColumnBuilder::LineString(LineStringColumnBuilder::from_column(ls))
            }
        }
    }

    pub fn repeat(scalar: GeometryScalar, n: usize) -> Self {
        match scalar {
            GeometryScalar::Point(p) => {
                GeometryColumnBuilder::Point(PointColumnBuilder::repeat(p, n))
            }
            GeometryScalar::LineString(ls) => {
                GeometryColumnBuilder::LineString(LineStringColumnBuilder::repeat(ls, n))
            }
        }
    }
    pub fn len(&self) -> usize {
        crate::with_geometry_type!(|GEO_TYPE| match self {
            GeometryColumnBuilder::GEO_TYPE(builder) => builder.len(),
        })
    }

    pub fn memory_size(&self) -> usize {
        crate::with_geometry_type!(|GEO_TYPE| match self {
            GeometryColumnBuilder::GEO_TYPE(builder) => builder.memory_size(),
        })
    }

    pub fn with_capacity(ty: &GeometryDataType, capacity: usize) -> Self {
        match ty {
            GeometryDataType::Point => {
                GeometryColumnBuilder::Point(PointColumnBuilder::with_capacity(capacity))
            }
            GeometryDataType::LineString => {
                GeometryColumnBuilder::LineString(LineStringColumnBuilder::with_capacity(capacity))
            }
        }
    }

    pub fn push(&mut self, item: GeometryScalar) {
        crate::with_geometry_type!(|GEO_TYPE| match (self, item) {
            (GeometryColumnBuilder::GEO_TYPE(builder), GeometryScalar::GEO_TYPE(scalar)) =>
                builder.push(scalar),
            (builder, scalar) => unreachable!("unable to push {scalar:?} to {builder:?}"),
        })
    }

    pub fn push_default(&mut self) {
        crate::with_geometry_type!(|GEO_TYPE| match self {
            GeometryColumnBuilder::GEO_TYPE(builder) => builder.push_default(),
        })
    }

    pub fn push_binary(&mut self, bytes: &mut &[u8]) -> Result<()> {
        crate::with_geometry_type!(|GEO_TYPE| match self {
            GeometryColumnBuilder::GEO_TYPE(builder) => builder.push_binary(bytes),
        })
    }

    pub fn pop(&mut self) -> Option<GeometryScalar> {
        crate::with_geometry_type!(|GEO_TYPE| match self {
            GeometryColumnBuilder::GEO_TYPE(builder) =>
                builder.pop().map(|scalar| GeometryScalar::GEO_TYPE(scalar)),
        })
    }

    pub fn append_column(&mut self, other: &GeometryColumn) {
        crate::with_geometry_type!(|GEO_TYPE| match (self, other) {
            (GeometryColumnBuilder::GEO_TYPE(builder), GeometryColumn::GEO_TYPE(col)) =>
                builder.append_column(col),
            (builder, column) => unreachable!("unable to append column {column:?} to {builder:?}"),
        })
    }

    pub fn build(self) -> GeometryColumn {
        crate::with_geometry_type!(|GEO_TYPE| match self {
            GeometryColumnBuilder::GEO_TYPE(builder) => GeometryColumn::GEO_TYPE(builder.build()),
        })
    }

    pub fn build_scalar(self) -> GeometryScalar {
        crate::with_geometry_type!(|GEO_TYPE| match self {
            GeometryColumnBuilder::GEO_TYPE(builder) =>
                GeometryScalar::GEO_TYPE(builder.build_scalar()),
        })
    }
}

pub enum GeometryColumnIterator<'a> {
    Point(PointColumnIterator<'a>),
    LineString(LineStringColumnIterator<'a>),
}
