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

use std::ops::Range;

use crate::property::Domain;
use crate::types::string::StringColumn;
use crate::types::string::StringColumnBuilder;
use crate::types::string::StringIterator;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::values::Column;
use crate::values::Scalar;
use crate::values::ScalarRef;
use crate::ColumnBuilder;

/// JSONB bytes representation of `null`.
pub const DEFAULT_JSONB: &[u8] = &[0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VariantType;

impl ValueType for VariantType {
    type Scalar = Vec<u8>;
    type ScalarRef<'a> = &'a [u8];
    type Column = StringColumn;
    type Domain = ();
    type ColumnIterator<'a> = StringIterator<'a>;
    type ColumnBuilder = StringColumnBuilder;

    fn to_owned_scalar<'a>(scalar: Self::ScalarRef<'a>) -> Self::Scalar {
        scalar.to_vec()
    }

    fn to_scalar_ref<'a>(scalar: &'a Self::Scalar) -> Self::ScalarRef<'a> {
        scalar
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        scalar.as_variant().cloned()
    }

    fn try_downcast_column<'a>(col: &'a Column) -> Option<Self::Column> {
        col.as_variant().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        if domain.is_undefined() {
            Some(())
        } else {
            None
        }
    }

    fn try_downcast_builder<'a>(
        builder: &'a mut ColumnBuilder,
    ) -> Option<&'a mut Self::ColumnBuilder> {
        match builder {
            crate::ColumnBuilder::Variant(builder) => Some(builder),
            _ => None,
        }
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        Scalar::Variant(scalar)
    }

    fn upcast_column(col: Self::Column) -> Column {
        Column::Variant(col)
    }

    fn upcast_domain(_domain: Self::Domain) -> Domain {
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
        StringColumnBuilder::from_column(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>) {
        builder.put_slice(item);
        builder.commit_row();
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        builder.put_slice(DEFAULT_JSONB);
        builder.commit_row();
    }

    fn append_builder(builder: &mut Self::ColumnBuilder, other_builder: &Self::ColumnBuilder) {
        builder.append(other_builder)
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.build()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        builder.build_scalar()
    }
}

impl ArgType for VariantType {
    fn data_type() -> DataType {
        DataType::Variant
    }

    fn create_builder(capacity: usize, _: &GenericMap) -> Self::ColumnBuilder {
        StringColumnBuilder::with_capacity(capacity, 0)
    }
}
