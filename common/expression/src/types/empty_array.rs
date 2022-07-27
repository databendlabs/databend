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
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::values::Column;
use crate::values::Scalar;
use crate::ScalarRef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmptyArrayType;

impl ValueType for EmptyArrayType {
    type Scalar = ();
    type ScalarRef<'a> = ();
    type Column = usize;
    type Domain = ();
    type ColumnIterator<'a> = std::iter::Take<std::iter::Repeat<()>>;
    type ColumnBuilder = usize;

    fn to_owned_scalar<'a>(scalar: Self::ScalarRef<'a>) -> Self::Scalar {
        scalar
    }

    fn to_scalar_ref<'a>(scalar: &'a Self::Scalar) -> Self::ScalarRef<'a> {
        *scalar
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        match scalar {
            ScalarRef::EmptyArray => Some(()),
            _ => None,
        }
    }

    fn try_downcast_column<'a>(col: &'a Column) -> Option<Self::Column> {
        match col {
            Column::EmptyArray { len } => Some(*len),
            _ => None,
        }
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        match domain {
            Domain::Array(None) => Some(()),
            _ => None,
        }
    }

    fn upcast_scalar(_: Self::Scalar) -> Scalar {
        Scalar::EmptyArray
    }

    fn upcast_column(len: Self::Column) -> Column {
        Column::EmptyArray { len }
    }

    fn upcast_domain(_: Self::Domain) -> Domain {
        Domain::Array(None)
    }

    fn column_len<'a>(len: &'a Self::Column) -> usize {
        *len
    }

    fn index_column<'a>(len: &'a Self::Column, index: usize) -> Option<Self::ScalarRef<'a>> {
        if index < *len { Some(()) } else { None }
    }

    fn slice_column<'a>(len: &'a Self::Column, range: Range<usize>) -> Self::Column {
        assert!(range.end <= *len, "range {range:?} out of 0..{len}");
        range.end - range.start
    }

    fn iter_column<'a>(len: &'a Self::Column) -> Self::ColumnIterator<'a> {
        std::iter::repeat(()).take(*len)
    }

    fn column_to_builder(len: Self::Column) -> Self::ColumnBuilder {
        len
    }

    fn builder_len(len: &Self::ColumnBuilder) -> usize {
        *len
    }

    fn push_item(len: &mut Self::ColumnBuilder, _: Self::Scalar) {
        *len += 1
    }

    fn push_default(len: &mut Self::ColumnBuilder) {
        *len += 1
    }

    fn append_builder(len: &mut Self::ColumnBuilder, other_len: &Self::ColumnBuilder) {
        *len += other_len
    }

    fn build_column(len: Self::ColumnBuilder) -> Self::Column {
        len
    }

    fn build_scalar(len: Self::ColumnBuilder) -> Self::Scalar {
        assert_eq!(len, 1);
    }
}

impl ArgType for EmptyArrayType {
    fn data_type() -> DataType {
        DataType::EmptyArray
    }

    fn full_domain(_: &GenericMap) -> Self::Domain {}

    fn create_builder(_capacity: usize, _generics: &GenericMap) -> Self::ColumnBuilder {
        0
    }

    fn column_from_iter(iter: impl Iterator<Item = Self::Scalar>, _: &GenericMap) -> Self::Column {
        iter.count()
    }
}
