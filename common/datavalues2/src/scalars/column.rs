// Copyright 2021 Datafuse Labs.
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

use std::iter::TrustedLen;

use super::type_::Scalar;
use super::type_::ScalarRef;
use crate::prelude::*;

/// ScalarColumn is a sub trait of Column

// This is idea from `https://github.com/skyzh/type-exercise-in-rust`
// Thanks Mr Chi.
pub trait ScalarColumn: Column + Send + Sync + Sized + 'static
where for<'a> Self::OwnedItem: Scalar<RefType<'a> = Self::RefItem<'a>>
{
    type Builder: ScalarColumnBuilder<ColumnType = Self>;
    type OwnedItem: Scalar<ColumnType = Self>;
    type RefItem<'a>: ScalarRef<'a, ScalarType = Self::OwnedItem, ColumnType = Self>
    where Self: 'a;
    type Iterator<'a>: Iterator<Item = Self::RefItem<'a>> + ExactSizeIterator + TrustedLen;

    // Note: get_data has bad performance, avoid call this function inside the loop
    // Use `iter` instead
    fn get_data(&self, idx: usize) -> Self::RefItem<'_>;

    /// Get iterator of this column.
    fn scalar_iter(&self) -> Self::Iterator<'_>;

    /// Bulid array from slice
    fn from_slice(data: &[Self::RefItem<'_>]) -> Self {
        let mut builder = Self::Builder::with_capacity(data.len());
        for item in data {
            builder.push(*item);
        }
        builder.finish()
    }

    fn from_iterator<'a>(it: impl Iterator<Item = Self::RefItem<'a>>) -> Self {
        let mut builder = Self::Builder::with_capacity(get_iter_capacity(&it));
        for item in it {
            builder.push(item);
        }
        builder.finish()
    }

    fn from_owned_iterator(it: impl Iterator<Item = Self::OwnedItem>) -> Self {
        let mut builder = Self::Builder::with_capacity(get_iter_capacity(&it));
        for item in it {
            builder.push(item.as_scalar_ref());
        }
        builder.finish()
    }

    fn from_vecs(values: Vec<Self::OwnedItem>) -> Self {
        let it = values.iter();
        let mut builder = Self::Builder::with_capacity(get_iter_capacity(&it));
        for item in it {
            builder.push(item.as_scalar_ref());
        }
        builder.finish()
    }
}

pub trait ScalarColumnBuilder: MutableColumn {
    type ColumnType: ScalarColumn<Builder = Self>;

    fn with_capacity(capacity: usize) -> Self;

    /// Append a value to builder.
    fn push(&mut self, value: <Self::ColumnType as ScalarColumn>::RefItem<'_>);

    /// Append a value to builder.
    fn build_const(
        &mut self,
        value: <Self::ColumnType as ScalarColumn>::RefItem<'_>,
        size: usize,
    ) -> ColumnRef {
        self.push(value);
        let col = self.finish();
        ConstColumn::new(col.arc(), size).arc()
    }

    /// Finish build and return a new array.
    /// Did not consume itself, we can reuse this builder
    fn finish(&mut self) -> Self::ColumnType;
}
