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

use super::type_::Scalar;
use super::type_::ScalarRef;
use crate::prelude::*;

/// ScalarColumn is a sub trait of Column

// This is inspired by `https://github.com/skyzh/type-exercise-in-rust`
// Thanks Mr Chi.
pub trait ScalarColumn: Column + Send + Sync + Sized + 'static
where for<'a> Self::OwnedItem: Scalar<RefType<'a> = Self::RefItem<'a>>
{
    type Builder: ScalarColumnBuilder<ColumnType = Self>;
    type OwnedItem: Scalar<ColumnType = Self>;
    type RefItem<'a>: ScalarRef<'a, ScalarType = Self::OwnedItem, ColumnType = Self>
    where Self: 'a;

    fn get_data(&self, idx: usize) -> Self::RefItem<'_>;
    /// Get iterator of this column.
    fn iter(&self) -> ScalarColumnIterator<Self>;

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
}

pub trait ScalarColumnBuilder: MutableColumn {
    type ColumnType: ScalarColumn<Builder = Self>;
    /// Append a value to builder.
    fn push(&mut self, value: <Self::ColumnType as ScalarColumn>::RefItem<'_>);

    /// Finish build and return a new array.
    /// Did not consume itself, we can reuse this builder
    fn finish(&mut self) -> Self::ColumnType;
}

/// An iterator that iterators on any [`ScalarColumn`] type.
pub struct ScalarColumnIterator<'a, A: ScalarColumn> {
    column: &'a A,
    pos: usize,
}

impl<'a, A: ScalarColumn> Iterator for ScalarColumnIterator<'a, A> {
    type Item = A::RefItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.column.len() {
            None
        } else {
            let item = self.column.get_data(self.pos);
            self.pos += 1;
            Some(item)
        }
    }
}

impl<'a, A: ScalarColumn> ScalarColumnIterator<'a, A> {
    /// Create an [`ScalarColumnIterator`] from [`column`].
    pub fn new(column: &'a A) -> Self {
        Self { column, pos: 0 }
    }
}
