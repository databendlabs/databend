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

use std::cmp::Reverse;
use std::marker::PhantomData;
use std::ops::Range;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::ValueType;

use super::RowConverter;
use super::Rows;
use super::SortKeyDescription;

/// Rows structure for single simple types. (numbers, date, timestamp)
#[derive(Clone, Debug)]
pub struct SimpleRowsAsc<T: ValueType> {
    inner: T::Column,
}

impl<T> Rows for SimpleRowsAsc<T>
where
    T: ArgType,
    for<'a> T::ScalarRef<'a>: Ord + Send,
{
    const IS_ASC_COLUMN: bool = true;
    type Item<'a>
        = T::ScalarRef<'a>
    where Self: 'a;

    type Type = T;
    type Converter = SimpleRowConverter<T>;

    fn len(&self) -> usize {
        T::column_len(&self.inner)
    }

    fn row(&self, index: usize) -> T::ScalarRef<'_> {
        unsafe { T::index_column_unchecked(&self.inner, index) }
    }

    fn to_column(&self) -> Column {
        T::upcast_column_with_type(self.inner.clone(), &T::data_type())
    }

    fn from_column(col: &Column) -> Result<Self> {
        match T::try_downcast_column(col) {
            Ok(inner) => Ok(Self { inner }),
            Err(err) => Err(err.add_message("Order column type mismatched.")),
        }
    }

    fn slice(&self, range: Range<usize>) -> Self {
        Self {
            inner: T::slice_column(&self.inner, range),
        }
    }

    fn scalar_as_item<'a>(s: &'a Scalar) -> Self::Item<'a> {
        let s = &s.as_ref();
        T::try_downcast_scalar(s).unwrap()
    }

    fn owned_item(item: Self::Item<'_>) -> Scalar {
        T::upcast_scalar(T::to_owned_scalar(item))
    }
}

/// Rows structure for single simple types. (numbers, date, timestamp)
#[derive(Clone, Debug)]
pub struct SimpleRowsDesc<T: ValueType> {
    inner: T::Column,
}

impl<T> Rows for SimpleRowsDesc<T>
where
    T: ArgType,
    for<'a> T::ScalarRef<'a>: Ord + Send,
{
    const IS_ASC_COLUMN: bool = false;
    type Item<'a>
        = Reverse<T::ScalarRef<'a>>
    where Self: 'a;

    type Type = T;
    type Converter = SimpleRowConverter<T>;

    fn len(&self) -> usize {
        T::column_len(&self.inner)
    }

    fn row(&self, index: usize) -> Reverse<T::ScalarRef<'_>> {
        let value = unsafe { T::index_column_unchecked(&self.inner, index) };
        Reverse(value)
    }

    fn to_column(&self) -> Column {
        T::upcast_column_with_type(self.inner.clone(), &T::data_type())
    }

    fn from_column(col: &Column) -> Result<Self> {
        match T::try_downcast_column(col) {
            Ok(inner) => Ok(Self { inner }),
            Err(err) => Err(err.add_message("Order column type mismatched.")),
        }
    }

    fn slice(&self, range: Range<usize>) -> Self {
        Self {
            inner: T::slice_column(&self.inner, range),
        }
    }

    fn scalar_as_item<'a>(s: &'a Scalar) -> Self::Item<'a> {
        let s = &s.as_ref();
        Reverse(T::try_downcast_scalar(s).unwrap())
    }

    fn owned_item(item: Self::Item<'_>) -> Scalar {
        T::upcast_scalar(T::to_owned_scalar(item.0))
    }
}

/// If there is only one sort field and its type is a primitive type,
/// use this converter.
#[derive(Debug)]
pub struct SimpleRowConverter<T> {
    sort_offset: usize,
    _t: PhantomData<T>,
}

impl<T> RowConverter<SimpleRowsAsc<T>> for SimpleRowConverter<T>
where
    T: ArgType,
    for<'a> T::ScalarRef<'a>: Ord + Send,
{
    fn new(desc: SortKeyDescription) -> Result<Self> {
        let sort_offset = desc.into_single_sort_offset(true);
        Ok(Self {
            sort_offset,
            _t: PhantomData,
        })
    }

    fn convert(&self, data_block: &DataBlock) -> Result<SimpleRowsAsc<T>> {
        self.convert_rows(data_block.get_by_offset(self.sort_offset), true)
    }

    fn support_data_type(d: &DataType) -> bool {
        T::data_type() == *d
    }
}

impl<T> RowConverter<SimpleRowsDesc<T>> for SimpleRowConverter<T>
where
    T: ArgType,
    for<'a> T::ScalarRef<'a>: Ord + Send,
{
    fn new(desc: SortKeyDescription) -> Result<Self> {
        let sort_offset = desc.into_single_sort_offset(false);
        Ok(Self {
            sort_offset,
            _t: PhantomData,
        })
    }

    fn convert(&self, data_block: &DataBlock) -> Result<SimpleRowsDesc<T>> {
        self.convert_rows(data_block.get_by_offset(self.sort_offset), false)
    }

    fn support_data_type(d: &DataType) -> bool {
        T::data_type() == *d
    }
}

impl<T: ArgType> SimpleRowConverter<T> {
    fn convert_rows<R: Rows>(&self, entry: &BlockEntry, asc: bool) -> Result<R> {
        assert!(asc == R::IS_ASC_COLUMN);
        if entry.data_type() != T::data_type() {
            return Err(ErrorCode::Internal(format!(
                "Cannot convert simple column. Expect data type {:?}, found {:?}",
                T::data_type(),
                entry.data_type()
            )));
        }

        match entry {
            BlockEntry::Const(_, _, _) => {
                let col = entry.to_column();
                R::from_column(&col)
            }
            BlockEntry::Column(c) => R::from_column(c),
        }
    }
}
