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
use databend_common_expression::types::ArgType;
use databend_common_expression::types::ValueType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::Value;

use super::RowConverter;
use super::Rows;

/// Rows structure for single simple types. (numbers, date, timestamp)
#[derive(Clone, Debug)]
pub struct SimpleRowsAsc<T: ValueType> {
    inner: T::Column,
}

impl<T> Rows for SimpleRowsAsc<T>
where
    T: ArgType,
    for<'a> T::ScalarRef<'a>: Ord,
{
    type Item<'a>
        = T::ScalarRef<'a>
    where Self: 'a;

    type Type = T;

    fn len(&self) -> usize {
        T::column_len(&self.inner)
    }

    fn row(&self, index: usize) -> T::ScalarRef<'_> {
        unsafe { T::index_column_unchecked(&self.inner, index) }
    }

    fn to_column(&self) -> Column {
        T::upcast_column(self.inner.clone())
    }

    fn try_from_column(col: &Column, desc: &[SortColumnDescription]) -> Option<Self> {
        let inner = T::try_downcast_column(col)?;

        if desc[0].asc {
            Some(Self { inner })
        } else {
            None
        }
    }

    fn slice(&self, range: Range<usize>) -> Self {
        Self {
            inner: T::slice_column(&self.inner, range),
        }
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
    for<'a> T::ScalarRef<'a>: Ord,
{
    type Item<'a>
        = Reverse<T::ScalarRef<'a>>
    where Self: 'a;

    type Type = T;

    fn len(&self) -> usize {
        T::column_len(&self.inner)
    }

    fn row(&self, index: usize) -> Reverse<T::ScalarRef<'_>> {
        let value = unsafe { T::index_column_unchecked(&self.inner, index) };
        Reverse(value)
    }

    fn to_column(&self) -> Column {
        T::upcast_column(self.inner.clone())
    }

    fn try_from_column(col: &Column, desc: &[SortColumnDescription]) -> Option<Self> {
        let inner = T::try_downcast_column(col)?;

        if !desc[0].asc {
            Some(Self { inner })
        } else {
            None
        }
    }

    fn slice(&self, range: Range<usize>) -> Self {
        Self {
            inner: T::slice_column(&self.inner, range),
        }
    }
}

/// If there is only one sort field and its type is a primitive type,
/// use this converter.
pub struct SimpleRowConverter<T> {
    _t: PhantomData<T>,
}

impl<T> RowConverter<SimpleRowsAsc<T>> for SimpleRowConverter<T>
where
    T: ArgType,
    for<'a> T::ScalarRef<'a>: Ord,
{
    fn create(
        sort_columns_descriptions: &[SortColumnDescription],
        _: DataSchemaRef,
    ) -> Result<Self> {
        assert!(sort_columns_descriptions.len() == 1);
        assert!(sort_columns_descriptions[0].asc);
        Ok(Self { _t: PhantomData })
    }

    fn convert(&mut self, columns: &[BlockEntry], num_rows: usize) -> Result<SimpleRowsAsc<T>> {
        self.convert_rows(columns, num_rows, true)
    }
}

impl<T> RowConverter<SimpleRowsDesc<T>> for SimpleRowConverter<T>
where
    T: ArgType,
    for<'a> T::ScalarRef<'a>: Ord,
{
    fn create(
        sort_columns_descriptions: &[SortColumnDescription],
        _: DataSchemaRef,
    ) -> Result<Self> {
        assert!(sort_columns_descriptions.len() == 1);
        assert!(!sort_columns_descriptions[0].asc);
        Ok(Self { _t: PhantomData })
    }

    fn convert(&mut self, columns: &[BlockEntry], num_rows: usize) -> Result<SimpleRowsDesc<T>> {
        self.convert_rows(columns, num_rows, false)
    }
}

impl<T: ArgType> SimpleRowConverter<T> {
    fn convert_rows<R: Rows>(
        &mut self,
        columns: &[BlockEntry],
        num_rows: usize,
        asc: bool,
    ) -> Result<R> {
        assert!(columns.len() == 1);
        let col = &columns[0];
        if col.data_type != T::data_type() {
            return Err(ErrorCode::Internal(format!(
                "Cannot convert simple column. Expect data type {:?}, found {:?}",
                T::data_type(),
                col.data_type
            )));
        }

        let col = match &col.value {
            Value::Scalar(v) => {
                let builder = ColumnBuilder::repeat(&v.as_ref(), num_rows, &col.data_type);
                builder.build()
            }
            Value::Column(c) => c.clone(),
        };

        let desc = [SortColumnDescription {
            offset: 0,
            asc,
            nulls_first: false,
        }];

        R::from_column(&col, &desc)
    }
}
