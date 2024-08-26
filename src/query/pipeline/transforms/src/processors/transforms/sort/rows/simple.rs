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
use std::marker::PhantomData;
use std::ops::Range;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::ValueType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::Value;

use super::RowConverter;
use super::Rows;

pub type DateRows = SimpleRows<DateType>;
pub type TimestampRows = SimpleRows<TimestampType>;
pub type StringRows = SimpleRows<StringType>;

/// Row structure for single simple types. (numbers, date, timestamp)
#[derive(Clone, Copy, Debug)]
pub struct SimpleRow<T: ValueType> {
    inner: T::Scalar,
    desc: bool,
}

/// Rows structure for single simple types. (numbers, date, timestamp)
#[derive(Clone)]
pub struct SimpleRows<T: ValueType> {
    inner: T::Column,
    desc: bool,
}

impl<T> Ord for SimpleRow<T>
where
    T: ValueType,
    T::Scalar: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        if self.desc {
            self.inner.cmp(&other.inner).reverse()
        } else {
            self.inner.cmp(&other.inner)
        }
    }
}

impl<T> PartialOrd for SimpleRow<T>
where
    T: ValueType,
    T::Scalar: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> PartialEq for SimpleRow<T>
where
    T: ValueType,
    T::Scalar: Ord,
{
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner && self.desc == other.desc
    }
}

impl<T> Eq for SimpleRow<T>
where
    T: ValueType,
    T::Scalar: Ord,
{
}

impl<T> Rows for SimpleRows<T>
where
    T: ArgType,
    T::Scalar: Ord,
{
    type Item<'a> = SimpleRow<T>;
    type Type = T;

    fn len(&self) -> usize {
        T::column_len(&self.inner)
    }

    fn row(&self, index: usize) -> Self::Item<'_> {
        let inner = unsafe { T::index_column_unchecked(&self.inner, index) };
        SimpleRow {
            inner: T::to_owned_scalar(inner),
            desc: self.desc,
        }
    }

    fn to_column(&self) -> Column {
        T::upcast_column(self.inner.clone())
    }

    fn try_from_column(col: &Column, desc: &[SortColumnDescription]) -> Option<Self> {
        let inner = T::try_downcast_column(col)?;
        Some(Self {
            inner,
            desc: !desc[0].asc,
        })
    }

    fn slice(&self, range: Range<usize>) -> Self {
        Self {
            inner: T::slice_column(&self.inner, range),
            desc: self.desc,
        }
    }
}

pub type DateConverter = SimpleRowConverter<DateType>;
pub type TimestampConverter = SimpleRowConverter<TimestampType>;
pub type StringConverter = SimpleRowConverter<StringType>;

/// If there is only one sort field and its type is a primitive type,
/// use this converter.
pub struct SimpleRowConverter<T> {
    desc: bool,
    _t: PhantomData<T>,
}

impl<T> RowConverter<SimpleRows<T>> for SimpleRowConverter<T>
where
    T: ArgType,
    T::Scalar: Ord,
{
    fn create(
        sort_columns_descriptions: &[SortColumnDescription],
        _: DataSchemaRef,
    ) -> Result<Self> {
        assert!(sort_columns_descriptions.len() == 1);

        Ok(Self {
            desc: !sort_columns_descriptions[0].asc,
            _t: PhantomData,
        })
    }

    fn convert(&mut self, columns: &[BlockEntry], num_rows: usize) -> Result<SimpleRows<T>> {
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

        let rows = SimpleRows {
            inner: T::try_downcast_column(&col).unwrap(),
            desc: self.desc,
        };
        Ok(rows)
    }
}
