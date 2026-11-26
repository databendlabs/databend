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
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::IntervalType;
use databend_common_expression::types::Number;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::ValueType;

use super::RowConverter;
use super::Rows;
use super::SortKeyDescription;

/// A type that [`SimpleRowsAsc`] and [`SimpleRowsDesc`] sort by, with how two of its column
/// values are compared.
pub trait SimpleRowsType: ArgType
where for<'a> Self::ScalarRef<'a>: Ord + Send
{
    /// Compares row `i` of `a` with row `j` of `b`, the same as comparing their values.
    #[inline(always)]
    fn compare_rows(a: &Self::Column, i: usize, b: &Self::Column, j: usize) -> Ordering {
        unsafe { Self::index_column_unchecked(a, i).cmp(&Self::index_column_unchecked(b, j)) }
    }

    /// Whether row `i` of `a` equals row `j` of `b`.
    #[inline(always)]
    fn equal_rows(a: &Self::Column, i: usize, b: &Self::Column, j: usize) -> bool {
        unsafe { Self::index_column_unchecked(a, i) == Self::index_column_unchecked(b, j) }
    }

    /// The number of rows at the start of `rows` of `a` whose order against row `j` of `b`
    /// satisfies `keep`.
    #[inline]
    fn leading_rows(
        a: &Self::Column,
        rows: Range<usize>,
        b: &Self::Column,
        j: usize,
        keep: impl Fn(Ordering) -> bool,
    ) -> usize {
        let value = unsafe { Self::index_column_unchecked(b, j) };
        rows.take_while(|&i| keep(unsafe { Self::index_column_unchecked(a, i) }.cmp(&value)))
            .count()
    }

    /// The number of rows at the start of `rows` of `a` that equal row `j` of `b`.
    #[inline]
    fn leading_rows_eq(a: &Self::Column, rows: Range<usize>, b: &Self::Column, j: usize) -> usize {
        let value = unsafe { Self::index_column_unchecked(b, j) };
        rows.take_while(|&i| unsafe { Self::index_column_unchecked(a, i) } == value)
            .count()
    }
}

impl<N: Number> SimpleRowsType for NumberType<N> {}
impl SimpleRowsType for BooleanType {}
impl SimpleRowsType for DateType {}
impl SimpleRowsType for TimestampType {}
impl SimpleRowsType for IntervalType {}

/// Compares the views first, so the value bytes are only read when the length and the first
/// bytes kept in the views do not decide.
impl SimpleRowsType for StringType {
    #[inline(always)]
    fn compare_rows(a: &Self::Column, i: usize, b: &Self::Column, j: usize) -> Ordering {
        Self::Column::compare(a, i, b, j)
    }

    #[inline(always)]
    fn equal_rows(a: &Self::Column, i: usize, b: &Self::Column, j: usize) -> bool {
        Self::Column::equal(a, i, b, j)
    }

    /// Takes the view of row `j` once, so each row is compared with it in place.
    #[inline]
    fn leading_rows(
        a: &Self::Column,
        rows: Range<usize>,
        b: &Self::Column,
        j: usize,
        keep: impl Fn(Ordering) -> bool,
    ) -> usize {
        let probe = b.probe(j);
        rows.take_while(|&i| keep(a.compare_probe(i, &probe)))
            .count()
    }

    #[inline]
    fn leading_rows_eq(a: &Self::Column, rows: Range<usize>, b: &Self::Column, j: usize) -> usize {
        let probe = b.probe(j);
        rows.take_while(|&i| a.equal_probe(i, &probe)).count()
    }
}

/// Rows structure for single simple types. (numbers, date, timestamp)
#[derive(Clone, Debug)]
pub struct SimpleRowsAsc<T: ValueType> {
    inner: T::Column,
}

impl<T> Rows for SimpleRowsAsc<T>
where
    T: SimpleRowsType,
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

    #[inline(always)]
    fn cmp_rows(&self, i: usize, other: &Self, j: usize) -> Ordering {
        T::compare_rows(&self.inner, i, &other.inner, j)
    }

    #[inline(always)]
    fn eq_rows(&self, i: usize, other: &Self, j: usize) -> bool {
        T::equal_rows(&self.inner, i, &other.inner, j)
    }

    #[inline]
    fn leading_rows_le(&self, rows: Range<usize>, other: &Self, j: usize) -> usize {
        T::leading_rows(&self.inner, rows, &other.inner, j, Ordering::is_le)
    }

    #[inline]
    fn leading_rows_eq(&self, rows: Range<usize>, other: &Self, j: usize) -> usize {
        T::leading_rows_eq(&self.inner, rows, &other.inner, j)
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
    T: SimpleRowsType,
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

    #[inline(always)]
    fn cmp_rows(&self, i: usize, other: &Self, j: usize) -> Ordering {
        T::compare_rows(&other.inner, j, &self.inner, i)
    }

    #[inline(always)]
    fn eq_rows(&self, i: usize, other: &Self, j: usize) -> bool {
        T::equal_rows(&self.inner, i, &other.inner, j)
    }

    #[inline]
    fn leading_rows_le(&self, rows: Range<usize>, other: &Self, j: usize) -> usize {
        T::leading_rows(&self.inner, rows, &other.inner, j, Ordering::is_ge)
    }

    #[inline]
    fn leading_rows_eq(&self, rows: Range<usize>, other: &Self, j: usize) -> usize {
        T::leading_rows_eq(&self.inner, rows, &other.inner, j)
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
    T: SimpleRowsType,
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
    T: SimpleRowsType,
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
