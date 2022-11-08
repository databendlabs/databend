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

use std::cmp::Ordering;
use std::marker::PhantomData;

use common_arrow::arrow::array::PrimitiveArray;
use common_arrow::arrow::types::NativeType;
use common_arrow::ArrayRef;
use common_datablocks::SortColumnDescription;
use common_datavalues::DataSchemaRef;
use common_exception::Result;

use super::RowConverter;
use super::Rows;

#[derive(Clone, Copy)]
pub struct SimpleRow<T> {
    inner: T,
    desc: bool,
}

pub struct SimpleRows<T: NativeType> {
    inner: PrimitiveArray<T>,
    desc: bool,
}

impl<T: NativeType + Ord> Ord for SimpleRow<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.desc {
            self.inner.cmp(&other.inner).reverse()
        } else {
            self.inner.cmp(&other.inner)
        }
    }
}

impl<T: NativeType + Ord> PartialOrd for SimpleRow<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: NativeType + Ord> PartialEq for SimpleRow<T> {
    fn eq(&self, other: &Self) -> bool {
        self == other
    }
}

impl<T: NativeType + Ord> Eq for SimpleRow<T> {}

impl<T: NativeType + Ord> Rows for SimpleRows<T> {
    type Item<'a> = SimpleRow<T>;

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn row(&self, index: usize) -> Self::Item<'_> {
        SimpleRow {
            inner: self.inner.value(index),
            desc: self.desc,
        }
    }

    fn row_unchecked(&self, index: usize) -> Self::Item<'_> {
        let inner = unsafe { self.inner.value_unchecked(index) };
        SimpleRow {
            inner,
            desc: self.desc,
        }
    }
}

/// If there is only one sort field and its type is a primitive type,
/// use this converter.
pub struct SimpleRowConverter<T: NativeType> {
    desc: bool,
    _t: PhantomData<T>,
}

impl<T: NativeType + Ord> RowConverter<SimpleRows<T>> for SimpleRowConverter<T> {
    fn create(
        sort_columns_descriptions: Vec<SortColumnDescription>,
        _: DataSchemaRef,
    ) -> Result<Self> {
        assert!(sort_columns_descriptions.len() == 1);

        Ok(Self {
            desc: !sort_columns_descriptions[0].asc,
            _t: PhantomData,
        })
    }

    fn convert(&mut self, columns: &[ArrayRef]) -> Result<SimpleRows<T>> {
        assert!(columns.len() == 1);
        let inner = columns[0]
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .unwrap()
            .clone();
        let rows = SimpleRows {
            inner,
            desc: self.desc,
        };
        Ok(rows)
    }
}
