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

mod common;
mod simple;

use std::fmt::Debug;

pub use common::*;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SortColumnDescription;
pub use simple::*;

/// Convert columns to rows.
pub trait RowConverter<T: Rows>
where Self: Sized
{
    fn create(
        sort_columns_descriptions: &[SortColumnDescription],
        output_schema: DataSchemaRef,
    ) -> Result<Self>;
    fn convert(&mut self, columns: &[BlockEntry], num_rows: usize) -> Result<T>;
}

/// Rows can be compared.
pub trait Rows
where Self: Sized + Clone
{
    type Item<'a>: Ord + Debug
    where Self: 'a;
    type Type: ArgType;

    fn len(&self) -> usize;
    fn row(&self, index: usize) -> Self::Item<'_>;
    fn to_column(&self) -> Column;

    fn from_column(col: &Column, desc: &[SortColumnDescription]) -> Result<Self> {
        Self::try_from_column(col, desc).ok_or_else(|| {
            ErrorCode::BadDataValueType(format!(
                "Order column type mismatched. Expecetd {} but got {}",
                Self::data_type(),
                col.data_type()
            ))
        })
    }

    fn try_from_column(col: &Column, desc: &[SortColumnDescription]) -> Option<Self>;

    fn data_type() -> DataType {
        Self::Type::data_type()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn first(&self) -> Self::Item<'_> {
        self.row(0)
    }

    fn last(&self) -> Self::Item<'_> {
        self.row(self.len() - 1)
    }

    fn slice(&self, range: std::ops::Range<usize>) -> Self;
}
