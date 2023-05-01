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

mod arrow;
mod simple;

use common_exception::Result;
use common_expression::BlockEntry;
use common_expression::DataSchemaRef;
use common_expression::SortColumnDescription;
pub use simple::*;

/// Convert columns to rows.
pub trait RowConverter<T: Rows>
where Self: Sized
{
    fn create(
        sort_columns_descriptions: Vec<SortColumnDescription>,
        output_schema: DataSchemaRef,
    ) -> Result<Self>;
    fn convert(&mut self, columns: &[BlockEntry], num_rows: usize) -> Result<T>;
}

/// Rows can be compared.
pub trait Rows {
    type Item<'a>: Ord
    where Self: 'a;

    fn len(&self) -> usize;
    fn row(&self, index: usize) -> Self::Item<'_>;
}
