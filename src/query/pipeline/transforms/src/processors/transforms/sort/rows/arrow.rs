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

use common_arrow::arrow::compute::sort::row::Row as ArrowRow;
use common_arrow::arrow::compute::sort::row::RowConverter as ArrowRowConverter;
use common_arrow::arrow::compute::sort::row::Rows as ArrowRows;
use common_arrow::arrow::compute::sort::row::SortField as ArrowSortField;
use common_arrow::arrow::compute::sort::SortOptions as ArrowSortOptions;
use common_exception::Result;
use common_expression::arrow::column_to_arrow_array;
use common_expression::BlockEntry;
use common_expression::DataSchemaRef;
use common_expression::SortColumnDescription;

use super::RowConverter;
use super::Rows;

impl Rows for ArrowRows {
    type Item<'a> = ArrowRow<'a>;

    fn len(&self) -> usize {
        self.len()
    }

    fn row(&self, index: usize) -> Self::Item<'_> {
        self.row_unchecked(index)
    }
}

impl RowConverter<ArrowRows> for ArrowRowConverter {
    fn create(
        sort_columns_descriptions: Vec<SortColumnDescription>,
        output_schema: DataSchemaRef,
    ) -> Result<Self> {
        let sort_fields = sort_columns_descriptions
            .iter()
            .map(|d| {
                let data_type = output_schema.field(d.offset).data_type();
                Ok(ArrowSortField::new_with_options(
                    data_type.into(),
                    ArrowSortOptions {
                        descending: !d.asc,
                        nulls_first: d.nulls_first,
                    },
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(ArrowRowConverter::new(sort_fields))
    }

    fn convert(&mut self, columns: &[BlockEntry], num_rows: usize) -> Result<ArrowRows> {
        let arrays = columns
            .iter()
            .map(|col| column_to_arrow_array(col, num_rows))
            .collect::<Vec<_>>();
        Ok(self.convert_columns(&arrays)?)
    }
}
