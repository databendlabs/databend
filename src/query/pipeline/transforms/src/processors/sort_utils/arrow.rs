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

use common_arrow::arrow::compute::sort::row::Row as ARow;
use common_arrow::arrow::compute::sort::row::RowConverter as ARowConverter;
use common_arrow::arrow::compute::sort::row::Rows as ARows;
use common_arrow::arrow::compute::sort::row::SortField;
use common_arrow::arrow::compute::sort::SortOptions;
use common_arrow::arrow::datatypes::DataType;
use common_arrow::ArrayRef;
use common_datablocks::SortColumnDescription;
use common_datavalues::DataSchemaRef;
use common_exception::Result;

use super::RowConverter;
use super::Rows;

impl Rows for ARows {
    type Item<'a> = ARow<'a>;

    fn len(&self) -> usize {
        self.len()
    }

    fn row(&self, index: usize) -> Self::Item<'_> {
        self.row(index)
    }

    fn row_unchecked(&self, index: usize) -> Self::Item<'_> {
        self.row_unchecked(index)
    }
}

impl RowConverter<ARows> for ARowConverter {
    fn create(
        sort_columns_descriptions: Vec<SortColumnDescription>,
        output_schema: DataSchemaRef,
    ) -> Result<Self> {
        let sort_fields = sort_columns_descriptions
            .iter()
            .map(|d| {
                let data_type = match output_schema
                    .field_with_name(&d.column_name)?
                    .to_arrow()
                    .data_type()
                {
                    // The actual data type of `Data` and `Timestmap` will be `Int32` and `Int64`.
                    DataType::Date32 | DataType::Time32(_) => DataType::Int32,
                    DataType::Date64 | DataType::Time64(_) | DataType::Timestamp(_, _) => {
                        DataType::Int64
                    }
                    date_type => date_type.clone(),
                };
                Ok(SortField::new_with_options(data_type, SortOptions {
                    descending: !d.asc,
                    nulls_first: d.nulls_first,
                }))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(ARowConverter::new(sort_fields))
    }
    fn convert(&mut self, columns: &[ArrayRef]) -> Result<ARows> {
        let rows = self.convert_columns(columns)?;
        Ok(rows)
    }
}
