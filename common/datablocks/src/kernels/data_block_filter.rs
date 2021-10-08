// Copyright 2020 Datafuse Labs.
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

use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::compute::filter::build_filter;
use common_arrow::arrow::compute::filter::Filter;
use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::IntoSeries;
use common_datavalues::series::Series;
use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::DataBlock;

impl DataBlock {
    pub fn filter_block(block: &DataBlock, predicate: Series) -> Result<DataBlock> {
        if block.num_columns() == 0 || block.num_rows() == 0 {
            return Ok(block.clone());
        }

        let predicate_series = predicate.cast_with_type(&DataType::Boolean)?;
        if predicate_series.len() != block.num_rows() {
            return Err(ErrorCode::BadPredicateRows(format!(
                "DataBlock rows({}) must be equals predicate rows({})",
                block.num_rows(),
                predicate_series.len()
            )));
        }

        let predicate_filter = build_filter(predicate_series.bool()?.inner())?;

        let mut filtered_count = None;
        let mut filtered_columns = Vec::with_capacity(block.num_columns());

        // Filter array column and clone const column.
        for data_column in block.columns() {
            match data_column {
                // We'll process constant columns later
                DataColumn::Constant(_, _) => filtered_columns.push(data_column.clone()),
                DataColumn::Array(array) => {
                    let array = array.get_array_ref();
                    let filtered_data = (*predicate_filter)(array.as_ref());
                    let array: ArrayRef = filtered_data.into();

                    filtered_count = Some(array.len());
                    filtered_columns.push(DataColumn::Array(array.into_series()));
                }
            };
        }

        match filtered_count {
            None => Self::filter_with_all_const_columns(block, predicate_filter),
            Some(rows) => {
                // resize const column to filtered column.
                for filtered_column in &mut filtered_columns {
                    if let DataColumn::Constant(value, _) = filtered_column {
                        *filtered_column = DataColumn::Constant(value.clone(), rows);
                    }
                }

                Ok(DataBlock::create(block.schema().clone(), filtered_columns))
            }
        }
    }

    fn filter_column(column: &DataColumn, filter: Filter) -> Result<Series> {
        Ok(ArrayRef::from((*filter)(column.to_array()?.get_array_ref().as_ref())).into_series())
    }

    fn filter_with_all_const_columns(block: &DataBlock, filter: Filter) -> Result<DataBlock> {
        let block_columns = block.columns();
        let mut filtered_columns = Vec::with_capacity(block_columns.len());

        let first_column = DataBlock::filter_column(&block_columns[0], filter)?;
        filtered_columns.push(DataColumn::Array(first_column));

        for index in 1..block_columns.len() {
            if let DataColumn::Constant(value, _) = &block_columns[index] {
                let prev_column_len = filtered_columns[index - 1].len();
                filtered_columns.push(DataColumn::Constant(value.clone(), prev_column_len));
            }
        }

        if filtered_columns.len() != block_columns.len() {
            return Err(ErrorCode::LogicalError(
                "Logical error, schema missing match.",
            ));
        }

        Ok(DataBlock::create(block.schema().clone(), filtered_columns))
    }
}
