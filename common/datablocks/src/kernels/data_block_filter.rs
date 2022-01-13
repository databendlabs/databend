// Copyright 2021 Datafuse Labs.
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
use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::IntoSeries;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::DataBlock;

impl DataBlock {
    pub fn filter_block(block: &DataBlock, predicate: &DataColumn) -> Result<DataBlock> {
        if block.num_columns() == 0 || block.num_rows() == 0 {
            return Ok(block.clone());
        }

        if predicate.len() != block.num_rows() {
            return Err(ErrorCode::BadPredicateRows(format!(
                "DataBlock rows({}) must be equal to predicate rows({})",
                block.num_rows(),
                predicate.len()
            )));
        }

        let predicate = predicate.cast_with_type(&DataType::Boolean)?;

        // faster path for constant filter
        if let DataColumn::Constant(DataValue::Boolean(v), _) = predicate {
            let ok = v.unwrap_or(false);
            if ok {
                return Ok(block.clone());
            } else {
                return Ok(DataBlock::empty_with_schema(block.schema().clone()));
            }
        }

        let predicate_series = predicate.to_array()?;
        let predicate_array = predicate_series.bool()?.inner();

        let before_filter_rows = predicate_array.values().len();
        let after_filter_rows = before_filter_rows - predicate_array.values().null_count();

        if after_filter_rows == 0 {
            return Ok(DataBlock::empty_with_schema(block.schema().clone()));
        }

        let predicate_filter = build_filter(predicate_array)?;
        let mut after_columns = Vec::with_capacity(block.num_columns());
        for data_column in block.columns() {
            match data_column {
                DataColumn::Constant(value, _) => {
                    after_columns.push(DataColumn::Constant(value.clone(), after_filter_rows));
                }
                DataColumn::Array(array) => {
                    let array = array.get_array_ref();
                    let filtered_data = (*predicate_filter)(array.as_ref());
                    let array: ArrayRef = filtered_data.into();
                    after_columns.push(DataColumn::Array(array.into_series()));
                }
            };
        }

        Ok(DataBlock::create(block.schema().clone(), after_columns))
    }
}
