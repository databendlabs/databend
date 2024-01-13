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

use std::ops::Not;

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::DataBlock;

pub struct MergeIntoSplitMutator {
    pub row_id_idx: u32,
}

impl MergeIntoSplitMutator {
    #[allow(dead_code)]
    pub fn try_create(row_id_idx: u32) -> Self {
        Self { row_id_idx }
    }

    // (matched_block,not_matched_block)
    pub fn split_data_block(&mut self, block: &DataBlock) -> Result<(DataBlock, DataBlock)> {
        let row_id_column = &block.columns()[self.row_id_idx as usize];
        assert_eq!(
            row_id_column.data_type,
            DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt64))),
        );

        // get row_id do check duplicate and get filter
        let filter: Bitmap = match &row_id_column.value {
            databend_common_expression::Value::Scalar(scalar) => {
                // fast judge
                if scalar.is_null() {
                    return Ok((DataBlock::empty(), block.clone()));
                } else {
                    return Ok((block.clone(), DataBlock::empty()));
                }
            }
            databend_common_expression::Value::Column(column) => match column {
                databend_common_expression::Column::Nullable(nullable_column) => {
                    nullable_column.validity.clone()
                }
                _ => {
                    return Err(ErrorCode::InvalidRowIdIndex(
                        "row id column should be a nullable column, but it's a normal column",
                    ));
                }
            },
        };
        Ok((
            block.clone().filter_with_bitmap(&filter)?,
            block.clone().filter_with_bitmap(&filter.not())?,
        ))
    }
}
