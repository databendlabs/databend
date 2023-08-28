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

use std::collections::HashSet;
use std::ops::Not;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::DataBlock;

pub struct MergeIntoSplitMutator {
    pub row_id_idx: u32,
    pub row_id_set: HashSet<u64>,
}

impl MergeIntoSplitMutator {
    pub fn try_create(row_id_idx: u32) -> Self {
        Self {
            row_id_idx,
            row_id_set: HashSet::new(),
        }
    }

    // (matched_block,not_matched_block)
    pub fn split_data_block(&mut self, block: &DataBlock) -> Result<(DataBlock, DataBlock)> {
        let row_id_column = &block.columns()[self.row_id_idx as usize];
        assert_eq!(
            row_id_column.data_type,
            DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt64))),
        );

        let filter = match row_id_column.value.clone().wrap_nullable(None) {
            common_expression::Value::Scalar(_) => {
                return Err(ErrorCode::InvalidRowIdIndex(
                    "row id column should be a column, but it's a scalar",
                ));
            }
            common_expression::Value::Column(c) => match c {
                common_expression::Column::Nullable(c2) => c2.validity,
                _ => {
                    return Err(ErrorCode::InvalidRowIdIndex(
                        "row id column should be a column, but it's a scalar",
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
