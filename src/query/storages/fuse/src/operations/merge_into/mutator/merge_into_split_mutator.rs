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

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::nullable::NullableColumn;
use common_expression::types::DataType;
use common_expression::types::NullableType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberScalar;
use common_expression::BlockEntry;
use common_expression::DataBlock;
use common_expression::ScalarRef;

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

        for row_id_offset in 0..block.num_rows() {
            self.check_duplicate(row_id_column, row_id_offset)?
        }

        // get row_id do check duplicate and get filter
        let filter: Bitmap = match &row_id_column.value {
            common_expression::Value::Scalar(scalar) => {
                let mut mutable_bitmap = MutableBitmap::new();
                if scalar.is_null() {
                    mutable_bitmap.push(false)
                } else {
                    mutable_bitmap.push(true);
                }
                mutable_bitmap.into()
            }
            common_expression::Value::Column(column) => match column {
                common_expression::Column::Nullable(nullable_column) => {
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

    fn check_duplicate(&mut self, row_id_column: &BlockEntry, row_id_offset: usize) -> Result<()> {
        match row_id_column.value.index(row_id_offset).ok_or_else(|| {
            ErrorCode::Internal("can't get row_id_col when do merge into operations")
        })? {
            ScalarRef::Null => Ok(()),
            ScalarRef::Number(NumberScalar::UInt64(v)) => {
                if self.row_id_set.contains(&v) {
                    Err(ErrorCode::UnresolvableConflict(
                        "multi rows from source match one and the same row in the target_table multi times",
                    ))
                } else {
                    self.row_id_set.insert(v);
                    Ok(())
                }
            }
            _ => Err(ErrorCode::Internal(
                "row_id_type must be UInt64 for merge into",
            )),
        }
    }
}
