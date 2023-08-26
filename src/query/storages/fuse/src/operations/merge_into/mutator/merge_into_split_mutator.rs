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

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
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
        let mut matched_block: Option<DataBlock> = None;
        let mut not_matched_block: Option<DataBlock> = None;

        for row_idx in 0..block.num_rows() {
            let single_row = block.slice(row_idx..row_idx + 1);
            if self.is_matched(row_id_column, row_idx)? {
                if matched_block.is_none() {
                    matched_block = Some(single_row);
                } else {
                    matched_block = Some(DataBlock::concat(&[matched_block.unwrap(), single_row])?);
                }
            } else if not_matched_block.is_none() {
                not_matched_block = Some(single_row);
            } else {
                not_matched_block = Some(DataBlock::concat(&[
                    not_matched_block.unwrap(),
                    single_row,
                ])?);
            }
        }
        match (matched_block, not_matched_block) {
            (None, None) => Ok((DataBlock::empty(), DataBlock::empty())),
            (Some(match_block), None) => Ok((match_block, DataBlock::empty())),
            (None, Some(not_matched_block)) => Ok((DataBlock::empty(), not_matched_block)),
            (Some(matched_block), Some(not_matched_block)) => {
                Ok((matched_block, not_matched_block))
            }
        }
    }

    fn is_matched(&mut self, row_id_column: &BlockEntry, row_idx: usize) -> Result<bool> {
        match row_id_column.value.index(row_idx).ok_or_else(|| {
            ErrorCode::Internal("can't get row_id_col when do merge into operations")
        })? {
            ScalarRef::Null => Ok(false),
            ScalarRef::Number(NumberScalar::UInt64(v)) => {
                if self.row_id_set.contains(&v) {
                    Err(ErrorCode::UnresolvableConflict(
                        "multi rows from source match one and the same row in the target_table multi times",
                    ))
                } else {
                    self.row_id_set.insert(v);
                    Ok(true)
                }
            }
            _ => Err(ErrorCode::Internal(
                "row_id_type must be UInt64 for merge into",
            )),
        }
    }
}
