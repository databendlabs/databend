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

use std::sync::Arc;

use common_arrow::arrow::array::growable::make_growable;
use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::ArrayRef;
use common_datavalues::ColumnRef;
use common_datavalues::IntoColumn;
use common_exception::Result;

use crate::DataBlock;

impl DataBlock {
    // indices: [ (block_index,  row_index_in_block), ... ]
    // indices size must be equal to sum of blocks size
    pub fn gather_blocks(blocks: &[DataBlock], indices: &[(usize, usize)]) -> Result<DataBlock> {
        debug_assert!(indices.len() == blocks.iter().map(|b| b.num_rows()).sum::<usize>());
        debug_assert!(!blocks.is_empty());

        let num_cols = blocks[0].num_columns();

        let mut result_columns = Vec::with_capacity(num_cols);
        for i in 0..num_cols {
            let mut columns_to_gather = Vec::with_capacity(blocks.len());
            for block in blocks {
                columns_to_gather.push(block.column(i).clone());
            }

            let nullable = blocks[0].column(i).is_nullable();
            result_columns.push(Self::gather_columns(&columns_to_gather, indices, nullable)?);
        }

        Ok(DataBlock::create(
            blocks[0].schema().clone(),
            result_columns,
        ))
    }

    pub fn gather_columns(
        columns: &[ColumnRef],
        indices: &[(usize, usize)],
        nullable: bool,
    ) -> Result<ColumnRef> {
        let arrays = columns
            .iter()
            .map(|c| c.as_arrow_array())
            .collect::<Vec<ArrayRef>>();

        let arrays: Vec<&dyn Array> = arrays.iter().map(|array| array.as_ref()).collect();
        // use_validity set to false, it will be checked inside `make_growable`
        let mut growable = make_growable(&arrays, false, indices.len());
        for index in indices.iter() {
            growable.extend(index.0, index.1, 1);
        }

        let result = growable.as_box();
        let result: ArrayRef = Arc::from(result);

        match nullable {
            false => Ok(result.into_column()),
            true => Ok(result.into_nullable_column()),
        }
    }
}
