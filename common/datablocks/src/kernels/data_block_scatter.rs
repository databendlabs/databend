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

use common_datavalues::prelude::*;
use common_exception::Result;

use crate::DataBlock;

impl DataBlock {
    pub fn scatter_block(
        block: &DataBlock,
        indices: &DataColumn,
        scatter_size: usize,
    ) -> Result<Vec<DataBlock>> {
        let array = indices.to_array()?;
        let array = array.u64()?;

        let columns_size = block.num_columns();
        let mut scattered_columns = Vec::with_capacity(scatter_size);

        for column_index in 0..columns_size {
            let mut indices = array.into_no_null_iter().copied();

            let columns = unsafe {
                block
                    .column(column_index)
                    .scatter_unchecked(&mut indices, scatter_size)
            }?;
            scattered_columns.push(columns);
        }

        let mut scattered_blocks = Vec::with_capacity(scatter_size);
        for index in 0..scatter_size {
            let mut block_columns = vec![];

            for item in scattered_columns.iter() {
                block_columns.push(item[index].clone())
            }
            scattered_blocks.push(DataBlock::create(block.schema().clone(), block_columns));
        }

        Ok(scattered_blocks)
    }
}
