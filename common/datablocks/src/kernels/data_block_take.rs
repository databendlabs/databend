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

use common_datavalues::prelude::*;
use common_exception::Result;

use crate::DataBlock;

impl DataBlock {
    pub fn block_take_by_indices(raw: &DataBlock, indices: &[u32]) -> Result<DataBlock> {
        if indices.is_empty() {
            return Ok(DataBlock::empty_with_schema(raw.schema().clone()));
        }
        let fields = raw.schema().fields();
        let columns = fields
            .iter()
            .map(|f| {
                let column = raw.try_column_by_name(f.name())?;
                Series::take(column, indices)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(DataBlock::create(raw.schema().clone(), columns))
    }
}
