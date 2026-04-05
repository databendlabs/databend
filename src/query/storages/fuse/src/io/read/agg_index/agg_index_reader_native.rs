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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;

use super::AggIndexReader;
use crate::io::NativeSourceData;

impl AggIndexReader {
    pub fn deserialize_native_data(&self, data: &mut NativeSourceData) -> Result<DataBlock> {
        let mut all_columns_arrays = vec![];

        for (index, column_node) in self.reader.project_column_nodes.iter().enumerate() {
            let readers = data.remove(&index).unwrap();
            let array_iter = self.reader.build_column_iter(column_node, readers)?;
            let arrays = array_iter.map(|a| Ok(a?)).collect::<Result<Vec<_>>>()?;
            all_columns_arrays.push(arrays);
        }
        if all_columns_arrays.is_empty() {
            return Ok(DataBlock::empty_with_schema(&self.reader.data_schema()));
        }
        debug_assert!(
            all_columns_arrays
                .iter()
                .all(|a| a.len() == all_columns_arrays[0].len())
        );
        let page_num = all_columns_arrays[0].len();
        let mut blocks = Vec::with_capacity(page_num);

        for i in 0..page_num {
            let mut columns = Vec::with_capacity(all_columns_arrays.len());
            for cs in all_columns_arrays.iter() {
                columns.push(cs[i].clone());
            }
            let block = DataBlock::new_from_columns(columns);
            blocks.push(block);
        }
        let block = DataBlock::concat(&blocks)?;
        self.apply_agg_info(block)
    }
}
