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

use crate::io::BlockReader;

impl BlockReader {
    pub fn build_default_values_block(&self, num_rows: usize) -> Result<DataBlock> {
        let data_schema = self.data_schema();
        let default_vals = self.default_vals.clone();
        DataBlock::create_with_default_value(&data_schema, &default_vals, num_rows)
    }
}
