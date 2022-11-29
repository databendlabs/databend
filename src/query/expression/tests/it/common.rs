// Copyright 2022 Datafuse Labs.
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

use common_expression::types::DataType;
use common_expression::Chunk;
use common_expression::ChunkEntry;
use common_expression::Column;
use common_expression::Value;

pub fn new_chunk(columns: &[(DataType, Column)]) -> Chunk {
    let len = columns.get(0).map_or(1, |(_, c)| c.len());
    let columns = columns
        .iter()
        .enumerate()
        .map(|(id, (ty, col))| ChunkEntry {
            id,
            data_type: ty.clone(),
            value: Value::Column(col.clone()),
        })
        .collect();

    Chunk::new(columns, len)
}
