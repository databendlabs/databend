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

use std::collections::HashMap;

use serde::Deserialize;
use serde::Serialize;

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct MultiTableInsertStatus {
    pub insert_rows: HashMap<u64, u64>,
}

impl MultiTableInsertStatus {
    pub fn merge(&mut self, other: Self) {
        for (table_id, insert_rows) in other.insert_rows {
            match self.insert_rows.get_mut(&table_id) {
                Some(rows) => *rows += insert_rows,
                None => {
                    self.insert_rows.insert(table_id, insert_rows);
                }
            }
        }
    }
}
