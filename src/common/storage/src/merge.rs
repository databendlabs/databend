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

use serde::Deserialize;
use serde::Serialize;

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct MutationStatus {
    pub insert_rows: u64,
    pub deleted_rows: u64,
    pub update_rows: u64,
}

impl MutationStatus {
    pub fn add_insert_rows(&mut self, insert_rows: u64) {
        self.insert_rows += insert_rows;
    }

    pub fn add_deleted_rows(&mut self, deleted_rows: u64) {
        self.deleted_rows += deleted_rows
    }

    pub fn add_update_rows(&mut self, update_rows: u64) {
        self.update_rows += update_rows
    }

    pub fn merge_mutation_status(&mut self, mutation_status: MutationStatus) {
        self.insert_rows += mutation_status.insert_rows;
        self.deleted_rows += mutation_status.deleted_rows;
        self.update_rows += mutation_status.update_rows;
    }
}
