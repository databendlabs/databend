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
pub struct MergeStatus {
    pub insert_rows: usize,
    pub deleted_rows: usize,
    pub update_rows: usize,
}

impl MergeStatus {
    pub fn add_insert_rows(&mut self, insert_rows: usize) {
        self.insert_rows += insert_rows;
    }

    pub fn add_deleted_rows(&mut self, deleted_rows: usize) {
        self.deleted_rows += deleted_rows
    }

    pub fn add_update_rows(&mut self, update_rows: usize) {
        self.update_rows += update_rows
    }

    pub fn merge_status(&mut self, merge_status: MergeStatus) {
        self.insert_rows += merge_status.insert_rows;
        self.deleted_rows += merge_status.deleted_rows;
        self.update_rows += merge_status.update_rows;
    }
}
