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
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;

use databend_common_storage::MutationStatus;

#[derive(Default)]
pub struct MutationState {
    mutation_status: Arc<RwLock<MutationStatus>>,
    multi_table_insert_rows: Arc<Mutex<HashMap<u64, u64>>>,
}

impl MutationState {
    pub fn add_mutation_status(&self, mutation_status: MutationStatus) {
        self.mutation_status
            .write()
            .unwrap()
            .merge_mutation_status(mutation_status)
    }

    pub fn mutation_status(&self) -> Arc<RwLock<MutationStatus>> {
        self.mutation_status.clone()
    }

    pub fn set_multi_table_insert_rows(&self, insert_rows: HashMap<u64, u64>) {
        let mut current_insert_rows = self.multi_table_insert_rows.lock().unwrap();
        *current_insert_rows = insert_rows;
    }

    pub fn multi_table_insert_rows(&self) -> Arc<Mutex<HashMap<u64, u64>>> {
        self.multi_table_insert_rows.clone()
    }
}
