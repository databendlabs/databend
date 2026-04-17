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

use std::sync::Arc;

use databend_common_storage::MultiTableInsertStatus;
use databend_common_storage::MutationStatus;
use parking_lot::Mutex;
use parking_lot::RwLock;

pub trait TableContextMutationStatus: Send + Sync {
    fn add_mutation_status(&self, mutation_status: MutationStatus);

    fn get_mutation_status(&self) -> Arc<RwLock<MutationStatus>>;

    fn update_multi_table_insert_status(&self, table_id: u64, num_rows: u64);

    fn get_multi_table_insert_status(&self) -> Arc<Mutex<MultiTableInsertStatus>>;
}
