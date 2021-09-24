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

use common_sled_store::get_sled_db;

use crate::configs::RaftConfig;

pub struct RaftTestContext {
    pub raft_config: RaftConfig,
    pub db: sled::Db,
}

/// Create a new context for testing sled
pub fn new_raft_test_context() -> RaftTestContext {
    // config for unit test of sled db, meta_sync() is true by default.
    let mut config = RaftConfig::empty();

    config.sled_tree_prefix = format!("test-{}-", 30900 + common_uniq_id::uniq_usize());

    RaftTestContext {
        raft_config: config,
        db: get_sled_db(),
    }
}
