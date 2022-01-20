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

use common_base::GlobalSequence;
use common_meta_raft_store::config::RaftConfig;
use common_meta_sled_store::get_sled_db;
use common_meta_sled_store::sled;

pub struct RaftTestContext {
    pub raft_config: RaftConfig,
    pub db: sled::Db,
}

/// Create a new context for testing sled
pub fn new_raft_test_context() -> RaftTestContext {
    // config for unit test of sled db, meta_sync() is true by default.
    let mut config = RaftConfig::empty();

    config.sled_tree_prefix = format!("test-{}-", 30900 + GlobalSequence::next());

    RaftTestContext {
        raft_config: config,
        db: get_sled_db(),
    }
}

/// 1. Open a temp sled::Db for all tests.
/// 2. Initialize a global tracing.
/// 3. Create a span for a test case. One needs to enter it by `span.enter()` and keeps the guard held.
#[macro_export]
macro_rules! init_raft_store_ut {
    () => {{
        let t = tempfile::tempdir().expect("create temp dir to sled db");
        common_meta_sled_store::init_temp_sled_db(t);

        // common_tracing::init_tracing(&format!("ut-{}", name), "./_logs")
        common_tracing::init_meta_ut_tracing();

        let name = common_tracing::func_name!();
        let span =
            common_tracing::tracing::debug_span!("ut", "{}", name.split("::").last().unwrap());
        ((), span)
    }};
}
