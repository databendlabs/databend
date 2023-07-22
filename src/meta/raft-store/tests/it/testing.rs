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

use std::sync::Arc;
use std::sync::Mutex;

use common_base::base::GlobalSequence;
use common_meta_raft_store::config::RaftConfig;
use common_meta_sled_store::get_sled_db;
use common_meta_sled_store::sled;
use common_tracing::init_logging;
use common_tracing::Config;
use log::info;
use once_cell::sync::Lazy;

pub struct RaftTestContext {
    pub raft_config: RaftConfig,
    pub db: sled::Db,
}

/// Create a new context for testing sled
pub fn new_raft_test_context() -> RaftTestContext {
    // config for unit test of sled db, meta_sync() is true by default.
    let config = RaftConfig {
        sled_tree_prefix: format!("test-{}-", 30900 + GlobalSequence::next()),
        ..Default::default()
    };

    RaftTestContext {
        raft_config: config,
        db: get_sled_db(),
    }
}

#[allow(dyn_drop)]
struct MetaLogGuard {
    #[allow(dead_code)]
    log_guards: Vec<Box<dyn Drop + Send + Sync + 'static>>,
}

static META_LOG_GUARD: Lazy<Arc<Mutex<Option<MetaLogGuard>>>> =
    Lazy::new(|| Arc::new(Mutex::new(None)));

/// Initialize unit test tracing for metasrv
#[ctor::ctor]
fn init_meta_ut_tracing() {
    let guards = init_logging("meta_unittests", &Config::new_testing());
    info!("start");

    *META_LOG_GUARD.as_ref().lock().unwrap() = Some(MetaLogGuard { log_guards: guards });

    let t = tempfile::tempdir().expect("create temp dir to sled db");
    common_meta_sled_store::init_temp_sled_db(t);
}

#[ctor::dtor]
fn cleanup_meta_ut_tracing() {
    META_LOG_GUARD.as_ref().lock().unwrap().take();
}
