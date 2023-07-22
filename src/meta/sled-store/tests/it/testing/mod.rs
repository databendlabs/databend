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

pub mod fake_key_spaces;
pub mod fake_state_machine_meta;

use std::sync::Arc;
use std::sync::Mutex;

use common_base::base::GlobalSequence;
use common_meta_sled_store::get_sled_db;
use common_tracing::init_logging;
use common_tracing::Config;
use log::info;
use once_cell::sync::Lazy;
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

pub struct SledTestContext {
    pub tree_name: String,
    pub db: sled::Db,
}

/// Create a new context for testing sled
pub fn new_sled_test_context() -> SledTestContext {
    SledTestContext {
        tree_name: format!("test-{}-", next_seq()),
        db: get_sled_db(),
    }
}

pub fn next_seq() -> u32 {
    29000u32 + (GlobalSequence::next() as u32)
}
