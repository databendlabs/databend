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

use std::collections::BTreeMap;
use std::sync::Once;

use databend_common_base::base::GlobalSequence;
use databend_common_meta_raft_store::config::RaftConfig;
use databend_common_meta_sled_store::get_sled_db;
use databend_common_meta_sled_store::sled;
use databend_common_tracing::closure_name;
use databend_common_tracing::init_logging;
use databend_common_tracing::Config;
use fastrace::prelude::*;

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

pub fn raft_store_test_harness<F, Fut>(test: F)
where
    F: FnOnce() -> Fut + 'static,
    Fut: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
{
    setup_test();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(3)
        .enable_all()
        .build()
        .unwrap();
    let root = Span::root(closure_name::<F>(), SpanContext::random());
    let test = test().in_span(root);
    rt.block_on(test).unwrap();

    shutdown_test();
}

fn setup_test() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let t = tempfile::tempdir().expect("create temp dir to sled db");
        databend_common_meta_sled_store::init_temp_sled_db(t);

        let guards = init_logging("meta_unittests", &Config::new_testing(), BTreeMap::new());
        Box::leak(Box::new(guards));
    });
}

fn shutdown_test() {
    fastrace::flush();
}
