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
use std::sync::LazyLock;

use databend_common_base::base::tokio::sync::Mutex;
use databend_common_meta_raft_store::config::RaftConfig;
use databend_common_meta_raft_store::state_machine::StateMachine;
use databend_common_meta_stoerr::MetaStorageError;
use log::warn;

/// Local storage that provides the API defined by `kvapi::KVApi+SchemaApi`.
///
/// It is just a wrapped `StateMachine`, which is the same one used by raft driven metasrv.
/// For a local kv, there is no distributed WAL involved,
/// thus it just bypasses the raft log and operate directly on the `StateMachine`.
///
/// Since `StateMachine` is backed with sled::Tree, this impl has the same limitation as metasrv:
/// - A sled::Db has to be a singleton, according to sled doc.
/// - Every unit test has to generate a unique sled::Tree name to create a `MetaEmbedded`.
#[derive(Clone)]
pub struct MetaEmbedded {
    pub(crate) inner: Arc<Mutex<StateMachine>>,
}

static GLOBAL_META_EMBEDDED: LazyLock<Arc<Mutex<Option<Arc<MetaEmbedded>>>>> =
    LazyLock::new(|| Arc::new(Mutex::new(None)));

impl MetaEmbedded {
    /// Creates a kvapi::KVApi impl backed with a `StateMachine`.
    ///
    /// A MetaEmbedded is identified by the `name`.
    /// Caveat: Two instances with the same `name` reference to the same underlying sled::Tree.
    ///
    /// One of the following has to be called to initialize a process-wise sled::Db,
    /// before using `MetaEmbedded`:
    /// - `common_meta_sled_store::init_sled_db`
    /// - `common_meta_sled_store::init_temp_sled_db`
    pub async fn new(name: &str) -> Result<MetaEmbedded, MetaStorageError> {
        let mut config = RaftConfig {
            sled_tree_prefix: format!("{}-local-kv", name),
            ..Default::default()
        };

        if cfg!(target_os = "macos") {
            warn!("Disabled fsync for meta data tests. fsync on mac is quite slow");
            config.no_sync = true;
        }

        let sm = StateMachine::open(&config, 0).await?;
        Ok(MetaEmbedded {
            inner: Arc::new(Mutex::new(sm)),
        })
    }

    /// Initialize a sled db to store embedded meta data.
    /// Initialize a global embedded meta store.
    /// The data in `path` won't be removed after program exit.
    pub async fn init_global_meta_store(path: String) -> Result<(), MetaStorageError> {
        databend_common_meta_sled_store::init_sled_db(path, 64 * 1024 * 1024 * 1024);

        {
            let mut m = GLOBAL_META_EMBEDDED.as_ref().lock().await;
            let r = m.as_ref();

            if r.is_none() {
                let meta = MetaEmbedded::new("global").await?;
                let meta = Arc::new(meta);
                *m = Some(meta);
                return Ok(());
            }
        }

        panic!("global meta store can not init twice");
    }
}
