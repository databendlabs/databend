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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use common_base::tokio::sync::Mutex;
use common_meta_raft_store::config::RaftConfig;
use common_meta_raft_store::state_machine::StateMachine;
pub use common_meta_sled_store::init_temp_sled_db;
use common_tracing::tracing;
use once_cell::sync::Lazy;

/// Local storage that provides the API defined by `KVApi+MetaApi`.
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

static GLOBAL_META_EMBEDDED: Lazy<Arc<std::sync::Mutex<Option<Arc<MetaEmbedded>>>>> =
    Lazy::new(|| Arc::new(std::sync::Mutex::new(None)));

impl MetaEmbedded {
    /// Creates a KVApi impl backed with a `StateMachine`.
    ///
    /// A MetaEmbedded is identified by the `name`.
    /// Caveat: Two instances with the same `name` reference to the same underlying sled::Tree.
    ///
    /// One of the following has to be called to initialize a process-wise sled::Db,
    /// before using `MetaEmbedded`:
    /// - `common_meta_sled_store::init_sled_db`
    /// - `common_meta_sled_store::init_temp_sled_db`
    pub async fn new(name: &str) -> common_exception::Result<MetaEmbedded> {
        let mut config = RaftConfig::empty();

        config.sled_tree_prefix = format!("{}-local-kv", name);

        if cfg!(target_os = "macos") {
            tracing::warn!("Disabled fsync for meta data tests. fsync on mac is quite slow");
            config.no_sync = true;
        }

        Ok(MetaEmbedded {
            // StateMachine does not need to be replaced, thus we always use id=0
            inner: Arc::new(Mutex::new(StateMachine::open(&config, 0).await?)),
        })
    }

    /// Creates a KVApi impl with a random and unique name.
    pub async fn new_temp() -> common_exception::Result<MetaEmbedded> {
        let temp_dir = tempfile::tempdir()?;
        common_meta_sled_store::init_temp_sled_db(temp_dir);

        // generate a unique id as part of the name of sled::Tree

        static GLOBAL_SEQ: AtomicUsize = AtomicUsize::new(0);
        let x = GLOBAL_SEQ.fetch_add(1, Ordering::SeqCst);
        let id = 29000_u64 + (x as u64);

        let name = format!("temp-{}", id);

        Self::new(&name).await
    }

    /// Initialize a sled db to store embedded meta data.
    /// Initialize a global embedded meta store.
    /// The data in `path` won't be removed after program exit.
    pub async fn init_global_meta_store(path: String) -> common_exception::Result<()> {
        common_meta_sled_store::init_sled_db(path);

        {
            let mut m = GLOBAL_META_EMBEDDED.as_ref().lock().unwrap();
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

    /// If global meta store is initialized, return it(production use).
    /// Otherwise, return a meta store backed with temp dir for test.
    pub async fn get_meta() -> common_exception::Result<Arc<MetaEmbedded>> {
        {
            let m = GLOBAL_META_EMBEDDED.as_ref().lock().unwrap();
            let r = m.as_ref();

            if let Some(x) = r {
                return Ok(x.clone());
            }
        }

        let meta = MetaEmbedded::new_temp().await?;
        let meta = Arc::new(meta);
        Ok(meta)
    }
}
