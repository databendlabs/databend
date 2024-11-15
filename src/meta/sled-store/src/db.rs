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

//! Define a process-wise global sled::Db.
//! sled::Db does not allow to open multiple db in one process.
//! One of the known issue is that `flush_async()` in different tokio runtime on different db result in a deadlock.

use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;

use log::warn;
use tempfile::TempDir;

pub(crate) struct GlobalSledDb {
    /// When opening a db on a temp dir, the temp dir guard must be held.
    pub(crate) temp_dir: Option<TempDir>,
    pub(crate) path: String,
    pub(crate) db: sled::Db,
}

impl GlobalSledDb {
    pub(crate) fn new(path: String, cache_size: u64) -> Self {
        let db = sled::Config::default()
            .path(&path)
            .cache_capacity(cache_size as usize)
            .mode(sled::Mode::HighThroughput)
            .open()
            .unwrap_or_else(|e| panic!("open global sled::Db(path: {}): {}", path, e));

        GlobalSledDb {
            temp_dir: None,
            path: path.clone(),
            db,
        }
    }
}

static GLOBAL_SLED: LazyLock<Arc<Mutex<Option<GlobalSledDb>>>> =
    LazyLock::new(|| Arc::new(Mutex::new(None)));

pub fn init_sled_db(path: String, cache_size: u64) {
    let (inited_as_temp, curr_path) = {
        let mut g = GLOBAL_SLED.as_ref().lock().unwrap();
        if let Some(gdb) = g.as_ref() {
            (gdb.temp_dir.is_some(), gdb.path.clone())
        } else {
            *g = Some(GlobalSledDb::new(path, cache_size));
            return;
        }
    };

    if inited_as_temp {
        warn!(
            "sled db is already initialized with temp dir: {}, can not re-init with path {}",
            curr_path, path
        );
    }
}

pub fn get_sled_db() -> sled::Db {
    {
        let guard = GLOBAL_SLED.as_ref().lock().unwrap();
        let glb_opt = guard.as_ref();
        match glb_opt {
            None => {}
            Some(g) => return g.db.clone(),
        }
    }

    panic!("init_sled_db() or init_temp_sled_db() has to be called before using get_sled_db()");
}

pub fn init_get_sled_db(path: String, cache_size: u64) -> sled::Db {
    init_sled_db(path, cache_size);
    get_sled_db()
}

/// Drop the global sled db.
///
/// Which means this program will not use sled db anymore.
pub fn drop_sled_db() {
    {
        let mut guard = GLOBAL_SLED.as_ref().lock().unwrap();
        *guard = None;
    }
}
