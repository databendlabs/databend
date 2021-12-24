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

//! Define a process-wise global sled::Db.
//! sled::Db does not allow to open multiple db in one process.
//! One of the known issue is that `flush_asynce()` in different tokio runtime on different db result in a deadlock.

use std::sync::Arc;
use std::sync::Mutex;

use once_cell::sync::Lazy;
use tempfile::TempDir;

pub(crate) struct GlobalSledDb {
    /// When opening a db on a temp dir, the temp dir guard must be held.
    #[allow(dead_code)]
    pub(crate) temp_dir: Option<TempDir>,
    pub(crate) path: String,
    pub(crate) db: sled::Db,
}

impl GlobalSledDb {
    pub(crate) fn new_temp(temp_dir: TempDir) -> Self {
        let temp_path = temp_dir.path().to_str().unwrap().to_string();

        GlobalSledDb {
            temp_dir: Some(temp_dir),
            path: temp_path.clone(),
            db: sled::open(temp_path).expect("open global sled::Db"),
        }
    }

    pub(crate) fn new(path: String) -> Self {
        GlobalSledDb {
            temp_dir: None,
            path: path.clone(),
            db: sled::open(path).expect("open global sled::Db"),
        }
    }
}

static GLOBAL_SLED: Lazy<Arc<Mutex<Option<GlobalSledDb>>>> =
    Lazy::new(|| Arc::new(Mutex::new(None)));

/// Open a db at a temp dir. For test purpose only.
pub fn init_temp_sled_db(temp_dir: TempDir) {
    let temp_path = temp_dir.path().to_str().unwrap().to_string();

    let (inited_as_temp, curr_path) = {
        let mut g = GLOBAL_SLED.as_ref().lock().unwrap();
        if let Some(gdb) = g.as_ref() {
            (gdb.temp_dir.is_some(), gdb.path.clone())
        } else {
            *g = Some(GlobalSledDb::new_temp(temp_dir));
            return;
        }
    };

    if !inited_as_temp {
        panic!("sled db is already initialized with specified path: {}, can not re-init with temp path {}", curr_path, temp_path);
    }
}

pub fn init_sled_db(path: String) {
    let (inited_as_temp, curr_path) = {
        let mut g = GLOBAL_SLED.as_ref().lock().unwrap();
        if let Some(gdb) = g.as_ref() {
            (gdb.temp_dir.is_some(), gdb.path.clone())
        } else {
            *g = Some(GlobalSledDb::new(path));
            return;
        }
    };

    if inited_as_temp {
        panic!(
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
