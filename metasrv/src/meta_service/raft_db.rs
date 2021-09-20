// Copyright 2020 Datafuse Labs.
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

use lazy_static::lazy_static;
use tempfile::TempDir;

pub(crate) struct GlobalSledDb {
    /// When opening a db on a temp dir, the temp dir guard must be held.
    #[allow(dead_code)]
    pub(crate) temp_dir: Option<TempDir>,
    pub(crate) db: sled::Db,
}

lazy_static! {
    static ref GLOBAL_SLED: Arc<Mutex<Option<GlobalSledDb>>> = Arc::new(Mutex::new(None));
}

/// Open a db at a temp dir. For test purpose only.
pub fn init_temp_sled_db(temp_dir: TempDir) {
    let mut g = GLOBAL_SLED.as_ref().lock().unwrap();

    if g.is_some() {
        return;
    }

    let path = temp_dir.path().to_str().unwrap().to_string();

    *g = Some(GlobalSledDb {
        temp_dir: Some(temp_dir),
        db: sled::open(path).expect("open global sled::Db"),
    });
}

pub fn init_sled_db(path: String) {
    let mut g = GLOBAL_SLED.as_ref().lock().unwrap();

    if g.is_some() {
        return;
    }

    *g = Some(GlobalSledDb {
        temp_dir: None,
        db: sled::open(path).expect("open global sled::Db"),
    });
}

pub fn get_sled_db() -> sled::Db {
    let x = GLOBAL_SLED.as_ref().lock().unwrap();
    let y = x.as_ref().expect(
        "init_sled_db() or init_temp_sled_db() has to be called before using get_sled_db()",
    );
    y.db.clone()
}
