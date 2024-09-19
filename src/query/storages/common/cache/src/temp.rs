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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::ops::Drop;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;

use databend_common_base::base::GlobalUniqName;

pub struct TempFileManager {
    root: Box<Path>,
    total_limit: usize,
    dir_limit: usize,
    _reserved: usize,

    group: Mutex<Group>,
}

struct Group {
    dirs: HashMap<Box<Path>, Arc<DirInfo>>,
}

impl Group {
    fn size(&self) -> usize {
        self.dirs.values().map(|v| *v.size.lock().unwrap()).sum()
    }
}

#[derive(Debug, Default)]
struct DirInfo {
    count: AtomicUsize,
    size: Mutex<usize>,
}

impl TempFileManager {
    pub fn get_dir(self: &Arc<TempFileManager>, id: &str) -> TempDir {
        let path = self.root.join(id).into_boxed_path();

        let mut group = self.group.lock().unwrap();

        match group.dirs.entry(path.clone()) {
            Entry::Occupied(o) => TempDir {
                path,
                dir_info: o.get().clone(),
                manager: self.clone(),
            },
            Entry::Vacant(v) => {
                let dir_info = Arc::new(DirInfo::default());
                v.insert(dir_info.clone());
                TempDir {
                    path,
                    dir_info,
                    manager: self.clone(),
                }
            }
        }
    }

    // pub fn used(&self,size :usize) -> Result<usize> {
    //     let stat = rustix::fs::statvfs(self.root)?;
    //     stat.f_bavail > self.reserved + (size +stat.f_frsize-1)/stat.f_frsize
    // }
}

pub struct TempDir {
    path: Box<Path>,
    dir_info: Arc<DirInfo>,
    manager: Arc<TempFileManager>,
}

impl TempDir {
    pub fn new_file_with_size(&self, size: usize) -> Option<TempFile> {
        let path = self.path.join(GlobalUniqName::unique()).into_boxed_path();

        let dir_info = self.dir_info.clone();

        let group = self.manager.group.lock().unwrap();
        let mut dir_size = dir_info.size.lock().unwrap();
        if self.manager.dir_limit < *dir_size + size
            || self.manager.total_limit < group.size() + size
        {
            return None;
        }

        *dir_size += size;
        drop(dir_size);

        dir_info.count.fetch_add(1, Ordering::SeqCst);
        Some(TempFile {
            path,
            size,
            dir_info,
        })
    }
}

#[derive(Debug)]
pub struct TempFile {
    path: Box<Path>,
    size: usize,
    dir_info: Arc<DirInfo>,
}

impl Drop for TempFile {
    fn drop(&mut self) {
        self.dir_info.count.fetch_sub(1, Ordering::SeqCst);

        let mut guard = self.dir_info.size.lock().unwrap();
        *guard -= self.size;

        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_xxx() {
        println!("aa")
    }
}
