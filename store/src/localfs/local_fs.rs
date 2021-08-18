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
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use anyhow::Context;
use async_trait::async_trait;
use common_exception::exception;
use common_exception::ErrorCode;
use common_exception::ToErrorCode;
use common_tracing::tracing;

use crate::fs::FileSystem;
use crate::fs::ListResult;

pub struct LocalFS {
    root: PathBuf,
}

/// IFS implementation on local file-system.
impl LocalFS {
    pub fn try_create(root: String) -> common_exception::Result<LocalFS> {
        let f = LocalFS {
            root: PathBuf::from(root),
        };
        Ok(f)
    }
}

#[async_trait]
impl FileSystem for LocalFS {
    #[tracing::instrument(level = "debug", skip(self, data))]
    async fn add(&self, path: &str, data: &[u8]) -> common_exception::Result<()> {
        // TODO: test atomicity: write temp file and rename it
        let p = Path::new(self.root.as_path()).join(path);
        let mut an = p.ancestors();
        let _tail = an.next();
        let base = an.next();
        if let Some(b) = base {
            std::fs::create_dir_all(b)
                .with_context(|| format!("LocalFS: fail create dir {}", b.display()))?
        };

        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(p.as_path())
            .with_context(|| format!("LocalFS: fail to open {}", path))?;

        f.write_all(data)
            .with_context(|| format!("LocalFS: fail to write {}", path))?;

        f.sync_all()
            .with_context(|| format!("LocalFS: fail to sync {}", path))?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn read_all(&self, path: &str) -> exception::Result<Vec<u8>> {
        let p = Path::new(self.root.as_path()).join(path);
        tracing::info!("read: {}", p.as_path().display());

        let data = std::fs::read(p.as_path()).map_err_to_code(ErrorCode::FileDamaged, || {
            format!("LocalFS: fail to read: {:?}", path)
        })?;
        Ok(data)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn list(&self, path: &str) -> common_exception::Result<ListResult> {
        let p = Path::new(self.root.as_path()).join(path);
        let entries = std::fs::read_dir(p.as_path())
            .with_context(|| format!("LocalFS: fail to list {}", path))?;

        let mut dirs = vec![];
        let mut files = vec![];
        for ent in entries {
            match ent {
                Ok(x) => {
                    let f = x.file_name().into_string().map_err(|x| {
                        ErrorCode::IllegalFileName(format!(
                            "{:?} when list local files",
                            x.to_str()
                        ))
                    })?;

                    let typ = x.file_type()?;

                    if typ.is_dir() {
                        dirs.push(f);
                    } else {
                        files.push(f);
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }

        Ok(ListResult { dirs, files })
    }
}
