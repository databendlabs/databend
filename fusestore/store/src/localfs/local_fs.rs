// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use anyhow::Context;
use async_trait::async_trait;
use common_exception::exception;
use common_exception::ErrorCodes;
use common_exception::ToErrorCodes;

use crate::fs::FileSystem;
use crate::fs::ListResult;

pub struct LocalFS {
    root: PathBuf,
}

/// IFS implementation on local file-system.
impl LocalFS {
    pub fn try_create(root: String) -> anyhow::Result<LocalFS> {
        let f = LocalFS {
            root: PathBuf::from(root),
        };
        Ok(f)
    }
}

#[async_trait]
impl FileSystem for LocalFS {
    #[tracing::instrument(level = "debug", skip(self, data))]
    async fn add(&self, path: String, data: &[u8]) -> anyhow::Result<()> {
        // TODO: test atomicity: write temp file and rename it
        let p = Path::new(self.root.as_path()).join(&path);
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
    async fn read_all(&self, path: String) -> exception::Result<Vec<u8>> {
        let p = Path::new(self.root.as_path()).join(&path);
        tracing::info!("read: {}", p.as_path().display());

        let data = std::fs::read(p.as_path()).map_err_to_code(ErrorCodes::FileDamaged, || {
            format!("localfs: fail to read: {:?}", path)
        })?;
        Ok(data)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn list(&self, path: String) -> anyhow::Result<ListResult> {
        let p = Path::new(self.root.as_path()).join(&path);
        let entries = std::fs::read_dir(p.as_path())
            .with_context(|| format!("LocalFS: fail to list {}", path))?;

        let mut dirs = vec![];
        let mut files = vec![];
        for ent in entries {
            match ent {
                Ok(x) => {
                    let f = x
                        .file_name()
                        .into_string()
                        .map_err(|e| anyhow::anyhow!("LocalFS: invalid fn: {:?}", e))?;

                    let typ = x.file_type()?;

                    if typ.is_dir() {
                        dirs.push(f);
                    } else {
                        files.push(f);
                    }
                }
                Err(e) => return Err(e).context("LocalFS: fail to read entry"),
            }
        }

        Ok(ListResult { dirs, files })
    }
}
