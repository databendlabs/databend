// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use async_trait::async_trait;
use common_exception::ErrorCode;
use common_exception::Result;
use common_runtime::tokio::sync::Mutex;

use crate::backends::Backend;
use crate::backends::Lock;

pub struct LocalBackend {
    db: sled::Db,
    lock: Arc<Mutex<()>>,
}

impl LocalBackend {
    pub fn create(_addr: String) -> Self {
        Self {
            db: sled::Config::new().temporary(true).open().unwrap(),
            lock: Arc::new(Mutex::new(())),
        }
    }
}

#[async_trait]
impl Backend for LocalBackend {
    async fn get(&self, key: String) -> Result<Option<String>> {
        Ok(self
            .db
            .get(key)
            .map_err(|e| ErrorCode::UnknownException(e.to_string()))?
            .map(|v| std::str::from_utf8(&v).unwrap().to_owned()))
    }

    async fn get_from_prefix(&self, prefix: String) -> Result<Vec<(String, String)>> {
        Ok(self
            .db
            .scan_prefix(prefix)
            .map(|v| {
                v.map(|(key, value)| {
                    (
                        std::str::from_utf8(&key).unwrap().to_owned(),
                        std::str::from_utf8(&value).unwrap().to_owned(),
                    )
                })
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| ErrorCode::UnknownException(e.to_string()))?)
    }

    async fn put(&self, key: String, value: String) -> Result<()> {
        self.db
            .insert(key.as_bytes(), value.as_bytes())
            .map_err(|e| ErrorCode::UnknownException(e.to_string()))
            .map(|_| ())
    }

    async fn remove(&self, key: String) -> Result<()> {
        self.db
            .remove(key)
            .map_err(|e| ErrorCode::UnknownException(e.to_string()))
            .map(|_| ())
    }

    async fn lock(&self, _key: String) -> Result<Box<dyn Lock>> {
        Ok(Box::new(self.lock.clone().lock_owned().await))
    }
}
