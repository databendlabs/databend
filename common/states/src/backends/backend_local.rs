// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;

use async_trait::async_trait;
use common_exception::Result;
use common_runtime::tokio::sync::RwLock;

use crate::backends::StateBackend;
use crate::Backend;

pub struct LocalBackend {
    db: RwLock<HashMap<String, String>>,
}

impl LocalBackend {
    pub fn create(_addr: String) -> Self {
        Self {
            db: RwLock::new(HashMap::default()),
        }
    }
}

#[async_trait]
impl StateBackend for LocalBackend {
    async fn put(&self, key: String, value: String) -> Result<()> {
        let mut db = self.db.write().await;
        db.insert(key, value);
        Ok(())
    }

    async fn remove(&self, key: String) -> Result<()> {
        let mut db = self.db.write().await;
        db.remove(key.as_str());
        Ok(())
    }

    async fn get(&self, key: String) -> Result<Option<String>> {
        let mut db = self.db.write().await;
        let res = db.get(key.as_str());
        Ok(match res {
            None => None,
            Some(v) => Some(v.clone()),
        })
    }
}
