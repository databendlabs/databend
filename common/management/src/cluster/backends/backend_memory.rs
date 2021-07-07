// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;

use async_trait::async_trait;
use common_exception::Result;
use common_runtime::tokio::sync::RwLock;

use crate::cluster::backend_api::BackendApi;
use crate::ClusterMeta;

pub struct MemoryBackend {
    db: RwLock<HashMap<String, Vec<ClusterMeta>>>,
}

impl MemoryBackend {
    pub fn create() -> Self {
        Self {
            db: RwLock::new(HashMap::default()),
        }
    }
}

#[async_trait]
impl BackendApi for MemoryBackend {
    async fn put(&self, key: String, meta: &ClusterMeta) -> Result<()> {
        let mut db = self.db.write().await;

        let metas = db.get_mut(&key);
        match metas {
            None => {
                db.insert(key, vec![meta.clone()]);
            }
            Some(values) => {
                let mut new_values = vec![];
                for value in values {
                    if value != meta {
                        new_values.push(value.clone());
                    }
                }
                new_values.push(meta.clone());
                db.insert(key, new_values);
            }
        };
        Ok(())
    }

    async fn get(&self, key: String) -> Result<Vec<ClusterMeta>> {
        let db = self.db.read().await;
        let metas = db.get(&key);
        let res = match metas {
            None => vec![],
            Some(v) => v.clone(),
        };
        Ok(res)
    }
}
