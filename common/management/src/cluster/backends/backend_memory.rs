// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;

use async_trait::async_trait;
use common_exception::Result;
use common_runtime::tokio::sync::RwLock;

use crate::cluster::cluster_backend::ClusterBackend;
use crate::ClusterExecutor;

pub struct MemoryBackend {
    db: RwLock<HashMap<String, Vec<ClusterExecutor>>>,
}

impl MemoryBackend {
    pub fn create() -> Self {
        Self {
            db: RwLock::new(HashMap::default()),
        }
    }
}

#[async_trait]
impl ClusterBackend for MemoryBackend {
    async fn put(&self, namespace: String, executor: &ClusterExecutor) -> Result<()> {
        let mut db = self.db.write().await;

        let executors = db.get_mut(&namespace);
        match executors {
            None => {
                db.insert(namespace, vec![executor.clone()]);
            }
            Some(values) => {
                let mut new_values = vec![];
                for value in values {
                    if value != executor {
                        new_values.push(value.clone());
                    }
                }
                new_values.push(executor.clone());
                db.insert(namespace, new_values);
            }
        };
        Ok(())
    }

    async fn remove(&self, namespace: String, executor: &ClusterExecutor) -> Result<()> {
        let mut db = self.db.write().await;

        let executors = db.get_mut(&namespace);
        match executors {
            None => return Ok(()),
            Some(values) => {
                let mut new_values = vec![];
                for value in values {
                    if value != executor {
                        new_values.push(value.clone());
                    }
                }
                db.insert(namespace, new_values);
            }
        };
        Ok(())
    }

    async fn get(&self, namespace: String) -> Result<Vec<ClusterExecutor>> {
        let db = self.db.read().await;
        let executors = db.get(&namespace);
        let res = match executors {
            None => vec![],
            Some(v) => v.clone(),
        };
        Ok(res)
    }
}
