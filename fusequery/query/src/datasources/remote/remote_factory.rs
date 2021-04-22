// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use anyhow::Result;

use crate::configs::Config;
use crate::datasources::remote::RemoteDatabase;
use crate::datasources::IDatabase;

pub struct RemoteFactory {
    conf: Config
}

impl RemoteFactory {
    pub fn create(conf: Config) -> Self {
        Self { conf }
    }

    pub fn load_databases(&self) -> Result<Vec<Arc<dyn IDatabase>>> {
        // Load databases from remote.
        let databases: Vec<Arc<dyn IDatabase>> = vec![Arc::new(RemoteDatabase::create(
            self.conf.clone(),
            "for_test".to_string()
        ))];
        Ok(databases)
    }
}
