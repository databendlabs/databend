// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::Result;

use crate::datasources::local::LocalDatabase;
use crate::datasources::IDatabase;

pub struct LocalFactory;

impl LocalFactory {
    pub fn create() -> Self {
        Self
    }

    pub fn load_databases(&self) -> Result<Vec<Arc<dyn IDatabase>>> {
        let databases: Vec<Arc<dyn IDatabase>> = vec![Arc::new(LocalDatabase::create())];
        Ok(databases)
    }
}
