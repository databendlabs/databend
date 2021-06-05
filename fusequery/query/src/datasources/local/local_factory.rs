// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::Result;

use crate::datasources::local::LocalDatabase;
use crate::datasources::Database;

pub struct LocalFactory;

impl LocalFactory {
    pub fn create() -> Self {
        Self
    }

    pub fn load_databases(&self) -> Result<Vec<Arc<dyn Database>>> {
        let databases: Vec<Arc<dyn Database>> = vec![Arc::new(LocalDatabase::create())];
        Ok(databases)
    }
}
