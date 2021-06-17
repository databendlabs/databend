// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_exception::Result;

use crate::datasources::system::SystemDatabase;
use crate::datasources::Database;

pub struct SystemFactory;

impl SystemFactory {
    pub fn create() -> Self {
        Self
    }

    pub fn load_databases(&self) -> Result<Vec<Arc<dyn Database>>> {
        let databases: Vec<Arc<dyn Database>> = vec![Arc::new(SystemDatabase::create())];
        Ok(databases)
    }
}
