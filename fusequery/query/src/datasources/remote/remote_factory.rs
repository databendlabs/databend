// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use anyhow::Result;

use crate::datasources::IDatabase;

pub struct RemoteFactory;

impl RemoteFactory {
    pub fn create() -> Self {
        Self
    }

    pub fn load_databases(&self) -> Result<Vec<Arc<dyn IDatabase>>> {
        Ok(vec![])
    }
}
