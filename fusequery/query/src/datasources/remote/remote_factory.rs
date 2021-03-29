// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;

use anyhow::Result;

use crate::datasources::datasource::DatabaseHashMap;

pub struct RemoteFactory;

impl RemoteFactory {
    pub fn create() -> Self {
        Self
    }

    pub fn get_tables(&self) -> Result<DatabaseHashMap> {
        let hashmap: DatabaseHashMap = HashMap::default();
        Ok(hashmap)
    }
}
