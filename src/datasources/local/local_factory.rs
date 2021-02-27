// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;

use crate::datasources::datasource::DatabaseHashMap;
use crate::error::FuseQueryResult;

pub struct LocalFactory;

impl LocalFactory {
    pub fn create() -> Self {
        Self
    }

    pub fn get_tables(&self) -> FuseQueryResult<DatabaseHashMap> {
        let hashmap: DatabaseHashMap = HashMap::default();
        Ok(hashmap)
    }
}
