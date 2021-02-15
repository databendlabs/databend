// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

use std::collections::HashMap;

use crate::datasources::datasource::DatabaseHashMap;
use crate::error::FuseQueryResult;

pub struct RemoteFactory;

impl RemoteFactory {
    pub fn create() -> Self {
        Self
    }

    pub fn get_tables(&self) -> FuseQueryResult<DatabaseHashMap> {
        let hashmap: DatabaseHashMap = HashMap::default();
        Ok(hashmap)
    }
}
