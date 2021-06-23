// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

//! This crate defines data types used in meta data storage service.

use std::collections::HashMap;
use std::collections::HashSet;

use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct Database {
    pub database_id: u64,

    /// tables belong to this database.
    pub tables: HashMap<String, u64>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct Table {
    pub table_id: u64,

    /// serialized schema
    pub schema: Vec<u8>,

    /// name of parts that belong to this table.
    pub parts: HashSet<String>,
}
