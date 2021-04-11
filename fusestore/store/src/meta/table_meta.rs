// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::time::SystemTime;

use common_datavalues::DataSchema;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct TableMeta {
    // Never change if created
    pub table_uuid: u128,
    pub db_name: String,
    // may be altered
    pub tbl_name: String,

    // TODO
    // We need another data structure for schema evolution,
    // sth like a mapping from column id to column metadata?
    pub schema: DataSchema,

    //TODO use chrono time
    pub create_ts: SystemTime,
    pub creator: String,

    // TODO refine this later
    pub data_files: Vec<String>,
}

impl TableMeta {
    pub fn new() -> Self {
        todo!()
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct TableSnapshot {
    // version of table specification
    pub spec_version: u8,
    // consecutive increasing
    pub sequence: u64,
    // pointer to the table's metadata
    pub meta_uri: String,

    // redundancy info, for the convenience of ...
    pub table_name: String,
    pub db_name: String,
    pub table_uuid: u128,
}
