// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

//! This crate defines data types used in meta data storage service.

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Formatter;

pub use errors::ConflictSeq;
pub use match_seq::MatchSeq;
pub use match_seq::MatchSeqExt;
use serde::Deserialize;
use serde::Serialize;

mod errors;
mod match_seq;

#[cfg(test)]
mod match_seq_test;

/// Value with a corresponding sequence number
pub type SeqValue<T = Vec<u8>> = (u64, T);

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct Database {
    pub database_id: u64,

    /// tables belong to this database.
    pub tables: HashMap<String, u64>,
}

impl fmt::Display for Database {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "database id: {}", self.database_id)
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct Table {
    pub table_id: u64,

    /// serialized schema
    pub schema: Vec<u8>,

    /// name of parts that belong to this table.
    pub parts: HashSet<String>,
}

impl fmt::Display for Table {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "table id: {}", self.table_id)
    }
}

pub type MetaVersion = u64;
pub type MetaId = u64;
