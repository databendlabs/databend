// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

//! This crate defines data types used in meta data storage service.

use std::collections::HashMap;
use std::collections::HashSet;

use serde::Deserialize;
use serde::Serialize;

/// Value with a corresponding sequence number
pub type SeqValue = (u64, Vec<u8>);

/// Describes what `seq` an operation must match to take effect.
/// Every value written to meta data has a unique `seq` bound.
/// Any conditioned or non-conditioned write operation can be done through the corresponding MatchSeq.
#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq)]
pub enum MatchSeq {
    /// Any value is acceptable, i.e. does not check seq at all.
    Any,

    /// To match an exact value of seq.
    /// E.g., CAS updates the exact version of some value,
    /// and put-if-absent adds a value only when seq is 0.
    Exact(u64),

    /// To match a seq that is greater-or-equal some value.
    /// E.g., GE(1) perform an update on any existent value.
    GE(u64),
}

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
