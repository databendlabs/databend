// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[derive(Clone, Debug)]
pub struct Statistics {
    /// Total rows of the query read.
    pub read_rows: usize,
    /// Total bytes of the query read.
    pub read_bytes: u64,
}

impl Statistics {
    pub fn default() -> Self {
        Statistics {
            read_rows: 0,
            read_bytes: 0,
        }
    }
}
