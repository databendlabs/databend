// Copyright 2020-2021 The FuseQuery Authors.
//
// Code is licensed under Apache License, Version 2.0.

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

    pub fn clear(&mut self) {
        self.read_rows = 0;
        self.read_bytes = 0;
    }
}
