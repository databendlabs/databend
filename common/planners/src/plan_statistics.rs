// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Clone, Debug, Default)]
pub struct Statistics {
    /// Total rows of the query read.
    pub read_rows: usize,
    /// Total bytes of the query read.
    pub read_bytes: usize,
    /// Is the statistics exact.
    pub is_exact: bool,
}

impl Statistics {
    pub fn new_estimated(read_rows: usize, read_bytes: usize) -> Self {
        Statistics {
            read_rows,
            read_bytes,
            is_exact: false,
        }
    }

    pub fn new_exact(read_rows: usize, read_bytes: usize) -> Self {
        Statistics {
            read_rows,
            read_bytes,
            is_exact: true,
        }
    }

    pub fn clear(&mut self) {
        *self = Self::default();
    }
}
