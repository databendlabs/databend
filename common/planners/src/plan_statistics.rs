// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq, Clone, Debug, Default)]
pub struct Statistics {
    /// Total rows of the query read.
    pub read_rows: usize,
    /// Total bytes of the query read.
    pub read_bytes: usize,
}

impl Statistics {
    pub fn clear(&mut self) {
        *self = Self::default();
    }
}
