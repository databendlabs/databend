// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_infallible::RwLock;

pub type ProgressRef = Arc<Progress>;
pub type ProgressCallback = fn(&ProgressRef);

#[derive(Clone)]
pub struct Progress {
    read_rows: Arc<RwLock<usize>>,
    read_bytes: Arc<RwLock<usize>>,
}

impl Progress {
    pub fn create() -> ProgressRef {
        Arc::new(Self {
            read_rows: Arc::new(RwLock::new(0)),
            read_bytes: Arc::new(RwLock::new(0)),
        })
    }

    pub fn add_rows(&self, rows: usize) {
        *self.read_rows.write() += rows;
    }

    pub fn add_bytes(&self, bytes: usize) {
        *self.read_bytes.write() += bytes;
    }

    pub fn reset(&self) {
        *self.read_rows.write() = 0;
        *self.read_bytes.write() = 0;
    }
}
