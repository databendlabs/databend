// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_infallible::RwLock;

pub type ProgressRef = Arc<Progress>;

/// Progress callback is called with progress about the stream read progress.
pub type ProgressCallback = Box<dyn FnMut(&ProgressRef) + Send + Sync + 'static>;

#[derive(Debug)]
pub struct ProgressValues {
    pub read_rows: usize,
    pub read_bytes: usize,
}

#[derive(Clone, Debug)]
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

    pub fn get_values(&self) -> ProgressValues {
        let read_rows = *self.read_rows.read() as usize;
        let read_bytes = *self.read_bytes.read() as usize;

        ProgressValues {
            read_rows,
            read_bytes,
        }
    }

    pub fn reset(&self) {
        *self.read_rows.write() = 0;
        *self.read_bytes.write() = 0;
    }

    // Placeholder for default callback init.
    pub fn default_callback(_: &ProgressRef) {}
}
