// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

/// Progress callback is called with progress about the stream read progress.
pub type ProgressCallback = Box<dyn FnMut(&ProgressValues) + Send + Sync + 'static>;

#[derive(Debug)]
pub struct ProgressValues {
    pub read_rows: usize,
    pub read_bytes: usize
}

#[derive(Debug)]
pub struct Progress {
    read_rows: AtomicU64,
    read_bytes: AtomicU64
}

impl Progress {
    pub fn create() -> Self {
        Self {
            read_rows: AtomicU64::new(0),
            read_bytes: AtomicU64::new(0)
        }
    }

    pub fn add_rows(&mut self, rows: usize) {
        self.read_rows.fetch_add(rows as u64, Ordering::Relaxed);
    }

    pub fn add_bytes(&mut self, bytes: usize) {
        self.read_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn get_values(&self) -> ProgressValues {
        let read_rows = self.read_rows.load(Ordering::Relaxed) as usize;
        let read_bytes = self.read_bytes.load(Ordering::Relaxed) as usize;
        ProgressValues {
            read_rows,
            read_bytes
        }
    }

    pub fn reset(&mut self) {
        self.read_rows = AtomicU64::new(0);
        self.read_bytes = AtomicU64::new(0);
    }

    // Placeholder for default callback init.
    pub fn default_callback(_: &Progress) {}
}
