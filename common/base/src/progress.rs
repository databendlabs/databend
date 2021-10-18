// Copyright 2020 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

/// Progress callback is called with progress about the stream read progress.
pub type ProgressCallback = Box<dyn FnMut(&ProgressValues) + Send + Sync + 'static>;

#[derive(Debug)]
pub struct ProgressValues {
    pub read_rows: usize,
    pub read_bytes: usize,
    pub total_rows_to_read: usize,
}

#[derive(Debug)]
pub struct Progress {
    read_rows: AtomicUsize,
    read_bytes: AtomicUsize,
    total_rows_to_read: AtomicUsize,
}

impl Progress {
    pub fn create() -> Self {
        Self {
            read_rows: AtomicUsize::new(0),
            read_bytes: AtomicUsize::new(0),
            total_rows_to_read: AtomicUsize::new(0),
        }
    }

    pub fn incr(&self, progress_values: &ProgressValues) {
        self.read_rows
            .fetch_add(progress_values.read_rows, Ordering::Relaxed);
        self.read_bytes
            .fetch_add(progress_values.read_bytes, Ordering::Relaxed);
        self.total_rows_to_read
            .fetch_add(progress_values.total_rows_to_read, Ordering::Relaxed);
    }

    pub fn get_values(&self) -> ProgressValues {
        let read_rows = self.read_rows.load(Ordering::Relaxed) as usize;
        let read_bytes = self.read_bytes.load(Ordering::Relaxed) as usize;
        let total_rows_to_read = self.total_rows_to_read.load(Ordering::Relaxed) as usize;
        ProgressValues {
            read_rows,
            read_bytes,
            total_rows_to_read,
        }
    }

    pub fn reset(&self) {
        self.read_rows.store(0, Ordering::Relaxed);
        self.read_bytes.store(0, Ordering::Relaxed);
        self.total_rows_to_read.store(0, Ordering::Relaxed);
    }

    pub fn get_and_reset(&self) -> ProgressValues {
        let read_rows = self.read_rows.fetch_and(0, Ordering::Relaxed) as usize;
        let read_bytes = self.read_bytes.fetch_and(0, Ordering::Relaxed) as usize;
        let total_rows_to_read = self.total_rows_to_read.fetch_and(0, Ordering::Relaxed) as usize;
        ProgressValues {
            read_rows,
            read_bytes,
            total_rows_to_read,
        }
    }

    pub fn add_total_rows_approx(&self, total_rows: usize) {
        self.total_rows_to_read
            .fetch_add(total_rows, Ordering::Relaxed);
    }

    // Placeholder for default callback init.
    pub fn default_callback(_: &Progress) {}
}
