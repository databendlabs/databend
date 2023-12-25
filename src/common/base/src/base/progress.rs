// Copyright 2021 Datafuse Labs
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

use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct ProgressValues {
    pub rows: usize,
    pub bytes: usize,
}

#[derive(Debug)]
pub struct Progress {
    rows: AtomicUsize,
    bytes: AtomicUsize,
}

impl Progress {
    pub fn create() -> Self {
        Self {
            rows: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
        }
    }

    pub fn incr(&self, progress_values: &ProgressValues) {
        self.rows.fetch_add(progress_values.rows, Ordering::Relaxed);
        self.bytes
            .fetch_add(progress_values.bytes, Ordering::Relaxed);
    }

    pub fn set(&self, progress_values: &ProgressValues) {
        self.rows.store(progress_values.rows, Ordering::Relaxed);
        self.bytes.store(progress_values.bytes, Ordering::Relaxed);
    }

    pub fn fetch(&self) -> ProgressValues {
        let rows = self.rows.fetch_min(0, Ordering::SeqCst);
        let bytes = self.bytes.fetch_min(0, Ordering::SeqCst);

        ProgressValues { rows, bytes }
    }

    pub fn get_values(&self) -> ProgressValues {
        let rows = self.rows.load(Ordering::Relaxed);
        let bytes = self.bytes.load(Ordering::Relaxed);
        ProgressValues { rows, bytes }
    }
}
