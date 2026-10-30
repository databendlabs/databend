// Copyright 2026 Datafuse Labs
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

use std::collections::VecDeque;

use hashbrown::HashMap;

/// Bounded FIFO of key hashes with access counts. Remembers keys that are not cached.
pub(crate) struct Ghost {
    counts: HashMap<u64, u8>,
    order: VecDeque<u64>,
    capacity: usize,
}

impl Ghost {
    pub(crate) fn new(capacity: usize) -> Self {
        Ghost {
            counts: HashMap::new(),
            order: VecDeque::new(),
            capacity,
        }
    }

    pub(crate) fn take(&mut self, hash: u64) -> Option<u8> {
        self.counts.remove(&hash)
    }

    pub(crate) fn record(&mut self, hash: u64, count: u8) {
        if self.capacity == 0 {
            return;
        }
        if self.counts.insert(hash, count).is_none() {
            self.order.push_back(hash);
        }
        while self.order.len() > self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.counts.remove(&oldest);
            }
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.counts.len()
    }
}
