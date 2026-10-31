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

use std::collections::VecDeque;

use hashbrown::HashMap;

/// Bounded FIFO of key hashes with access counts. Remembers keys that are not cached.
///
/// A record taken and then recorded again goes to the back of the FIFO. Its old position stays
/// in the queue until it reaches the front; each position carries the sequence number it was
/// recorded with, so an old position never removes a newer record of the same hash.
pub(crate) struct Ghost {
    /// Hash to (count, sequence number of its live position).
    counts: HashMap<u64, (u8, u64)>,
    order: VecDeque<(u64, u64)>,
    capacity: usize,
    next_seq: u64,
}

impl Ghost {
    pub(crate) fn new(capacity: usize) -> Self {
        Ghost {
            counts: HashMap::new(),
            order: VecDeque::new(),
            capacity,
            next_seq: 0,
        }
    }

    pub(crate) fn take(&mut self, hash: u64) -> Option<u8> {
        self.counts.remove(&hash).map(|(count, _)| count)
    }

    pub(crate) fn record(&mut self, hash: u64, count: u8) {
        if self.capacity == 0 {
            return;
        }
        match self.counts.get_mut(&hash) {
            // Still recorded: the count changes, the position stays.
            Some((recorded, _)) => *recorded = count,
            None => {
                let seq = self.next_seq;
                self.next_seq += 1;
                self.counts.insert(hash, (count, seq));
                self.order.push_back((hash, seq));
            }
        }
        while self.counts.len() > self.capacity {
            let Some((oldest, seq)) = self.order.pop_front() else {
                break;
            };
            self.remove_if_live(oldest, seq);
        }
        // Positions of taken records are dropped in bulk, so the queue stays within twice the
        // capacity.
        if self.order.len() > 2 * self.capacity {
            let counts = &self.counts;
            self.order
                .retain(|(hash, seq)| counts.get(hash).is_some_and(|(_, live)| live == seq));
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.counts.len()
    }

    fn remove_if_live(&mut self, hash: u64, seq: u64) {
        if self.counts.get(&hash).is_some_and(|(_, live)| *live == seq) {
            self.counts.remove(&hash);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Records taken and recorded again are all kept: their old positions remove nothing.
    #[test]
    fn test_old_positions_keep_new_records() {
        const N: u64 = 16_384;
        let mut ghost = Ghost::new(N as usize);
        for hash in 0..N {
            ghost.record(hash, 1);
        }
        for hash in 0..N {
            assert_eq!(ghost.take(hash), Some(1));
        }
        for hash in 0..N {
            ghost.record(hash, 2);
        }
        assert_eq!(ghost.len(), N as usize);
        assert!((0..N).all(|hash| ghost.take(hash) == Some(2)));
    }

    /// A key seen again goes to the back of the window: its first position evicts nothing.
    #[test]
    fn test_retaken_record_moves_to_the_back() {
        let mut ghost = Ghost::new(2);
        ghost.record(1, 1);
        ghost.record(2, 1);
        assert_eq!(ghost.take(1), Some(1));
        ghost.record(1, 2);
        ghost.record(3, 1);
        // 2 is now the oldest record, not 1.
        assert_eq!(ghost.take(2), None);
        assert_eq!(ghost.take(1), Some(2));
        assert_eq!(ghost.take(3), Some(1));
    }

    /// The queue does not grow with records that are taken and recorded again.
    #[test]
    fn test_queue_stays_bounded() {
        let mut ghost = Ghost::new(8);
        for round in 0..=255u8 {
            for hash in 0..8 {
                ghost.take(hash);
                ghost.record(hash, round);
            }
            assert!(ghost.order.len() <= 16);
        }
        assert_eq!(ghost.len(), 8);
    }
}
