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

/// Bounded FIFO of the hashes of keys evicted from probation, which are no longer cached.
///
/// Bounded by records and by the bytes of the entries they stand for, as libCacheSim and foyer
/// size their ghosts in bytes: a ghost of large entries remembers about as much data as the
/// cache holds rather than many times more. The newest record is kept even when larger than
/// the byte bound alone, so that any entry can be seen twice.
///
/// A record taken and then recorded again goes to the back of the FIFO. Its old position stays
/// in the queue until it reaches the front; each position carries the sequence number it was
/// recorded with, so an old position never removes a newer record of the same hash.
pub(crate) struct Ghost {
    /// Hash to its record.
    records: HashMap<u64, Record>,
    order: VecDeque<(u64, u64)>,
    capacity: usize,
    byte_capacity: usize,
    /// Bytes of the recorded entries.
    bytes: usize,
    next_seq: u64,
}

struct Record {
    bytes: usize,
    /// Sequence number of the live position.
    seq: u64,
}

impl Ghost {
    pub(crate) fn new(capacity: usize, byte_capacity: usize) -> Self {
        Ghost {
            records: HashMap::new(),
            order: VecDeque::new(),
            capacity,
            byte_capacity,
            bytes: 0,
            next_seq: 0,
        }
    }

    /// True if `hash` was recorded, and forgets it.
    pub(crate) fn take(&mut self, hash: u64) -> bool {
        let Some(record) = self.records.remove(&hash) else {
            return false;
        };
        self.bytes -= record.bytes;
        true
    }

    /// Remembers an entry of `bytes` that is no longer cached.
    pub(crate) fn record(&mut self, hash: u64, bytes: usize) {
        if self.capacity == 0 || self.byte_capacity == 0 {
            return;
        }
        match self.records.get_mut(&hash) {
            // Still recorded: the size changes, the position stays.
            Some(record) => {
                self.bytes = self.bytes - record.bytes + bytes;
                record.bytes = bytes;
            }
            None => {
                let seq = self.next_seq;
                self.next_seq += 1;
                self.records.insert(hash, Record { bytes, seq });
                self.bytes += bytes;
                self.order.push_back((hash, seq));
            }
        }
        self.trim();
    }

    /// Changes the byte bound, dropping the oldest records past it.
    pub(crate) fn set_byte_capacity(&mut self, byte_capacity: usize) {
        self.byte_capacity = byte_capacity;
        if byte_capacity == 0 {
            self.records.clear();
            self.order.clear();
            self.bytes = 0;
        }
        self.trim();
    }

    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }

    fn over(&self) -> bool {
        self.records.len() > self.capacity
            || (self.bytes > self.byte_capacity && self.records.len() > 1)
    }

    fn trim(&mut self) {
        while self.over() {
            let Some((oldest, seq)) = self.order.pop_front() else {
                break;
            };
            self.remove_if_live(oldest, seq);
        }
        // Positions of taken records are dropped in bulk, so the queue stays within twice the
        // capacity.
        if self.order.len() > 2 * self.capacity {
            let records = &self.records;
            self.order
                .retain(|(hash, seq)| records.get(hash).is_some_and(|r| r.seq == *seq));
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.records.len()
    }

    fn remove_if_live(&mut self, hash: u64, seq: u64) {
        if self.records.get(&hash).is_some_and(|r| r.seq == seq) {
            self.take(hash);
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
        let mut ghost = Ghost::new(N as usize, usize::MAX);
        for hash in 0..N {
            ghost.record(hash, 1);
        }
        for hash in 0..N {
            assert!(ghost.take(hash));
        }
        for hash in 0..N {
            ghost.record(hash, 2);
        }
        assert_eq!(ghost.len(), N as usize);
        assert_eq!(ghost.bytes(), 2 * N as usize);
        assert!((0..N).all(|hash| ghost.take(hash)));
    }

    /// A key seen again goes to the back of the window: its first position evicts nothing.
    #[test]
    fn test_retaken_record_moves_to_the_back() {
        let mut ghost = Ghost::new(2, usize::MAX);
        ghost.record(1, 1);
        ghost.record(2, 1);
        assert!(ghost.take(1));
        ghost.record(1, 1);
        ghost.record(3, 1);
        // 2 is now the oldest record, not 1.
        assert!(!ghost.take(2));
        assert!(ghost.take(1));
        assert!(ghost.take(3));
    }

    /// The queue does not grow with records that are taken and recorded again.
    #[test]
    fn test_queue_stays_bounded() {
        let mut ghost = Ghost::new(8, usize::MAX);
        for _ in 0..256 {
            for hash in 0..8 {
                ghost.take(hash);
                ghost.record(hash, 1);
            }
            assert!(ghost.order.len() <= 16);
        }
        assert_eq!(ghost.len(), 8);
    }

    /// Old records leave once the recorded bytes pass the byte bound.
    #[test]
    fn test_byte_capacity_drops_oldest() {
        let mut ghost = Ghost::new(100, 250);
        for hash in 0..5 {
            ghost.record(hash, 100);
        }
        assert_eq!((ghost.len(), ghost.bytes()), (2, 200));
        assert!(!ghost.take(2));
        assert!(ghost.take(3));
        assert_eq!(ghost.bytes(), 100);
    }

    /// A record larger than the byte bound is kept until the next one comes.
    #[test]
    fn test_newest_record_is_kept_even_when_large() {
        let mut ghost = Ghost::new(100, 250);
        ghost.record(1, 10);
        ghost.record(2, 1000);
        assert_eq!((ghost.len(), ghost.bytes()), (1, 1000));
        assert!(ghost.take(2));
        ghost.record(2, 1000);
        ghost.record(3, 10);
        assert!(!ghost.take(2));
        assert_eq!(ghost.bytes(), 10);
    }

    /// Re-recording a hash updates its size; lowering the bound trims; zero clears.
    #[test]
    fn test_bytes_follow_updates_and_bound_changes() {
        let mut ghost = Ghost::new(100, usize::MAX);
        ghost.record(1, 10);
        ghost.record(1, 30);
        assert_eq!((ghost.len(), ghost.bytes()), (1, 30));
        for hash in 2..10 {
            ghost.record(hash, 10);
        }
        ghost.set_byte_capacity(50);
        assert_eq!((ghost.len(), ghost.bytes()), (5, 50));
        assert!(!ghost.take(1));
        ghost.set_byte_capacity(0);
        assert_eq!((ghost.len(), ghost.bytes()), (0, 0));
        ghost.record(1, 1);
        assert_eq!(ghost.len(), 0);
    }

    /// The byte count always equals the sum of the live records.
    #[test]
    fn test_bytes_match_records() {
        let mut ghost = Ghost::new(64, 2000);
        let mut x = 7u64;
        for _ in 0..20_000 {
            x = x
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let hash = (x >> 33) % 200;
            if x.is_multiple_of(3) {
                ghost.take(hash);
            } else {
                ghost.record(hash, (x >> 20) as usize % 100 + 1);
            }
            let sum: usize = ghost.records.values().map(|r| r.bytes).sum();
            assert_eq!(ghost.bytes(), sum);
            assert!(ghost.len() <= 64);
            assert!(ghost.bytes() <= 2000 || ghost.len() == 1);
            assert!(ghost.order.len() <= 128);
        }
    }
}
