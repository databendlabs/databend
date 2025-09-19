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

use std::ops::Range;

use super::hash_index::TableAdapter;
use super::Entry;
use super::BATCH_SIZE;
use crate::ProbeState;

#[derive(Debug)]
pub struct Elastic {
    delta: f64,
    c: f64,

    entries: Vec<Entry>,

    batch_limit: usize,
    count: usize,
    i: usize,
    // load_limit: usize,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Slot(usize);

impl Elastic {
    pub fn with_capacity(n: usize, delta: f64) -> Self {
        assert!(n.is_power_of_two());
        Self {
            entries: vec![Entry::default(); n],
            c: 1000.0,
            count: 0,
            delta,
            i: 0,
            batch_limit: (n >> 3) * 3,
            // load_limit: n - (n as f64 * delta) as usize,
        }
    }

    fn max_batch(&self) -> usize {
        self.entries.len().trailing_zeros() as usize + 1
    }

    fn zone_range(&self, i: usize) -> std::ops::Range<usize> {
        debug_assert!(i >= 1);
        debug_assert!(i <= self.max_batch());

        let n = self.entries.len();
        let size = n >> (i - 1);
        let start = n - size;
        let end = n - (size >> 1);
        start..end
    }

    fn zone(&self, i: usize) -> &[Entry] {
        &self.entries[self.zone_range(i)]
    }

    fn cur_batch(&self) -> Option<usize> {
        if self.i > self.max_batch() {
            return None;
        }

        debug_assert!(self.count < self.batch_limit);
        Some(self.i)
    }

    fn batch_size(&self, i: usize) -> usize {
        let n = self.entries.len();
        if i == 0 {
            return (n >> 3) * 3;
        }
        1.max(((1.0 - self.delta) / 2.0 * (n >> i) as f64) as usize + (n >> (i + 3)))
    }

    fn epsilon(&self, i: usize) -> f64 {
        let r = self.zone_range(i);
        let size = r.len();
        let empty = self.entries[r].iter().filter(|x| !x.is_occupied()).count();
        empty as f64 / size as f64
    }

    fn max_probe(&self, i: usize) -> usize {
        10
        // let t1 = (1.0 / self.epsilon(i)).ln().powi(2);
        // let t2 = (1.0 / self.delta).ln();
        // self.c * t1.min(t2) as _
    }

    fn probe_zone(
        zone: &[Entry],
        mut j: usize,
        salt: Option<u16>,
        end: Option<usize>,
    ) -> Option<usize> {
        for _ in 0..zone.len() {
            let entry = &zone[j];
            if !entry.is_occupied()
                || salt
                    .map(|salt| salt == entry.get_salt())
                    .unwrap_or_default()
            {
                return Some(j);
            }

            j += 1;
            if j >= zone.len() {
                j = 0;
            }

            if Some(j) == end {
                return None;
            }
        }
        unreachable!("")
    }

    fn max_to_end(j: usize, max: usize, capacity: usize) -> usize {
        (j + max) & (capacity - 1)
    }

    fn is_case3(&self, i: usize) -> bool {
        let zone = self.zone(i + 1);
        let size = zone.len();
        let empty = zone.iter().filter(|x| !x.is_occupied()).count();
        empty * 4 <= size
    }

    fn is_case1(&self, i: usize) -> bool {
        let zone = self.zone(i);
        let size = zone.len();
        let empty = zone.iter().filter(|x| !x.is_occupied()).count();
        (2 * empty) as f64 > self.delta * size as f64
    }

    fn init_j(&self, hash: u64, capacity: usize) -> usize {
        debug_assert!(capacity.is_power_of_two());
        hash as usize & (capacity - 1)
    }

    fn probe_core(&mut self, hash: u64, salt: Option<u16>) -> (usize, usize, Range<usize>) {
        let i = self.cur_batch().unwrap();
        if i == 0 {
            let range = self.zone_range(1);
            let j = self.init_j(hash, range.len());
            let j = Self::probe_zone(&self.entries[range.clone()], j, salt, None).unwrap();
            return (1, j, range);
        }

        if self.is_case3(i) {
            let range = self.zone_range(i);
            let j = self.init_j(hash, range.len());
            let j = Self::probe_zone(&self.entries[range.clone()], j, salt, None).unwrap();
            return (i, j, range);
        }

        if self.is_case1(i) {
            let range = self.zone_range(i);
            let j = self.init_j(hash, range.len());
            let end = Self::max_to_end(j, self.max_probe(i), range.len());
            if let Some(j) = Self::probe_zone(&self.entries[range.clone()], j, salt, Some(end)) {
                return (i, j, range);
            }
        }

        // case 2
        let range = self.zone_range(i + 1);
        let j = self.init_j(hash, range.len());
        let j = Self::probe_zone(&self.entries[range.clone()], j, salt, None).unwrap();
        (i + 1, j, range)
    }

    fn count_inc(&mut self) {
        self.count += 1;
        if self.count >= self.batch_limit {
            self.i += 1;
            self.batch_limit += self.batch_size(self.i);
        }
    }

    pub fn find(&self, hash: u64, start: Option<Slot>) -> Option<Slot> {
        let (start_i, mut j) = match start {
            Some(solt) => {
                let (i, j) = self.coord(solt);
                (i, Some(j))
            }
            None => {
                let i = 1;
                (i, None)
            }
        };

        for i in start_i..=(self.i + 1) {
            let range = self.zone_range(i);
            let zone = &self.entries[range.clone()];
            let found = match j.take() {
                Some(j) => {
                    let size = zone.len();
                    let j0 = self.init_j(hash, size);
                    let j = if j + 1 >= size { 0 } else { j + 1 };
                    Self::probe_zone(zone, j, Some(Entry::hash_to_salt(hash)), Some(j0))
                }
                None => {
                    let j = self.init_j(hash, zone.len());
                    Self::probe_zone(zone, j, Some(Entry::hash_to_salt(hash)), Some(j))
                }
            };
            let Some(j) = found else {
                continue;
            };
            if !zone[j].is_occupied() {
                continue;
            }
            return Some(Slot(range.start + j));
        }
        None
    }

    fn find_or_insert(&mut self, hash: u64, start: Option<Slot>) -> (Slot, bool) {
        let (start_i, mut j) = match start {
            Some(solt) => {
                let (i, j) = self.coord(solt);
                (i, Some(j))
            }
            None => {
                let i = 1;
                (i, None)
            }
        };

        for i in start_i..=(self.i + 1) {
            let range = self.zone_range(i);
            let zone = &self.entries[range.clone()];
            let found = match j.take() {
                Some(j) => {
                    let size = zone.len();
                    let j0 = self.init_j(hash, size);
                    let j = if j + 1 >= size { 0 } else { j + 1 };
                    Self::probe_zone(zone, j, Some(Entry::hash_to_salt(hash)), Some(j0))
                }
                None => {
                    let j = self.init_j(hash, zone.len());
                    Self::probe_zone(zone, j, Some(Entry::hash_to_salt(hash)), Some(j))
                }
            };
            let Some(j) = found else {
                continue;
            };
            let slot = range.start + j;
            let entry = &mut self.entries[slot];
            let is_occupied = entry.is_occupied();
            if is_occupied {
                debug_assert_eq!(entry.get_salt(), Entry::hash_to_salt(hash))
            } else {
                entry.set_hash(hash);
                self.count_inc();
            }
            return (Slot(slot), !is_occupied);
        }
        unreachable!()
    }

    pub fn probe_slot(&mut self, hash: u64) -> Slot {
        let (_, j, range) = self.probe_core(hash, None);
        debug_assert!(!self.entries[range.start + j].is_occupied(), "hash {hash}");
        Slot(range.start + j)
    }

    pub fn probe(&mut self, hash: u64, salt: u16) -> Slot {
        let (_, j, range) = self.probe_core(hash, Some(salt));
        Slot(range.start + j)
    }

    fn mut_entry(&mut self, slot: Slot) -> &mut Entry {
        &mut self.entries[slot.0]
    }

    pub fn insert_slot(&mut self, slot: Slot, value: Entry) {
        let entry = &mut self.entries[slot.0];
        assert!(!entry.is_occupied());
        *entry = value;
        self.count_inc();
    }

    pub fn coord(&self, slot: Slot) -> (usize, usize) {
        let mut slot = slot.0;
        let mut size = self.entries.len();
        for i in 1..=self.max_batch() {
            size >>= 1;
            if slot < size {
                return (i, slot);
            }
            slot -= size;
        }
        unreachable!()
    }

    pub fn init_slot(&self, hash: u64) -> Slot {
        let i = self.cur_batch().unwrap();
        let range = self.zone_range(i);
        let j = self.init_j(hash, range.len());
        Slot(range.start + j)
    }
}

impl Elastic {
    pub fn probe_and_create(
        &mut self,
        state: &mut ProbeState,
        row_count: usize,
        mut adapter: impl TableAdapter,
    ) -> usize {
        #[derive(Default, Clone, Copy, Debug)]
        struct Item {
            slot: Option<Slot>,
            hash: u64,
        }

        let mut items = [Item::default(); BATCH_SIZE];

        for row in 0..row_count {
            items[row] = Item {
                slot: None,
                hash: state.group_hashes[row],
            };
            state.no_match_vector[row] = row;
        }

        let mut new_group_count = 0;
        let mut remaining_entries = row_count;

        while remaining_entries > 0 {
            let mut new_entry_count = 0;
            let mut need_compare_count = 0;
            let mut no_match_count = 0;

            // 1. inject new_group_count, new_entry_count, need_compare_count, no_match_count
            for row in state.no_match_vector[..remaining_entries].iter().copied() {
                let item = &mut items[row];

                let (slot, is_new) = self.find_or_insert(item.hash, item.slot);
                item.slot = Some(slot);

                if is_new {
                    state.empty_vector[new_entry_count] = row;
                    new_entry_count += 1;
                } else {
                    state.group_compare_vector[need_compare_count] = row;
                    need_compare_count += 1;
                }
            }

            // 2. append new_group_count to payload
            if new_entry_count != 0 {
                new_group_count += new_entry_count;

                adapter.append_rows(state, new_entry_count);

                for row in state.empty_vector[..new_entry_count].iter().copied() {
                    let entry = self.mut_entry(items[row].slot.unwrap());
                    entry.set_pointer(state.addresses[row]);
                    debug_assert_eq!(entry.get_pointer(), state.addresses[row]);
                }
            }

            // 3. set address of compare vector
            if need_compare_count > 0 {
                for row in state.group_compare_vector[..need_compare_count]
                    .iter()
                    .copied()
                {
                    let entry = self.mut_entry(items[row].slot.unwrap());

                    debug_assert!(entry.is_occupied());
                    debug_assert_eq!(entry.get_salt(), (items[row].hash >> 48) as u16);
                    state.addresses[row] = entry.get_pointer();
                }

                // 4. compare
                no_match_count = adapter.compare(state, need_compare_count, no_match_count);
            }

            remaining_entries = no_match_count;
        }

        new_group_count
    }
}

mod tests {
    use std::collections::BinaryHeap;

    use super::*;
    use crate::aggregate::row_ptr::RowPtr;

    #[test]
    fn test_main() {
        let mut elastic = Elastic::with_capacity(128, 0.2);

        for value in 200..305 {
            println!("start {value}");

            let hash = value << 48 | value;
            let slot = elastic.probe(hash, Entry::hash_to_salt(hash));

            let mut entry = Entry::default();
            entry.set_hash(hash);
            let (i, j) = elastic.coord(slot);
            elastic.insert_slot(slot, entry);

            if i != 0 {
                let f = elastic.count as f64 / elastic.entries.len() as f64;
                println!("  probe {i} {j} {f:.3}");
            }
        }

        println!("{elastic:?}")
    }

    #[test]
    fn test_cur_batch() {
        // when i>=1,  B(i).len = A(i).len - A(i).len * delta/2 - 0.75 * A(i).len + 0.75 * A( i+1 ).len
        {
            let mut index = Elastic::with_capacity(128, 0.25);
            let mut ls = vec![];
            while index.cur_batch().is_some() {
                ls.push(index.batch_limit);
                index.count = index.batch_limit - 1;
                index.count_inc();
            }
            assert_eq!(&ls, &[48, 80, 96, 104, 108, 109, 110, 111, 112]);
        }

        {
            let mut index = Elastic::with_capacity(2048, 0.1);
            let mut ls = vec![];
            while index.cur_batch().is_some() {
                ls.push(index.batch_limit);
                index.count = index.batch_limit - 1;
                index.count_inc();
            }
            assert_eq!(&ls, &[
                768, 1356, 1650, 1797, 1870, 1906, 1924, 1933, 1937, 1938, 1939, 1940, 1941
            ]);
        }
    }

    const SALT_SIZE: u64 = 3;

    #[test]
    fn test_find() {
        let mut elastic = Elastic::with_capacity(128, 0.3);

        let heap = BinaryHeap::from_iter(1_u64..90);
        for value in heap.as_slice().iter().copied() {
            let mut entry = Entry::default();
            let hash = (value % SALT_SIZE) << 48 | value;
            entry.set_hash(hash);
            entry.set_pointer(RowPtr::new(value as _));

            let slot = elastic.probe_slot(hash);
            elastic.insert_slot(slot, entry);
        }

        for i in 1..=elastic.max_batch() {
            println!("{i} {} {:?}\n", elastic.epsilon(i), elastic.zone(i))
        }

        run_find(&elastic, 50, &[((1, 50), 50)]);

        // run_find(&elastic, 15, &[((2, 15), 15)]);
        run_find(&elastic, 15, &[
            ((1, 16), 12),
            ((1, 17), 81),
            ((1, 20), 84),
            ((1, 23), 87),
            ((1, 27), 27),
            ((1, 28), 24),
            ((1, 33), 6),
            ((2, 15), 15),
        ]);

        // run_find(&elastic, 42, &[((3, 10), 42)]);
        run_find(&elastic, 42, &[
            ((2, 13), 9),
            ((2, 15), 15),
            ((2, 18), 18),
            ((3, 10), 42),
        ]);
    }

    fn run_find(elastic: &Elastic, value: u64, want: &[((usize, usize), u64)]) {
        let hash = (value % SALT_SIZE) << 48 | value;
        let mut got = Vec::new();
        let mut start = None;
        for _ in 0..20 {
            let slot = elastic.find(hash, start).unwrap();
            let entry = elastic.entries[slot.0];
            assert_eq!(entry.get_salt(), (value % SALT_SIZE) as u16);
            let v = entry.get_pointer().as_ptr() as u64;
            got.push((elastic.coord(slot), v));

            if v == value {
                break;
            }
            start = Some(slot);
        }
        assert_eq!(want, got);
    }

    #[test]
    fn test_coord() {
        let elastic = Elastic::with_capacity(128, 0.2);

        assert_eq!(
            (1, 10),
            elastic.coord(Slot(elastic.zone_range(1).start + 10))
        );
        assert_eq!((2, 0), elastic.coord(Slot(elastic.zone_range(2).start + 0)));
        assert_eq!((3, 4), elastic.coord(Slot(elastic.zone_range(3).start + 4)));
    }
}
