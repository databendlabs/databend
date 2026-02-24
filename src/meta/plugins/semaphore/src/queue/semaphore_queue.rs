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

use std::collections::BTreeMap;
use std::fmt;

use crate::PermitEntry;
use crate::PermitSeq;
use crate::queue::semaphore_event::PermitEvent;

/// Manage the acquired and waiting semaphore entries.
///
/// It put as many as possible acquired semaphores into the `acquired` map unless the capacity is not enough.
///
/// The `waiting` map contains the pending semaphore entries that are waiting for other semaphore to be released.
///
/// Note that the capacity is not centrally managed and can be different on different client.
pub struct SemaphoreQueue {
    size: u64,
    capacity: u64,
    acquired: BTreeMap<PermitSeq, PermitEntry>,
    waiting: BTreeMap<PermitSeq, PermitEntry>,
}

impl fmt::Display for SemaphoreQueue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SemaphoreQueue{{ {}/{}", self.size, self.capacity,)?;

        write!(f, ", acquired: [")?;
        for (seq, entry) in &self.acquired {
            write!(f, "{}:{} ", seq, entry)?;
        }
        write!(f, "]")?;

        write!(f, ", waiting: [")?;
        for (seq, entry) in &self.waiting {
            write!(f, "{}:{} ", seq, entry)?;
        }
        write!(f, "]")?;

        write!(f, "}}")?;
        Ok(())
    }
}

impl SemaphoreQueue {
    pub fn new(capacity: u64) -> Self {
        SemaphoreQueue {
            size: 0,
            capacity,
            acquired: BTreeMap::new(),
            waiting: BTreeMap::new(),
        }
    }

    /// Insert a new semaphore entry into the queue.
    ///
    /// Returns state changes that include newly acquired entries and released entries.
    ///
    /// In order not to starve any acquire requests(such as, smaller `value` should not starve one with large `value`)
    /// new entry enters acquired queue if:
    /// - there is enough capacity;
    /// - or the first `waiting` has smaller seq than the last in the `acquired`,
    ///   this ensure no permit waits each other when a smaller seq entry is received after a larger seq entry.
    ///
    /// Otherwise, it will be added to the waiting queue.
    pub fn insert(&mut self, sem_seq: PermitSeq, entry: PermitEntry) -> Vec<PermitEvent> {
        self.waiting.insert(sem_seq, entry);

        self.move_waiting_to_acquired()
            .into_iter()
            .map(|(seq, entry)| acquired(seq, entry))
            .collect()
    }

    /// Remove a semaphore entry from the queue.
    ///
    /// Returns state changes that include released entries and newly acquired entries.
    ///
    /// 1. removed semaphores, containing at most one element, which is previously in the `acquired` or not in the `acquired`.
    /// 2. newly acquired semaphores, containing one or more elements, which are previously in `waiting`.
    pub fn remove(&mut self, sem_seq: PermitSeq) -> Vec<PermitEvent> {
        let removed = if self.is_acquired(&sem_seq) {
            // Safe unwrap(): it is acquired.
            let entry = self.acquired.remove(&sem_seq).unwrap();
            self.size -= entry.permits;
            Some(entry)
        } else {
            self.waiting.remove(&sem_seq)
        };

        let Some(removed) = removed else {
            return vec![];
        };

        let removed = vec![PermitEvent::new_removed(sem_seq, removed)];
        let acquired = self
            .move_waiting_to_acquired()
            .into_iter()
            .map(|(seq, entry)| acquired(seq, entry));
        removed.into_iter().chain(acquired).collect()
    }

    /// Add a semaphore entry to the acquired map if it is not already acquired.
    fn try_acquire(&mut self, sem_seq: PermitSeq, entry: PermitEntry) -> bool {
        // TODO: test if the semaphore is already acquired
        if self.is_acquired(&sem_seq) {
            return false;
        }

        self.size += entry.permits;
        self.acquired.insert(sem_seq, entry);
        true
    }

    /// Move the waiting semaphores to acquired.
    ///
    /// Move one entry if:
    /// - there is enough capacity,
    /// - or the first `waiting` has smaller seq than the last in the `acquired`.
    ///
    /// The second scenario happens when a smaller seq entry is received after a larger seq entry.
    /// In this case, the smaller seq entry must be inserted too, even it exceeds the capacity.
    ///
    /// Otherwise, two semaphore entries would BLOCK EACH OTHER.
    ///
    /// For example, `A` has seq 1, `B` has seq 2. the capacity is 1.
    /// - Process `A` received sem `B` first, then sem `A`;
    /// - Process `B` received sem `A` first, then sem `B`.
    ///
    /// ```text
    /// |             acquired    waiting
    /// | Process A:  [ B ]       [ A ]
    /// | Process B:  [ A ]       [ B ]
    /// ```
    ///
    /// And then two processes blocks each other:
    /// - Process `A` await sem `B` to be removed from `acquired` queue, which await Process `B` to remove sem `B`.
    /// - Process `B` await sem `A` to be removed from `acquired` queue, which await Process `A` to remove sem `A`.
    fn move_waiting_to_acquired(&mut self) -> Vec<(PermitSeq, PermitEntry)> {
        let mut moved = vec![];

        loop {
            let first = self.waiting.first_key_value();

            let Some((seq, entry)) = first else {
                break;
            };

            // Consistency guarantee: see the method doc

            // The less-equal `<=` should be used here instead of `<` to ensure that:
            //
            // Databend-meta has a bug that emit duplicated None->Some events.
            // Thus, a permit might be added to this queue multiple times.
            // This is a workaround that skips the entry if it is already acquired.
            if self.is_capacity_enough(entry) || Some(*seq) <= self.last_acquired_seq() {
                // go on moving waiting to acquired.
            } else {
                break;
            }

            let (seq, entry) = self.waiting.pop_first().unwrap();
            if self.try_acquire(seq, entry.clone()) {
                moved.push((seq, entry));
            }
        }
        moved
    }

    fn last_acquired_seq(&self) -> Option<PermitSeq> {
        let last_acquired = self.acquired.last_key_value();
        last_acquired.map(|(seq, _)| *seq)
    }

    fn is_acquired(&self, sem_seq: &PermitSeq) -> bool {
        self.acquired.contains_key(sem_seq)
    }

    fn is_capacity_enough(&self, entry: &PermitEntry) -> bool {
        self.size + entry.permits <= self.capacity
    }
}

/// Create an acquired event.
fn acquired(seq: PermitSeq, entry: PermitEntry) -> PermitEvent {
    PermitEvent::new_acquired(seq, entry)
}

/// Create a removed event.
#[allow(dead_code)]
fn removed(seq: PermitSeq, entry: PermitEntry) -> PermitEvent {
    PermitEvent::new_removed(seq, entry)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::PermitEntry;
    use crate::queue::semaphore_queue::acquired;
    use crate::queue::semaphore_queue::removed;
    use crate::queue::*;

    #[test]
    fn test_display() {
        let queue = SemaphoreQueue {
            size: 10,
            capacity: 20,
            acquired: BTreeMap::from([(1, ent("t1", 3)), (2, ent("t2", 4))]),
            waiting: BTreeMap::from([(3, ent("t3", 5)), (4, ent("t4", 6))]),
        };

        assert_eq!(
            format!("{}", queue),
            "SemaphoreQueue{ 10/20, acquired: [1:PermitEntry(id:t1, n:3) 2:PermitEntry(id:t2, n:4) ], waiting: [3:PermitEntry(id:t3, n:5) 4:PermitEntry(id:t4, n:6) ]}"
        );
    }

    #[test]
    fn test_insert() {
        // Test case 1: Insert when there is enough capacity
        {
            let mut queue = SemaphoreQueue {
                size: 0,
                capacity: 10,
                acquired: BTreeMap::new(),
                waiting: BTreeMap::new(),
            };

            let entry1 = || ent("t1", 3);
            let events = queue.insert(1, entry1());

            // Verify entry was immediately acquired
            assert_eq!(events, vec![acquired(1, entry1())]);
            assert_eq!(queue.size, 3);
            assert!(queue.is_acquired(&1));
            assert!(queue.waiting.is_empty());

            // Insert another entry within capacity
            let entry2 = || ent("t2", 4);
            let events = queue.insert(2, entry2());

            assert_eq!(events, vec![acquired(2, entry2())]);
            assert_eq!(queue.size, 7);
            assert!(queue.is_acquired(&2));
            assert!(queue.waiting.is_empty());
        }

        // Test case 2: Insert when there is not enough capacity
        {
            let mut queue = SemaphoreQueue {
                size: 8,
                capacity: 10,
                acquired: BTreeMap::from([(1, ent("t1", 3)), (2, ent("t2", 5))]),
                waiting: BTreeMap::new(),
            };

            // This entry should go to waiting since it exceeds capacity
            let entry3 = || ent("t3", 4);
            let events = queue.insert(3, entry3());

            // Verify no events were returned and entry went to waiting
            assert!(events.is_empty());
            assert_eq!(queue.size, 8); // Size unchanged
            assert!(!queue.is_acquired(&3));
            assert_eq!(queue.waiting, BTreeMap::from([(3, entry3())]));

            // Add another waiting entry
            let entry4 = || ent("t4", 4);
            let events = queue.insert(4, entry4());

            assert!(events.is_empty());
            assert_eq!(queue.size, 8); // Size still unchanged
            assert!(!queue.is_acquired(&4));
            assert_eq!(
                queue.waiting,
                BTreeMap::from([(3, entry3()), (4, entry4())])
            );

            // Insert smaller value entry still can not be acquired, because there are entries in waiting queue.
            let entry5 = || ent("t5", 2);
            let events = queue.insert(5, entry5());

            assert!(events.is_empty());
            assert_eq!(queue.size, 8);
            assert!(!queue.is_acquired(&5));
            assert_eq!(
                queue.waiting,
                BTreeMap::from([(3, entry3()), (4, entry4()), (5, entry5())])
            );
        }

        // Test case 3: entry in waiting blocks new entries
        {
            let mut queue = SemaphoreQueue {
                size: 8,
                capacity: 10,
                acquired: BTreeMap::from([(1, ent("t1", 3)), (2, ent("t2", 5))]),
                waiting: BTreeMap::from([(3, ent("t3", 4))]),
            };

            // The capacity is enough, but the entry in waiting blocks new entries
            let events = queue.insert(4, ent("t4", 1));

            // Verify no events were returned and entry went to waiting
            assert!(events.is_empty());
            assert_eq!(queue.size, 8); // Size unchanged
            assert_eq!(
                queue.acquired,
                BTreeMap::from([(1, ent("t1", 3)), (2, ent("t2", 5))])
            );
            assert_eq!(
                queue.waiting,
                BTreeMap::from([(3, ent("t3", 4)), (4, ent("t4", 1))])
            );
        }
    }

    /// When a smaller seq permit is inserted, it should be acquired immediately.
    ///
    /// See [`SemaphoreQueue::move_waiting_to_acquired`] for more details.
    #[test]
    fn test_insert_smaller_seq() {
        let mut queue = SemaphoreQueue {
            size: 0,
            capacity: 1,
            acquired: BTreeMap::new(),
            waiting: BTreeMap::new(),
        };

        let entry1 = || ent("t1", 1);
        let entry2 = || ent("t2", 1);
        let entry3 = || ent("t3", 1);

        // 2 is acquired, 3 is waiting.

        let events = queue.insert(2, entry2());
        assert_eq!(events, vec![acquired(2, entry2())]);
        let events = queue.insert(3, entry3());
        assert_eq!(events, vec![]);

        assert!(queue.is_acquired(&2));
        assert!(!queue.is_acquired(&3));
        assert_eq!(queue.waiting, BTreeMap::from([(3, ent("t3", 1))]));

        // 1 is acquired because it smaller than 2.

        let events = queue.insert(1, entry1());

        assert_eq!(events, vec![acquired(1, entry1())]);
        assert_eq!(queue.size, 2);
        assert!(queue.is_acquired(&1));
        assert!(queue.is_acquired(&2));
        assert_eq!(queue.waiting, BTreeMap::from([(3, ent("t3", 1))]));
    }

    /// When a equal seq permit is inserted, it should be skipped automatically.
    ///
    /// See [`SemaphoreQueue::move_waiting_to_acquired`] for more details.
    #[test]
    fn test_insert_equal_seq() {
        // case-1: equal the last in `acquired`
        {
            let mut queue = SemaphoreQueue {
                size: 0,
                capacity: 1,
                acquired: BTreeMap::new(),
                waiting: BTreeMap::new(),
            };

            let entry2 = || ent("t2", 1);
            let entry3 = || ent("t3", 1);

            // 2 is acquired, 3 is waiting.

            let events = queue.insert(2, entry2());
            assert_eq!(events, vec![acquired(2, entry2())]);

            let events = queue.insert(3, entry3());
            assert_eq!(events, vec![]);

            assert!(queue.is_acquired(&2));
            assert!(!queue.is_acquired(&3));
            assert_eq!(queue.waiting, BTreeMap::from([(3, ent("t3", 1))]));

            // 1 is acquired because it smaller than 2.

            let events = queue.insert(2, entry2());

            assert_eq!(events, vec![]);
            assert_eq!(queue.size, 1);
            assert!(queue.is_acquired(&2));
            assert!(!queue.is_acquired(&3));
            assert_eq!(queue.waiting, BTreeMap::from([(3, ent("t3", 1))]));
        }

        // case-2: equal the second last in `acquired`
        {
            let mut queue = SemaphoreQueue {
                size: 0,
                capacity: 2,
                acquired: BTreeMap::new(),
                waiting: BTreeMap::new(),
            };

            let entry1 = || ent("t1", 1);
            let entry2 = || ent("t2", 1);
            let entry3 = || ent("t3", 1);

            let events = queue.insert(2, entry2());
            assert_eq!(events, vec![acquired(2, entry2())]);

            let events = queue.insert(1, entry1());
            assert_eq!(events, vec![acquired(1, entry1())]);

            let events = queue.insert(3, entry3());
            assert_eq!(events, vec![]);

            assert!(queue.is_acquired(&1));
            assert!(queue.is_acquired(&2));
            assert!(!queue.is_acquired(&3));
            assert_eq!(queue.waiting, BTreeMap::from([(3, ent("t3", 1))]));

            let events = queue.insert(1, entry1());

            assert_eq!(events, vec![]);
            assert_eq!(queue.size, 2);
            assert!(queue.is_acquired(&1));
            assert!(queue.is_acquired(&2));
            assert!(!queue.is_acquired(&3));
            assert_eq!(queue.waiting, BTreeMap::from([(3, ent("t3", 1))]));
        }
    }

    /// Before this commit, with capacity=1:
    /// `insert 1, insert 1, remove 1` emit the events `acquired 1, removed 1, acquired 1`,
    /// which is a bug of duplicated acquired.
    ///
    /// The second `insert 1` should be skipped, and the expected events should be `acquired 1, removed 1`.
    #[test]
    fn test_no_waiting_dup_become_acquired() {
        let mut queue = SemaphoreQueue {
            size: 0,
            capacity: 1,
            acquired: BTreeMap::new(),
            waiting: BTreeMap::new(),
        };

        let entry1 = || ent("t1", 1);

        let events = queue.insert(1, entry1());
        assert_eq!(events, vec![acquired(1, entry1())]);

        let events = queue.insert(1, entry1());
        assert_eq!(events, vec![]);

        let events = queue.remove(1);
        assert_eq!(events, vec![removed(1, entry1())]);
    }

    #[test]
    fn test_remove() {
        // Remove an acquired entry, all waiting entries are moved to acquired
        {
            let mut queue = SemaphoreQueue {
                size: 7,
                capacity: 10,
                acquired: BTreeMap::from([(1, ent("t1", 3)), (2, ent("t2", 4))]),
                waiting: BTreeMap::from([(3, ent("t3", 3)), (4, ent("t4", 2))]),
            };

            let entry1 = || ent("t1", 3);
            let events = queue.remove(1);

            assert_eq!(events, vec![
                removed(1, entry1()),
                acquired(3, ent("t3", 3)),
                acquired(4, ent("t4", 2)),
            ]);

            assert_eq!(queue.size, 9);
            assert_eq!(
                queue.acquired,
                BTreeMap::from([(4, ent("t4", 2)), (3, ent("t3", 3)), (2, ent("t2", 4)),])
            );
            assert!(queue.waiting.is_empty());
        }

        // Remove an acquired entry, 2 waiting entries are moved to acquired
        {
            let mut queue = SemaphoreQueue {
                size: 7,
                capacity: 10,
                acquired: BTreeMap::from([(1, ent("t1", 3)), (2, ent("t2", 4))]),
                waiting: BTreeMap::from([(3, ent("t3", 3)), (4, ent("t4", 2)), (8, ent("t8", 8))]),
            };

            let events = queue.remove(2);

            assert_eq!(events, vec![
                removed(2, ent("t2", 4)),
                acquired(3, ent("t3", 3)),
                acquired(4, ent("t4", 2)),
            ]);

            assert_eq!(queue.size, 8);
            assert_eq!(
                queue.acquired,
                BTreeMap::from([(1, ent("t1", 3)), (3, ent("t3", 3)), (4, ent("t4", 2)),])
            );
            assert_eq!(queue.waiting, BTreeMap::from([(8, ent("t8", 8))]));
        }

        // Remove an acquired entry, no waiting entries are moved to acquired
        {
            let mut queue = SemaphoreQueue {
                size: 7,
                capacity: 10,
                acquired: BTreeMap::from([(1, ent("t1", 3)), (2, ent("t2", 4))]),
                waiting: BTreeMap::from([(8, ent("t8", 8))]),
            };

            let events = queue.remove(2);

            assert_eq!(events, vec![removed(2, ent("t2", 4)),]);

            assert_eq!(queue.size, 3);
            assert_eq!(queue.acquired, BTreeMap::from([(1, ent("t1", 3)),]));
            assert_eq!(queue.waiting, BTreeMap::from([(8, ent("t8", 8))]));
        }

        // Remove a waiting entry, one waiting entry is moved to acquired
        {
            let mut queue = SemaphoreQueue {
                size: 8,
                capacity: 10,
                acquired: BTreeMap::from([(1, ent("t1", 3)), (2, ent("t2", 5))]),
                waiting: BTreeMap::from([(3, ent("t3", 4)), (4, ent("t4", 2)), (8, ent("t8", 8))]),
            };

            let events = queue.remove(3);

            assert_eq!(events, vec![
                removed(3, ent("t3", 4)),
                acquired(4, ent("t4", 2)),
            ]);

            assert_eq!(queue.size, 10);
            assert_eq!(
                queue.acquired,
                BTreeMap::from([(1, ent("t1", 3)), (2, ent("t2", 5)), (4, ent("t4", 2)),])
            );
            assert_eq!(queue.waiting, BTreeMap::from([(8, ent("t8", 8))]));
        }

        // Remove a waiting entry, no waiting entry is moved to acquired
        {
            let mut queue = SemaphoreQueue {
                size: 8,
                capacity: 10,
                acquired: BTreeMap::from([(1, ent("t1", 3)), (2, ent("t2", 5))]),
                waiting: BTreeMap::from([(3, ent("t3", 4)), (4, ent("t4", 3)), (8, ent("t8", 8))]),
            };

            let events = queue.remove(3);

            assert_eq!(events, vec![removed(3, ent("t3", 4)),]);

            assert_eq!(queue.size, 8);
            assert_eq!(
                queue.acquired,
                BTreeMap::from([(1, ent("t1", 3)), (2, ent("t2", 5)),])
            );
            assert_eq!(
                queue.waiting,
                BTreeMap::from([(4, ent("t4", 3)), (8, ent("t8", 8))])
            );
        }

        // Remove non-existent entry
        {
            let mut queue = SemaphoreQueue {
                size: 8,
                capacity: 10,
                acquired: BTreeMap::from([(1, ent("t1", 3)), (2, ent("t2", 5))]),
                waiting: BTreeMap::from([(3, ent("t3", 4)), (4, ent("t4", 3)), (8, ent("t8", 8))]),
            };

            let events = queue.remove(6);

            assert_eq!(events, vec![]);

            assert_eq!(queue.size, 8);
            assert_eq!(
                queue.acquired,
                BTreeMap::from([(1, ent("t1", 3)), (2, ent("t2", 5)),])
            );
            assert_eq!(
                queue.waiting,
                BTreeMap::from([(3, ent("t3", 4)), (4, ent("t4", 3)), (8, ent("t8", 8))])
            );
        }
    }

    #[test]
    fn test_try_acquire() {
        let mut queue = SemaphoreQueue {
            size: 0,
            capacity: 10,
            acquired: BTreeMap::new(),
            waiting: BTreeMap::new(),
        };

        // Test successful acquisition
        let entry1 = || ent("t1", 3);
        let result1 = queue.try_acquire(1, entry1());
        assert!(result1);
        assert_eq!(queue.size, 3);
        assert_eq!(queue.acquired[&1], entry1());

        // Test another successful acquisition
        let entry2 = || ent("t2", 4);
        let result2 = queue.try_acquire(2, entry2());
        assert!(result2);
        assert_eq!(queue.size, 7);
        assert_eq!(queue.acquired[&2], entry2());

        // Test trying to acquire an already acquired semaphore
        let entry3 = || ent("t1_duplicate", 2);
        let result3 = queue.try_acquire(1, entry3());
        assert!(!result3);
        assert_eq!(queue.size, 7);
        assert_eq!(queue.acquired[&1], entry1());
    }

    #[test]
    fn test_move_waiting_to_acquired() {
        let mut queue = SemaphoreQueue {
            size: 0,
            capacity: 5,
            acquired: BTreeMap::new(),
            waiting: BTreeMap::new(),
        };

        queue.waiting.insert(1, ent("t1", 3));
        queue.waiting.insert(2, ent("t2", 3));
        queue.waiting.insert(3, ent("t3", 2));
        queue.waiting.insert(4, ent("t4", 1));

        // Fill 1:3 to acquired
        {
            let moved = queue.move_waiting_to_acquired();
            assert_eq!(moved, vec![(1, ent("t1", 3)),]);
            assert_eq!(queue.acquired, BTreeMap::from([(1, ent("t1", 3)),]));
            assert_eq!(
                queue.waiting,
                BTreeMap::from([(2, ent("t2", 3)), (3, ent("t3", 2)), (4, ent("t4", 1)),])
            );
            assert_eq!(queue.size, 3);
        }

        // pop one, Fill 2:4 to acquired
        {
            queue.acquired.remove(&1);
            queue.size -= 3;

            let moved = queue.move_waiting_to_acquired();
            assert_eq!(moved, vec![(2, ent("t2", 3)), (3, ent("t3", 2)),]);

            assert_eq!(
                queue.acquired,
                BTreeMap::from([(2, ent("t2", 3)), (3, ent("t3", 2)),])
            );
            assert_eq!(queue.waiting, BTreeMap::from([(4, ent("t4", 1)),]));
            assert_eq!(queue.size, 5);
        }
    }

    #[test]
    fn test_is_acquired() {
        let mut queue = SemaphoreQueue {
            size: 0,
            capacity: 5,
            acquired: BTreeMap::new(),
            waiting: BTreeMap::new(),
        };

        assert!(!queue.is_acquired(&1));

        queue.acquired.insert(1, ent("t1", 3));
        assert!(queue.is_acquired(&1));
        assert!(!queue.is_acquired(&2));
    }

    #[test]
    fn test_is_capacity_enough() {
        let mut queue = SemaphoreQueue {
            size: 0,
            capacity: 5,
            acquired: BTreeMap::new(),
            waiting: BTreeMap::new(),
        };

        assert!(queue.is_capacity_enough(&ent("t1", 3)));
        assert!(queue.is_capacity_enough(&ent("t2", 5)));
        assert!(!queue.is_capacity_enough(&ent("t3", 6)));

        queue.size = 3;
        assert!(queue.is_capacity_enough(&ent("t1", 2)));
        assert!(!queue.is_capacity_enough(&ent("t2", 3)));
    }

    /// Create an entry with a given id and value.
    fn ent(id: impl ToString, value: u64) -> PermitEntry {
        PermitEntry::new(id.to_string(), value)
    }
}
